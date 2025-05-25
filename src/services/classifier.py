import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime
import asyncio

from services.ai_client import AnthropicClient, PromptBuilder
from storage.target_mongo import TargetMongoStore
from models.domain import ProductStatus

logger = logging.getLogger(__name__)


class StageOneClassifier:
    """Классификатор первого этапа"""

    def __init__(
            self,
            ai_client: AnthropicClient,
            target_store: TargetMongoStore,
            batch_size: int = 50,
            worker_id: str = None
    ):
        self.ai_client = ai_client
        self.target_store = target_store
        self.batch_size = batch_size
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.prompt_builder = PromptBuilder()

    async def process_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Обработать батч товаров

        Returns:
            Результаты обработки
        """
        if not products:
            logger.warning("Empty products list provided")
            return {
                "batch_id": "",
                "total": 0,
                "classified": 0,
                "none_classified": 0,
                "results": {}
            }

        batch_id = f"batch_{uuid.uuid4().hex[:8]}"
        logger.info(f"Processing batch {batch_id} with {len(products)} products")

        # Товары уже помечены как processing при получении через get_pending_products_atomic
        product_ids = [str(p["_id"]) for p in products]

        try:
            # Готовим данные для промпта
            product_map = {}
            product_names = []

            for product in products:
                product_id = str(product["_id"])
                product_name = product["title"]
                product_map[product_name] = product_id
                product_names.append(product_name)

            # Формируем промпт
            prompt = self.prompt_builder.build_stage_one_prompt(product_names)

            # Отправляем запрос к AI
            response = await self.ai_client.classify_batch(prompt)

            # Парсим результаты
            results = self.prompt_builder.parse_classification_response(response, product_map)

            # Обновляем товары в БД
            await self._update_products_with_results(products, results, batch_id)

            # Статистика
            classified_count = len(results)
            none_classified_count = len(products) - classified_count

            logger.info(
                f"Batch {batch_id} completed: "
                f"{classified_count} classified, {none_classified_count} not classified"
            )

            return {
                "batch_id": batch_id,
                "total": len(products),
                "classified": classified_count,
                "none_classified": none_classified_count,
                "results": results
            }

        except Exception as e:
            logger.error(f"Error processing batch {batch_id}: {e}")
            # Помечаем все товары как failed
            await self._mark_products_failed(product_ids, str(e))
            raise

    async def _update_products_with_results(
            self,
            products: List[Dict[str, Any]],
            results: Dict[str, List[str]],
            batch_id: str
    ):
        """Обновить товары с результатами классификации"""
        updates = []

        for product in products:
            product_id = str(product["_id"])

            if product_id in results:
                # Товар классифицирован
                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stg1": ProductStatus.CLASSIFIED.value,
                        "okpd_group": results[product_id],
                        "batch_id": batch_id,
                        "worker_id": self.worker_id
                    }
                })
            else:
                # Товар не классифицирован
                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stg1": ProductStatus.NONE_CLASSIFIED.value,
                        "batch_id": batch_id,
                        "worker_id": self.worker_id
                    }
                })

        await self.target_store.bulk_update_products(updates)

    async def _mark_products_failed(self, product_ids: List[str], error_message: str):
        """Пометить товары как failed"""
        updates = []
        for product_id in product_ids:
            updates.append({
                "_id": product_id,
                "data": {
                    "status_stg1": ProductStatus.FAILED.value,
                    "error_message": error_message,
                    "worker_id": self.worker_id
                }
            })

        await self.target_store.bulk_update_products(updates)

    async def run_continuous_classification(self):
        """Запустить непрерывную классификацию"""
        logger.info(f"Starting continuous classification for worker {self.worker_id}...")

        while True:
            try:
                # Получаем pending товары атомарно
                products = await self.target_store.get_pending_products_atomic(
                    self.batch_size,
                    self.worker_id
                )

                if not products:
                    logger.info(f"Worker {self.worker_id}: No pending products, waiting...")
                    await asyncio.sleep(10)
                    continue

                # Обрабатываем батч
                await self.process_batch(products)

                # Небольшая пауза между батчами
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error in continuous classification for worker {self.worker_id}: {e}")
                await asyncio.sleep(30)