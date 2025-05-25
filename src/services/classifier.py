import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime
import asyncio
import os

from src.services.ai_client import AnthropicClient, PromptBuilder
from src.storage.target_mongo import TargetMongoStore
from src.models.domain import ProductStatus

logger = logging.getLogger(__name__)


class StageOneClassifier:
    """Классификатор первого этапа с динамическим размером батча"""

    def __init__(
            self,
            ai_client: AnthropicClient,
            target_store: TargetMongoStore,
            batch_size: int = 50,
            worker_id: str = None
    ):
        self.ai_client = ai_client
        self.target_store = target_store
        self.initial_batch_size = batch_size
        self.current_batch_size = batch_size
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.prompt_builder = PromptBuilder()

        # Rate limit settings
        self.rate_limit_delay = int(os.getenv("RATE_LIMIT_DELAY", "10"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        # Динамические настройки
        self.consecutive_successes = 0
        self.consecutive_failures = 0
        self.min_batch_size = 5
        self.max_batch_size = 50

        logger.info(
            f"Classifier initialized with batch_size={batch_size}, "
            f"rate_limit_delay={self.rate_limit_delay}s, "
            f"max_retries={self.max_retries}"
        )

    def adjust_batch_size(self, success: bool):
        """Динамически корректировать размер батча"""
        if success:
            self.consecutive_successes += 1
            self.consecutive_failures = 0

            # Увеличиваем размер батча после 3 успешных запросов
            if self.consecutive_successes >= 3:
                self.current_batch_size = min(
                    int(self.current_batch_size * 1.5),
                    self.max_batch_size
                )
                self.consecutive_successes = 0
                logger.info(f"Increased batch size to {self.current_batch_size}")
        else:
            self.consecutive_failures += 1
            self.consecutive_successes = 0

            # Уменьшаем размер батча после неудачи
            self.current_batch_size = max(
                int(self.current_batch_size * 0.7),
                self.min_batch_size
            )
            logger.info(f"Decreased batch size to {self.current_batch_size}")

    async def run_continuous_classification(self):
        """Запустить непрерывную классификацию с динамическим размером батча"""
        logger.info(f"Starting continuous classification for worker {self.worker_id}...")

        while True:
            try:
                # Получаем pending товары с текущим размером батча
                products = await self.target_store.get_pending_products_atomic(
                    self.current_batch_size,
                    self.worker_id
                )

                if not products:
                    logger.info(f"Worker {self.worker_id}: No pending products, waiting...")
                    await asyncio.sleep(10)
                    continue

                # Обрабатываем батч
                try:
                    await self.process_batch(products)
                    self.adjust_batch_size(success=True)
                except Exception as e:
                    if "429" in str(e) or "rate_limit" in str(e):
                        self.adjust_batch_size(success=False)
                    raise

                # Динамическая задержка в зависимости от размера батча
                dynamic_delay = max(
                    self.rate_limit_delay,
                    int(self.rate_limit_delay * (self.current_batch_size / self.initial_batch_size))
                )

                logger.info(
                    f"Worker {self.worker_id}: Waiting {dynamic_delay}s before next batch "
                    f"(current batch size: {self.current_batch_size})..."
                )
                await asyncio.sleep(dynamic_delay)

            except Exception as e:
                logger.error(f"Error in continuous classification for worker {self.worker_id}: {e}")
                await asyncio.sleep(30)

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

        retry_count = 0
        while retry_count < self.max_retries:
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

                # Отправляем запрос к AI с retry логикой
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
                error_str = str(e)

                # Проверяем, является ли это rate limit ошибкой
                if "429" in error_str or "rate_limit_error" in error_str:
                    retry_count += 1

                    if retry_count < self.max_retries:
                        # Экспоненциальная задержка: 30s, 60s, 120s
                        wait_time = 30 * (2 ** (retry_count - 1))

                        logger.warning(
                            f"Rate limit hit for batch {batch_id}. "
                            f"Retry {retry_count}/{self.max_retries} after {wait_time}s delay. "
                            f"Error: {error_str}"
                        )

                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Max retries reached for batch {batch_id}. Marking as failed.")

                # Если это не rate limit или превышены попытки
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
