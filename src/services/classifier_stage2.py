import uuid
import logging
import time
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

from src.services.ai_client import AnthropicClient
from src.services.ai_client_stage2 import PromptBuilderStage2
from src.storage.target_mongo import TargetMongoStore
from src.models.domain import ProductStatus
from src.models.domain_stage2 import ProductStatusStage2
from src.core.metrics import metrics_collector, ClassificationMetrics

logger = logging.getLogger(__name__)


class StageTwoClassifier:
    """Классификатор второго этапа для точных кодов ОКПД2"""

    def __init__(
            self,
            ai_client: AnthropicClient,
            target_store: TargetMongoStore,
            batch_size: int = 15,  # Меньше, т.к. промпт будет больше
            worker_id: str = None
    ):
        self.ai_client = ai_client
        self.target_store = target_store
        self.batch_size = batch_size
        self.worker_id = worker_id or f"stage2_worker_{uuid.uuid4().hex[:8]}"
        self.prompt_builder = PromptBuilderStage2()

        # Rate limit settings
        self.rate_limit_delay = int(os.getenv("RATE_LIMIT_DELAY", "10"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        logger.info(f"Stage 2 Classifier initialized with batch_size={batch_size}, "
                    f"rate_limit_delay={self.rate_limit_delay}s")

    async def process_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработать батч товаров второго этапа"""
        if not products:
            return {
                "batch_id": "",
                "total": 0,
                "classified": 0,
                "none_classified": 0,
                "results": {}
            }

        batch_id = f"s2_batch_{uuid.uuid4().hex[:8]}"
        logger.info(f"Processing stage 2 batch {batch_id} with {len(products)} products")

        start_time = time.time()
        product_ids = [p["_id"] for p in products]

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Готовим данные для промпта
                product_map = {}
                for product in products:
                    product_id = product["_id"]
                    product_name = product["title"]
                    product_map[product_name] = product_id

                # Формируем промпт со всеми группами всех товаров
                prompt = self.prompt_builder.build_stage_two_prompt(products)

                # Отправляем запрос к AI
                response = await self.ai_client.classify_batch(prompt, max_tokens=4000)

                # Парсим результаты
                results = self.prompt_builder.parse_stage2_response(response, product_map)

                # Обновляем товары с результатами
                await self._update_products_with_results(products, results)

                # Статистика
                classified_count = len(results)
                none_classified_count = len(products) - classified_count

                logger.info(
                    f"Stage 2 batch {batch_id} completed: "
                    f"{classified_count} classified, {none_classified_count} not classified"
                )

                # Записываем метрику
                processing_time = time.time() - start_time
                metric = ClassificationMetrics(
                    timestamp=datetime.utcnow(),
                    worker_id=self.worker_id,
                    batch_size=len(products),
                    processing_time=processing_time,
                    success_count=classified_count,
                    failure_count=none_classified_count
                )
                await metrics_collector.record_classification(metric)

                return {
                    "batch_id": batch_id,
                    "total": len(products),
                    "classified": classified_count,
                    "none_classified": none_classified_count,
                    "results": results
                }

            except Exception as e:
                error_str = str(e)

                # Проверяем rate limit
                if "429" in error_str or "rate_limit_error" in error_str:
                    await metrics_collector.record_rate_limit(self.worker_id)
                    retry_count += 1

                    if retry_count < self.max_retries:
                        wait_time = 30 * (2 ** (retry_count - 1))
                        logger.warning(
                            f"Rate limit hit for stage 2 batch {batch_id}. "
                            f"Retry {retry_count}/{self.max_retries} after {wait_time}s delay."
                        )
                        await asyncio.sleep(wait_time)
                        continue

                logger.error(f"Error processing stage 2 batch {batch_id}: {e}")

                # Помечаем все товары как failed
                await self._mark_products_failed(product_ids)
                raise

    async def _update_products_with_results(
            self,
            products: List[Dict[str, Any]],
            results: Dict[Any, Dict[str, str]]
    ):
        """Обновить товары с результатами второго этапа"""
        updates = []
        current_time = datetime.utcnow()

        for product in products:
            product_id = product["_id"]

            if product_id in results:
                # Товар классифицирован с точным кодом
                result = results[product_id]
                code = result["code"]

                # Получаем описание кода из дерева ОКПД2
                code_name = self.prompt_builder.get_code_description(code)

                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stg2": ProductStatusStage2.CLASSIFIED.value,
                        "okpd2_code": code,
                        "okpd2_name": code_name,
                        "stage2_completed_at": current_time,
                        "stage2_batch_id": self.worker_id
                    }
                })
                logger.debug(f"Product {product_id} classified with exact code: {code}")
            else:
                # Товар не классифицирован - точный код не найден
                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stg2": ProductStatusStage2.NONE_CLASSIFIED.value,
                        "stage2_completed_at": current_time
                    }
                })
                logger.debug(f"Product {product_id} not classified in stage 2")

        if updates:
            await self.target_store.bulk_update_products(updates)

    async def _mark_products_failed(self, product_ids: List[Any]):
        """Пометить товары как failed для второго этапа"""
        updates = []
        current_time = datetime.utcnow()

        for product_id in product_ids:
            updates.append({
                "_id": product_id,
                "data": {
                    "status_stg2": ProductStatusStage2.FAILED.value,
                    "stage2_completed_at": current_time
                }
            })

        if updates:
            await self.target_store.bulk_update_products(updates)

    async def get_pending_products_batch(self, limit: int) -> List[Dict[str, Any]]:
        """Получить батч pending товаров для второго этапа"""
        products = []

        # Атомарно получаем и блокируем товары
        for _ in range(limit):
            doc = await self.target_store.products.find_one_and_update(
                {
                    "status_stg1": ProductStatus.CLASSIFIED.value,
                    "okpd_group": {"$exists": True, "$ne": []},
                    "$or": [
                        {"status_stg2": {"$exists": False}},
                        {"status_stg2": ProductStatusStage2.PENDING.value}
                    ]
                },
                {
                    "$set": {
                        "status_stg2": ProductStatusStage2.PROCESSING.value,
                        "stage2_started_at": datetime.utcnow(),
                        "stage2_worker_id": self.worker_id
                    }
                },
                return_document=True
            )

            if doc:
                products.append(doc)
            else:
                break

        if products:
            logger.info(f"Locked {len(products)} products for stage 2 processing")

        return products

    async def run_continuous_classification(self):
        """Запустить непрерывную классификацию второго этапа"""
        logger.info(f"Starting continuous stage 2 classification for worker {self.worker_id}")

        while True:
            try:
                # Получаем товары для обработки
                products = await self.get_pending_products_batch(self.batch_size)

                if not products:
                    logger.info("No pending products for stage 2, waiting...")
                    await asyncio.sleep(10)
                    continue

                logger.info(f"Worker {self.worker_id}: Got {len(products)} products for stage 2")

                # Обрабатываем батч
                result = await self.process_batch(products)

                logger.info(
                    f"Worker {self.worker_id}: Stage 2 batch processed - "
                    f"classified: {result['classified']}, "
                    f"not classified: {result['none_classified']}"
                )

                # Задержка между батчами
                logger.info(f"Worker {self.worker_id}: Waiting {self.rate_limit_delay}s before next batch...")
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Error in continuous stage 2 classification: {e}", exc_info=True)
                await asyncio.sleep(30)