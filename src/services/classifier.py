import uuid
import logging
from typing import List, Dict, Any
from datetime import datetime
import asyncio
import os
import time

from src.services.ai_client import AnthropicClient, PromptBuilder
from src.storage.target_mongo import TargetMongoStore
from src.models.domain import ProductStatus

logger = logging.getLogger(__name__)


class StageOneClassifier:
    """Классификатор первого этапа с поддержкой prompt caching"""

    def __init__(
            self,
            ai_client: AnthropicClient,
            target_store: TargetMongoStore,
            batch_size: int = 300,
            worker_id: str = None,
            collection_name: str = None
    ):
        self.ai_client = ai_client
        self.target_store = target_store
        self.batch_size = batch_size
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.collection_name = collection_name
        self.prompt_builder = PromptBuilder()

        # Получаем кэшируемый контент один раз
        self.cached_content = self.prompt_builder.get_cached_content()
        logger.info(f"Cached content size: {len(self.cached_content):,} characters")

        # Rate limit settings
        self.rate_limit_delay = int(os.getenv("RATE_LIMIT_DELAY", "60"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        # Cache refresh
        self.last_cache_refresh = time.time()
        self.cache_refresh_interval = 240  # 4 минуты

        logger.info(f"Classifier initialized with batch_size={batch_size}, "
                    f"rate_limit_delay={self.rate_limit_delay}s, "
                    f"max_retries={self.max_retries}")

        if collection_name:
            logger.info(f"Classifier will process only collection: {collection_name}")

    async def process_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработать батч товаров с использованием кэша"""
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

        start_time = time.time()
        product_ids = [p["_id"] for p in products]

        # Проверяем необходимость обновления кэша
        await self._refresh_cache_if_needed()

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Готовим данные для промпта
                product_map = {}
                product_names = []

                for product in products:
                    product_id = product["_id"]
                    # Создаем расширенное название товара с учетом дополнительных полей
                    product_name = self._build_product_name(product)
                    product_map[product_name] = product_id
                    product_names.append(product_name)

                # Формируем только динамическую часть промпта
                dynamic_prompt = self.prompt_builder.build_products_prompt(product_names)

                # Отправляем запрос с кэшированным контентом
                response = await self.ai_client.classify_batch(
                    prompt=dynamic_prompt,
                    cached_content=self.cached_content
                )

                # Парсим результаты
                results = self.prompt_builder.parse_classification_response(response, product_map)

                # Обновляем товары в БД
                await self._update_products_with_results(products, results)

                # Статистика
                classified_count = len(results)
                none_classified_count = len(products) - classified_count

                logger.info(
                    f"Batch {batch_id} completed: "
                    f"{classified_count} classified, {none_classified_count} not classified"
                )

                processing_time = time.time() - start_time
                logger.info(f"Batch processing time: {processing_time:.2f}s")

                return {
                    "batch_id": batch_id,
                    "total": len(products),
                    "classified": classified_count,
                    "none_classified": none_classified_count,
                    "results": results
                }

            except Exception as e:
                error_str = str(e)

                # Обработка различных типов ошибок (таймауты, rate limits и т.д.)
                if "timeout" in error_str.lower() or "timed out" in error_str.lower():
                    retry_count += 1
                    if retry_count < self.max_retries:
                        wait_time = 10 * retry_count
                        logger.warning(f"Request timeout. Retry {retry_count}/{self.max_retries} after {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                elif "429" in error_str or "rate_limit_error" in error_str:
                    retry_count += 1
                    if retry_count < self.max_retries:
                        wait_time = 30 * (2 ** (retry_count - 1))
                        logger.warning(f"Rate limit hit. Retry {retry_count}/{self.max_retries} after {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue

                logger.error(f"Error processing batch {batch_id}: {e}")
                await self._mark_products_failed(product_ids)
                raise

    def _build_product_name(self, product: Dict[str, Any]) -> str:
        """Построить название товара для классификации"""
        # Просто возвращаем title, так как теперь только он есть в БД
        return product.get("title", "")

    async def _refresh_cache_if_needed(self):
        """Обновить кэш если прошло больше 4 минут"""
        current_time = time.time()
        if current_time - self.last_cache_refresh > self.cache_refresh_interval:
            logger.info("Refreshing cache to prevent expiration")

            # Отправляем пустой запрос для обновления кэша
            try:
                await self.ai_client.classify_batch(
                    prompt="Тестовый товар",
                    cached_content=self.cached_content,
                    max_tokens=10
                )
                self.last_cache_refresh = current_time
                logger.info("Cache refreshed successfully")
            except Exception as e:
                logger.warning(f"Failed to refresh cache: {e}")

    async def _update_products_with_results(
            self,
            products: List[Dict[str, Any]],
            results: Dict[Any, List[str]]
    ):
        """Обновить товары с результатами классификации"""
        updates = []

        for product in products:
            product_id = product["_id"]

            if product_id in results:
                # Товар классифицирован
                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stage1": ProductStatus.CLASSIFIED.value,
                        "okpd_groups": results[product_id]
                    }
                })
                logger.debug(f"Product {product_id} classified with groups: {results[product_id]}")
            else:
                # Товар не классифицирован
                updates.append({
                    "_id": product_id,
                    "data": {
                        "status_stage1": ProductStatus.NONE_CLASSIFIED.value
                    }
                })
                logger.debug(f"Product {product_id} not classified")

        if updates:
            await self.target_store.bulk_update_products(updates)

    async def _mark_products_failed(self, product_ids: List[Any]):
        """Пометить товары как failed"""
        updates = []
        for product_id in product_ids:
            updates.append({
                "_id": product_id,
                "data": {
                    "status_stage1": ProductStatus.FAILED.value
                }
            })

        if updates:
            await self.target_store.bulk_update_products(updates)

    async def run_continuous_classification(self):
        """Запустить непрерывную классификацию"""
        logger.info(f"Starting continuous classification for worker {self.worker_id}...")
        logger.info(f"Using prompt caching with Claude 3.5 Sonnet")

        if self.collection_name:
            logger.info(f"Processing only collection: {self.collection_name}")

        first_batch = True
        consecutive_timeouts = 0
        current_batch_size = self.batch_size

        while True:
            batch_size = 1 if first_batch else current_batch_size

            try:
                # Получаем pending товары атомарно с фильтрацией по коллекции
                products = await self.target_store.get_pending_products_atomic_by_collection(
                    batch_size,
                    self.worker_id,
                    self.collection_name
                )
                first_batch = False

                if not products:
                    logger.info(f"Worker {self.worker_id}: No pending products in {self.collection_name}, waiting...")
                    await asyncio.sleep(10)

                    # Обновляем кэш даже во время простоя
                    await self._refresh_cache_if_needed()

                    # Сбрасываем счетчик таймаутов при простое
                    consecutive_timeouts = 0
                    current_batch_size = self.batch_size
                    continue

                logger.info(f"Worker {self.worker_id}: Got {len(products)} products to process")

                # Обрабатываем батч
                result = await self.process_batch(products)

                logger.info(
                    f"Worker {self.worker_id}: Batch processed - "
                    f"classified: {result['classified']}, "
                    f"not classified: {result['none_classified']}"
                )

                # Успешная обработка - сбрасываем счетчик таймаутов
                consecutive_timeouts = 0

                # Постепенно увеличиваем размер батча после успешной обработки
                if current_batch_size < self.batch_size:
                    current_batch_size = min(current_batch_size * 2, self.batch_size)
                    logger.info(f"Increasing batch size to {current_batch_size}")

                # Задержка между батчами
                logger.info(f"Worker {self.worker_id}: Waiting {self.rate_limit_delay}s before next batch...")
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                error_str = str(e).lower()

                # Особая обработка таймаутов
                if "timeout" in error_str or "timed out" in error_str:
                    consecutive_timeouts += 1

                    # Уменьшаем размер батча при частых таймаутах
                    if consecutive_timeouts >= 2:
                        current_batch_size = max(10, current_batch_size // 2)
                        logger.warning(
                            f"Multiple timeouts detected. Reducing batch size to {current_batch_size}"
                        )

                    # Увеличиваем задержку при таймаутах
                    timeout_delay = min(300, 30 * consecutive_timeouts)
                    logger.error(
                        f"Timeout error in continuous classification. "
                        f"Waiting {timeout_delay}s before retry..."
                    )
                    await asyncio.sleep(timeout_delay)
                else:
                    logger.error(f"Error in continuous classification for worker {self.worker_id}: {e}", exc_info=True)
                    await asyncio.sleep(30)