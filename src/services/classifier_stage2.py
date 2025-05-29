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

logger = logging.getLogger(__name__)


class StageTwoClassifier:
    """Классификатор второго этапа с кэшированием по классам"""

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
        self.worker_id = worker_id or f"stage2_worker_{uuid.uuid4().hex[:8]}"
        self.prompt_builder = PromptBuilderStage2()

        # Rate limit settings
        self.rate_limit_delay = int(os.getenv("RATE_LIMIT_DELAY", "5"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        # Cache refresh tracking
        self.last_cache_refresh = {}  # По классам
        self.cache_refresh_interval = 240  # 4 минуты

        logger.info(f"Stage 2 Classifier initialized with batch_size={batch_size}, "
                    f"rate_limit_delay={self.rate_limit_delay}s")

    async def process_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработать батч товаров второго этапа с кэшированием"""
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

        # Группируем товары по основным классам для оптимального кэширования
        products_by_class = self._group_products_by_class(products)

        all_results = {}

        for class_code, class_products in products_by_class.items():
            logger.info(f"Processing {len(class_products)} products for class {class_code}")

            # Обновляем кэш для класса при необходимости
            await self._refresh_class_cache_if_needed(class_code)

            retry_count = 0
            while retry_count < self.max_retries:
                try:
                    # Готовим данные для промпта
                    product_map = {}
                    for product in class_products:
                        product_id = product["_id"]
                        product_name = product["title"]
                        product_map[product_name] = product_id

                    # Получаем кэшированный контент для групп товаров
                    # Берем группы первого товара (они должны быть из одного класса)
                    sample_groups = class_products[0].get("okpd_group", [])
                    cached_content = self.prompt_builder.get_cached_content_for_groups(sample_groups)

                    if not cached_content:
                        logger.warning(f"No cached content for class {class_code}, skipping")
                        break

                    # Формируем динамическую часть
                    dynamic_prompt = self.prompt_builder.build_products_prompt_stage2(class_products)

                    # Отправляем запрос к AI с кэшированием
                    response = await self.ai_client.classify_batch(
                        prompt=dynamic_prompt,
                        cached_content=cached_content,
                        max_tokens=4000
                    )

                    # Парсим результаты
                    results = self.prompt_builder.parse_stage2_response(response, product_map)
                    all_results.update(results)

                    break  # Успешно обработали

                except Exception as e:
                    error_str = str(e)

                    # Проверяем rate limit
                    if "429" in error_str or "rate_limit_error" in error_str:
                        retry_count += 1

                        if retry_count < self.max_retries:
                            wait_time = 30 * (2 ** (retry_count - 1))
                            logger.warning(
                                f"Rate limit hit for class {class_code}. "
                                f"Retry {retry_count}/{self.max_retries} after {wait_time}s delay."
                            )
                            await asyncio.sleep(wait_time)
                            continue

                    logger.error(f"Error processing class {class_code}: {e}")
                    break

        # Обновляем товары с результатами
        await self._update_products_with_results(products, all_results)

        # Статистика
        classified_count = len(all_results)
        none_classified_count = len(products) - classified_count

        logger.info(
            f"Stage 2 batch {batch_id} completed: "
            f"{classified_count} classified, {none_classified_count} not classified"
        )

        processing_time = time.time() - start_time
        logger.info(f"Batch processing time: {processing_time:.2f}s")

        return {
            "batch_id": batch_id,
            "total": len(products),
            "classified": classified_count,
            "none_classified": none_classified_count,
            "results": all_results
        }

    def _group_products_by_class(self, products: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Группировать товары по основному классу ОКПД2"""
        products_by_class = {}

        for product in products:
            okpd_groups = product.get("okpd_group", [])
            if okpd_groups:
                # Берем класс из первой группы
                main_class = okpd_groups[0][:2]

                if main_class not in products_by_class:
                    products_by_class[main_class] = []

                products_by_class[main_class].append(product)

        return products_by_class

    async def _refresh_class_cache_if_needed(self, class_code: str):
        """Обновить кэш класса если необходимо"""
        current_time = time.time()
        last_refresh = self.last_cache_refresh.get(class_code, 0)

        if current_time - last_refresh > self.cache_refresh_interval:
            logger.info(f"Refreshing cache for class {class_code}")

            # Отправляем минимальный запрос для обновления кэша
            try:
                cached_content = self.prompt_builder._class_caches.get(class_code)
                if cached_content:
                    await self.ai_client.classify_batch(
                        prompt="Тестовый товар",
                        cached_content=cached_content,
                        max_tokens=10
                    )
                    self.last_cache_refresh[class_code] = current_time
                    logger.info(f"Cache for class {class_code} refreshed")
            except Exception as e:
                logger.warning(f"Failed to refresh cache for class {class_code}: {e}")

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
        logger.info("Using per-class caching for optimal performance")

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