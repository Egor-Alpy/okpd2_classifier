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
from src.core.metrics import metrics_collector, ClassificationMetrics

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

        # Rate limit settings
        self.rate_limit_delay = int(os.getenv("RATE_LIMIT_DELAY", "10"))
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))

        logger.info(f"Classifier initialized with batch_size={batch_size}, "
                    f"rate_limit_delay={self.rate_limit_delay}s, "
                    f"max_retries={self.max_retries}")

    async def process_batch(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Обработать батч товаров с записью метрик

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

        # Засекаем время
        start_time = time.time()

        # ВАЖНО: Извлекаем ID из документов MongoDB правильно
        product_ids = []
        for p in products:
            # MongoDB документ может содержать ObjectId в поле _id
            if "_id" in p:
                product_ids.append(p["_id"])  # Сохраняем оригинальный ObjectId
            else:
                logger.error(f"Product without _id found: {p}")

        retry_count = 0
        while retry_count < self.max_retries:
            try:
                # Готовим данные для промпта
                product_map = {}
                product_names = []

                for product in products:
                    # Используем оригинальный ObjectId для маппинга
                    product_id = product["_id"]  # Это ObjectId из MongoDB
                    product_name = product["title"]
                    product_map[product_name] = product_id
                    product_names.append(product_name)

                # Формируем промпт
                prompt = self.prompt_builder.build_stage_one_prompt(product_names)

                # Отправляем запрос к AI с retry логикой
                response = await self.ai_client.classify_batch(prompt)

                # Парсим результаты (parse_classification_response теперь вернет ObjectId в результатах)
                results = self._parse_response_with_objectid(response, product_map)

                # Обновляем товары в БД
                await self._update_products_with_results(products, results, batch_id)

                # Статистика
                classified_count = len(results)
                none_classified_count = len(products) - classified_count

                logger.info(
                    f"Batch {batch_id} completed: "
                    f"{classified_count} classified, {none_classified_count} not classified"
                )

                # Записываем метрику успешной обработки
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

                # Проверяем, является ли это rate limit ошибкой
                if "429" in error_str or "rate_limit_error" in error_str:
                    # Записываем метрику о rate limit
                    await metrics_collector.record_rate_limit(self.worker_id)

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

                # Записываем метрику об ошибке
                processing_time = time.time() - start_time
                metric = ClassificationMetrics(
                    timestamp=datetime.utcnow(),
                    worker_id=self.worker_id,
                    batch_size=len(products),
                    processing_time=processing_time,
                    success_count=0,
                    failure_count=len(products)
                )
                await metrics_collector.record_classification(metric)

                # Помечаем все товары как failed
                await self._mark_products_failed(product_ids, str(e))
                raise

    def _parse_response_with_objectid(self, response: str, product_map: Dict[str, Any]) -> Dict[Any, List[str]]:
        """
        Парсинг ответа от AI с сохранением ObjectId

        Args:
            response: Ответ от AI
            product_map: Маппинг {название товара: ObjectId}

        Returns:
            Dict с результатами {ObjectId: [группы]}
        """
        import re
        results = {}

        for line in response.strip().split('\n'):
            if '|' not in line:
                continue

            parts = line.split('|')
            if len(parts) < 2:
                continue

            product_name = parts[0].strip()

            # Ищем товар в маппинге
            product_id = None
            for name, pid in product_map.items():
                if product_name.lower() in name.lower() or name.lower() in product_name.lower():
                    product_id = pid  # Это ObjectId
                    break

            if product_id:
                # Извлекаем группы
                groups = []
                for group in parts[1:]:
                    group = group.strip()
                    # Проверяем, что это двузначное число
                    if re.match(r'^\d{2}$', group):
                        groups.append(group)

                if groups:
                    results[product_id] = groups  # Ключ - это ObjectId

        return results

    async def _update_products_with_results(
            self,
            products: List[Dict[str, Any]],
            results: Dict[Any, List[str]],
            batch_id: str
    ):
        """Обновить товары с результатами классификации"""
        updates = []

        for product in products:
            # Используем оригинальный ObjectId из документа
            product_id = product["_id"]  # Это ObjectId

            if product_id in results:
                # Товар классифицирован
                updates.append({
                    "_id": product_id,  # Передаем ObjectId как есть
                    "data": {
                        "status_stg1": ProductStatus.CLASSIFIED.value,
                        "okpd_group": results[product_id],
                        "batch_id": batch_id,
                        "worker_id": self.worker_id,
                        "updated_at": datetime.utcnow()
                    }
                })
                logger.debug(f"Product {product_id} classified with groups: {results[product_id]}")
            else:
                # Товар не классифицирован
                updates.append({
                    "_id": product_id,  # Передаем ObjectId как есть
                    "data": {
                        "status_stg1": ProductStatus.NONE_CLASSIFIED.value,
                        "batch_id": batch_id,
                        "worker_id": self.worker_id,
                        "updated_at": datetime.utcnow()
                    }
                })
                logger.debug(f"Product {product_id} not classified")

        if updates:
            logger.info(f"Sending {len(updates)} updates to bulk_update_products")
            await self.target_store.bulk_update_products(updates)
        else:
            logger.warning("No updates to send to database")

    async def _mark_products_failed(self, product_ids: List[Any], error_message: str):
        """Пометить товары как failed"""
        updates = []
        for product_id in product_ids:
            updates.append({
                "_id": product_id,  # ObjectId передаем как есть
                "data": {
                    "status_stg1": ProductStatus.FAILED.value,
                    "error_message": error_message,
                    "worker_id": self.worker_id,
                    "updated_at": datetime.utcnow()
                }
            })

        if updates:
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

                logger.info(f"Worker {self.worker_id}: Got {len(products)} products to process")

                # Обрабатываем батч
                result = await self.process_batch(products)

                logger.info(
                    f"Worker {self.worker_id}: Batch processed - "
                    f"classified: {result['classified']}, "
                    f"not classified: {result['none_classified']}"
                )

                # ВАЖНО: Задержка между батчами для избежания rate limit
                logger.info(f"Worker {self.worker_id}: Waiting {self.rate_limit_delay}s before next batch...")
                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Error in continuous classification for worker {self.worker_id}: {e}", exc_info=True)
                await asyncio.sleep(30)