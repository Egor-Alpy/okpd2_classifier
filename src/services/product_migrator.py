import uuid
import logging
from typing import Optional
import asyncio
import time
from datetime import datetime

from src.storage.source_mongo import SourceMongoStore
from src.storage.target_mongo import TargetMongoStore
from src.core.config import settings
from src.core.metrics import metrics_collector, MigrationMetrics

logger = logging.getLogger(__name__)


class ProductMigrator:
    """Сервис миграции товаров из исходной БД в целевую"""

    def __init__(
            self,
            source_store: SourceMongoStore,
            target_store: TargetMongoStore,
            batch_size: int = 1000
    ):
        self.source_store = source_store
        self.target_store = target_store
        self.batch_size = batch_size

    async def start_migration(self, job_id: Optional[str] = None) -> str:
        """
        Начать миграцию товаров

        Returns:
            ID задачи миграции
        """
        if not job_id:
            job_id = f"migration_{uuid.uuid4().hex[:8]}"

        logger.info(f"Starting migration job {job_id}")

        # Считаем общее количество товаров
        total_products = await self.source_store.count_total_products()
        logger.info(f"Total products to migrate: {total_products}")

        # Создаем задачу миграции
        await self.target_store.create_migration_job(job_id, total_products)

        # Запускаем миграцию в фоне
        asyncio.create_task(self._run_migration(job_id, total_products))

        return job_id

    async def _run_migration(self, job_id: str, total_products: int):
        """Выполнить миграцию"""
        migrated_count = 0
        last_id = None

        try:
            while migrated_count < total_products:
                # Засекаем время батча
                batch_start_time = time.time()

                # Получаем батч из исходной БД
                products = await self.source_store.get_products_batch(
                    limit=self.batch_size,
                    last_id=last_id
                )

                if not products:
                    logger.info("No more products to migrate")
                    break

                # Вставляем в целевую БД
                inserted = await self.target_store.insert_products_batch(
                    products,
                    self.source_store.collection_name
                )

                # Записываем метрику миграции
                batch_processing_time = time.time() - batch_start_time
                duplicates = len(products) - inserted

                metric = MigrationMetrics(
                    timestamp=datetime.utcnow(),
                    batch_size=len(products),
                    processing_time=batch_processing_time,
                    inserted_count=inserted,
                    duplicate_count=duplicates
                )
                await metrics_collector.record_migration(metric)

                migrated_count += inserted

                # Запоминаем последний ID
                if products:
                    last_id = products[-1]["_id"]

                # Обновляем прогресс
                await self.target_store.update_migration_job(
                    job_id,
                    migrated_count,
                    last_id
                )

                logger.info(
                    f"Migration progress: {migrated_count}/{total_products} "
                    f"({migrated_count / total_products * 100:.1f}%) "
                    f"- Batch time: {batch_processing_time:.2f}s"
                )

                # Небольшая пауза между батчами
                await asyncio.sleep(0.1)

            # Миграция завершена
            await self.target_store.update_migration_job(
                job_id,
                migrated_count,
                last_id,
                status="completed"
            )

            logger.info(f"Migration {job_id} completed: {migrated_count} products migrated")

        except Exception as e:
            logger.error(f"Migration {job_id} failed: {e}")
            await self.target_store.update_migration_job(
                job_id,
                migrated_count,
                last_id,
                status="failed"
            )
            raise

    async def resume_migration(self, job_id: str):
        """Продолжить прерванную миграцию"""
        job = await self.target_store.get_migration_job(job_id)

        if not job:
            raise ValueError(f"Migration job {job_id} not found")

        if job["status"] == "completed":
            logger.info(f"Migration {job_id} already completed")
            return

        logger.info(f"Resuming migration {job_id} from {job['migrated_products']} products")

        # Продолжаем с последнего обработанного ID
        await self._run_migration(job_id, job["total_products"])