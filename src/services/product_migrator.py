import uuid
import logging
from typing import Optional, Dict, Any
import asyncio
import time
from datetime import datetime

from src.storage.source_mongo import SourceMongoStore
from src.storage.target_mongo import TargetMongoStore
from src.core.config import settings

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
        Начать миграцию товаров из всех коллекций

        Returns:
            ID задачи миграции
        """
        if not job_id:
            job_id = f"migration_{uuid.uuid4().hex[:8]}"

        logger.info(f"Starting migration job {job_id}")

        try:
            # Проверяем подключения
            logger.info("Verifying database connections...")

            if not await self.source_store.test_connection():
                raise Exception("Cannot connect to source MongoDB")

            if not await self.target_store.test_connection():
                raise Exception("Cannot connect to target MongoDB")

            # Получаем список всех коллекций и считаем товары
            collection_counts = await self.source_store.count_all_products()
            total_products = sum(collection_counts.values())

            logger.info(f"Total products to migrate: {total_products}")
            logger.info(f"Collections breakdown: {collection_counts}")

            if total_products == 0:
                logger.warning("No products found to migrate")
                # Создаем завершенную задачу
                await self.target_store.create_migration_job(job_id, 0)
                await self.target_store.update_migration_job(job_id, 0, status="completed")
                return job_id

            # Создаем задачу миграции
            await self.target_store.create_migration_job(job_id, total_products)

            # Запускаем миграцию в фоне
            asyncio.create_task(self._run_migration_all_collections(job_id, collection_counts))

            return job_id

        except Exception as e:
            logger.error(f"Failed to start migration: {e}")
            # Создаем failed задачу
            try:
                await self.target_store.create_migration_job(job_id, 0)
                await self.target_store.update_migration_job(job_id, 0, status="failed")
            except:
                pass
            raise

    async def _run_migration_all_collections(self, job_id: str, collection_counts: Dict[str, int]):
        """Выполнить миграцию из всех коллекций"""
        total_migrated = 0
        start_time = time.time()

        try:
            # Обрабатываем каждую коллекцию
            for collection_name, count in collection_counts.items():
                if count == 0:
                    logger.info(f"Skipping empty collection: {collection_name}")
                    continue

                logger.info(f"Starting migration from collection: {collection_name} ({count} products)")

                try:
                    migrated_from_collection = await self._migrate_collection(
                        job_id,
                        collection_name,
                        total_migrated
                    )

                    total_migrated += migrated_from_collection

                    logger.info(
                        f"Completed migration from {collection_name}: "
                        f"{migrated_from_collection} products migrated"
                    )

                except Exception as e:
                    logger.error(f"Error migrating collection {collection_name}: {e}")
                    # Продолжаем с другими коллекциями
                    continue

                # Небольшая пауза между коллекциями
                await asyncio.sleep(1)

            # Миграция завершена
            elapsed_time = time.time() - start_time
            logger.info(
                f"Migration {job_id} completed in {elapsed_time:.2f}s: "
                f"{total_migrated} products migrated"
            )

            await self.target_store.update_migration_job(
                job_id,
                total_migrated,
                status="completed"
            )

        except Exception as e:
            logger.error(f"Migration {job_id} failed after {total_migrated} products: {e}")
            await self.target_store.update_migration_job(
                job_id,
                total_migrated,
                status="failed"
            )
            raise

    async def _migrate_collection(self, job_id: str, collection_name: str, already_migrated: int) -> int:
        """Мигрировать товары из одной коллекции"""
        migrated_count = 0
        last_id = None
        batch_count = 0

        while True:
            # Засекаем время батча
            batch_start_time = time.time()
            batch_count += 1

            try:
                # Получаем батч из коллекции
                products = await self.source_store.get_products_batch(
                    limit=self.batch_size,
                    last_id=last_id,
                    collection_name=collection_name
                )

                if not products:
                    logger.info(f"No more products in collection {collection_name}")
                    break

                # Вставляем в целевую БД с указанием исходной коллекции
                inserted = await self.target_store.insert_products_batch(
                    products,
                    collection_name  # Передаем имя коллекции
                )

                # Логирование
                batch_processing_time = time.time() - batch_start_time
                duplicates = len(products) - inserted

                logger.info(
                    f"[{collection_name}] Batch {batch_count}: "
                    f"{inserted} inserted, {duplicates} duplicates, "
                    f"time: {batch_processing_time:.2f}s"
                )

                migrated_count += inserted

                # Запоминаем последний ID
                if products:
                    last_id = products[-1]["_id"]

                # Обновляем общий прогресс
                await self.target_store.update_migration_job(
                    job_id,
                    already_migrated + migrated_count,
                    last_id
                )

                # Небольшая пауза между батчами
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error processing batch {batch_count} from {collection_name}: {e}")
                # Пробуем продолжить со следующего батча
                if products and len(products) > 0:
                    last_id = products[-1]["_id"]
                await asyncio.sleep(1)
                continue

        return migrated_count

    async def resume_migration(self, job_id: str):
        """Продолжить прерванную миграцию"""
        job = await self.target_store.get_migration_job(job_id)

        if not job:
            raise ValueError(f"Migration job {job_id} not found")

        if job["status"] == "completed":
            logger.info(f"Migration {job_id} already completed")
            return

        logger.info(f"Resuming migration {job_id} from {job['migrated_products']} products")

        # Обновляем статус на running
        await self.target_store.update_migration_job(
            job_id,
            job['migrated_products'],
            status="running"
        )

        # Получаем список коллекций и их размеры
        collection_counts = await self.source_store.count_all_products()

        # Продолжаем миграцию
        await self._run_migration_all_collections(job_id, collection_counts)