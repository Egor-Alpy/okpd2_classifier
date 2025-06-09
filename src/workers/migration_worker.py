#!/usr/bin/env python3
"""
Migration worker для работы со всеми коллекциями
"""
import asyncio
import logging
import sys
from typing import Optional
from redis.asyncio import Redis

from src.storage.source_mongo import SourceMongoStore
from src.storage.target_mongo import TargetMongoStore
from src.services.product_migrator import ProductMigrator
from src.core.config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class MigrationWorker:
    """Воркер для миграции товаров из всех коллекций"""

    def __init__(self):
        self.source_store = None
        self.target_store = None
        self.migrator = None
        self.redis_client = None
        self.running = False

    async def initialize_stores(self):
        """Инициализировать подключения к БД"""
        # Инициализируем source store
        logger.info("Connecting to source MongoDB...")
        self.source_store = SourceMongoStore(
            settings.source_mongodb_database,
            None  # Не указываем коллекцию - будем работать со всеми
        )

        # Проверяем подключение к source
        if not await self.source_store.test_connection():
            logger.error("Failed to connect to source MongoDB!")
            logger.error("Please check your .env file settings:")
            logger.error("- SOURCE_MONGO_HOST, SOURCE_MONGO_PORT")
            logger.error("- SOURCE_MONGO_USER, SOURCE_MONGO_PASS")
            logger.error("- SOURCE_MONGO_AUTHSOURCE")
            logger.error("- SOURCE_MONGO_DIRECT_CONNECTION")
            raise Exception("Cannot connect to source MongoDB")

        # Инициализируем target store
        logger.info("Connecting to target MongoDB...")
        logger.info(f"Using target collection name from settings: {settings.target_collection_name}")
        self.target_store = TargetMongoStore(
            settings.target_mongodb_database,
            settings.target_collection_name
        )

        # Проверяем подключение к target
        if not await self.target_store.test_connection():
            logger.error("Failed to connect to target MongoDB!")
            logger.error("Please check your .env file settings:")
            logger.error("- TARGET_MONGO_HOST, TARGET_MONGO_PORT")
            logger.error("- TARGET_MONGO_USER, TARGET_MONGO_PASS")
            logger.error("- TARGET_MONGO_AUTHSOURCE")
            logger.error("- TARGET_MONGO_DIRECT_CONNECTION")
            raise Exception("Cannot connect to target MongoDB")

        # Инициализируем target store (создание индексов)
        logger.info("Initializing target database and creating indexes...")
        await self.target_store.initialize()

        # Создаем migrator
        logger.info(f"Creating migrator with batch_size={settings.migration_batch_size}")
        self.migrator = ProductMigrator(
            self.source_store,
            self.target_store,
            settings.migration_batch_size
        )

    async def check_and_start_migration(self):
        """Проверить и начать миграцию если нужно"""
        logger.info("Checking migration status...")

        # Проверяем, есть ли активные миграции
        active_jobs = await self.target_store.migration_jobs.find({
            "status": "running"
        }).to_list(length=None)

        if active_jobs:
            # Есть активная миграция, продолжаем её
            job = active_jobs[0]
            logger.info(f"Found active migration job: {job['job_id']}")
            logger.info(f"Progress: {job['migrated_products']}/{job['total_products']}")

            # Проверяем через Redis, обрабатывается ли она другим воркером
            if self.redis_client:
                lock_key = f"migration_lock:{job['job_id']}"
                lock_acquired = await self.redis_client.set(
                    lock_key, "locked", nx=True, ex=300  # 5 минут TTL
                )

                if not lock_acquired:
                    logger.info("Another worker is handling this migration")
                    return None

            return job['job_id']

        # Проверяем, есть ли завершенные миграции
        completed_jobs = await self.target_store.migration_jobs.find({
            "status": "completed"
        }).to_list(length=None)

        if completed_jobs:
            # Проверяем, есть ли новые товары для миграции
            last_job = completed_jobs[-1]

            # Считаем товары во всех коллекциях source
            source_counts = await self.source_store.count_all_products()
            source_total = sum(source_counts.values())

            # Считаем товары в target
            target_count = await self.target_store.products.count_documents({})

            logger.info(f"Source products (all collections): {source_total}")
            logger.info(f"Target products: {target_count}")

            if source_total > target_count:
                logger.info(f"Found {source_total - target_count} new products to migrate")
                # Начинаем новую миграцию
                return await self.migrator.start_migration()
            else:
                logger.info("All products are already migrated")
                return None

        # Нет миграций вообще - начинаем первую
        logger.info("No migration jobs found, starting initial migration...")

        # Проверяем, есть ли товары в source
        source_counts = await self.source_store.count_all_products()
        source_total = sum(source_counts.values())

        if source_total == 0:
            logger.error("No products found in source database!")
            logger.error(f"Source database: {settings.source_mongodb_database}")
            logger.error("Please check if the database contains product collections")
            return None

        logger.info(f"Found {source_total} products across {len(source_counts)} collections")
        logger.info(f"Collections: {list(source_counts.keys())}")

        return await self.migrator.start_migration()

    async def monitor_migration(self, job_id: str):
        """Мониторить прогресс миграции"""
        last_progress = 0

        while self.running:
            job = await self.target_store.get_migration_job(job_id)

            if not job:
                logger.error(f"Migration job {job_id} not found!")
                break

            if job["status"] == "completed":
                logger.info(f"Migration {job_id} completed successfully!")
                logger.info(f"Total migrated: {job['migrated_products']} products")
                break

            if job["status"] == "failed":
                logger.error(f"Migration {job_id} failed!")
                break

            # Показываем прогресс
            current_progress = job['migrated_products']
            if current_progress != last_progress:
                percentage = (current_progress / job['total_products'] * 100) if job['total_products'] > 0 else 0
                logger.info(
                    f"Migration progress: {current_progress}/{job['total_products']} "
                    f"({percentage:.1f}%)"
                )
                last_progress = current_progress

            # Обновляем блокировку в Redis
            if self.redis_client:
                lock_key = f"migration_lock:{job_id}"
                await self.redis_client.expire(lock_key, 300)  # Продлеваем на 5 минут

            await asyncio.sleep(5)

    async def start(self, job_id: Optional[str] = None):
        """Запустить воркер"""
        logger.info("=" * 60)
        logger.info("Starting Migration Worker (Multi-Collection)")
        logger.info("=" * 60)

        try:
            # Инициализируем Redis для координации
            try:
                self.redis_client = await Redis.from_url(settings.redis_url)
                await self.redis_client.ping()
                logger.info("Connected to Redis for coordination")
            except Exception as e:
                logger.warning(f"Redis not available: {e}")
                self.redis_client = None

            # Показываем параметры подключения
            logger.info("Connection parameters:")
            logger.info(f"Source host: {settings.source_mongo_host}:{settings.source_mongo_port}")
            logger.info(f"Source database: {settings.source_mongodb_database}")
            logger.info(f"Source collection: {settings.source_collection_name or 'ALL'}")
            logger.info(f"Target host: {settings.target_mongo_host}:{settings.target_mongo_port}")
            logger.info(f"Target database: {settings.target_mongodb_database}")
            logger.info(f"Target collection: {settings.target_collection_name}")

            # ВАЖНО: Инициализируем stores ДО их использования
            await self.initialize_stores()

            # Получаем список коллекций
            collections = await self.source_store.get_collections_list()
            logger.info(f"Found {len(collections)} product collections: {collections}")

            # Проверяем количество товаров
            source_counts = await self.source_store.count_all_products()
            total_count = sum(source_counts.values())

            logger.info(f"Total products across all collections: {total_count}")
            for coll, count in source_counts.items():
                logger.info(f"  {coll}: {count} products")

            if total_count == 0:
                logger.error("No products found in source database!")
                logger.error("Please check that the source database contains product collections")
                return

            self.running = True

            # Если передан job_id - продолжаем его
            if job_id:
                logger.info(f"Resuming migration job: {job_id}")
                await self.migrator.resume_migration(job_id)
                await self.monitor_migration(job_id)
            else:
                # Автоматически определяем что делать
                auto_job_id = await self.check_and_start_migration()

                if auto_job_id:
                    logger.info(f"Migration job started/resumed: {auto_job_id}")
                    await self.monitor_migration(auto_job_id)
                else:
                    logger.info("No migration needed at this time")

                    # Ждем новые задачи
                    logger.info("Waiting for new migration tasks...")
                    while self.running:
                        # Периодически проверяем, не появились ли новые товары
                        await asyncio.sleep(60)  # Проверка раз в минуту

                        auto_job_id = await self.check_and_start_migration()
                        if auto_job_id:
                            logger.info(f"New migration job started: {auto_job_id}")
                            await self.monitor_migration(auto_job_id)

        except KeyboardInterrupt:
            logger.info("Migration worker interrupted by user")
        except Exception as e:
            logger.error(f"Migration worker error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info("Stopping migration worker...")
        self.running = False

        if self.redis_client:
            await self.redis_client.close()

        if self.source_store:
            await self.source_store.close()

        if self.target_store:
            await self.target_store.close()

        logger.info("Migration worker stopped")


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Migration worker')
    parser.add_argument('--job-id', help='Resume specific job')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')
    args = parser.parse_args()

    # Настройка уровня логирования
    log_level = getattr(logging, args.log_level.upper())
    logging.getLogger().setLevel(log_level)
    logging.getLogger('src').setLevel(log_level)

    worker = MigrationWorker()
    await worker.start(args.job_id)


if __name__ == "__main__":
    asyncio.run(main())