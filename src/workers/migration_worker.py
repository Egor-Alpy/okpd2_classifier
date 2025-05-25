import asyncio
import logging
from typing import Optional

from storage.source_mongo import SourceMongoStore
from storage.target_mongo import TargetMongoStore
from services.product_migrator import ProductMigrator
from core.config import settings

logger = logging.getLogger(__name__)


class MigrationWorker:
    """Воркер для миграции товаров"""

    def __init__(self):
        self.source_store = None
        self.target_store = None
        self.migrator = None
        self.running = False

    async def start(self, job_id: Optional[str] = None):
        """Запустить воркер"""
        logger.info("Starting migration worker...")

        # Инициализируем хранилища
        self.source_store = SourceMongoStore(
            settings.source_mongodb_url,
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        self.target_store = TargetMongoStore(
            settings.target_mongodb_url,
            settings.target_mongodb_database
        )

        self.migrator = ProductMigrator(
            self.source_store,
            self.target_store,
            settings.migration_batch_size
        )

        self.running = True

        try:
            if job_id:
                # Продолжаем существующую миграцию
                await self.migrator.resume_migration(job_id)
            else:
                # Начинаем новую миграцию
                job_id = await self.migrator.start_migration()
                logger.info(f"Started new migration job: {job_id}")

                # Ждем завершения
                while self.running:
                    job = await self.target_store.get_migration_job(job_id)
                    if job and job["status"] in ["completed", "failed"]:
                        break
                    await asyncio.sleep(5)

        except Exception as e:
            logger.error(f"Migration worker error: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info("Stopping migration worker...")
        self.running = False

        if self.source_store:
            await self.source_store.close()

        if self.target_store:
            await self.target_store.close()


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Migration worker')
    parser.add_argument('--job-id', help='Resume existing job')
    args = parser.parse_args()

    worker = MigrationWorker()
    await worker.start(args.job_id)


if __name__ == "__main__":
    asyncio.run(main())
    