import asyncio
import os
from datetime import datetime
from typing import Dict, List, Optional
import aioredis
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError
import logging
import signal
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MigrationWorker:
    def __init__(self):
        # Source MongoDB (не трогаем)
        self.source_client = AsyncIOMotorClient(
            os.getenv('SOURCE_MONGO_URI', 'mongodb://mongodb.angora-ide.ts.net:27017/')
        )
        self.source_db = self.source_client[os.getenv('SOURCE_MONGO_DB', 'TenderDB')]

        # Target MongoDB (наша новая база)
        self.target_client = AsyncIOMotorClient(
            os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        )
        self.target_db = self.target_client[os.getenv('MONGO_DB', 'okpd2_classifier')]
        self.target_collection = self.target_db['products_stage_one']

        # Redis
        self.redis = None
        self.redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')

        # Параметры миграции
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '1000'))
        self.running = True

    async def connect_redis(self):
        """Подключение к Redis"""
        try:
            self.redis = await aioredis.from_url(self.redis_url)
            await self.redis.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def get_migration_task(self) -> Optional[Dict]:
        """Получить активную задачу миграции"""
        try:
            # Получаем все задачи миграции
            cursor = self.target_db['migration_tasks'].find({
                'status': {'$in': ['pending', 'running']}
            }).sort('created_at', 1)

            async for task in cursor:
                return task

            return None
        except Exception as e:
            logger.error(f"Error getting migration task: {e}")
            return None

    async def update_migration_progress(self, task_id: str, processed: int, total: int, status: str = 'running'):
        """Обновить прогресс миграции"""
        try:
            update_data = {
                'processed': processed,
                'total': total,
                'status': status,
                'updated_at': datetime.utcnow()
            }

            if status == 'completed':
                update_data['completed_at'] = datetime.utcnow()

            await self.target_db['migration_tasks'].update_one(
                {'_id': task_id},
                {'$set': update_data}
            )

            # Обновляем прогресс в Redis для API
            if self.redis:
                await self.redis.setex(
                    f"migration:progress:{task_id}",
                    300,  # TTL 5 минут
                    f"{processed}/{total}"
                )
        except Exception as e:
            logger.error(f"Error updating migration progress: {e}")

    async def check_existing_products(self, source_ids: List[str]) -> set:
        """Проверить какие товары уже есть в целевой базе"""
        try:
            # Находим существующие товары по old_mongo_id
            existing = await self.target_collection.find(
                {'old_mongo_id': {'$in': source_ids}},
                {'old_mongo_id': 1}
            ).to_list(None)

            return {str(doc['old_mongo_id']) for doc in existing}
        except Exception as e:
            logger.error(f"Error checking existing products: {e}")
            return set()

    async def migrate_batch(self, source_collection_name: str, products: List[Dict]) -> Dict:
        """Мигрировать батч товаров"""
        if not products:
            return {'inserted': 0, 'skipped': 0}

        try:
            # Получаем ID товаров из source
            source_ids = [str(p['_id']) for p in products]

            # Проверяем какие уже есть
            existing_ids = await self.check_existing_products(source_ids)

            # Подготавливаем документы для вставки
            documents_to_insert = []
            skipped_count = 0

            for product in products:
                product_id = str(product['_id'])

                # Пропускаем если уже есть
                if product_id in existing_ids:
                    skipped_count += 1
                    continue

                # Создаем документ для вставки
                doc = {
                    'source_collection': source_collection_name,
                    'old_mongo_id': product_id,
                    'title': product.get('title', ''),
                    'okpd_group': None,  # Будет заполнено после классификации
                    'status_stg1': 'pending',
                    'created_at': datetime.utcnow(),
                    'raw_data': {
                        'description': product.get('description', ''),
                        'category': product.get('category', ''),
                        'brand': product.get('brand', ''),
                        'attributes': product.get('attributes', [])
                    }
                }

                documents_to_insert.append(doc)

            # Вставляем новые документы
            inserted_count = 0
            if documents_to_insert:
                try:
                    result = await self.target_collection.insert_many(
                        documents_to_insert,
                        ordered=False
                    )
                    inserted_count = len(result.inserted_ids)
                except BulkWriteError as bwe:
                    # При bulk write некоторые документы могут быть вставлены
                    inserted_count = bwe.details.get('nInserted', 0)
                    logger.warning(f"Bulk write error: {bwe}")

            logger.info(f"Batch processed: {inserted_count} inserted, {skipped_count} skipped")

            return {
                'inserted': inserted_count,
                'skipped': skipped_count
            }

        except Exception as e:
            logger.error(f"Error migrating batch: {e}")
            return {'inserted': 0, 'skipped': 0}

    async def run_migration(self, task: Dict):
        """Выполнить миграцию для задачи"""
        task_id = str(task['_id'])
        source_collection_name = task['source_collection']

        logger.info(f"Starting migration task {task_id} for collection {source_collection_name}")

        try:
            # Обновляем статус на running
            await self.target_db['migration_tasks'].update_one(
                {'_id': task['_id']},
                {'$set': {'status': 'running', 'started_at': datetime.utcnow()}}
            )

            # Получаем source collection
            source_collection = self.source_db[source_collection_name]

            # Считаем общее количество
            total_count = await source_collection.count_documents({})
            logger.info(f"Total products to migrate: {total_count}")

            # Мигрируем батчами
            processed = 0
            total_inserted = 0
            total_skipped = 0

            while processed < total_count and self.running:
                # Получаем батч
                cursor = source_collection.find({}).skip(processed).limit(self.batch_size)
                batch = await cursor.to_list(self.batch_size)

                if not batch:
                    break

                # Мигрируем батч
                result = await self.migrate_batch(source_collection_name, batch)
                total_inserted += result['inserted']
                total_skipped += result['skipped']

                processed += len(batch)

                # Обновляем прогресс
                await self.update_migration_progress(task_id, processed, total_count)

                # Небольшая задержка чтобы не перегружать базу
                await asyncio.sleep(0.1)

            # Завершаем миграцию
            final_status = 'completed' if processed >= total_count else 'stopped'
            await self.update_migration_progress(task_id, processed, total_count, final_status)

            logger.info(
                f"Migration completed: {total_inserted} inserted, "
                f"{total_skipped} skipped, {processed}/{total_count} processed"
            )

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            await self.target_db['migration_tasks'].update_one(
                {'_id': task['_id']},
                {'$set': {
                    'status': 'failed',
                    'error': str(e),
                    'updated_at': datetime.utcnow()
                }}
            )

    async def start(self):
        """Запустить worker"""
        logger.info("Migration worker starting...")

        # Подключаемся к Redis
        await self.connect_redis()

        # Создаем индексы
        await self.create_indexes()

        while self.running:
            try:
                # Получаем активную задачу миграции
                task = await self.get_migration_task()

                if task:
                    await self.run_migration(task)
                else:
                    # Нет задач, ждем
                    logger.debug("No migration tasks, waiting...")
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(10)

        logger.info("Migration worker stopped")

    async def create_indexes(self):
        """Создать индексы для оптимизации"""
        try:
            # Индекс для проверки дубликатов
            await self.target_collection.create_index(
                [('old_mongo_id', 1)],
                unique=True,
                background=True
            )

            # Индекс для поиска по статусу
            await self.target_collection.create_index(
                [('status_stg1', 1)],
                background=True
            )

            # Индекс для поиска по группе ОКПД
            await self.target_collection.create_index(
                [('okpd_group', 1)],
                background=True
            )

            # Составной индекс для эффективной выборки
            await self.target_collection.create_index(
                [('okpd_group', 1), ('status_stg1', 1)],
                background=True
            )

            logger.info("Indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    def stop(self):
        """Остановить worker"""
        logger.info("Stopping migration worker...")
        self.running = False

    async def cleanup(self):
        """Очистить ресурсы"""
        if self.redis:
            await self.redis.close()
        self.source_client.close()
        self.target_client.close()


# Обработчики сигналов
worker = None


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    if worker:
        worker.stop()
    sys.exit(0)


async def main():
    global worker

    # Устанавливаем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Создаем и запускаем worker
    worker = MigrationWorker()

    try:
        await worker.start()
    finally:
        await worker.cleanup()


if __name__ == "__main__":
    asyncio.run(main())