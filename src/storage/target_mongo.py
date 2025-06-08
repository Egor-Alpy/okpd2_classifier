from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId
import logging
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from src.core.config import settings
from src.models.domain import ProductStatus, ProductStatusStage2

logger = logging.getLogger(__name__)


class TargetMongoStore:
    """Работа с целевой MongoDB (наша новая БД)"""

    def __init__(self, database_name: str, collection_name: str = None):
        """
        Инициализация хранилища

        Args:
            database_name: Имя базы данных
            collection_name: Имя коллекции (если не указано, берется из настроек)
        """
        # Используем имя коллекции из параметра или настроек
        if collection_name is None:
            collection_name = settings.target_collection_name

        connection_string = settings.target_mongodb_connection_string
        logger.info(f"Connecting to Target MongoDB...")
        logger.info(f"Database: {database_name}, Collection: {collection_name}")

        # Добавляем параметры подключения для лучшей совместимости
        self.client = AsyncIOMotorClient(
            connection_string,
            directConnection=settings.target_mongo_direct_connection,
            serverSelectionTimeoutMS=10000,  # Увеличиваем таймаут
            connectTimeoutMS=10000,
            socketTimeoutMS=10000,
            # Явно указываем механизм аутентификации если он задан
            authMechanism=settings.target_mongo_authmechanism if settings.target_mongo_user else None
        )
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.products = self.db[collection_name]
        self.migration_jobs = self.db.migration_jobs

    async def initialize(self):
        """Инициализация хранилища"""
        connected = await self.test_connection()
        if not connected:
            raise Exception("Failed to connect to target MongoDB")
        await self._setup_indexes()

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            # Пробуем выполнить простую операцию на нашей БД, а не admin
            await self.db.command('ping')
            logger.info(f"Successfully connected to target MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to target MongoDB: {e}")
            # Дополнительная отладка
            logger.error(
                f"Connection string (masked): mongodb://***:***@{settings.target_mongo_host}:{settings.target_mongo_port}")
            logger.error(f"Database: {self.db.name}")
            logger.error(f"Auth source: {settings.target_mongo_authsource}")
            return False

    async def _setup_indexes(self):
        """Создать необходимые индексы"""
        try:
            # Проверяем существующие индексы
            existing_indexes = await self.products.list_indexes().to_list(length=None)
            existing_names = {idx['name'] for idx in existing_indexes}

            # Создаем только недостающие индексы
            # Уникальный составной индекс
            if 'source_id_1_source_collection_1' not in existing_names:
                await self.products.create_index(
                    [("source_id", 1), ("source_collection", 1)],
                    unique=True
                )

            # Индексы для поиска
            if 'status_stage1_1' not in existing_names:
                await self.products.create_index("status_stage1")
            if 'created_at_1' not in existing_names:
                await self.products.create_index("created_at")
            if 'okpd_groups_1' not in existing_names:
                await self.products.create_index("okpd_groups")
            if 'status_stage2_1' not in existing_names:
                await self.products.create_index("status_stage2")
            if 'source_collection_1' not in existing_names:
                await self.products.create_index("source_collection")

            # Индекс для migration_jobs
            await self.migration_jobs.create_index("job_id", unique=True)

            logger.info("MongoDB indexes created/verified successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes (may already exist): {e}")

    async def insert_products_batch(self, products: List[Dict[str, Any]], collection_name: str) -> int:
        """
        Вставить батч товаров в целевую БД
        Используем существующую схему: source_collection, source_id, title, okpd_groups, status_stage1, created_at
        """
        if not products:
            return 0

        documents = []
        for product in products:
            doc = {
                "source_collection": collection_name,
                "source_id": str(product["_id"]),
                "title": product.get("title", ""),
                "okpd_groups": None,
                "status_stage1": "pending",
                "created_at": datetime.utcnow()
            }
            documents.append(doc)

        # Логируем первый документ для отладки
        if documents:
            logger.debug(f"Sample document to insert: {documents[0]}")

        try:
            result = await self.products.insert_many(documents, ordered=False)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} products to target DB")
            return inserted_count

        except BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            duplicate_count = sum(1 for error in write_errors if error['code'] == 11000)
            inserted_count = e.details.get('nInserted', 0)

            # Логируем пример ошибки дубликата
            if write_errors and duplicate_count > 0:
                sample_error = write_errors[0]
                logger.debug(f"Sample duplicate error: {sample_error}")

            logger.warning(
                f"Batch insert completed with {inserted_count} inserted, "
                f"{duplicate_count} duplicates skipped"
            )
            return inserted_count

        except Exception as e:
            logger.error(f"Error during batch insert: {e}")
            raise

    async def get_pending_products(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Получить товары для классификации"""
        cursor = self.products.find(
            {"status_stage1": "pending"}
        ).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_pending_products_atomic(self, limit: int = 50, worker_id: str = None) -> List[Dict[str, Any]]:
        """Атомарно получить и заблокировать товары для классификации"""
        products = []

        for _ in range(limit):
            doc = await self.products.find_one_and_update(
                {"status_stage1": "pending"},
                {"$set": {"status_stage1": "processing"}},
                return_document=True
            )

            if doc:
                products.append(doc)
            else:
                break

        if products:
            logger.info(f"Locked {len(products)} products for processing")

        return products

    async def get_pending_products_atomic_by_collection(
            self,
            limit: int,
            worker_id: str,
            collection_name: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Атомарно получить и заблокировать товары для классификации с фильтрацией по коллекции"""
        products = []

        # Базовый фильтр
        filter_query = {"status_stage1": "pending"}

        # Добавляем фильтр по коллекции если указана
        if collection_name:
            filter_query["source_collection"] = collection_name
            logger.info(f"Filtering products by collection: {collection_name}")

        for _ in range(limit):
            doc = await self.products.find_one_and_update(
                filter_query,
                {"$set": {"status_stage1": "processing"}},
                return_document=True
            )

            if doc:
                products.append(doc)
            else:
                break

        if products:
            logger.info(f"Locked {len(products)} products from collection '{collection_name}' for processing")

        return products

    async def bulk_update_products(self, updates: List[Dict[str, Any]]):
        """Массовое обновление товаров"""
        if not updates:
            return

        bulk_operations = []
        for update in updates:
            try:
                product_id = update["_id"]

                # Формируем filter
                if isinstance(product_id, ObjectId):
                    filter_query = {"_id": product_id}
                elif isinstance(product_id, str) and ObjectId.is_valid(product_id):
                    filter_query = {"_id": ObjectId(product_id)}
                else:
                    logger.error(f"Invalid product_id: {product_id}")
                    continue

                # Обновляем данные
                update_data = update.get("data", {})

                if update_data:
                    operation = UpdateOne(filter_query, {"$set": update_data})
                    bulk_operations.append(operation)

            except Exception as e:
                logger.error(f"Error preparing bulk operation: {e}")
                continue

        if bulk_operations:
            try:
                result = await self.products.bulk_write(bulk_operations)
                logger.info(f"Bulk update: {result.modified_count} products updated")
            except Exception as e:
                logger.error(f"Bulk update error: {e}")
                raise

    async def get_statistics(self) -> Dict[str, int]:
        """Получить статистику по товарам"""
        total = await self.products.count_documents({})

        # Используем существующие имена полей
        pending = await self.products.count_documents({"status_stage1": "pending"})
        processing = await self.products.count_documents({"status_stage1": "processing"})
        classified = await self.products.count_documents({"status_stage1": "classified"})
        none_classified = await self.products.count_documents({"status_stage1": "none_classified"})
        failed = await self.products.count_documents({"status_stage1": "failed"})

        return {
            "total": total,
            "pending": pending,
            "processing": processing,
            "classified": classified,
            "none_classified": none_classified,
            "failed": failed
        }

    async def create_migration_job(self, job_id: str, total_products: int) -> Dict[str, Any]:
        """Создать задачу миграции"""
        job = {
            "job_id": job_id,
            "status": "running",
            "total_products": total_products,
            "migrated_products": 0,
            "last_processed_id": None,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        await self.migration_jobs.insert_one(job)
        return job

    async def update_migration_job(
            self,
            job_id: str,
            migrated_products: int,
            last_processed_id: Optional[str] = None,
            status: Optional[str] = None
    ):
        """Обновить прогресс миграции"""
        update_data = {
            "migrated_products": migrated_products,
            "updated_at": datetime.utcnow()
        }

        if last_processed_id:
            update_data["last_processed_id"] = last_processed_id

        if status:
            update_data["status"] = status

        await self.migration_jobs.update_one(
            {"job_id": job_id},
            {"$set": update_data}
        )

    async def get_migration_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Получить информацию о задаче миграции"""
        return await self.migration_jobs.find_one({"job_id": job_id})

    async def close(self):
        """Закрыть соединение"""
        self.client.close()