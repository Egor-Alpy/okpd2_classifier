from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId
import logging
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from src.core.config import settings
from src.models.domain import ProductStatus

logger = logging.getLogger(__name__)


class TargetMongoStore:
    """Работа с целевой MongoDB (наша новая БД)"""

    def __init__(self, database_name: str, collection_name: str = "classified_products"):
        # Используем connection string из settings
        connection_string = settings.target_mongodb_connection_string

        logger.info(f"Connecting to Target MongoDB...")
        logger.debug(f"Database: {database_name}")
        logger.debug(f"Collection: {collection_name}")

        # Маскируем пароль в логах
        masked_cs = connection_string
        if '@' in connection_string:
            # Находим часть с учетными данными
            parts = connection_string.split('@')
            if len(parts) >= 2 and '://' in parts[0]:
                creds_part = parts[0].split('://')[-1]
                if ':' in creds_part:
                    user_part = creds_part.split(':')[0]
                    masked_cs = connection_string.replace(creds_part, f"{user_part}:***")

        logger.info(f"Connection string: {masked_cs}")

        # Создаем клиент с дополнительными параметрами
        self.client = AsyncIOMotorClient(
            connection_string,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
            maxPoolSize=100,
            minPoolSize=10,
            # Важно для записи - настройки write concern
            w=1,  # Подтверждение записи от primary
            journal=True  # Запись в журнал
        )

        self.db: AsyncIOMotorDatabase = self.client[database_name]
        # Используем настраиваемое имя коллекции
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
            # Пробуем выполнить команду ping
            await self.client.admin.command('ping')
            logger.info(f"Successfully connected to target MongoDB")

            # Проверяем доступ к базе данных
            collections = await self.db.list_collection_names()
            logger.info(f"Successfully accessed database, found {len(collections)} collections")

            # Проверяем права на запись
            test_doc = {"_id": "test_connection", "timestamp": datetime.utcnow()}
            await self.db.test_collection.replace_one(
                {"_id": "test_connection"},
                test_doc,
                upsert=True
            )
            await self.db.test_collection.delete_one({"_id": "test_connection"})
            logger.info("Write permissions verified")

            return True
        except Exception as e:
            logger.error(f"Failed to connect to target MongoDB: {e}")
            logger.error("Check your connection parameters:")
            logger.error(f"- Host: {settings.target_mongo_host}")
            logger.error(f"- Port: {settings.target_mongo_port}")
            logger.error(f"- Database: {settings.target_mongodb_database}")
            logger.error(f"- User: {settings.target_mongo_user or 'Not set'}")
            logger.error(f"- Auth Source: {settings.target_mongo_authsource or 'Not set'}")
            logger.error(f"- Auth Mechanism: {settings.target_mongo_authmechanism}")
            logger.error(f"- Direct Connection: {settings.target_mongo_direct_connection}")

            # Дополнительная информация об ошибке аутентификации
            if "authentication" in str(e).lower() or "unauthorized" in str(e).lower():
                logger.error("Authentication failed! Please check:")
                logger.error("1. Username and password are correct")
                logger.error("2. Auth source database is correct (usually 'admin')")
                logger.error("3. User has proper permissions on the target database")

            return False

    async def _setup_indexes(self):
        """Создать необходимые индексы"""
        try:
            # Уникальный составной индекс
            await self.products.create_index(
                [("source_id", 1), ("source_collection", 1)],
                unique=True,
                background=True
            )

            # Индексы для поиска
            await self.products.create_index("status_stage1", background=True)
            await self.products.create_index("status_stage2", background=True)
            await self.products.create_index("created_at", background=True)
            await self.products.create_index("okpd_groups", background=True)
            await self.products.create_index("source_collection", background=True)
            await self.products.create_index("worker_id", background=True)

            # Составной индекс для эффективного поиска pending товаров
            await self.products.create_index(
                [("status_stage1", 1), ("created_at", 1)],
                background=True
            )

            # Составной индекс для второго этапа
            await self.products.create_index(
                [("status_stage1", 1), ("status_stage2", 1)],
                background=True
            )

            # Индекс для migration_jobs
            await self.migration_jobs.create_index("job_id", unique=True, background=True)

            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            # Если ошибка аутентификации - это критично
            if "authentication" in str(e).lower() or "unauthorized" in str(e).lower():
                logger.error(f"Authentication error when creating indexes: {e}")
                raise
            else:
                # Другие ошибки (например, индексы уже существуют) - это некритично
                logger.warning(f"Error creating indexes (may already exist): {e}")

    async def insert_products_batch(self, products: List[Dict[str, Any]], collection_name: str) -> int:
        """
        Вставить батч товаров в целевую БД
        """
        if not products:
            return 0

        documents = []
        for product in products:
            doc = {
                "title": product["title"],
                "created_at": datetime.utcnow(),
                "source_collection": collection_name,
                "source_id": product["_id"],
                "status_stage1": ProductStatus.PENDING.value,
                # okpd_groups, okpd2_code, okpd2_name будут добавлены при классификации
            }
            documents.append(doc)

        try:
            result = await self.products.insert_many(documents, ordered=False)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} products to target DB")
            return inserted_count

        except BulkWriteError as e:
            write_errors = e.details.get('writeErrors', [])
            duplicate_count = sum(1 for error in write_errors if error['code'] == 11000)
            inserted_count = e.details.get('nInserted', 0)

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
            {"status_stage1": ProductStatus.PENDING.value}
        ).limit(limit)
        return await cursor.to_list(length=limit)

    async def get_pending_products_atomic(self, limit: int = 50, worker_id: str = None) -> List[Dict[str, Any]]:
        """Атомарно получить и заблокировать товары для классификации"""
        products = []

        for _ in range(limit):
            doc = await self.products.find_one_and_update(
                {"status_stage1": ProductStatus.PENDING.value},
                {
                    "$set": {
                        "status_stage1": ProductStatus.PROCESSING.value,
                        "worker_id": worker_id,
                        "processing_started_at": datetime.utcnow()
                    }
                },
                return_document=True
            )

            if doc:
                products.append(doc)
            else:
                break

        if products:
            logger.info(f"Locked {len(products)} products for processing by {worker_id}")

        return products

    async def bulk_update_products(self, updates: List[Dict[str, Any]]):
        """Массовое обновление товаров"""
        if not updates:
            return

        bulk_operations = []
        current_time = datetime.utcnow()

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

                # Обновляем поля
                update_data = {"updated_at": current_time}
                data = update.get("data", {})

                # Поля первого этапа
                if "status_stage1" in data:
                    update_data["status_stage1"] = data["status_stage1"]

                if "okpd_group" in data:  # Переименовываем в okpd_groups
                    update_data["okpd_groups"] = data["okpd_group"]

                # Поля второго этапа
                if "status_stage2" in data:
                    update_data["status_stage2"] = data["status_stage2"]

                if "okpd2_code" in data:
                    update_data["okpd2_code"] = data["okpd2_code"]

                if "okpd2_name" in data:
                    update_data["okpd2_name"] = data["okpd2_name"]

                if "worker_id" in data:
                    update_data["worker_id"] = data["worker_id"]

                # Если товар классифицирован на любом этапе - обновляем processed_at
                if (data.get("status_stage1") == ProductStatus.CLASSIFIED.value or
                        data.get("status_stage2") == ProductStatus.CLASSIFIED.value):
                    update_data["processed_at"] = current_time

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
        pending = await self.products.count_documents({"status_stage1": ProductStatus.PENDING.value})
        processing = await self.products.count_documents({"status_stage1": ProductStatus.PROCESSING.value})
        classified = await self.products.count_documents({"status_stage1": ProductStatus.CLASSIFIED.value})
        none_classified = await self.products.count_documents({"status_stage1": ProductStatus.NONE_CLASSIFIED.value})
        failed = await self.products.count_documents({"status_stage1": ProductStatus.FAILED.value})

        return {
            "total": total,
            "pending": pending,
            "processing": processing,
            "classified": classified,
            "none_classified": none_classified,
            "failed": failed
        }

    async def get_statistics_by_source_collection(self) -> Dict[str, Dict[str, int]]:
        """Получить статистику по исходным коллекциям"""
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "source_collection": "$source_collection",
                        "status": "$status_stage1"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.source_collection",
                    "statuses": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    },
                    "total": {"$sum": "$count"}
                }
            }
        ]

        cursor = self.products.aggregate(pipeline)
        results = await cursor.to_list(length=None)

        stats = {}
        for result in results:
            collection_name = result["_id"]
            stats[collection_name] = {"total": result["total"]}

            for status_info in result["statuses"]:
                stats[collection_name][status_info["status"]] = status_info["count"]

        return stats

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