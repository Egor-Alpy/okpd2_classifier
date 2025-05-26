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

    def __init__(self, database_name: str):
        # Используем настройки из конфига
        self.client = AsyncIOMotorClient(
            settings.target_mongodb_connection_string,
            directConnection=settings.target_mongo_direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.products = self.db.products_stage_one
        self.migration_jobs = self.db.migration_jobs
        self.batches = self.db.classification_batches

    async def initialize(self):
        """Инициализация хранилища"""
        # Проверяем подключение
        connected = await self.test_connection()
        if not connected:
            raise Exception("Failed to connect to target MongoDB")

        # Создаем индексы
        await self._setup_indexes()

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            # Пробуем выполнить простую команду
            await self.client.admin.command('ping')
            logger.info(f"Successfully connected to target MongoDB: {settings.target_mongodb_connection_string}")

            return True
        except Exception as e:
            logger.error(f"Failed to connect to target MongoDB: {e}")
            return False

    async def _setup_indexes(self):
        """Создать необходимые индексы"""
        try:
            # Индексы для products_stage_one
            await self.products.create_index([("old_mongo_id", 1), ("collection_name", 1)], unique=True)
            await self.products.create_index("status_stg1")
            await self.products.create_index("created_at")
            await self.products.create_index("okpd_group")
            await self.products.create_index([("status_stg1", 1), ("created_at", 1)])
            await self.products.create_index("updated_at")  # Новый индекс для метрик
            await self.products.create_index("worker_id")  # Новый индекс для воркеров
            await self.products.create_index("processing_started_at")  # Для поиска застрявших

            # Индексы для других коллекций
            await self.migration_jobs.create_index("job_id", unique=True)
            await self.batches.create_index("batch_id", unique=True)

            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Error creating indexes (may already exist): {e}")

    async def insert_products_batch(self, products: List[Dict[str, Any]], collection_name: str) -> int:
        """
        Вставить батч товаров в целевую БД с улучшенной обработкой дубликатов

        Returns:
            Количество вставленных товаров
        """
        if not products:
            return 0

        # Подготавливаем документы для вставки
        documents = []
        for product in products:
            doc = {
                "collection_name": collection_name,
                "old_mongo_id": product["_id"],
                "title": product["title"],
                "okpd_group": None,
                "status_stg1": ProductStatus.PENDING.value,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "error_message": None,
                "batch_id": None,
                "worker_id": None,
                "processing_started_at": None
            }
            documents.append(doc)

        try:
            # Используем ordered=False для продолжения при дубликатах
            result = await self.products.insert_many(documents, ordered=False)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} products to target DB")
            return inserted_count
        except BulkWriteError as e:
            # Более точная обработка BulkWriteError
            write_errors = e.details.get('writeErrors', [])
            duplicate_count = sum(1 for error in write_errors if error['code'] == 11000)
            inserted_count = e.details.get('nInserted', 0)

            logger.warning(
                f"Batch insert completed with {inserted_count} inserted, "
                f"{duplicate_count} duplicates skipped"
            )
            return inserted_count
        except Exception as e:
            logger.error(f"Unexpected error during batch insert: {e}")
            raise

    async def get_pending_products(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Получить товары для классификации"""
        cursor = self.products.find(
            {"status_stg1": ProductStatus.PENDING.value}
        ).limit(limit)

        return await cursor.to_list(length=limit)

    async def get_pending_products_atomic(self, limit: int = 50, worker_id: str = None) -> List[Dict[str, Any]]:
        """Атомарно получить и заблокировать товары для классификации"""
        products = []

        # Используем find_one_and_update для атомарного обновления
        for _ in range(limit):
            doc = await self.products.find_one_and_update(
                {
                    "status_stg1": ProductStatus.PENDING.value
                },
                {
                    "$set": {
                        "status_stg1": ProductStatus.PROCESSING.value,
                        "processing_started_at": datetime.utcnow(),
                        "worker_id": worker_id
                    }
                },
                return_document=True
            )

            if doc:
                products.append(doc)
            else:
                break  # Нет больше pending товаров

        if products:
            logger.info(f"Worker {worker_id} locked {len(products)} products for processing")

        return products

    async def update_product_status(
            self,
            product_id: str,
            status: ProductStatus,
            okpd_groups: Optional[List[str]] = None,
            error_message: Optional[str] = None,
            batch_id: Optional[str] = None
    ):
        """Обновить статус товара после классификации"""
        update_data = {
            "status_stg1": status.value,
            "updated_at": datetime.utcnow()
        }

        if okpd_groups is not None:
            update_data["okpd_group"] = okpd_groups

        if error_message:
            update_data["error_message"] = error_message

        if batch_id:
            update_data["batch_id"] = batch_id

        # ВАЖНО: Обнуляем processing_started_at при завершении обработки
        if status.value != ProductStatus.PROCESSING.value:
            update_data["processing_started_at"] = None

        await self.products.update_one(
            {"_id": ObjectId(product_id)},
            {"$set": update_data}
        )

    async def bulk_update_products(self, updates: List[Dict[str, Any]]):
        """Массовое обновление товаров"""
        if not updates:
            return

        bulk_operations = []
        for update in updates:
            try:
                # ИСПРАВЛЕНИЕ: Проверяем тип _id и преобразуем при необходимости
                product_id = update["_id"]

                # Если это уже ObjectId, используем как есть
                if isinstance(product_id, ObjectId):
                    filter_query = {"_id": product_id}
                # Если это строка, преобразуем в ObjectId
                elif isinstance(product_id, str):
                    # Проверяем, является ли строка валидным ObjectId
                    if ObjectId.is_valid(product_id):
                        filter_query = {"_id": ObjectId(product_id)}
                    else:
                        logger.error(f"Invalid ObjectId string: {product_id}")
                        continue
                # Если это dict (документ из MongoDB), берем _id
                elif isinstance(product_id, dict) and "_id" in product_id:
                    filter_query = {"_id": product_id["_id"]}
                else:
                    logger.error(f"Invalid product_id type: {type(product_id)}, value: {product_id}")
                    continue

                # ВАЖНО: Обнуляем processing_started_at при завершении обработки
                update_data = update["data"].copy()
                if update_data.get("status_stg1") != ProductStatus.PROCESSING.value:
                    update_data["processing_started_at"] = None

                operation = UpdateOne(
                    filter_query,
                    {"$set": update_data}
                )
                bulk_operations.append(operation)

            except Exception as e:
                logger.error(f"Error preparing bulk operation for {update.get('_id')}: {e}")
                continue

        if bulk_operations:
            try:
                # Отправляем список операций UpdateOne
                result = await self.products.bulk_write(bulk_operations)
                logger.info(
                    f"Bulk update successful: {result.modified_count} products updated out of {len(bulk_operations)}")

                # Проверяем, были ли не обновленные документы
                if result.modified_count < len(bulk_operations):
                    logger.warning(f"Not all documents were updated: {result.modified_count}/{len(bulk_operations)}")

            except Exception as e:
                logger.error(f"Bulk update error: {e}")
                logger.error(f"Number of operations: {len(bulk_operations)}")

                # Fallback на индивидуальные обновления
                logger.info("Falling back to individual updates...")
                success_count = 0
                for update in updates:
                    try:
                        product_id = update["_id"]
                        if isinstance(product_id, str) and ObjectId.is_valid(product_id):
                            await self.products.update_one(
                                {"_id": ObjectId(product_id)},
                                {"$set": update["data"]}
                            )
                            success_count += 1
                        elif isinstance(product_id, ObjectId):
                            await self.products.update_one(
                                {"_id": product_id},
                                {"$set": update["data"]}
                            )
                            success_count += 1
                    except Exception as ind_e:
                        logger.error(f"Individual update error for {update.get('_id')}: {ind_e}")

                logger.info(f"Individual updates completed: {success_count}/{len(updates)} successful")

    async def get_statistics(self) -> Dict[str, int]:
        """Получить статистику по товарам"""
        total = await self.products.count_documents({})
        pending = await self.products.count_documents({"status_stg1": ProductStatus.PENDING.value})
        classified = await self.products.count_documents({"status_stg1": ProductStatus.CLASSIFIED.value})
        none_classified = await self.products.count_documents({"status_stg1": ProductStatus.NONE_CLASSIFIED.value})
        failed = await self.products.count_documents({"status_stg1": ProductStatus.FAILED.value})
        processing = await self.products.count_documents({"status_stg1": ProductStatus.PROCESSING.value})

        return {
            "total": total,
            "pending": pending,
            "classified": classified,
            "none_classified": none_classified,
            "failed": failed,
            "processing": processing
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