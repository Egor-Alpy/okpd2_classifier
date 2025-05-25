from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from datetime import datetime
from bson import ObjectId
import logging

from models.domain import ProductStageOne, ProductStatus

logger = logging.getLogger(__name__)


class TargetMongoStore:
    """Работа с целевой MongoDB (наша новая БД)"""

    def __init__(self, connection_url: str, database_name: str):
        self.client = AsyncIOMotorClient(connection_url)
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.products = self.db.products_stage_one
        self.migration_jobs = self.db.migration_jobs
        self.batches = self.db.classification_batches

        # Создаем индексы при инициализации
        self._setup_indexes()

    def _setup_indexes(self):
        """Создать необходимые индексы"""
        # Индексы для products_stage_one
        self.products.create_index([("old_mongo_id", 1), ("collection_name", 1)], unique=True)
        self.products.create_index("status_stg1")
        self.products.create_index("created_at")
        self.products.create_index("okpd_group")

        # Индексы для других коллекций
        self.migration_jobs.create_index("job_id", unique=True)
        self.batches.create_index("batch_id", unique=True)

    async def insert_products_batch(self, products: List[Dict[str, Any]], collection_name: str) -> int:
        """
        Вставить батч товаров в целевую БД

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
                "updated_at": None,
                "error_message": None,
                "batch_id": None
            }
            documents.append(doc)

        try:
            # Используем ordered=False для продолжения при дубликатах
            result = await self.products.insert_many(documents, ordered=False)
            inserted_count = len(result.inserted_ids)
            logger.info(f"Inserted {inserted_count} products to target DB")
            return inserted_count
        except Exception as e:
            # При дубликатах pymongo выбросит BulkWriteError
            if "duplicate key error" in str(e).lower():
                logger.warning(f"Some products already exist, continuing...")
                # Подсчитываем сколько реально вставилось
                return len([d for d in documents if d not in e.details.get('writeErrors', [])])
            raise

    async def get_pending_products(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Получить товары для классификации"""
        cursor = self.products.find(
            {"status_stg1": ProductStatus.PENDING.value}
        ).limit(limit)

        return await cursor.to_list(length=limit)

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
            bulk_operations.append(
                {
                    "updateOne": {
                        "filter": {"_id": ObjectId(update["_id"])},
                        "update": {"$set": update["data"]}
                    }
                }
            )

        await self.products.bulk_write(bulk_operations)

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
            "updated_at": None
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