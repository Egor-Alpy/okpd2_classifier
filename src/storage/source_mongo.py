from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from bson import ObjectId
import logging
from src.core.config import settings

logger = logging.getLogger(__name__)


class SourceMongoStore:
    """Работа с исходной MongoDB (только чтение)"""

    def __init__(self, database_name: str, collection_name: str = None):
        # Используем connection string из settings
        connection_string = settings.source_mongodb_connection_string

        logger.info(f"Connecting to Source MongoDB...")
        logger.debug(f"Database: {database_name}")
        logger.debug(f"Collection: {collection_name or 'ALL'}")

        # Создаем клиент с дополнительными параметрами
        self.client = AsyncIOMotorClient(
            connection_string,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
            maxPoolSize=100,
            minPoolSize=10
        )

        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.collection_name = collection_name

        # Если указана конкретная коллекция
        if collection_name:
            self.collection = self.db[collection_name]
        else:
            self.collection = None

    async def get_collections_list(self) -> List[str]:
        """Получить список коллекций с товарами"""
        # Исключаем системные коллекции
        exclude_collections = ['admin', 'config', 'local', 'reference_data', 'system.indexes']

        try:
            all_collections = await self.db.list_collection_names()

            # Фильтруем только коллекции с товарами
            product_collections = [
                coll for coll in all_collections
                if coll not in exclude_collections
                   and not coll.startswith('system.')
            ]

            logger.info(f"Found {len(product_collections)} product collections: {product_collections}")
            return product_collections

        except Exception as e:
            logger.error(f"Error getting collections list: {e}")
            raise

    async def get_products_batch(
            self,
            skip: int = 0,
            limit: int = 1000,
            last_id: Optional[str] = None,
            collection_name: str = None
    ) -> List[Dict[str, Any]]:
        """
        Получить батч товаров из конкретной коллекции
        Извлекаем ТОЛЬКО _id и title
        """
        try:
            # Используем переданную коллекцию или текущую
            if collection_name:
                collection = self.db[collection_name]
            else:
                collection = self.collection

            if collection is None:
                raise ValueError("No collection specified")

            query = {}
            if last_id:
                query = {"_id": {"$gt": ObjectId(last_id)}}

            # Получаем ТОЛЬКО нужные поля
            cursor = collection.find(
                query,
                {"_id": 1, "title": 1}  # Проекция - только эти поля
            ).limit(limit)

            products = []
            async for product in cursor:
                products.append({
                    "_id": str(product["_id"]),
                    "title": product.get("title", "")
                })

            logger.info(f"Fetched {len(products)} products from collection {collection_name or self.collection_name}")
            return products

        except Exception as e:
            logger.error(f"Error fetching products from source DB: {e}")
            raise

    async def count_total_products(self, collection_name: str = None) -> int:
        """Подсчитать количество товаров в коллекции"""
        if collection_name:
            collection = self.db[collection_name]
        else:
            collection = self.collection

        if collection is None:
            raise ValueError("No collection specified")

        return await collection.count_documents({})

    async def count_all_products(self) -> Dict[str, int]:
        """Подсчитать количество товаров во всех коллекциях"""
        collections = await self.get_collections_list()
        counts = {}

        for coll_name in collections:
            count = await self.count_total_products(coll_name)
            counts[coll_name] = count

        total = sum(counts.values())
        logger.info(f"Total products across all collections: {total}")

        return counts

    async def test_connection(self) -> bool:
        """Проверить подключение к БД"""
        try:
            # Пробуем выполнить команду ping
            await self.client.admin.command('ping')
            logger.info("Successfully connected to source MongoDB")

            # Проверяем доступ к базе данных
            collections = await self.db.list_collection_names()
            logger.info(f"Successfully accessed database, found {len(collections)} collections")

            return True
        except Exception as e:
            logger.error(f"Failed to connect to source MongoDB: {e}")
            logger.error("Check your connection parameters:")
            logger.error(f"- Host: {settings.source_mongo_host}")
            logger.error(f"- Port: {settings.source_mongo_port}")
            logger.error(f"- Database: {settings.source_mongodb_database}")
            logger.error(f"- Auth Source: {settings.source_mongo_authsource}")
            logger.error(f"- Direct Connection: {settings.source_mongo_direct_connection}")
            return False

    async def close(self):
        """Закрыть соединение"""
        self.client.close()