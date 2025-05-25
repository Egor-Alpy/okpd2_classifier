from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import List, Dict, Any, Optional
from bson import ObjectId
import logging

logger = logging.getLogger(__name__)


class SourceMongoStore:
    """Работа с исходной MongoDB (только чтение)"""

    def __init__(self, connection_url: str, database_name: str, collection_name: str):
        self.client = AsyncIOMotorClient(connection_url)
        self.db: AsyncIOMotorDatabase = self.client[database_name]
        self.collection_name = collection_name
        self.collection = self.db[collection_name]

    async def get_products_batch(
            self,
            skip: int = 0,
            limit: int = 1000,
            last_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Получить батч товаров из исходной БД

        Args:
            skip: Сколько товаров пропустить
            limit: Размер батча
            last_id: ID последнего обработанного товара (для продолжения)
        """
        try:
            query = {}
            if last_id:
                query = {"_id": {"$gt": ObjectId(last_id)}}

            cursor = self.collection.find(query).limit(limit)

            products = []
            async for product in cursor:
                # Извлекаем только нужные поля
                products.append({
                    "_id": str(product["_id"]),
                    "title": product.get("title", ""),
                    "description": product.get("description", ""),
                    "category": product.get("category", ""),
                    "brand": product.get("brand", ""),
                    "article": product.get("article", "")
                })

            logger.info(f"Fetched {len(products)} products from source DB")
            return products

        except Exception as e:
            logger.error(f"Error fetching products from source DB: {e}")
            raise

    async def count_total_products(self) -> int:
        """Подсчитать общее количество товаров"""
        return await self.collection.count_documents({})

    async def close(self):
        """Закрыть соединение"""
        self.client.close()
