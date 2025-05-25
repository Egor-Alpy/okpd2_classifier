from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from typing import Optional

from core.config import settings  # Убрать src. из импорта


class Database:
    client: Optional[AsyncIOMotorClient] = None
    redis: Optional[Redis] = None


db = Database()


async def connect_to_mongo():
    # Здесь нужно использовать target_mongodb_url, а не mongodb_url
    db.client = AsyncIOMotorClient(settings.target_mongodb_url)


async def connect_to_redis():
    db.redis = await Redis.from_url(settings.redis_url)


async def close_mongo_connection():
    if db.client:
        db.client.close()


async def close_redis_connection():
    if db.redis:
        await db.redis.close()


def get_database():
    return db.client[settings.target_mongodb_database]