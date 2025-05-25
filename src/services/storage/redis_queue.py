import json
from typing import List, Optional, Dict, Any
from redis.asyncio import Redis
import time


class RedisQueue:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def add_to_stage_one_queue(self, product_ids: List[str], priority: int = 0):
        """Добавить товары в очередь первого этапа"""
        timestamp = time.time() - priority  # Меньший timestamp = выше приоритет

        pipe = self.redis.pipeline()
        for product_id in product_ids:
            pipe.zadd("queue:stage_one:pending", {product_id: timestamp})
        await pipe.execute()

    async def get_from_stage_one_queue(self, count: int) -> List[str]:
        """Получить товары из очереди первого этапа"""
        # Получаем и удаляем элементы атомарно
        product_ids = await self.redis.zpopmin("queue:stage_one:pending", count)
        return [pid for pid, _ in product_ids]

    async def add_to_stage_two_queue(self, class_code: str, products: List[Dict[str, Any]]):
        """Добавить товары в очередь второго этапа"""
        queue_key = f"queue:stage_two:{class_code}:pending"
        timestamp = time.time()

        pipe = self.redis.pipeline()
        for product in products:
            pipe.zadd(queue_key, {json.dumps(product): timestamp})
        await pipe.execute()

    async def get_from_stage_two_queue(self, class_code: str, count: int) -> List[Dict[str, Any]]:
        """Получить товары из очереди второго этапа"""
        queue_key = f"queue:stage_two:{class_code}:pending"
        items = await self.redis.zpopmin(queue_key, count)

        return [json.loads(item) for item, _ in items]

    async def acquire_lock(self, key: str, ttl: int = 300) -> bool:
        """Получить блокировку"""
        return await self.redis.set(f"lock:{key}", "1", nx=True, ex=ttl)

    async def release_lock(self, key: str):
        """Освободить блокировку"""
        await self.redis.delete(f"lock:{key}")

    async def increment_stats(self, stat_key: str, amount: int = 1):
        """Увеличить счетчик статистики"""
        await self.redis.incrby(f"stats:{stat_key}", amount)

    async def get_stats(self) -> Dict[str, int]:
        """Получить статистику"""
        keys = await self.redis.keys("stats:*")
        if not keys:
            return {}

        values = await self.redis.mget(keys)
        return {
            key.decode().replace("stats:", ""): int(val or 0)
            for key, val in zip(keys, values)
        }