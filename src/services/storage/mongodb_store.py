from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase


class MongoDBStore:
    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db
        self.products = db.products
        self.stage_one_results = db.classification_stage_one
        self.final_results = db.classification_final
        self.jobs = db.classification_jobs
        self.okpd2_ref = db.okpd2_reference

    async def get_unprocessed_products(self, limit: int, skip: int = 0) -> List[Dict[str, Any]]:
        """Получить необработанные товары"""
        # Находим товары, которых нет в stage_one_results
        processed_ids = await self.stage_one_results.distinct("product_id")

        cursor = self.products.find(
            {"_id": {"$nin": processed_ids}},
            limit=limit,
            skip=skip
        )
        return await cursor.to_list(length=limit)

    async def save_stage_one_results(self, results: List[Dict[str, Any]]):
        """Сохранить результаты первого этапа"""
        if results:
            await self.stage_one_results.insert_many(results)

    async def get_products_for_stage_two(self, main_class: str, limit: int) -> List[Dict[str, Any]]:
        """Получить товары для второго этапа по классу"""
        # Находим товары с этим классом, но без финального результата
        pipeline = [
            {"$match": {"main_classes": main_class, "status": "completed"}},
            {"$lookup": {
                "from": "classification_final",
                "localField": "product_id",
                "foreignField": "product_id",
                "as": "final_result"
            }},
            {"$match": {"final_result": {"$size": 0}}},
            {"$limit": limit}
        ]

        cursor = self.stage_one_results.aggregate(pipeline)
        return await cursor.to_list(length=limit)

    async def save_final_results(self, results: List[Dict[str, Any]]):
        """Сохранить финальные результаты"""
        if results:
            await self.final_results.insert_many(results)

    async def get_okpd2_structure(self, class_code: str) -> Dict[str, Any]:
        """Получить структуру ОКПД2 для класса"""
        cursor = self.okpd2_ref.find({
            "code": {"$regex": f"^{class_code}"}
        })
        return await cursor.to_list(length=None)

    async def update_job_progress(self, job_id: str, updates: Dict[str, Any]):
        """Обновить прогресс задачи"""
        updates["updated_at"] = datetime.utcnow()
        await self.jobs.update_one(
            {"job_id": job_id},
            {"$set": updates}
        )