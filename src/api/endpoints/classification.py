from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
import uuid

from src.api.dependencies import get_target_store, verify_api_key
from src.storage.source_mongo import SourceMongoStore
from src.services.product_migrator import ProductMigrator
from src.core.config import settings

router = APIRouter()


@router.post("/migration/start")
async def start_migration(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Начать миграцию товаров из source в target MongoDB"""
    try:
        # Создаем source store
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        # Проверяем подключение
        if not await source_store.test_connection():
            raise HTTPException(status_code=500, detail="Cannot connect to source MongoDB")

        # Создаем migrator
        migrator = ProductMigrator(
            source_store,
            target_store,
            settings.migration_batch_size
        )

        # Запускаем миграцию
        job_id = await migrator.start_migration()

        return {
            "job_id": job_id,
            "status": "started",
            "message": "Migration started successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/migration/{job_id}")
async def get_migration_status(
        job_id: str,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статус миграции"""
    job = await target_store.get_migration_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Migration job not found")

    # Добавляем процент выполнения
    progress_percentage = 0
    if job["total_products"] > 0:
        progress_percentage = round(job["migrated_products"] / job["total_products"] * 100, 2)

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "total_products": job["total_products"],
        "migrated_products": job["migrated_products"],
        "progress_percentage": progress_percentage,
        "last_processed_id": job.get("last_processed_id"),
        "created_at": job["created_at"]
    }


@router.post("/migration/{job_id}/resume")
async def resume_migration(
        job_id: str,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Возобновить прерванную миграцию"""
    job = await target_store.get_migration_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Migration job not found")

    if job["status"] == "completed":
        return {"message": "Migration already completed"}

    try:
        # Создаем source store
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        # Создаем migrator
        migrator = ProductMigrator(
            source_store,
            target_store,
            settings.migration_batch_size
        )

        # Возобновляем миграцию
        await migrator.resume_migration(job_id)

        return {
            "job_id": job_id,
            "status": "resumed",
            "message": "Migration resumed successfully"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/by-group")
async def get_stats_by_group(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по группам ОКПД2"""
    pipeline = [
        {"$match": {"status_stg1": "classified"}},
        {"$unwind": "$okpd_group"},
        {"$group": {
            "_id": "$okpd_group",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    cursor = target_store.products.aggregate(pipeline)
    groups = await cursor.to_list(length=None)

    return {
        "groups": [{"group": g["_id"], "count": g["count"]} for g in groups],
        "total_groups": len(groups)
    }


@router.get("/products/sample")
async def get_sample_products(
        status: Optional[str] = None,
        limit: int = 10,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить примеры товаров"""
    query = {}
    if status:
        query["status_stg1"] = status

    cursor = target_store.products.find(query).limit(limit)
    products = await cursor.to_list(length=limit)

    # Преобразуем ObjectId в строки
    for product in products:
        product["_id"] = str(product["_id"])

    return {
        "products": products,
        "count": len(products)
    }


@router.post("/reset-failed")
async def reset_failed_products(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить статус failed товаров на pending"""
    result = await target_store.products.update_many(
        {"status_stg1": "failed"},
        {
            "$set": {
                "status_stg1": "pending",
                "error_message": None,
                "batch_id": None,
                "worker_id": None
            }
        }
    )

    return {
        "reset_count": result.modified_count,
        "message": f"Reset {result.modified_count} failed products to pending"
    }