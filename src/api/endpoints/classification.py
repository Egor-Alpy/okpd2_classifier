from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
import uuid
import logging

from src.api.dependencies import get_target_store, verify_api_key
from src.storage.source_mongo import SourceMongoStore
from src.services.product_migrator import ProductMigrator
from src.core.config import settings
from src.models.domain import ProductStatus

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/migration/start")
async def start_migration(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Начать миграцию товаров из source в target MongoDB"""
    try:
        logger.info("Starting migration via classification endpoint...")

        # Создаем source store с обновленными настройками подключения
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        # Проверяем подключение
        logger.info("Testing source MongoDB connection...")
        if not await source_store.test_connection():
            logger.error("Failed to connect to source MongoDB")
            raise HTTPException(
                status_code=500,
                detail="Cannot connect to source MongoDB. Check connection parameters in .env file."
            )

        # Проверяем наличие товаров
        if settings.source_collection_name:
            count = await source_store.count_total_products(settings.source_collection_name)
            logger.info(f"Found {count} products in collection {settings.source_collection_name}")
        else:
            counts = await source_store.count_all_products()
            total = sum(counts.values())
            logger.info(f"Found {total} products across {len(counts)} collections")

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
            "message": "Migration started successfully. Run migration worker to process batches."
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in migration endpoint: {e}", exc_info=True)
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
        "created_at": job["created_at"],
        "updated_at": job.get("updated_at")
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
        logger.info(f"Resuming migration job {job_id}")

        # Создаем source store с обновленными настройками
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        # Проверяем подключение
        if not await source_store.test_connection():
            raise HTTPException(
                status_code=500,
                detail="Cannot connect to source MongoDB"
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

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming migration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_statistics(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить общую статистику классификации"""
    stats = await target_store.get_statistics()

    # Добавляем процентные показатели
    if stats["total"] > 0:
        stats["classified_percentage"] = round(stats["classified"] / stats["total"] * 100, 2)
        stats["pending_percentage"] = round(stats["pending"] / stats["total"] * 100, 2)
        stats["none_classified_percentage"] = round(stats["none_classified"] / stats["total"] * 100, 2)
        stats["failed_percentage"] = round(stats["failed"] / stats["total"] * 100, 2)
    else:
        stats["classified_percentage"] = 0
        stats["pending_percentage"] = 0
        stats["none_classified_percentage"] = 0
        stats["failed_percentage"] = 0

    return stats


@router.get("/stats/by-source-collection")
async def get_stats_by_source_collection(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по исходным коллекциям"""
    stats = await target_store.get_statistics_by_source_collection()

    # Добавляем процентные показатели для каждой коллекции
    for collection_name, collection_stats in stats.items():
        total = collection_stats.get("total", 0)
        if total > 0:
            for status in ["pending", "processing", "classified", "none_classified", "failed"]:
                count = collection_stats.get(status, 0)
                collection_stats[f"{status}_percentage"] = round(count / total * 100, 2)

    return stats


@router.get("/stats/by-group")
async def get_stats_by_group(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по группам ОКПД2"""
    pipeline = [
        {"$match": {"status_stage1": ProductStatus.CLASSIFIED.value}},
        {"$unwind": "$okpd_groups"},
        {"$group": {
            "_id": "$okpd_groups",
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
        source_collection: Optional[str] = None,
        limit: int = 10,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить примеры товаров"""
    query = {}
    if status:
        query["status_stage1"] = status
    if source_collection:
        query["source_collection"] = source_collection

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
        {"status_stage1": ProductStatus.FAILED.value},
        {"$set": {"status_stage1": ProductStatus.PENDING.value}}
    )

    return {
        "reset_count": result.modified_count,
        "message": f"Reset {result.modified_count} failed products to pending"
    }


@router.post("/cleanup-stuck")
async def cleanup_stuck_products(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить застрявшие в processing товары обратно в pending"""
    result = await target_store.products.update_many(
        {"status_stage1": ProductStatus.PROCESSING.value},
        {"$set": {"status_stage1": ProductStatus.PENDING.value}}
    )

    return {
        "cleaned_count": result.modified_count,
        "message": f"Reset {result.modified_count} stuck products to pending"
    }