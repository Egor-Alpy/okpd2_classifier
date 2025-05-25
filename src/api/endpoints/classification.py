from fastapi import APIRouter, Depends, HTTPException
from typing import Optional

from src.api.dependencies import get_target_store, verify_api_key
from src.services.product_migrator import ProductMigrator
from src.storage.source_mongo import SourceMongoStore
from src.core.config import settings

router = APIRouter()


@router.post("/migration/start")
async def start_migration(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Начать миграцию товаров из исходной БД"""
    source_store = SourceMongoStore(
        settings.source_mongodb_database,
        settings.source_collection_name
    )

    migrator = ProductMigrator(source_store, target_store)
    job_id = await migrator.start_migration()

    return {
        "job_id": job_id,
        "status": "started",
        "message": "Migration started successfully"
    }


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

    progress_percentage = (
        job["migrated_products"] / job["total_products"] * 100
        if job["total_products"] > 0 else 0
    )

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "total_products": job["total_products"],
        "migrated_products": job["migrated_products"],
        "progress_percentage": round(progress_percentage, 2),
        "created_at": job["created_at"]
    }


@router.post("/migration/{job_id}/resume")
async def resume_migration(
        job_id: str,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Продолжить прерванную миграцию"""
    job = await target_store.get_migration_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Migration job not found")

    if job["status"] == "completed":
        return {"message": "Migration already completed"}

    source_store = SourceMongoStore(
        settings.source_mongodb_database,
        settings.source_collection_name
    )

    migrator = ProductMigrator(source_store, target_store)
    await migrator.resume_migration(job_id)

    return {
        "job_id": job_id,
        "status": "resumed",
        "message": "Migration resumed successfully"
    }