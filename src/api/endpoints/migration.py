from fastapi import APIRouter, Depends, HTTPException
import logging

from src.api.dependencies import verify_api_key
from src.storage.source_mongo import SourceMongoStore
from src.storage.target_mongo import TargetMongoStore
from src.services.product_migrator import ProductMigrator
from src.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/start")
async def start_migration(api_key: str = Depends(verify_api_key)):
    """Запустить миграцию товаров из исходной БД"""
    try:
        # Создаем подключения
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )
        target_store = TargetMongoStore(
            settings.target_mongodb_database,
            settings.target_collection_name
        )

        # Проверяем подключения
        if not await source_store.test_connection():
            raise HTTPException(status_code=500, detail="Cannot connect to source MongoDB")

        if not await target_store.test_connection():
            raise HTTPException(status_code=500, detail="Cannot connect to target MongoDB")

        # Инициализируем target store
        await target_store.initialize()

        # Создаем мигратор
        migrator = ProductMigrator(
            source_store,
            target_store,
            settings.migration_batch_size
        )

        # Запускаем миграцию
        job_id = await migrator.start_migration()

        return {
            "status": "started",
            "job_id": job_id,
            "message": "Migration started. Run migration worker to process."
        }

    except Exception as e:
        logger.error(f"Error starting migration: {e}")
        raise HTTPException(status_code=500, detail=str(e))
