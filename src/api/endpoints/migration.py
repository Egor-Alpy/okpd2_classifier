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
        logger.info("Starting migration process...")
        logger.info(f"Source database: {settings.source_mongodb_database}")
        logger.info(f"Target database: {settings.target_mongodb_database}")
        logger.info(f"Target collection: {settings.target_collection_name}")

        # Создаем подключения с использованием обновленных классов
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            settings.source_collection_name
        )

        target_store = TargetMongoStore(
            settings.target_mongodb_database,
            settings.target_collection_name
        )

        # Проверяем подключения
        logger.info("Testing source MongoDB connection...")
        if not await source_store.test_connection():
            raise HTTPException(
                status_code=500,
                detail="Cannot connect to source MongoDB. Check connection parameters."
            )

        logger.info("Testing target MongoDB connection...")
        if not await target_store.test_connection():
            raise HTTPException(
                status_code=500,
                detail="Cannot connect to target MongoDB. Check connection parameters."
            )

        # Инициализируем target store (создание индексов)
        logger.info("Initializing target database...")
        await target_store.initialize()

        # Получаем информацию о коллекциях
        if settings.source_collection_name:
            logger.info(f"Will migrate from specific collection: {settings.source_collection_name}")
            collections = [settings.source_collection_name]
            total_count = await source_store.count_total_products(settings.source_collection_name)
            logger.info(f"Products to migrate: {total_count}")
        else:
            logger.info("Will migrate from all collections")
            collections = await source_store.get_collections_list()
            counts = await source_store.count_all_products()
            total_count = sum(counts.values())
            logger.info(f"Found {len(collections)} collections with {total_count} total products")

        if total_count == 0:
            logger.warning("No products found to migrate")
            return {
                "status": "completed",
                "message": "No products found to migrate",
                "collections": collections,
                "total_products": 0
            }

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
            "message": "Migration started. Run migration worker to process.",
            "collections": collections,
            "total_products": total_count
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting migration: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))