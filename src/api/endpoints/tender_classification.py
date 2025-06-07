from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
import logging
import asyncio
import uuid

from src.api.dependencies import verify_api_key
from src.services.ai_client import AnthropicClient
from src.services.ai_client_stage2 import PromptBuilderStage2
from src.services.classifier import StageOneClassifier
from src.services.classifier_stage2 import StageTwoClassifier
from src.storage.target_mongo import TargetMongoStore
from src.storage.source_mongo import SourceMongoStore
from src.services.product_migrator import ProductMigrator
from src.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/classify-tender")
async def classify_tender(
        tender_data: Dict[str, Any],
        api_key: str = Depends(verify_api_key)
):
    """
    Классифицировать товары в тендере, у которых нет ОКПД2 кода
    """
    try:
        # Проверяем наличие товаров
        if "items" not in tender_data:
            raise HTTPException(status_code=400, detail="No items found in tender data")

        items = tender_data["items"]
        items_to_classify = []
        items_map = {}

        # Находим товары без кода ОКПД2
        for idx, item in enumerate(items):
            okpd2_code = item.get("okpd2Code")

            # Если кода нет или он пустой - нужно классифицировать
            if not okpd2_code or okpd2_code == "":
                item_id = f"tender_item_{idx}"
                items_to_classify.append({
                    "_id": item_id,
                    "title": item.get("name", "")
                })
                items_map[item_id] = idx

        if not items_to_classify:
            logger.info("All items already have OKPD2 codes")
            return tender_data

        logger.info(f"Found {len(items_to_classify)} items without OKPD2 codes")

        # Инициализируем AI клиент
        ai_client = AnthropicClient(
            settings.anthropic_api_key,
            settings.anthropic_model
        )

        # Создаем временный store для результатов
        temp_store = TargetMongoStore(settings.target_mongodb_database, "temp_tender_classification")

        # Классификатор первого этапа
        classifier_stage1 = StageOneClassifier(
            ai_client,
            temp_store,
            batch_size=min(50, len(items_to_classify))
        )

        # Классифицируем первый этап
        logger.info("Starting stage 1 classification...")
        stage1_result = await classifier_stage1.process_batch(items_to_classify)

        # Обновляем товары с группами в памяти
        for item_id, groups in stage1_result["results"].items():
            idx = items_map[item_id]
            items_to_classify[items_map[item_id]]["okpd_groups"] = groups

        # Фильтруем товары с группами для второго этапа
        items_for_stage2 = [
            item for item in items_to_classify
            if item.get("okpd_groups") and len(item["okpd_groups"]) > 0
        ]

        if items_for_stage2:
            # Классификатор второго этапа
            classifier_stage2 = StageTwoClassifier(
                ai_client,
                temp_store,
                batch_size=min(15, len(items_for_stage2))
            )

            logger.info(f"Starting stage 2 classification for {len(items_for_stage2)} items...")
            stage2_result = await classifier_stage2.process_batch(items_for_stage2)

            # Обновляем исходные товары с точными кодами
            for item_id, result_data in stage2_result["results"].items():
                idx = items_map[item_id]
                # Обновляем только код, остальные поля не трогаем
                items[idx]["okpd2Code"] = result_data["code"]

                # Логируем успешную классификацию
                logger.info(f"Item '{items[idx]['name']}' classified with code: {result_data['code']}")

        # Возвращаем обновленный тендер
        classified_count = sum(1 for item_id in items_map if any(
            res.get(item_id) for res in [stage1_result.get("results", {}),
                                         stage2_result.get("results", {}) if 'stage2_result' in locals() else {}]
        ))

        logger.info(f"Classification completed. Classified {classified_count} items")

        # Очищаем временную коллекцию
        await temp_store.products.drop()
        await temp_store.close()

        return tender_data

    except Exception as e:
        logger.error(f"Error classifying tender: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start-collection-classification/stage1/{collection_name}")
async def start_collection_classification_stage1(
        collection_name: str,
        batch_size: int = 250,
        api_key: str = Depends(verify_api_key)
):
    """
    Запустить первый этап классификации для конкретной коллекции
    """
    try:
        # Создаем source store для конкретной коллекции
        source_store = SourceMongoStore(
            settings.source_mongodb_database,
            collection_name
        )

        # Проверяем существование коллекции
        collections = await source_store.get_collections_list()
        if collection_name not in collections:
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_name}' not found. Available: {collections}"
            )

        # Проверяем количество товаров
        total_count = await source_store.count_total_products(collection_name)
        if total_count == 0:
            raise HTTPException(
                status_code=400,
                detail=f"Collection '{collection_name}' is empty"
            )

        logger.info(f"Starting stage 1 classification for collection '{collection_name}' with {total_count} products")

        # Создаем target store
        target_store = TargetMongoStore(settings.target_mongodb_database)

        # Мигрируем товары из коллекции если их еще нет
        migrator = ProductMigrator(source_store, target_store, settings.migration_batch_size)

        # Проверяем, есть ли уже товары из этой коллекции
        existing_count = await target_store.products.count_documents({
            "source_collection": collection_name
        })

        if existing_count < total_count:
            logger.info(f"Migrating {total_count - existing_count} new products from {collection_name}")

            # Запускаем миграцию только для этой коллекции
            job_id = f"migration_{collection_name}_{uuid.uuid4().hex[:8]}"
            await target_store.create_migration_job(job_id, total_count)

            # Мигрируем синхронно для этого эндпоинта
            migrated = await migrator._migrate_collection(job_id, collection_name, 0)

            await target_store.update_migration_job(
                job_id,
                migrated,
                status="completed"
            )

        # Получаем количество товаров для классификации
        pending_count = await target_store.products.count_documents({
            "source_collection": collection_name,
            "status_stage1": "pending"
        })

        return {
            "collection": collection_name,
            "total_products": total_count,
            "migrated_products": existing_count,
            "pending_classification": pending_count,
            "message": f"Collection ready for stage 1 classification. Use workers to process.",
            "worker_command": f"python -m src.workers.classification_worker --collection {collection_name}"
        }

    except Exception as e:
        logger.error(f"Error starting collection classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start-collection-classification/stage2/{collection_name}")
async def start_collection_classification_stage2(
        collection_name: str,
        api_key: str = Depends(verify_api_key)
):
    """
    Запустить второй этап классификации для конкретной коллекции
    """
    try:
        target_store = TargetMongoStore(settings.target_mongodb_database)

        # Проверяем товары готовые для второго этапа
        stage2_ready = await target_store.products.count_documents({
            "source_collection": collection_name,
            "status_stage1": "classified",
            "okpd_groups": {"$exists": True, "$ne": []},
            "$or": [
                {"status_stage2": {"$exists": False}},
                {"status_stage2": "pending"}
            ]
        })

        if stage2_ready == 0:
            # Проверяем, может все уже классифицировано
            stage2_done = await target_store.products.count_documents({
                "source_collection": collection_name,
                "status_stage2": {"$in": ["classified", "none_classified"]}
            })

            if stage2_done > 0:
                return {
                    "collection": collection_name,
                    "message": f"Stage 2 already completed for {stage2_done} products",
                    "ready_for_stage2": 0
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"No products ready for stage 2. Complete stage 1 first."
                )

        # Получаем статистику
        stats = {
            "total": await target_store.products.count_documents({"source_collection": collection_name}),
            "stage1_classified": await target_store.products.count_documents({
                "source_collection": collection_name,
                "status_stage1": "classified"
            }),
            "stage2_pending": stage2_ready,
            "stage2_completed": await target_store.products.count_documents({
                "source_collection": collection_name,
                "status_stage2": {"$in": ["classified", "none_classified"]}
            })
        }

        return {
            "collection": collection_name,
            "stats": stats,
            "message": f"{stage2_ready} products ready for stage 2 classification",
            "worker_command": f"python -m src.workers.classification_worker_stage2 --collection {collection_name}"
        }

    except Exception as e:
        logger.error(f"Error starting stage 2 classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collection-stats/{collection_name}")
async def get_collection_stats(
        collection_name: str,
        api_key: str = Depends(verify_api_key)
):
    """
    Получить статистику классификации для коллекции
    """
    try:
        target_store = TargetMongoStore(settings.target_mongodb_database)

        # Общая статистика
        pipeline = [
            {"$match": {"source_collection": collection_name}},
            {"$facet": {
                "stage1": [
                    {"$group": {
                        "_id": "$status_stage1",
                        "count": {"$sum": 1}
                    }}
                ],
                "stage2": [
                    {"$match": {"status_stage1": "classified"}},
                    {"$group": {
                        "_id": "$status_stage2",
                        "count": {"$sum": 1}
                    }}
                ],
                "with_exact_code": [
                    {"$match": {"okpd2_code": {"$exists": True, "$ne": None}}},
                    {"$count": "count"}
                ],
                "total": [
                    {"$count": "count"}
                ]
            }}
        ]

        cursor = target_store.products.aggregate(pipeline)
        result = await cursor.to_list(length=1)

        if not result:
            raise HTTPException(status_code=404, detail=f"No products found for collection '{collection_name}'")

        facets = result[0]
        total = facets["total"][0]["count"] if facets["total"] else 0
        with_code = facets["with_exact_code"][0]["count"] if facets["with_exact_code"] else 0

        # Форматируем статистику
        stage1_stats = {s["_id"]: s["count"] for s in facets["stage1"]}
        stage2_stats = {s["_id"]: s["count"] for s in facets["stage2"] if s["_id"]}

        return {
            "collection": collection_name,
            "total_products": total,
            "stage1": {
                "pending": stage1_stats.get("pending", 0),
                "processing": stage1_stats.get("processing", 0),
                "classified": stage1_stats.get("classified", 0),
                "none_classified": stage1_stats.get("none_classified", 0),
                "failed": stage1_stats.get("failed", 0)
            },
            "stage2": {
                "pending": stage2_stats.get("pending", 0) + (
                            stage1_stats.get("classified", 0) - sum(stage2_stats.values())),
                "processing": stage2_stats.get("processing", 0),
                "classified": stage2_stats.get("classified", 0),
                "none_classified": stage2_stats.get("none_classified", 0),
                "failed": stage2_stats.get("failed", 0)
            },
            "products_with_exact_code": with_code,
            "completion_percentage": round(with_code / total * 100, 2) if total > 0 else 0
        }

    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
