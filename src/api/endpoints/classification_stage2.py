from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, List
import logging
from datetime import datetime

from src.api.dependencies import get_target_store, verify_api_key
from src.models.domain import ProductStatus
from src.models.domain_stage2 import ProductStatusStage2

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/available-classes")
async def get_available_classes(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить классы ОКПД2, готовые для второго этапа"""
    # Находим все уникальные 2-значные классы из 5-значных групп
    pipeline = [
        {"$match": {"status_stg1": ProductStatus.CLASSIFIED.value}},
        {"$unwind": "$okpd_group"},  # Разворачиваем массив
        {"$project": {
            "okpd_class": {"$substr": ["$okpd_group", 0, 2]}  # Теперь okpd_group - строка
        }},
        {"$group": {
            "_id": "$okpd_class",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    cursor = target_store.products.aggregate(pipeline)
    classes = await cursor.to_list(length=None)

    # Получаем статистику по второму этапу для каждого класса
    result = []
    for cls in classes:
        okpd_class = cls["_id"]

        # Считаем статистику второго этапа
        stats_pipeline = [
            {"$match": {
                "status_stg1": ProductStatus.CLASSIFIED.value,
                "okpd_group": {"$regex": f"^{okpd_class}\\."}
            }},
            {"$group": {
                "_id": "$status_stg2",
                "count": {"$sum": 1}
            }}
        ]

        stats_cursor = target_store.products.aggregate(stats_pipeline)
        stats = await stats_cursor.to_list(length=None)

        stats_dict = {s["_id"]: s["count"] for s in stats if s["_id"]}

        total = sum(stats_dict.values()) + (cls["count"] if not stats_dict else 0)
        pending = stats_dict.get(ProductStatusStage2.PENDING.value, 0)

        # Если нет status_stg2, значит все pending
        if not stats_dict:
            pending = cls["count"]

        result.append({
            "class": okpd_class,
            "total_products": total,
            "pending": pending,
            "processing": stats_dict.get(ProductStatusStage2.PROCESSING.value, 0),
            "classified": stats_dict.get(ProductStatusStage2.CLASSIFIED.value, 0),
            "none_classified": stats_dict.get(ProductStatusStage2.NONE_CLASSIFIED.value, 0),
            "failed": stats_dict.get(ProductStatusStage2.FAILED.value, 0)
        })

    return {
        "classes": result,
        "total_classes": len(result)
    }


@router.post("/start/{okpd_class}")
async def start_stage2_classification(
        okpd_class: str,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Начать классификацию второго этапа для конкретного класса"""
    # Проверяем формат класса
    if not okpd_class.isdigit() or len(okpd_class) != 2:
        raise HTTPException(status_code=400, detail="Invalid OKPD class format. Expected 2 digits.")

    # Проверяем, есть ли товары для классификации
    count = await target_store.products.count_documents({
        "status_stg1": ProductStatus.CLASSIFIED.value,
        "okpd_group": {"$regex": f"^{okpd_class}\\."},
        "$or": [
            {"status_stg2": {"$exists": False}},
            {"status_stg2": ProductStatusStage2.PENDING.value}
        ]
    })

    if count == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No pending products found for class {okpd_class}"
        )

    # Создаем задачу классификации
    job_id = f"stage2_{okpd_class}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    job = {
        "job_id": job_id,
        "okpd_class": okpd_class,
        "status": "running",
        "total_products": count,
        "classified_products": 0,
        "none_classified_products": 0,
        "failed_products": 0,
        "created_at": datetime.utcnow()
    }

    await target_store.db.classification_jobs_stage2.insert_one(job)

    logger.info(f"Started stage 2 classification job {job_id} for class {okpd_class}")

    return {
        "job_id": job_id,
        "okpd_class": okpd_class,
        "total_products": count,
        "status": "started",
        "message": f"Stage 2 classification started for class {okpd_class}"
    }


@router.get("/job/{job_id}")
async def get_stage2_job_status(
        job_id: str,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статус задачи второго этапа"""
    job = await target_store.db.classification_jobs_stage2.find_one({"job_id": job_id})

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Удаляем _id для сериализации
    job.pop("_id", None)

    # Добавляем прогресс
    if job["total_products"] > 0:
        processed = (job["classified_products"] +
                     job["none_classified_products"] +
                     job["failed_products"])
        job["progress_percentage"] = round(processed / job["total_products"] * 100, 2)
    else:
        job["progress_percentage"] = 0

    return job


@router.get("/stats/stage2")
async def get_stage2_statistics(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить общую статистику второго этапа"""
    # Считаем товары по статусам второго этапа
    pipeline = [
        {"$match": {"status_stg1": ProductStatus.CLASSIFIED.value}},
        {"$facet": {
            "by_status": [
                {"$group": {
                    "_id": "$status_stg2",
                    "count": {"$sum": 1}
                }}
            ],
            "total": [
                {"$count": "count"}
            ],
            "with_code": [
                {"$match": {"okpd2_code": {"$exists": True, "$ne": None}}},
                {"$count": "count"}
            ]
        }}
    ]

    cursor = target_store.products.aggregate(pipeline)
    result = await cursor.to_list(length=1)

    if not result:
        return {
            "total_stage1_classified": 0,
            "stage2_pending": 0,
            "stage2_processing": 0,
            "stage2_classified": 0,
            "stage2_none_classified": 0,
            "stage2_failed": 0,
            "with_exact_code": 0
        }

    facets = result[0]
    total = facets["total"][0]["count"] if facets["total"] else 0
    with_code = facets["with_code"][0]["count"] if facets["with_code"] else 0

    status_counts = {s["_id"]: s["count"] for s in facets["by_status"] if s["_id"]}

    # Товары без status_stg2 считаются pending
    pending = total - sum(status_counts.values())
    pending += status_counts.get(ProductStatusStage2.PENDING.value, 0)

    return {
        "total_stage1_classified": total,
        "stage2_pending": pending,
        "stage2_processing": status_counts.get(ProductStatusStage2.PROCESSING.value, 0),
        "stage2_classified": status_counts.get(ProductStatusStage2.CLASSIFIED.value, 0),
        "stage2_none_classified": status_counts.get(ProductStatusStage2.NONE_CLASSIFIED.value, 0),
        "stage2_failed": status_counts.get(ProductStatusStage2.FAILED.value, 0),
        "with_exact_code": with_code,
        "completion_percentage": round(with_code / total * 100, 2) if total > 0 else 0
    }


@router.get("/products/stage2/sample")
async def get_stage2_sample_products(
        okpd_class: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 10,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить примеры товаров второго этапа"""
    query = {"status_stg1": ProductStatus.CLASSIFIED.value}

    if okpd_class:
        query["okpd_group"] = {"$regex": f"^{okpd_class}\\."}

    if status:
        query["status_stg2"] = status

    cursor = target_store.products.find(query).limit(limit)
    products = await cursor.to_list(length=limit)

    # Преобразуем ObjectId в строки
    for product in products:
        product["_id"] = str(product["_id"])

    return {
        "products": products,
        "count": len(products)
    }


@router.post("/reset-failed-stage2")
async def reset_failed_stage2_products(
        okpd_class: Optional[str] = None,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить failed товары второго этапа на pending"""
    query = {
        "status_stg1": ProductStatus.CLASSIFIED.value,
        "status_stg2": ProductStatusStage2.FAILED.value
    }

    if okpd_class:
        query["okpd_group"] = {"$regex": f"^{okpd_class}\\."}

    result = await target_store.products.update_many(
        query,
        {"$set": {"status_stg2": ProductStatusStage2.PENDING.value}}
    )

    return {
        "reset_count": result.modified_count,
        "message": f"Reset {result.modified_count} failed products to pending for stage 2"
    }