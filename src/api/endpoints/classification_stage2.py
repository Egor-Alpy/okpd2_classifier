from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, List
import logging
from datetime import datetime

from src.api.dependencies import get_target_store, verify_api_key
from src.models.domain import ProductStatus, ProductStatusStage2

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/stats/stage2")
async def get_stage2_statistics(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить общую статистику второго этапа"""
    # Считаем товары по статусам второго этапа
    pipeline = [
        {"$match": {"status_stage1": ProductStatus.CLASSIFIED.value}},
        {"$facet": {
            "by_status": [
                {"$group": {
                    "_id": "$status_stage2",
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

    # Товары без status_stage2 считаются pending
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
        status: Optional[str] = None,
        limit: int = 10,
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить примеры товаров второго этапа"""
    query = {"status_stage1": ProductStatus.CLASSIFIED.value}

    if status:
        query["status_stage2"] = status

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
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить failed товары второго этапа на pending"""
    result = await target_store.products.update_many(
        {
            "status_stage1": ProductStatus.CLASSIFIED.value,
            "status_stage2": ProductStatusStage2.FAILED.value
        },
        {"$set": {"status_stage2": ProductStatusStage2.PENDING.value}}
    )

    return {
        "reset_count": result.modified_count,
        "message": f"Reset {result.modified_count} failed products to pending for stage 2"
    }


@router.post("/reset-processing-stage2")
async def reset_processing_stage2_products(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить застрявшие в processing товары второго этапа на pending"""
    result = await target_store.products.update_many(
        {
            "status_stage1": ProductStatus.CLASSIFIED.value,
            "status_stage2": ProductStatusStage2.PROCESSING.value
        },
        {"$set": {"status_stage2": ProductStatusStage2.PENDING.value}}
    )

    return {
        "reset_count": result.modified_count,
        "message": f"Reset {result.modified_count} processing products to pending for stage 2"
    }


@router.get("/stats/by-group-count")
async def get_stats_by_group_count(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по количеству групп у товаров"""
    pipeline = [
        {"$match": {"status_stage1": ProductStatus.CLASSIFIED.value}},
        {"$project": {
            "group_count": {"$size": {"$ifNull": ["$okpd_groups", []]}}
        }},
        {"$group": {
            "_id": "$group_count",
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]

    cursor = target_store.products.aggregate(pipeline)
    results = await cursor.to_list(length=None)

    return {
        "distribution": [
            {"groups_count": r["_id"], "products_count": r["count"]}
            for r in results
        ],
        "total_products": sum(r["count"] for r in results)
    }