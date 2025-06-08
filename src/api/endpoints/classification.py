from fastapi import APIRouter, Depends, HTTPException
from typing import Optional

from src.api.dependencies import get_target_store, verify_api_key
from src.models.domain import ProductStatus, ProductStatusStage2

router = APIRouter()


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


@router.get("/stats/stage2")
async def get_stage2_statistics(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику второго этапа"""
    # Считаем товары по статусам второго этапа
    pipeline = [
        {"$match": {"status_stage1": "classified"}},  # Используем существующее имя поля
        {"$facet": {
            "by_status": [
                {"$group": {
                    "_id": "$status_stage2",  # Используем существующее имя поля
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
    pending += status_counts.get("pending", 0)

    return {
        "total_stage1_classified": total,
        "stage2_pending": pending,
        "stage2_processing": status_counts.get("processing", 0),
        "stage2_classified": status_counts.get("classified", 0),
        "stage2_none_classified": status_counts.get("none_classified", 0),
        "stage2_failed": status_counts.get("failed", 0),
        "with_exact_code": with_code,
        "completion_percentage": round(with_code / total * 100, 2) if total > 0 else 0
    }


@router.get("/stats/by-source-collection")
async def get_stats_by_source_collection(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику по исходным коллекциям"""
    pipeline = [
        {"$group": {
            "_id": "$source_collection",  # Используем существующее имя поля
            "total": {"$sum": 1},
            "classified": {
                "$sum": {
                    "$cond": [{"$eq": ["$status_stage1", "classified"]}, 1, 0]
                }
            },
            "with_code": {
                "$sum": {
                    "$cond": [{"$ne": ["$okpd2_code", None]}, 1, 0]
                }
            }
        }},
        {"$sort": {"_id": 1}}
    ]

    cursor = target_store.products.aggregate(pipeline)
    collections = await cursor.to_list(length=None)

    return {
        "collections": [
            {
                "collection": c["_id"],
                "total": c["total"],
                "classified": c["classified"],
                "with_exact_code": c["with_code"]
            }
            for c in collections
        ],
        "total_collections": len(collections)
    }