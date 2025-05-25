from datetime import datetime, timedelta

from fastapi import APIRouter, Depends

from src.api.dependencies import get_target_store, verify_api_key

router = APIRouter()


@router.get("/stats")
async def get_statistics(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Получить статистику классификации"""
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


@router.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "service": "OKPD2 Stage One Classifier"
    }


# Добавить в src/api/endpoints/monitoring.py

@router.get("/workers/health")
async def get_workers_health(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Проверить здоровье воркеров"""
    # Проверяем воркеры по активности в последние 5 минут
    cutoff_time = datetime.utcnow() - timedelta(minutes=5)

    # Получаем активные воркеры из processing товаров
    pipeline = [
        {
            "$match": {
                "status_stg1": "processing",
                "processing_started_at": {"$gte": cutoff_time}
            }
        },
        {
            "$group": {
                "_id": "$worker_id",
                "last_activity": {"$max": "$processing_started_at"},
                "active_products": {"$sum": 1}
            }
        }
    ]

    cursor = target_store.products.aggregate(pipeline)
    active_workers = await cursor.to_list(length=None)

    # Получаем статистику по воркерам за последний час
    hour_ago = datetime.utcnow() - timedelta(hours=1)
    worker_pipeline = [
        {
            "$match": {
                "status_stg1": {"$in": ["classified", "none_classified"]},
                "updated_at": {"$gte": hour_ago}
            }
        },
        {
            "$group": {
                "_id": "$worker_id",
                "processed_count": {"$sum": 1},
                "classified_count": {
                    "$sum": {"$cond": [{"$eq": ["$status_stg1", "classified"]}, 1, 0]}
                }
            }
        }
    ]

    cursor = target_store.products.aggregate(worker_pipeline)
    worker_stats = await cursor.to_list(length=None)

    # Формируем ответ
    workers = {}

    # Добавляем активные воркеры
    for worker in active_workers:
        workers[worker["_id"]] = {
            "status": "active",
            "last_activity": worker["last_activity"],
            "active_products": worker["active_products"],
            "processed_last_hour": 0,
            "success_rate": 0
        }

    # Добавляем статистику
    for stat in worker_stats:
        worker_id = stat["_id"]
        if worker_id not in workers:
            workers[worker_id] = {
                "status": "inactive",
                "last_activity": None,
                "active_products": 0
            }

        workers[worker_id]["processed_last_hour"] = stat["processed_count"]
        workers[worker_id]["success_rate"] = (
            stat["classified_count"] / stat["processed_count"] * 100
            if stat["processed_count"] > 0 else 0
        )

    # Проверяем застрявшие товары
    stuck_products = await target_store.products.count_documents({
        "status_stg1": "processing",
        "processing_started_at": {"$lt": cutoff_time}
    })

    return {
        "workers": workers,
        "total_active_workers": len([w for w in workers.values() if w["status"] == "active"]),
        "stuck_products": stuck_products,
        "health_status": "healthy" if stuck_products < 100 else "degraded"
    }


@router.post("/workers/cleanup-stuck")
async def cleanup_stuck_products(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Очистить застрявшие в processing товары"""
    # Товары в processing более 10 минут считаются застрявшими
    cutoff_time = datetime.utcnow() - timedelta(minutes=10)

    result = await target_store.products.update_many(
        {
            "status_stg1": "processing",
            "processing_started_at": {"$lt": cutoff_time}
        },
        {
            "$set": {
                "status_stg1": "pending",
                "processing_started_at": None,
                "worker_id": None
            }
        }
    )

    return {
        "cleaned_count": result.modified_count,
        "message": f"Reset {result.modified_count} stuck products to pending"
    }
