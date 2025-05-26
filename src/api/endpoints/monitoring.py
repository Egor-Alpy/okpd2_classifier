from fastapi import APIRouter, Depends
from datetime import datetime, timedelta

from src.api.dependencies import get_target_store, verify_api_key
from src.core.metrics import metrics_collector
from src.models.domain import ProductStatus

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


@router.get("/metrics/classification")
async def get_classification_metrics(
        time_window: int = 60,
        api_key: str = Depends(verify_api_key)
):
    """Получить метрики классификации"""
    return await metrics_collector.get_classification_stats(time_window)


@router.get("/metrics/migration")
async def get_migration_metrics(
        time_window: int = 60,
        api_key: str = Depends(verify_api_key)
):
    """Получить метрики миграции"""
    return await metrics_collector.get_migration_stats(time_window)


@router.get("/metrics/summary")
async def get_metrics_summary(
        api_key: str = Depends(verify_api_key)
):
    """Получить сводку всех метрик"""
    classification_1h = await metrics_collector.get_classification_stats(60)
    classification_24h = await metrics_collector.get_classification_stats(1440)
    migration_1h = await metrics_collector.get_migration_stats(60)

    return {
        "classification": {
            "last_hour": classification_1h,
            "last_24_hours": classification_24h
        },
        "migration": {
            "last_hour": migration_1h
        },
        "system": {
            "active_workers": len(metrics_collector.worker_stats),
            "total_rate_limits": classification_1h['rate_limits_total']
        }
    }


@router.get("/workers/health")
async def get_workers_health(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Проверить здоровье воркеров через статистику обработки"""
    # Получаем статистику обработки за последний час
    hour_ago = datetime.utcnow() - timedelta(hours=1)

    # Группируем по статусам для анализа активности
    pipeline = [
        {
            "$match": {
                "created_at": {"$gte": hour_ago}
            }
        },
        {
            "$group": {
                "_id": "$status_stg1",
                "count": {"$sum": 1}
            }
        }
    ]

    cursor = target_store.products.aggregate(pipeline)
    status_stats = await cursor.to_list(length=None)

    # Проверяем застрявшие товары
    stuck_products = await target_store.products.count_documents({
        "status_stg1": ProductStatus.PROCESSING.value
    })

    # Формируем статистику
    stats_dict = {stat["_id"]: stat["count"] for stat in status_stats}
    processed_last_hour = stats_dict.get(ProductStatus.CLASSIFIED.value, 0) + \
                          stats_dict.get(ProductStatus.NONE_CLASSIFIED.value, 0)

    return {
        "processed_last_hour": processed_last_hour,
        "stuck_products": stuck_products,
        "health_status": "healthy" if stuck_products < 100 else "degraded",
        "metrics": {
            "classified": stats_dict.get(ProductStatus.CLASSIFIED.value, 0),
            "none_classified": stats_dict.get(ProductStatus.NONE_CLASSIFIED.value, 0),
            "failed": stats_dict.get(ProductStatus.FAILED.value, 0)
        }
    }


@router.post("/workers/cleanup-stuck")
async def cleanup_stuck_products(
        target_store=Depends(get_target_store),
        api_key: str = Depends(verify_api_key)
):
    """Сбросить застрявшие в processing товары обратно в pending"""
    result = await target_store.products.update_many(
        {"status_stg1": ProductStatus.PROCESSING.value},
        {"$set": {"status_stg1": ProductStatus.PENDING.value}}
    )

    return {
        "cleaned_count": result.modified_count,
        "message": f"Reset {result.modified_count} stuck products to pending"
    }