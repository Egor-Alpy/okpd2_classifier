from fastapi import APIRouter, Depends

from api.dependencies import get_target_store, verify_api_key

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
