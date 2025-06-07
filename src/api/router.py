from fastapi import APIRouter
from src.api.endpoints import classification, tender_classification

router = APIRouter()

# Статистика
router.include_router(
    classification.router,
    prefix="/stats",
    tags=["statistics"]
)

# Классификация позиций тендера
router.include_router(
    tender_classification.router,
    prefix="/tender",
    tags=["tender"]
)