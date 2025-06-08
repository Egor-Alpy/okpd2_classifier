from fastapi import APIRouter
from src.api.endpoints import classification, tender_classification, migration

router = APIRouter()

# Статистика
router.include_router(
    classification.router,
    prefix="/stats",
    tags=["statistics"]
)

# Классификация тендеров
router.include_router(
    tender_classification.router,
    prefix="/tender",
    tags=["tender"]
)

# Миграция
router.include_router(
    migration.router,
    prefix="/migration",
    tags=["migration"]
)