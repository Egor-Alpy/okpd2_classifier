from fastapi import APIRouter
from src.api.endpoints import classification, classification_stage2, tender_classification, migration

router = APIRouter()

# Статистика первого этапа
router.include_router(
    classification.router,
    prefix="/stats",
    tags=["statistics"]
)

# Статистика второго этапа
router.include_router(
    classification_stage2.router,
    prefix="/stats",
    tags=["statistics-stage2"]
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