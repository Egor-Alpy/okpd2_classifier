from fastapi import APIRouter
from src.api.endpoints import classification, classification_stage2

router = APIRouter()

router.include_router(
    classification.router,
    prefix="/classification",
    tags=["classification"]
)

router.include_router(
    classification_stage2.router,
    prefix="/classification/stage2",
    tags=["classification_stage2"]
)