from fastapi import APIRouter
from src.api.endpoints import classification, classification_stage2, tender_classification

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

router.include_router(
    tender_classification.router,
    prefix="/tender",
    tags=["tender"]
)