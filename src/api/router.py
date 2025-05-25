from fastapi import APIRouter
from src.api.endpoints import classification, monitoring

router = APIRouter()

router.include_router(
    classification.router,
    prefix="/classification",
    tags=["classification"]
)

router.include_router(
    monitoring.router,
    prefix="/monitoring",
    tags=["monitoring"]
)