from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class ProductStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    CLASSIFIED = "classified"
    NONE_CLASSIFIED = "none_classified"
    FAILED = "failed"


class ProductStageOne(BaseModel):
    """Модель товара для первого этапа классификации"""
    collection_name: str = Field(..., description="Название коллекции БД откуда взяли товар")
    old_mongo_id: str = Field(..., description="Mongo ID товара который мы взяли")
    title: str = Field(..., description="Наименование товара")
    okpd_group: Optional[List[str]] = Field(None, description="Массив 5-значных групп ОКПД2 (формат XX.XX.X)")
    status_stg1: ProductStatus = Field(ProductStatus.PENDING, description="Статус классификации первого этапа")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Когда начали классификацию")

    class Config:
        use_enum_values = True


class MigrationJob(BaseModel):
    """Задача миграции товаров"""
    job_id: str
    status: str = "running"
    total_products: int = 0
    migrated_products: int = 0
    last_processed_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None