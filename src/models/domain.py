from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class ProductStatus(str, Enum):
    PENDING = "pending"
    CLASSIFIED = "classified"
    NONE_CLASSIFIED = "none_classified"
    FAILED = "failed"
    PROCESSING = "processing"


class ProductStageOne(BaseModel):
    """Модель товара для первого этапа классификации"""
    collection_name: str = Field(..., description="Название коллекции источника")
    old_mongo_id: str = Field(..., description="ID товара в исходной MongoDB")
    title: str = Field(..., description="Наименование товара")
    okpd_group: Optional[List[str]] = Field(None, description="Основные категории ОКПД2 (2 цифры)")
    status_stg1: ProductStatus = Field(ProductStatus.PENDING, description="Статус классификации")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None
    batch_id: Optional[str] = None

    class Config:
        use_enum_values = True


class ClassificationBatch(BaseModel):
    """Батч для классификации"""
    batch_id: str
    products: List[ProductStageOne]
    status: str = "pending"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


class MigrationJob(BaseModel):
    """Задача миграции товаров"""
    job_id: str
    status: str = "running"
    total_products: int = 0
    migrated_products: int = 0
    last_processed_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
