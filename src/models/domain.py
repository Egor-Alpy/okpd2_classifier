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


class Product(BaseModel):
    """Модель товара в целевой БД"""
    title: str = Field(..., description="Наименование товара")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Время создания записи")
    okpd2_code: Optional[str] = Field(None, description="Точный код ОКПД2")
    okpd2_name: Optional[str] = Field(None, description="Название по ОКПД2")
    okpd_groups: Optional[List[str]] = Field(None, description="Массив групп ОКПД2 (5-значные коды)")
    processed_at: Optional[datetime] = Field(None, description="Время завершения классификации")
    source_collection: str = Field(..., description="Название исходной коллекции")
    source_id: str = Field(..., description="ID товара в исходной коллекции")
    status_stage1: ProductStatus = Field(ProductStatus.PENDING, description="Статус первого этапа")
    status_stage2: Optional[ProductStatus] = Field(None, description="Статус второго этапа")
    worker_id: Optional[str] = Field(None, description="ID воркера, обработавшего товар")

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