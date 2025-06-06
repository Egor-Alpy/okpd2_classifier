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


class ProductStatusStage2(str, Enum):
    """Статусы второго этапа классификации"""
    PENDING = "pending"
    PROCESSING = "processing"
    CLASSIFIED = "classified"
    NONE_CLASSIFIED = "none_classified"
    FAILED = "failed"


class Product(BaseModel):
    """Модель товара в БД"""
    title: str = Field(..., description="Наименование товара")
    source_collection: str = Field(..., description="Название исходной коллекции")
    source_id: str = Field(..., description="ID товара в исходной БД")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Дата создания записи")

    # Результаты классификации
    okpd_groups: Optional[List[str]] = Field(None, description="Массив 5-значных групп ОКПД2")
    okpd2_code: Optional[str] = Field(None, description="Точный код ОКПД2")
    okpd2_name: Optional[str] = Field(None, description="Название по ОКПД2")

    # Статусы
    status_stage1: ProductStatus = Field(ProductStatus.PENDING, description="Статус первого этапа")
    status_stage2: Optional[str] = Field(None, description="Статус второго этапа")

    # Метаданные обработки
    processed_at: Optional[datetime] = Field(None, description="Дата завершения классификации")
    worker_id: Optional[str] = Field(None, description="ID последнего воркера")

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