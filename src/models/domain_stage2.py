from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class ProductStatusStage2(str, Enum):
    """Статусы второго этапа классификации"""
    PENDING = "pending"  # Ожидает обработки
    PROCESSING = "processing"  # В процессе обработки
    CLASSIFIED = "classified"  # Успешно классифицирован с точным кодом
    NONE_CLASSIFIED = "none_classified"  # Не удалось найти точный код
    FAILED = "failed"  # Ошибка при обработке


class ProductStageTwo(BaseModel):
    """Модель для второго этапа классификации"""
    # Поля из первого этапа (не изменяем)
    collection_name: str
    old_mongo_id: str
    title: str
    okpd_group: List[str]  # 5-значные группы из первого этапа
    status_stg1: str
    created_at: datetime

    # Новые поля для второго этапа
    okpd2_code: Optional[str] = Field(None, description="Точный код ОКПД2")
    okpd2_name: Optional[str] = Field(None, description="Название по ОКПД2")
    status_stg2: ProductStatusStage2 = Field(
        ProductStatusStage2.PENDING,
        description="Статус второго этапа"
    )
    stage2_started_at: Optional[datetime] = Field(None, description="Начало второго этапа")
    stage2_completed_at: Optional[datetime] = Field(None, description="Завершение второго этапа")
    stage2_batch_id: Optional[str] = Field(None, description="ID батча второго этапа")
    stage2_worker_id: Optional[str] = Field(None, description="ID воркера второго этапа")

    class Config:
        use_enum_values = True


class ClassificationJobStage2(BaseModel):
    """Задача классификации второго этапа"""
    job_id: str
    okpd_class: str = Field(..., description="2-значный класс ОКПД2 для обработки")
    status: str = "running"
    total_products: int = 0
    classified_products: int = 0
    none_classified_products: int = 0
    failed_products: int = 0
    last_processed_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
