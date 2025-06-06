from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List

# Импортируем ProductStatusStage2 из domain.py
from src.models.domain import ProductStatusStage2


class ProductStageTwo(BaseModel):
    """Модель для второго этапа классификации"""
    # Поля из первого этапа (не изменяем)
    source_collection: str
    source_id: str
    title: str
    okpd_groups: List[str]  # 5-значные группы из первого этапа
    status_stage1: str
    created_at: datetime

    # Новые поля для второго этапа
    okpd2_code: Optional[str] = Field(None, description="Точный код ОКПД2")
    okpd2_name: Optional[str] = Field(None, description="Название по ОКПД2")
    status_stage2: ProductStatusStage2 = Field(
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