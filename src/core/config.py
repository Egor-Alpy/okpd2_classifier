from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # Source MongoDB (исходная база - только чтение)
    source_mongodb_url: str = "mongodb://localhost:27017"
    source_mongodb_database: str = "source_products"
    source_collection_name: str = "products"

    # Target MongoDB (наша новая база)
    target_mongodb_url: str = "mongodb://localhost:27018"
    target_mongodb_database: str = "okpd_classifier"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Anthropic
    anthropic_api_key: str
    anthropic_model: str = "claude-3-sonnet-20241022"

    # Processing
    migration_batch_size: int = 1000  # Сколько товаров мигрировать за раз
    classification_batch_size: int = 50  # Сколько товаров классифицировать за раз
    max_workers: int = 5

    # API
    api_key: str

    class Config:
        env_file = ".env"


settings = Settings()