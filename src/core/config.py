from pydantic_settings import BaseSettings
from typing import Optional
from urllib.parse import quote_plus


class Settings(BaseSettings):
    # Source MongoDB (исходная база - только чтение)
    source_mongo_host: str = "localhost"
    source_mongo_port: int = 27017
    source_mongo_user: Optional[str] = None
    source_mongo_pass: Optional[str] = None
    source_mongo_authsource: Optional[str] = None
    source_mongo_authmechanism: str = "SCRAM-SHA-256"
    source_mongo_direct_connection: bool = False
    source_mongodb_database: str = "source_products"
    source_collection_name: str = "products"

    # Target MongoDB (наша новая база)
    target_mongo_host: str = "localhost"
    target_mongo_port: int = 27017
    target_mongo_user: Optional[str] = None
    target_mongo_pass: Optional[str] = None
    target_mongo_authsource: Optional[str] = None
    target_mongo_authmechanism: str = "SCRAM-SHA-256"
    target_mongo_direct_connection: bool = False
    target_mongodb_database: str = "okpd_classifier"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Anthropic
    anthropic_api_key: str
    anthropic_model: str = "claude-3-haiku-20240307"  # Используем более быструю модель

    # Proxy settings for Anthropic API
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None
    socks_proxy: Optional[str] = None

    # Processing
    migration_batch_size: int = 1000
    classification_batch_size: int = 10  # Уменьшено для избежания rate limit
    max_workers: int = 1  # Уменьшено количество воркеров

    # Rate limit settings
    rate_limit_delay: int = 10  # Задержка между батчами в секундах
    max_retries: int = 3  # Количество попыток при rate limit

    # API
    api_key: str

    @property
    def source_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Source MongoDB"""
        if self.source_mongo_user and self.source_mongo_pass:
            connection_string = (
                f"mongodb://{self.source_mongo_user}:{quote_plus(self.source_mongo_pass)}@"
                f"{self.source_mongo_host}:{self.source_mongo_port}"
            )

            # Добавляем authSource если указан
            if self.source_mongo_authsource:
                connection_string += f"/{self.source_mongo_authsource}"
                connection_string += f"?authMechanism={self.source_mongo_authmechanism}"
            else:
                connection_string += f"/?authMechanism={self.source_mongo_authmechanism}"
        else:
            connection_string = f"mongodb://{self.source_mongo_host}:{self.source_mongo_port}"

        return connection_string

    @property
    def target_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Target MongoDB"""
        connection_string = "mongodb://localhost:27017"

        return connection_string

    @property
    def proxy_url(self) -> Optional[str]:
        """Получить URL прокси для Anthropic API"""
        # Приоритет: SOCKS -> HTTPS -> HTTP
        if self.socks_proxy:
            return self.socks_proxy
        elif self.https_proxy:
            return self.https_proxy
        elif self.http_proxy:
            return self.http_proxy
        return None

    class Config:
        env_file = ".env"


settings = Settings()