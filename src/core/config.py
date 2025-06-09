from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Optional
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # Source MongoDB (исходная база - только чтение)
    source_mongo_host: str = "localhost"
    source_mongo_port: int = 27017
    source_mongo_user: Optional[str] = None
    source_mongo_pass: Optional[str] = None
    source_mongo_authsource: Optional[str] = None
    source_mongo_authmechanism: str = "SCRAM-SHA-256"
    source_mongo_direct_connection: bool = False
    source_mongodb_database: str = "products"
    # Если пусто - берем из всех коллекций
    source_collection_name: Optional[str] = None

    # Target MongoDB (наша новая база)
    target_mongo_host: str = "localhost"
    target_mongo_port: int = 27017
    target_mongo_user: Optional[str] = None
    target_mongo_pass: Optional[str] = None
    target_mongo_authsource: Optional[str] = None
    target_mongo_authmechanism: str = "SCRAM-SHA-256"
    target_mongo_direct_connection: bool = False
    target_mongodb_database: str = "TenderDB"
    target_collection_name: str = "classified_products"

    # Redis
    redis_url: str = "redis://localhost:6379"

    # Anthropic
    anthropic_api_key: str
    anthropic_model: str = "claude-3-5-sonnet-20241022"

    # Prompt caching
    enable_prompt_caching: bool = True
    cache_ttl_minutes: int = 5

    # Proxy settings for Anthropic API
    http_proxy: Optional[str] = None
    https_proxy: Optional[str] = None
    socks_proxy: Optional[str] = None

    # Processing
    migration_batch_size: int = 1000
    classification_batch_size: int = 250
    max_workers: int = 1

    # Rate limit settings
    rate_limit_delay: int = 6
    max_retries: int = 3

    # API
    api_key: str

    @field_validator('source_mongo_user', 'source_mongo_pass', 'target_mongo_user', 'target_mongo_pass',
                     'source_mongo_authsource', 'target_mongo_authsource', 'source_collection_name',
                     'http_proxy', 'https_proxy', 'socks_proxy', mode='before')
    @classmethod
    def empty_str_to_none(cls, v: str) -> Optional[str]:
        """Преобразовать пустые строки в None"""
        if v == '':
            return None
        return v

    @property
    def source_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Source MongoDB"""
        # Базовая часть подключения
        if self.source_mongo_user and self.source_mongo_pass:
            # URL encode для безопасности
            user = quote_plus(self.source_mongo_user)
            password = quote_plus(self.source_mongo_pass)

            connection_string = (
                f"mongodb://{user}:{password}@"
                f"{self.source_mongo_host}:{self.source_mongo_port}"
            )
        else:
            connection_string = f"mongodb://{self.source_mongo_host}:{self.source_mongo_port}"

        # Параметры подключения
        params = []

        # authSource - ОБЯЗАТЕЛЬНО для аутентификации
        if self.source_mongo_authsource:
            params.append(f"authSource={self.source_mongo_authsource}")
        elif self.source_mongo_user:  # Если есть пользователь, но не указан authSource
            # По умолчанию используем admin для аутентификации
            params.append("authSource=admin")

        # authMechanism
        if self.source_mongo_authmechanism:
            params.append(f"authMechanism={self.source_mongo_authmechanism}")

        # directConnection - важно для подключения к конкретному узлу
        if self.source_mongo_direct_connection:
            params.append("directConnection=true")

        # Добавляем параметры к строке подключения
        if params:
            connection_string += "/?" + "&".join(params)

        logger.debug(
            f"Source MongoDB connection string: {connection_string.replace(password if 'password' in locals() else '', '***')}")

        return connection_string

    @property
    def target_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Target MongoDB"""
        # Базовая часть подключения
        if self.target_mongo_user and self.target_mongo_pass:
            # URL encode для безопасности
            user = quote_plus(self.target_mongo_user)
            password = quote_plus(self.target_mongo_pass)

            connection_string = (
                f"mongodb://{user}:{password}@"
                f"{self.target_mongo_host}:{self.target_mongo_port}"
            )
        else:
            connection_string = f"mongodb://{self.target_mongo_host}:{self.target_mongo_port}"

        # Параметры подключения
        params = []

        # authSource - ОБЯЗАТЕЛЬНО для аутентификации
        if self.target_mongo_authsource:
            params.append(f"authSource={self.target_mongo_authsource}")
        elif self.target_mongo_user:  # Если есть пользователь, но не указан authSource
            # По умолчанию используем admin для аутентификации
            params.append("authSource=admin")

        # authMechanism
        if self.target_mongo_authmechanism:
            params.append(f"authMechanism={self.target_mongo_authmechanism}")

        # directConnection - важно для подключения к конкретному узлу
        if self.target_mongo_direct_connection:
            params.append("directConnection=true")

        # Добавляем параметры к строке подключения
        if params:
            connection_string += "/?" + "&".join(params)

        logger.debug(
            f"Target MongoDB connection string: {connection_string.replace(password if 'password' in locals() else '', '***')}")

        return connection_string

    @property
    def proxy_url(self) -> Optional[str]:
        """Получить URL прокси для Anthropic API"""
        if self.socks_proxy:
            return self.socks_proxy
        elif self.https_proxy:
            return self.https_proxy
        elif self.http_proxy:
            return self.http_proxy
        return None

    class Config:
        env_file = ".env"
        # Явно указываем, что переменные окружения не чувствительны к регистру
        case_sensitive = False


settings = Settings()