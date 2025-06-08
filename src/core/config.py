from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Optional
from urllib.parse import quote


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
        # Для паролей логируем наличие специальных символов (для отладки)
        if isinstance(v, str) and any(c in v for c in ['+', '@', ':', '/', '?', '#', '[', ']', '%']):
            import logging
            logger = logging.getLogger(__name__)
            # Маскируем пароль, показывая только первые и последние символы
            masked = f"{v[:2]}...{v[-2:]}" if len(v) > 4 else "***"
            logger.debug(f"Password/value contains special characters: {masked}")
        return v

    @property
    def source_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Source MongoDB"""
        import logging
        logger = logging.getLogger(__name__)

        # Проверяем, что user и pass не пустые (не None и не пустая строка)
        if self.source_mongo_user and self.source_mongo_pass:
            # URL encode пароля для безопасности
            # Используем quote() вместо quote_plus() для MongoDB
            user = quote(self.source_mongo_user, safe='')
            password = quote(self.source_mongo_pass, safe='')

            connection_string = (
                f"mongodb://{user}:{password}@"
                f"{self.source_mongo_host}:{self.source_mongo_port}"
            )

            # Добавляем базу данных в путь
            connection_string += f"/{self.source_mongodb_database}"

            params = []
            if self.source_mongo_authsource:
                params.append(f"authSource={self.source_mongo_authsource}")
            else:
                # Если не указан authSource, используем исходную БД
                params.append(f"authSource={self.source_mongodb_database}")

            params.append(f"authMechanism={self.source_mongo_authmechanism}")

            if params:
                connection_string += "?" + "&".join(params)

            logger.debug(f"Source MongoDB connection params: {params}")
        else:
            connection_string = f"mongodb://{self.source_mongo_host}:{self.source_mongo_port}"

        return connection_string

    @property
    def target_mongodb_connection_string(self) -> str:
        """Формирование строки подключения для Target MongoDB"""
        import logging
        logger = logging.getLogger(__name__)

        # Проверяем, что user и pass не пустые (не None и не пустая строка)
        if self.target_mongo_user and self.target_mongo_pass:
            # URL encode для безопасности (особенно важно для паролей со спецсимволами)
            # Используем quote() вместо quote_plus() для MongoDB
            # safe='' означает, что кодируются ВСЕ специальные символы
            user = quote(self.target_mongo_user, safe='')
            password = quote(self.target_mongo_pass, safe='')

            # Логируем для отладки (маскируем пароль)
            logger.debug(f"Target MongoDB user: {self.target_mongo_user}")
            logger.debug(
                f"Target MongoDB password contains special chars: {any(c in self.target_mongo_pass for c in '+@:/?#[]%')}")
            logger.debug(f"Encoded user length: {len(user)}, password length: {len(password)}")

            connection_string = (
                f"mongodb://{user}:{password}@"
                f"{self.target_mongo_host}:{self.target_mongo_port}"
            )

            # Добавляем параметры аутентификации
            params = []

            # ВАЖНО: Добавляем базу данных в строку подключения
            connection_string += f"/{self.target_mongodb_database}"

            if self.target_mongo_authsource:
                params.append(f"authSource={self.target_mongo_authsource}")
            else:
                # Если не указан authSource, используем целевую БД
                params.append(f"authSource={self.target_mongodb_database}")

            params.append(f"authMechanism={self.target_mongo_authmechanism}")

            if params:
                connection_string += "?" + "&".join(params)

            logger.debug(f"Target MongoDB connection params: {params}")

        else:
            # Если нет учетных данных, подключаемся без аутентификации
            connection_string = f"mongodb://{self.target_mongo_host}:{self.target_mongo_port}"

            # Добавим предупреждение для отладки
            logger.warning(
                f"Target MongoDB connection without authentication. "
                f"User: {'set' if self.target_mongo_user else 'not set'}, "
                f"Pass: {'set' if self.target_mongo_pass else 'not set'}"
            )

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