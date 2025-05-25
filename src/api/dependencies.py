from fastapi import Depends, HTTPException, Header
from typing import Optional

from src.core.config import settings
from src.storage.target_mongo import TargetMongoStore

async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Проверка API ключа"""
    if not x_api_key or x_api_key != settings.api_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return x_api_key

async def get_target_store() -> TargetMongoStore:
    """Получить экземпляр TargetMongoStore"""
    return TargetMongoStore(settings.target_mongodb_database)