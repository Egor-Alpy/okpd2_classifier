import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis.asyncio import Redis

from api.router import router as api_router
from core.config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Глобальная переменная для Redis
redis_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    global redis_client

    # Startup
    logger.info("Starting application...")

    # Подключаемся к Redis если нужен для API
    try:
        redis_client = await Redis.from_url(settings.redis_url)
        await redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.warning(f"Failed to connect to Redis: {e}")
        redis_client = None

    yield

    # Shutdown
    logger.info("Shutting down application...")

    if redis_client:
        await redis_client.close()
        logger.info("Disconnected from Redis")


# Создание приложения
app = FastAPI(
    title="OKPD2 Stage One Classifier",
    description="Первый этап классификации товаров по ОКПД2",
    version="1.0.0",
    lifespan=lifespan
)

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(api_router, prefix="/api/v1")


# Health check
@app.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {
        "status": "healthy",
        "service": "OKPD2 Stage One Classifier"
    }


# Root endpoint
@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "service": "OKPD2 Stage One Classifier",
        "version": "1.0.0",
        "docs": "/docs"
    }