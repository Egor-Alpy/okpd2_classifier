import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.router import router as api_router

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    logger.info("Starting application...")
    yield
    # Shutdown
    logger.info("Shutting down application...")


# Создание приложения
app = FastAPI(
    title="OKPD2 Classifier API",
    description="API для классификации товаров по ОКПД2",
    version="2.0.0",
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
        "service": "OKPD2 Classifier API"
    }


# Root endpoint
@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "service": "OKPD2 Classifier API",
        "version": "2.0.0",
        "docs": "/docs",
        "endpoints": {
            "statistics": "/api/v1/stats",
            "tender_classification": "/api/v1/tender/classify-positions"
        }
    }