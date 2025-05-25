import asyncio
import logging
import sys

from src.services.ai_client import AnthropicClient
from src.storage.target_mongo import TargetMongoStore
from src.services.classifier import StageOneClassifier
from src.core.config import settings

# Настройка логирования для воркера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class ClassificationWorker:
    """Воркер для классификации товаров"""

    def __init__(self, worker_id: str = "worker_1"):
        self.worker_id = worker_id
        self.target_store = None
        self.classifier = None
        self.running = False
        logger.info(f"Initializing classification worker: {self.worker_id}")

    async def start(self):
        """Запустить воркер"""
        logger.info(f"Starting classification worker {self.worker_id}...")

        try:
            # Инициализируем компоненты
            logger.info("Connecting to target MongoDB...")
            self.target_store = TargetMongoStore(settings.target_mongodb_database)

            # Инициализируем target store (создание индексов)
            logger.info("Initializing target store and creating indexes...")
            await self.target_store.initialize()

            logger.info("Creating AI client...")
            logger.info(f"Using model: {settings.anthropic_model}")
            logger.info(f"Proxy configured: {'Yes' if settings.proxy_url else 'No'}")

            ai_client = AnthropicClient(
                settings.anthropic_api_key,
                settings.anthropic_model
            )

            logger.info(f"Creating classifier with batch_size={settings.classification_batch_size}")
            self.classifier = StageOneClassifier(
                ai_client,
                self.target_store,
                settings.classification_batch_size,
                worker_id=self.worker_id
            )

            self.running = True
            logger.info(f"Worker {self.worker_id} initialized successfully. Starting continuous classification...")

            # Запускаем непрерывную классификацию
            await self.classifier.run_continuous_classification()

        except KeyboardInterrupt:
            logger.info(f"Worker {self.worker_id} interrupted by user")
            raise
        except Exception as e:
            logger.error(f"Classification worker {self.worker_id} error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info(f"Stopping classification worker {self.worker_id}...")
        self.running = False

        if self.target_store:
            logger.info("Closing target store connection...")
            await self.target_store.close()

        logger.info(f"Worker {self.worker_id} stopped successfully")


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Classification worker')
    parser.add_argument('--worker-id', default='worker_1', help='Worker ID')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging level')
    args = parser.parse_args()

    # Настройка уровня логирования из аргументов
    log_level = getattr(logging, args.log_level.upper())
    logging.getLogger().setLevel(log_level)

    # Также настроим логирование для всех модулей src
    logging.getLogger('src').setLevel(log_level)

    logger.info("=" * 60)
    logger.info("OKPD2 Classification Worker Starting")
    logger.info("=" * 60)
    logger.info(f"Worker ID: {args.worker_id}")
    logger.info(f"Log Level: {args.log_level}")
    logger.info(f"Batch Size: {settings.classification_batch_size}")
    logger.info(f"Rate Limit Delay: {settings.rate_limit_delay}s")
    logger.info(f"Max Retries: {settings.max_retries}")
    logger.info("=" * 60)

    try:
        worker = ClassificationWorker(args.worker_id)
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())