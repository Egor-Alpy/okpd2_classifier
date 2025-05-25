import asyncio
import logging

from src.services.ai_client import AnthropicClient
from src.storage.target_mongo import TargetMongoStore
from src.services.classifier import StageOneClassifier
from src.core.config import settings

logger = logging.getLogger(__name__)


class ClassificationWorker:
    """Воркер для классификации товаров"""

    def __init__(self, worker_id: str = "worker_1"):
        self.worker_id = worker_id
        self.target_store = None
        self.classifier = None
        self.running = False

    async def start(self):
        """Запустить воркер"""
        logger.info(f"Starting classification worker {self.worker_id}...")

        # Инициализируем компоненты
        self.target_store = TargetMongoStore(settings.target_mongodb_database)

        # Инициализируем target store (создание индексов)
        await self.target_store.initialize()

        ai_client = AnthropicClient(
            settings.anthropic_api_key,
            settings.anthropic_model
        )

        self.classifier = StageOneClassifier(
            ai_client,
            self.target_store,
            settings.classification_batch_size,
            worker_id=self.worker_id  # Передаем worker_id
        )

        self.running = True

        try:
            # Запускаем непрерывную классификацию
            await self.classifier.run_continuous_classification()
        except Exception as e:
            logger.error(f"Classification worker {self.worker_id} error: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info(f"Stopping classification worker {self.worker_id}...")
        self.running = False

        if self.target_store:
            await self.target_store.close()


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Classification worker')
    parser.add_argument('--worker-id', default='worker_1', help='Worker ID')
    args = parser.parse_args()

    worker = ClassificationWorker(args.worker_id)
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())