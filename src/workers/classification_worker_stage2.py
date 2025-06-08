import asyncio
import logging
import sys
from typing import Optional

from src.services.ai_client import AnthropicClient
from src.storage.target_mongo import TargetMongoStore
from src.services.classifier_stage2 import StageTwoClassifier
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


class ClassificationWorkerStage2:
    """Воркер для классификации товаров на втором этапе"""

    def __init__(self, worker_id: str = "stage2_worker_1"):
        self.worker_id = worker_id
        self.target_store = None
        self.classifier = None
        self.running = False
        logger.info(f"Initializing stage 2 classification worker: {self.worker_id}")

    async def start(self):
        """Запустить воркер"""
        logger.info(f"Starting stage 2 classification worker {self.worker_id}...")

        try:
            # Инициализируем компоненты
            logger.info("Connecting to target MongoDB...")
            self.target_store = TargetMongoStore(
                settings.target_mongodb_database,
                settings.target_collection_name
            )

            # Инициализируем target store
            logger.info("Initializing target store...")
            await self.target_store.initialize()

            # Проверяем наличие товаров для второго этапа
            count = await self.target_store.products.count_documents({
                "status_stage1": "classified",
                "okpd_groups": {"$exists": True, "$ne": []},  # ✅ ИСПРАВЛЕНО
                "$or": [
                    {"status_stage2": {"$exists": False}},
                    {"status_stage2": "pending"}
                ]
            })
            logger.info(f"Found {count} products ready for stage 2 classification")

            if count == 0:
                logger.warning("No products found for stage 2 classification!")
                logger.info("Make sure stage 1 classification is completed first.")
                return

            logger.info("Creating AI client...")
            logger.info(f"Using model: {settings.anthropic_model}")
            logger.info(f"Proxy configured: {'Yes' if settings.proxy_url else 'No'}")

            ai_client = AnthropicClient(
                settings.anthropic_api_key,
                settings.anthropic_model
            )

            # Используем меньший размер батча для второго этапа
            batch_size = min(settings.classification_batch_size, 15)

            logger.info(f"Creating stage 2 classifier with batch_size={batch_size}")
            self.classifier = StageTwoClassifier(
                ai_client,
                self.target_store,
                batch_size,
                worker_id=self.worker_id
            )

            # Проверяем наличие файла с полным деревом ОКПД2
            import os
            if not os.path.exists("src/data/okpd2_full_tree.json"):
                logger.error("OKPD2 full tree file not found at src/data/okpd2_full_tree.json")
                logger.error("Please create this file with the complete OKPD2 hierarchy")
                logger.error("Format: {\"XX\": {\"XX.XX.X\": \"Description\", ...}, ...}")
                return

            self.running = True
            logger.info(f"Worker {self.worker_id} initialized successfully. Starting continuous classification...")

            # Запускаем непрерывную классификацию
            await self.classifier.run_continuous_classification()

        except KeyboardInterrupt:
            logger.info(f"Stage 2 worker {self.worker_id} interrupted by user")
            raise
        except Exception as e:
            logger.error(f"Stage 2 classification worker {self.worker_id} error: {e}", exc_info=True)
            raise
        finally:
            await self.stop()

    async def stop(self):
        """Остановить воркер"""
        logger.info(f"Stopping stage 2 classification worker {self.worker_id}...")
        self.running = False

        if self.target_store:
            logger.info("Closing target store connection...")
            await self.target_store.close()

        logger.info(f"Stage 2 worker {self.worker_id} stopped successfully")


async def main():
    """Запуск воркера из командной строки"""
    import argparse

    parser = argparse.ArgumentParser(description='Stage 2 Classification worker')
    parser.add_argument('--worker-id', default='stage2_worker_1', help='Worker ID')
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
    logger.info("OKPD2 Stage 2 Classification Worker Starting")
    logger.info("=" * 60)
    logger.info(f"Worker ID: {args.worker_id}")
    logger.info(f"Log Level: {args.log_level}")
    logger.info(f"Batch Size: {min(settings.classification_batch_size, 15)}")
    logger.info(f"Rate Limit Delay: {settings.rate_limit_delay}s")
    logger.info(f"Max Retries: {settings.max_retries}")
    logger.info("=" * 60)
    logger.info("ВАЖНО: Второй этап теперь обрабатывает все топ-5 групп одновременно!")
    logger.info("=" * 60)

    try:
        worker = ClassificationWorkerStage2(args.worker_id)
        await worker.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())