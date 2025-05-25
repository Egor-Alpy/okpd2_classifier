#!/usr/bin/env python3
"""
Скрипт инициализации базы данных
Создает необходимые индексы и проверяет подключения
"""
import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
import os
from dotenv import load_dotenv

load_dotenv()


async def check_source_mongo():
    """Проверка подключения к исходной MongoDB"""
    url = os.getenv("SOURCE_MONGODB_URL", "mongodb://localhost:27017")
    db_name = os.getenv("SOURCE_MONGODB_DATABASE", "source_products")
    collection_name = os.getenv("SOURCE_COLLECTION_NAME", "products")

    try:
        client = AsyncIOMotorClient(url)
        db = client[db_name]
        count = await db[collection_name].count_documents({})
        print(f"✅ Source MongoDB: Подключено. Найдено {count} товаров")
        client.close()
        return True
    except Exception as e:
        print(f"❌ Source MongoDB: Ошибка подключения - {e}")
        return False


async def init_target_mongo():
    """Инициализация целевой MongoDB"""
    url = os.getenv("TARGET_MONGODB_URL", "mongodb://localhost:27018")
    db_name = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")

    try:
        client = AsyncIOMotorClient(url)
        db = client[db_name]

        # Создаем индексы для products_stage_one
        products = db.products_stage_one
        await products.create_index([("old_mongo_id", 1), ("collection_name", 1)], unique=True)
        await products.create_index("status_stg1")
        await products.create_index("created_at")
        await products.create_index("okpd_group")

        # Индексы для migration_jobs
        migration_jobs = db.migration_jobs
        await migration_jobs.create_index("job_id", unique=True)

        # Индексы для classification_batches
        batches = db.classification_batches
        await batches.create_index("batch_id", unique=True)

        print("✅ Target MongoDB: Индексы созданы")

        # Выводим статистику
        count = await products.count_documents({})
        print(f"   Товаров в базе: {count}")

        client.close()
        return True
    except Exception as e:
        print(f"❌ Target MongoDB: Ошибка - {e}")
        return False


async def check_redis():
    """Проверка подключения к Redis"""
    url = os.getenv("REDIS_URL", "redis://localhost:6379")

    try:
        redis = await Redis.from_url(url)
        await redis.ping()
        print("✅ Redis: Подключено")
        await redis.close()
        return True
    except Exception as e:
        print(f"❌ Redis: Ошибка подключения - {e}")
        return False


async def check_anthropic():
    """Проверка наличия API ключа Anthropic"""
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if api_key and len(api_key) > 10:
        print("✅ Anthropic API key: Установлен")
        return True
    else:
        print("❌ Anthropic API key: Не установлен или неверный")
        return False


async def main():
    print("🚀 Инициализация OKPD2 Stage One Classifier\n")

    checks = [
        ("Source MongoDB", check_source_mongo()),
        ("Target MongoDB", init_target_mongo()),
        ("Redis", check_redis()),
        ("Anthropic API", check_anthropic())
    ]

    results = []
    for name, check in checks:
        result = await check
        results.append(result)

    print("\n" + "=" * 50)

    if all(results):
        print("✅ Все компоненты готовы к работе!")
        print("\nЗапустите миграцию командой:")
        print("python scripts/start_migration.py --api-key $API_KEY --monitor")
    else:
        print("❌ Некоторые компоненты не готовы. Проверьте настройки.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())