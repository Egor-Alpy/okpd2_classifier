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
from urllib.parse import quote_plus

load_dotenv()


def get_source_mongodb_connection_string():
    """Формирование строки подключения для Source MongoDB"""
    host = os.getenv("SOURCE_MONGO_HOST", "localhost")
    port = os.getenv("SOURCE_MONGO_PORT", "27017")
    user = os.getenv("SOURCE_MONGO_USER")
    password = os.getenv("SOURCE_MONGO_PASS")
    authsource = os.getenv("SOURCE_MONGO_AUTHSOURCE")
    authmechanism = os.getenv("SOURCE_MONGO_AUTHMECHANISM", "SCRAM-SHA-256")

    if user and password:
        connection_string = f"mongodb://{user}:{quote_plus(password)}@{host}:{port}"

        if authsource:
            connection_string += f"/{authsource}"
            connection_string += f"?authMechanism={authmechanism}"
        else:
            connection_string += f"/?authMechanism={authmechanism}"
    else:
        connection_string = f"mongodb://{host}:{port}"

    return connection_string


def get_target_mongodb_connection_string():
    """Формирование строки подключения для Target MongoDB"""
    host = os.getenv("TARGET_MONGO_HOST", "localhost")
    port = os.getenv("TARGET_MONGO_PORT", "27017")
    user = os.getenv("TARGET_MONGO_USER")
    password = os.getenv("TARGET_MONGO_PASS")
    authsource = os.getenv("TARGET_MONGO_AUTHSOURCE")
    authmechanism = os.getenv("TARGET_MONGO_AUTHMECHANISM", "SCRAM-SHA-256")

    if user and password:
        connection_string = f"mongodb://{user}:{quote_plus(password)}@{host}:{port}"

        if authsource:
            connection_string += f"/{authsource}"
            connection_string += f"?authMechanism={authmechanism}"
        else:
            connection_string += f"/?authMechanism={authmechanism}"
    else:
        connection_string = f"mongodb://{host}:{port}"

    return connection_string


async def check_source_mongo():
    """Проверка подключения к исходной MongoDB"""
    connection_string = get_source_mongodb_connection_string()
    db_name = os.getenv("SOURCE_MONGODB_DATABASE", "source_products")
    collection_name = os.getenv("SOURCE_COLLECTION_NAME", "products")
    direct_connection = os.getenv("SOURCE_MONGO_DIRECT_CONNECTION", "false").lower() == "true"

    try:
        client = AsyncIOMotorClient(
            connection_string,
            directConnection=direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
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
    connection_string = get_target_mongodb_connection_string()
    db_name = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")
    direct_connection = os.getenv("TARGET_MONGO_DIRECT_CONNECTION", "false").lower() == "true"

    try:
        client = AsyncIOMotorClient(
            connection_string,
            directConnection=direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        db = client[db_name]

        # Создаем индексы для products_stage_one
        products = db.products_stage_one
        await products.create_index([("old_mongo_id", 1), ("collection_name", 1)], unique=True)
        await products.create_index("status_stg1")
        await products.create_index("created_at")
        await products.create_index("okpd_group")
        await products.create_index([("status_stg1", 1), ("created_at", 1)])

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