#!/usr/bin/env python3
"""
Скрипт для тестирования подключения к MongoDB
"""
import asyncio
import sys
import os
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()


async def test_connection(
        host: str,
        port: int,
        user: str = None,
        password: str = None,
        authsource: str = None,
        authmechanism: str = "SCRAM-SHA-256",
        database: str = "test",
        direct_connection: bool = False
):
    """Тестирование подключения к MongoDB"""

    # Формируем connection string
    if user and password:
        connection_string = f"mongodb://{user}:{quote_plus(password)}@{host}:{port}"

        if authsource:
            connection_string += f"/{authsource}"
            connection_string += f"?authMechanism={authmechanism}"
        else:
            connection_string += f"/?authMechanism={authmechanism}"
    else:
        connection_string = f"mongodb://{host}:{port}"

    print(f"\n🔍 Тестирование подключения к MongoDB...")
    print(f"   Host: {host}:{port}")
    print(f"   User: {user or 'none'}")
    print(f"   Auth Source: {authsource or 'none'}")
    print(f"   Direct Connection: {direct_connection}")

    # Скрываем пароль в выводе
    safe_connection_string = connection_string.replace(password, "***") if password else connection_string
    print(f"   Connection String: {safe_connection_string}")

    try:
        client = AsyncIOMotorClient(
            connection_string,
            directConnection=direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )

        # Проверяем подключение
        result = await client.admin.command('ping')
        print(f"✅ Подключение успешно! Ping result: {result}")

        # Получаем список баз данных
        dbs = await client.list_database_names()
        print(f"\n📂 Доступные базы данных: {', '.join(dbs)}")

        # Проверяем конкретную базу если указана
        if database in dbs:
            db = client[database]
            collections = await db.list_collection_names()
            print(f"\n📄 Коллекции в базе '{database}': {', '.join(collections)}")

            # Считаем документы в первой коллекции
            if collections:
                first_collection = collections[0]
                count = await db[first_collection].count_documents({})
                print(f"   Документов в '{first_collection}': {count}")

        client.close()
        return True

    except Exception as e:
        print(f"\n❌ Ошибка подключения: {e}")
        return False


async def main():
    print("🔧 Тест подключения к MongoDB\n")

    # Проверяем Source MongoDB
    print("1️⃣ SOURCE MONGODB:")
    source_success = await test_connection(
        host=os.getenv("SOURCE_MONGO_HOST", "localhost"),
        port=int(os.getenv("SOURCE_MONGO_PORT", 27017)),
        user=os.getenv("SOURCE_MONGO_USER"),
        password=os.getenv("SOURCE_MONGO_PASS"),
        authsource=os.getenv("SOURCE_MONGO_AUTHSOURCE"),
        authmechanism=os.getenv("SOURCE_MONGO_AUTHMECHANISM", "SCRAM-SHA-256"),
        database=os.getenv("SOURCE_MONGODB_DATABASE", "source_products"),
        direct_connection=os.getenv("SOURCE_MONGO_DIRECT_CONNECTION", "false").lower() == "true"
    )

    print("\n" + "=" * 60 + "\n")

    # Проверяем Target MongoDB
    print("2️⃣ TARGET MONGODB:")
    target_success = await test_connection(
        host=os.getenv("TARGET_MONGO_HOST", "localhost"),
        port=int(os.getenv("TARGET_MONGO_PORT", 27017)),
        user=os.getenv("TARGET_MONGO_USER"),
        password=os.getenv("TARGET_MONGO_PASS"),
        authsource=os.getenv("TARGET_MONGO_AUTHSOURCE"),
        authmechanism=os.getenv("TARGET_MONGO_AUTHMECHANISM", "SCRAM-SHA-256"),
        database=os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier"),
        direct_connection=os.getenv("TARGET_MONGO_DIRECT_CONNECTION", "false").lower() == "true"
    )

    print("\n" + "=" * 60 + "\n")

    if source_success and target_success:
        print("✅ Все подключения работают!")
    else:
        print("❌ Есть проблемы с подключением. Проверьте настройки в .env")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())