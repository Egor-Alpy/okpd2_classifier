#!/usr/bin/env python3
"""
Скрипт для проверки данных в MongoDB и диагностики проблем
"""
import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()


async def check_mongodb():
    """Проверка данных в Target MongoDB"""
    # Параметры подключения
    host = os.getenv("TARGET_MONGO_HOST", "localhost")
    port = os.getenv("TARGET_MONGO_PORT", "27017")
    user = os.getenv("TARGET_MONGO_USER")
    password = os.getenv("TARGET_MONGO_PASS")
    authsource = os.getenv("TARGET_MONGO_AUTHSOURCE")
    authmechanism = os.getenv("TARGET_MONGO_AUTHMECHANISM", "SCRAM-SHA-256")
    database = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")

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

    print(f"🔍 Проверка MongoDB...")
    print(f"   Connection: {connection_string.replace(password or '', '***')}")
    print(f"   Database: {database}")
    print(f"   Host: {host}:{port}")
    print()

    try:
        # Подключаемся
        client = AsyncIOMotorClient(
            connection_string,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )

        # Проверяем подключение
        await client.admin.command('ping')
        print("✅ Подключение успешно!")

        # Получаем список всех баз данных
        all_dbs = await client.list_database_names()
        print(f"\n📂 Все базы данных: {', '.join(all_dbs)}")

        # Проверяем нашу базу
        if database in all_dbs:
            db = client[database]
            collections = await db.list_collection_names()
            print(f"\n📄 Коллекции в базе '{database}':")
            for collection in collections:
                count = await db[collection].count_documents({})
                print(f"   - {collection}: {count} документов")

            # Детальная статистика по products_stage_one
            if "products_stage_one" in collections:
                products = db.products_stage_one

                print(f"\n📊 Статистика products_stage_one:")

                # Общее количество
                total = await products.count_documents({})
                print(f"   Всего товаров: {total}")

                if total > 0:
                    # По статусам
                    statuses = ["pending", "processing", "classified", "none_classified", "failed"]
                    print(f"\n   По статусам:")
                    for status in statuses:
                        count = await products.count_documents({"status_stg1": status})
                        if count > 0:
                            print(f"   - {status}: {count}")

                    # Примеры товаров
                    print(f"\n   Примеры товаров:")
                    cursor = products.find().limit(3)
                    async for product in cursor:
                        print(f"\n   ID: {product['_id']}")
                        print(f"   Title: {product.get('title', 'N/A')}")
                        print(f"   Status: {product.get('status_stg1', 'N/A')}")
                        print(f"   OKPD Groups: {product.get('okpd_group', 'N/A')}")
                        print(f"   Created: {product.get('created_at', 'N/A')}")

                    # Проверяем migration_jobs
                    if "migration_jobs" in collections:
                        jobs = db.migration_jobs
                        job_count = await jobs.count_documents({})
                        print(f"\n   Migration jobs: {job_count}")

                        if job_count > 0:
                            last_job = await jobs.find_one({}, sort=[("created_at", -1)])
                            print(f"   Последняя миграция:")
                            print(f"   - Job ID: {last_job.get('job_id')}")
                            print(f"   - Status: {last_job.get('status')}")
                            print(f"   - Total: {last_job.get('total_products')}")
                            print(f"   - Migrated: {last_job.get('migrated_products')}")
                else:
                    print("\n⚠️  В базе нет товаров!")
                    print("\nВозможные причины:")
                    print("1. Миграция еще не запускалась")
                    print("2. Ошибка при миграции из source MongoDB")
                    print("3. Неправильные настройки подключения к source MongoDB")

        else:
            print(f"\n❌ База данных '{database}' не найдена!")
            print(f"   Доступные базы: {', '.join(all_dbs)}")

        # Закрываем соединение
        client.close()

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        return False

    return True


async def check_source_mongodb():
    """Проверка Source MongoDB"""
    print("\n" + "=" * 60)
    print("🔍 Проверка Source MongoDB...")

    host = os.getenv("SOURCE_MONGO_HOST", "localhost")
    port = os.getenv("SOURCE_MONGO_PORT", "27017")
    user = os.getenv("SOURCE_MONGO_USER")
    password = os.getenv("SOURCE_MONGO_PASS")
    authsource = os.getenv("SOURCE_MONGO_AUTHSOURCE")
    authmechanism = os.getenv("SOURCE_MONGO_AUTHMECHANISM", "SCRAM-SHA-256")
    database = os.getenv("SOURCE_MONGODB_DATABASE")
    collection = os.getenv("SOURCE_COLLECTION_NAME")
    direct_connection = os.getenv("SOURCE_MONGO_DIRECT_CONNECTION", "false").lower() == "true"

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

    print(f"   Connection: {connection_string.replace(password or '', '***')}")
    print(f"   Database: {database}")
    print(f"   Collection: {collection}")

    try:
        client = AsyncIOMotorClient(
            connection_string,
            directConnection=direct_connection,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )

        await client.admin.command('ping')
        print("✅ Подключение успешно!")

        if database and collection:
            db = client[database]
            count = await db[collection].count_documents({})
            print(f"   Товаров в source: {count}")

            if count > 0:
                # Показываем пример товара
                sample = await db[collection].find_one()
                if sample:
                    print("\n   Пример товара:")
                    print(f"   - ID: {sample.get('_id')}")
                    print(f"   - Title: {sample.get('title', 'N/A')}")
                    print(f"   - Category: {sample.get('category', 'N/A')}")

        client.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к Source MongoDB: {e}")


async def main():
    print("🔧 Диагностика MongoDB для OKPD2 Classifier\n")

    # Проверяем Target MongoDB
    await check_mongodb()

    # Проверяем Source MongoDB
    await check_source_mongodb()

    print("\n" + "=" * 60)
    print("\n💡 Советы по просмотру в MongoDB Compass:")
    print("1. Подключитесь к: mongodb://localhost:27017")
    print("2. Откройте базу данных: okpd_classifier")
    print("3. Откройте коллекцию: products_stage_one")
    print("\nЕсли данных нет:")
    print("- Убедитесь, что миграция была запущена")
    print("- Проверьте логи контейнеров: docker-compose logs -f")
    print("- Запустите миграцию: make migration-start API_KEY=your-key")


if __name__ == "__main__":
    asyncio.run(main())