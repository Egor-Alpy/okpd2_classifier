#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с миграцией
"""
import asyncio
import os
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from dotenv import load_dotenv
from urllib.parse import quote_plus
from datetime import datetime

load_dotenv()


async def check_migration_status():
    """Комплексная проверка статуса миграции"""

    print("🔍 ДИАГНОСТИКА МИГРАЦИИ OKPD2")
    print("=" * 60)

    # 1. Проверка Target MongoDB
    print("\n1️⃣ Проверка Target MongoDB...")
    host = os.getenv("TARGET_MONGO_HOST", "localhost")
    port = os.getenv("TARGET_MONGO_PORT", "27017")
    database = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")

    try:
        client = AsyncIOMotorClient(f"mongodb://{host}:{port}", serverSelectionTimeoutMS=5000)
        await client.admin.command('ping')
        print("✅ Подключение к Target MongoDB успешно")

        db = client[database]

        # Проверяем коллекцию products_stage_one
        products_count = await db.products_stage_one.count_documents({})
        print(f"📦 Товаров в products_stage_one: {products_count}")

        if products_count == 0:
            print("⚠️  В базе нет товаров!")
        else:
            # Статистика по статусам
            statuses = ["pending", "processing", "classified", "none_classified", "failed"]
            print("\n📊 Статистика по статусам:")
            for status in statuses:
                count = await db.products_stage_one.count_documents({"status_stg1": status})
                if count > 0:
                    print(f"   - {status}: {count}")

        # Проверяем migration_jobs
        print("\n📋 Проверка задач миграции...")
        jobs_count = await db.migration_jobs.count_documents({})
        print(f"   Всего задач миграции: {jobs_count}")

        if jobs_count > 0:
            # Последняя задача
            last_job = await db.migration_jobs.find_one({}, sort=[("created_at", -1)])
            if last_job:
                print(f"\n   Последняя задача миграции:")
                print(f"   - Job ID: {last_job.get('job_id')}")
                print(f"   - Status: {last_job.get('status')}")
                print(f"   - Total: {last_job.get('total_products')}")
                print(f"   - Migrated: {last_job.get('migrated_products')}")
                print(f"   - Created: {last_job.get('created_at')}")
                print(f"   - Updated: {last_job.get('updated_at')}")

                if last_job.get('status') == 'running':
                    # Проверяем, обновляется ли задача
                    if last_job.get('updated_at'):
                        time_since_update = (datetime.utcnow() - last_job['updated_at']).total_seconds()
                        if time_since_update > 60:
                            print(f"   ⚠️  Задача не обновлялась {time_since_update:.0f} секунд!")

        client.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к Target MongoDB: {e}")
        return False

    # 2. Проверка Source MongoDB
    print("\n2️⃣ Проверка Source MongoDB...")
    source_host = os.getenv("SOURCE_MONGO_HOST")
    source_port = os.getenv("SOURCE_MONGO_PORT", "27017")
    source_user = os.getenv("SOURCE_MONGO_USER")
    source_pass = os.getenv("SOURCE_MONGO_PASS")
    source_authsource = os.getenv("SOURCE_MONGO_AUTHSOURCE")
    source_database = os.getenv("SOURCE_MONGODB_DATABASE")
    source_collection = os.getenv("SOURCE_COLLECTION_NAME")

    if source_user and source_pass:
        connection_string = f"mongodb://{source_user}:{quote_plus(source_pass)}@{source_host}:{source_port}"
        if source_authsource:
            connection_string += f"/{source_authsource}?authMechanism=SCRAM-SHA-256"
    else:
        connection_string = f"mongodb://{source_host}:{source_port}"

    print(f"   Host: {source_host}:{source_port}")
    print(f"   Database: {source_database}")
    print(f"   Collection: {source_collection}")

    try:
        source_client = AsyncIOMotorClient(
            connection_string,
            directConnection=os.getenv("SOURCE_MONGO_DIRECT_CONNECTION", "false").lower() == "true",
            serverSelectionTimeoutMS=5000
        )
        await source_client.admin.command('ping')
        print("✅ Подключение к Source MongoDB успешно")

        if source_database and source_collection:
            source_db = source_client[source_database]
            source_count = await source_db[source_collection].count_documents({})
            print(f"📦 Товаров в source: {source_count}")

            # Пример товара
            sample = await source_db[source_collection].find_one()
            if sample:
                print("\n   Пример товара из source:")
                print(f"   - ID: {sample.get('_id')}")
                print(f"   - Title: {sample.get('title', 'N/A')[:50]}...")

        source_client.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к Source MongoDB: {e}")
        print("   Это критическая ошибка - без доступа к source миграция невозможна!")
        return False

    # 3. Проверка Redis
    print("\n3️⃣ Проверка Redis...")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    try:
        redis = await Redis.from_url(redis_url)
        await redis.ping()
        print("✅ Подключение к Redis успешно")

        # Проверяем ключи
        keys = await redis.keys("*")
        print(f"📊 Найдено ключей в Redis: {len(keys)}")

        if keys:
            print("   Примеры ключей:")
            for key in keys[:5]:
                print(f"   - {key.decode('utf-8')}")

        await redis.close()

    except Exception as e:
        print(f"❌ Ошибка подключения к Redis: {e}")

    # 4. Проверка воркеров через Docker
    print("\n4️⃣ Проверка Docker контейнеров...")
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "table {{.Names}}\t{{.Status}}\t{{.State}}"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("⚠️  Не удалось получить статус контейнеров")
    except:
        print("⚠️  Docker не доступен")

    # 5. Рекомендации
    print("\n" + "=" * 60)
    print("💡 РЕКОМЕНДАЦИИ:")

    if products_count == 0 and jobs_count == 0:
        print("\n❌ Миграция не запущена или не работает!")
        print("\nВозможные причины:")
        print("1. Migration worker не запущен")
        print("2. Ошибка подключения к Source MongoDB")
        print("3. Миграция не была инициирована через API")
        print("\nЧто делать:")
        print("1. Проверьте логи: docker-compose logs migration-worker")
        print("2. Убедитесь, что вы запустили миграцию:")
        print("   python scripts/start_migration.py --api-key YOUR_KEY")
        print("3. Проверьте настройки Source MongoDB в .env")

    elif products_count == 0 and jobs_count > 0:
        print("\n⚠️  Задача миграции создана, но товары не переносятся!")
        print("\nВозможные причины:")
        print("1. Ошибка доступа к Source MongoDB")
        print("2. Неправильные настройки SOURCE_COLLECTION_NAME")
        print("3. Migration worker завис или упал")
        print("\nПроверьте логи migration-worker для деталей")

    elif products_count > 0:
        print("\n✅ Миграция работает, товары переносятся!")
        if products_count < 100:
            print("   Миграция только началась, подождите...")
        print(f"\nТекущий прогресс: {products_count} товаров")
        print("\nДля мониторинга используйте:")
        print("   python scripts/monitor_progress.py --api-key YOUR_KEY")

    return True


async def check_logs_hint():
    """Подсказки по проверке логов"""
    print("\n📋 КОМАНДЫ ДЛЯ ПРОВЕРКИ ЛОГОВ:")
    print("\n# Все логи:")
    print("docker-compose logs -f")
    print("\n# Только migration-worker:")
    print("docker-compose logs -f migration-worker")
    print("\n# Только API:")
    print("docker-compose logs -f api")
    print("\n# Последние 100 строк:")
    print("docker-compose logs --tail=100 migration-worker")


if __name__ == "__main__":
    asyncio.run(check_migration_status())
    asyncio.run(check_logs_hint())