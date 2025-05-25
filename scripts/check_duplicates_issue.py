#!/usr/bin/env python3
"""
Проверка проблемы с дубликатами и поиск товаров
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv()


async def investigate_duplicates():
    """Исследование проблемы с дубликатами"""

    print("🔍 ИССЛЕДОВАНИЕ ПРОБЛЕМЫ С ДУБЛИКАТАМИ")
    print("=" * 60)

    # Подключение к Target MongoDB
    host = os.getenv("TARGET_MONGO_HOST", "localhost")
    port = os.getenv("TARGET_MONGO_PORT", "27017")
    database = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")

    client = AsyncIOMotorClient(f"mongodb://{host}:{port}")
    db = client[database]

    # 1. Проверяем все коллекции в базе
    print("\n1️⃣ Все коллекции в базе данных:")
    collections = await db.list_collection_names()
    for collection in collections:
        count = await db[collection].count_documents({})
        print(f"   - {collection}: {count} документов")

    # 2. Детальная проверка products_stage_one
    print("\n2️⃣ Детальная проверка products_stage_one:")
    products = db.products_stage_one

    # Общее количество
    total = await products.count_documents({})
    print(f"   Всего документов: {total}")

    # Проверяем индексы
    indexes = await products.list_indexes().to_list(length=None)
    print("\n   Индексы:")
    for index in indexes:
        print(f"   - {index['name']}: {index['key']}")
        if 'unique' in index:
            print(f"     unique: {index['unique']}")

    # 3. Попробуем найти хотя бы один документ
    print("\n3️⃣ Поиск любого документа:")
    any_doc = await products.find_one()
    if any_doc:
        print(f"   Найден документ: {any_doc}")
    else:
        print("   ❌ Документы не найдены!")

    # 4. Проверяем уникальный индекс
    print("\n4️⃣ Проблема с уникальным индексом:")
    print("   Migration worker пытается вставить документы с полями:")
    print("   - old_mongo_id (ID из source)")
    print("   - collection_name (имя коллекции source)")
    print(f"   - collection_name = '{os.getenv('SOURCE_COLLECTION_NAME')}'")

    # 5. Попробуем найти документ по collection_name
    collection_name = os.getenv("SOURCE_COLLECTION_NAME", "products")
    docs_with_collection = await products.count_documents({"collection_name": collection_name})
    print(f"\n   Документов с collection_name='{collection_name}': {docs_with_collection}")

    # 6. Проверим, может товары в другой коллекции
    print("\n5️⃣ Поиск товаров во всех коллекциях:")
    for collection in collections:
        if collection != "products_stage_one":
            sample = await db[collection].find_one()
            if sample and 'title' in sample:
                print(f"   ⚠️ Найден товар в коллекции '{collection}':")
                print(f"      Title: {sample.get('title', 'N/A')}")
                print(f"      Fields: {list(sample.keys())}")

    # 7. Рекомендации
    print("\n" + "=" * 60)
    print("💡 ВОЗМОЖНЫЕ РЕШЕНИЯ:")

    if total == 0:
        print("\n1. Удалить уникальный индекс и пересоздать:")
        print("   await products.drop_index('old_mongo_id_1_collection_name_1')")
        print("\n2. Очистить базу данных и начать заново:")
        print("   await db.drop_collection('products_stage_one')")
        print("   await db.drop_collection('migration_jobs')")
        print("\n3. Проверить, правильное ли имя коллекции в SOURCE_COLLECTION_NAME")
        print(f"   Текущее значение: '{os.getenv('SOURCE_COLLECTION_NAME')}'")

    client.close()


async def fix_database():
    """Исправление проблемы с базой данных"""
    print("\n\n🔧 ИСПРАВЛЕНИЕ БАЗЫ ДАННЫХ")
    print("=" * 60)

    answer = input("\n❓ Хотите очистить базу данных и начать миграцию заново? (yes/no): ")

    if answer.lower() in ['yes', 'y']:
        host = os.getenv("TARGET_MONGO_HOST", "localhost")
        port = os.getenv("TARGET_MONGO_PORT", "27017")
        database = os.getenv("TARGET_MONGODB_DATABASE", "okpd_classifier")

        client = AsyncIOMotorClient(f"mongodb://{host}:{port}")
        db = client[database]

        print("\n🗑️ Очистка базы данных...")

        # Удаляем коллекции
        await db.drop_collection('products_stage_one')
        print("   ✅ Удалена коллекция products_stage_one")

        await db.drop_collection('migration_jobs')
        print("   ✅ Удалена коллекция migration_jobs")

        await db.drop_collection('classification_batches')
        print("   ✅ Удалена коллекция classification_batches")

        print("\n✨ База данных очищена!")
        print("\n📝 Теперь:")
        print("1. Перезапустите систему:")
        print("   docker-compose down")
        print("   docker-compose up -d")
        print("\n2. Инициализируйте базу:")
        print("   python scripts/init_db.py")
        print("\n3. Запустите миграцию:")
        print("   python scripts/start_migration.py --api-key your-api-key --monitor")

        client.close()
    else:
        print("❌ Операция отменена")


if __name__ == "__main__":
    asyncio.run(investigate_duplicates())
    asyncio.run(fix_database())