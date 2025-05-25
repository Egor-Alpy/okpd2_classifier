#!/usr/bin/env python3
"""
Скрипт для сброса статуса failed товаров обратно в pending
"""
import asyncio
import sys
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()


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


async def reset_failed_products():
    """Сбросить статус failed товаров"""
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
        products = db.products_stage_one

        # Считаем сколько failed товаров
        failed_count = await products.count_documents({"status_stg1": "failed"})
        print(f"Found {failed_count} failed products")

        if failed_count > 0:
            # Сбрасываем статус на pending
            result = await products.update_many(
                {"status_stg1": "failed"},
                {
                    "$set": {
                        "status_stg1": "pending",
                        "error_message": None,
                        "batch_id": None,
                        "worker_id": None
                    }
                }
            )
            print(f"Reset {result.modified_count} products to pending status")

        client.close()
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False


async def get_stats():
    """Получить статистику"""
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
        products = db.products_stage_one

        total = await products.count_documents({})
        pending = await products.count_documents({"status_stg1": "pending"})
        processing = await products.count_documents({"status_stg1": "processing"})
        classified = await products.count_documents({"status_stg1": "classified"})
        none_classified = await products.count_documents({"status_stg1": "none_classified"})
        failed = await products.count_documents({"status_stg1": "failed"})

        print("\n📊 Product Statistics:")
        print(f"Total: {total}")
        print(f"Pending: {pending}")
        print(f"Processing: {processing}")
        print(f"Classified: {classified}")
        print(f"None Classified: {none_classified}")
        print(f"Failed: {failed}")

        if total > 0:
            print(f"\nProgress: {((classified + none_classified) / total * 100):.1f}%")

        client.close()

    except Exception as e:
        print(f"Error getting stats: {e}")


async def main():
    # Сначала показываем статистику
    await get_stats()

    # Спрашиваем подтверждение
    print("\n❓ Do you want to reset all failed products to pending status? (yes/no): ", end="")
    answer = input().strip().lower()

    if answer in ['yes', 'y']:
        await reset_failed_products()
        # Показываем обновленную статистику
        await get_stats()
    else:
        print("❌ Operation cancelled")


if __name__ == "__main__":
    asyncio.run(main())