#!/usr/bin/env python3
"""
Комплексная проверка готовности системы к работе
"""
import asyncio
import sys
import os
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from anthropic import AsyncAnthropic
from dotenv import load_dotenv
from urllib.parse import quote_plus
import aiohttp

load_dotenv()


class SystemReadinessChecker:
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []

    async def check_source_mongodb(self):
        """Проверка Source MongoDB"""
        print("\n1️⃣ Проверка Source MongoDB...")

        try:
            from src.core.config import settings
            from src.storage.source_mongo import SourceMongoStore

            store = SourceMongoStore(
                settings.source_mongodb_database,
                settings.source_collection_name
            )

            # Тест подключения
            if not await store.test_connection():
                self.checks_failed += 1
                print("   ❌ Не удалось подключиться")
                return False

            # Проверка данных
            count = await store.count_total_products()
            print(f"   ✅ Подключено. Найдено товаров: {count:,}")

            if count == 0:
                self.warnings.append("Source MongoDB не содержит товаров")

            # Проверка примера товара
            products = await store.get_products_batch(limit=1)
            if products:
                product = products[0]
                print(f"   ✅ Пример товара: {product.get('title', 'N/A')[:50]}...")

            await store.close()
            self.checks_passed += 1
            return True

        except Exception as e:
            self.checks_failed += 1
            print(f"   ❌ Ошибка: {e}")
            return False

    async def check_target_mongodb(self):
        """Проверка Target MongoDB"""
        print("\n2️⃣ Проверка Target MongoDB...")

        try:
            from src.core.config import settings
            from src.storage.target_mongo import TargetMongoStore

            store = TargetMongoStore(settings.target_mongodb_database)

            # Тест подключения
            if not await store.test_connection():
                self.checks_failed += 1
                print("   ❌ Не удалось подключиться")
                return False

            # Инициализация (создание индексов)
            await store.initialize()
            print("   ✅ Подключено и инициализировано")

            # Статистика
            stats = await store.get_statistics()
            print(f"   📊 Статистика:")
            print(f"      Всего: {stats['total']:,}")
            print(f"      Ожидает: {stats['pending']:,}")
            print(f"      Классифицировано: {stats['classified']:,}")
            print(f"      Не классифицировано: {stats['none_classified']:,}")
            print(f"      Ошибки: {stats['failed']:,}")
            print(f"      В обработке: {stats['processing']:,}")

            if stats['processing'] > 100:
                self.warnings.append(f"Много товаров застряло в обработке: {stats['processing']}")

            await store.close()
            self.checks_passed += 1
            return True

        except Exception as e:
            self.checks_failed += 1
            print(f"   ❌ Ошибка: {e}")
            return False

    async def check_redis(self):
        """Проверка Redis"""
        print("\n3️⃣ Проверка Redis...")

        try:
            from src.core.config import settings

            redis = await Redis.from_url(settings.redis_url)
            await redis.ping()
            print("   ✅ Подключено")

            # Проверка ключей
            keys = await redis.keys("*")
            print(f"   📊 Найдено ключей: {len(keys)}")

            await redis.close()
            self.checks_passed += 1
            return True

        except Exception as e:
            self.checks_failed += 1
            print(f"   ❌ Ошибка: {e}")
            return False

    async def check_anthropic_api(self):
        """Проверка Anthropic API"""
        print("\n4️⃣ Проверка Anthropic API...")

        try:
            from src.core.config import settings

            if not settings.anthropic_api_key:
                self.checks_failed += 1
                print("   ❌ API ключ не установлен")
                return False

            print(f"   🔑 API Key: {settings.anthropic_api_key[:10]}...{settings.anthropic_api_key[-5:]}")
            print(f"   🤖 Модель: {settings.anthropic_model}")

            # Тест вызова API
            from src.services.ai_client import AnthropicClient

            async with AnthropicClient(
                    settings.anthropic_api_key,
                    settings.anthropic_model
            ) as client:
                response = await client.classify_batch(
                    "Тест подключения. Ответь 'OK' если видишь это сообщение.",
                    max_tokens=10
                )

                if "OK" in response or "ok" in response.lower():
                    print(f"   ✅ API работает. Ответ: {response.strip()}")
                    self.checks_passed += 1
                    return True
                else:
                    print(f"   ⚠️  Неожиданный ответ: {response}")
                    self.warnings.append("Anthropic API вернул неожиданный ответ")
                    self.checks_passed += 1
                    return True

        except Exception as e:
            self.checks_failed += 1
            error_str = str(e)

            if "429" in error_str:
                print("   ❌ Rate limit превышен")
                self.warnings.append("Возможны проблемы с rate limits")
            elif "403" in error_str:
                print("   ❌ Доступ запрещен (проверьте API ключ)")
            elif "connection" in error_str.lower():
                print("   ❌ Проблема с подключением (возможно нужен прокси)")
                self.warnings.append("Рассмотрите использование прокси для Anthropic API")
            else:
                print(f"   ❌ Ошибка: {error_str[:100]}...")

            return False

    async def check_api_server(self):
        """Проверка API сервера"""
        print("\n5️⃣ Проверка API сервера...")

        try:
            async with aiohttp.ClientSession() as session:
                # Health check
                async with session.get("http://localhost:8000/health") as resp:
                    if resp.status == 200:
                        print("   ✅ API сервер работает")
                        self.checks_passed += 1
                        return True
                    else:
                        print(f"   ❌ API сервер вернул статус {resp.status}")
                        self.checks_failed += 1
                        return False

        except Exception as e:
            print("   ⚠️  API сервер не запущен (это нормально перед первым запуском)")
            self.warnings.append("API сервер не запущен")
            return True

    async def check_settings(self):
        """Проверка настроек"""
        print("\n6️⃣ Проверка настроек...")

        from src.core.config import settings

        # Проверка критичных настроек
        print(f"   📦 Размер батча для миграции: {settings.migration_batch_size}")
        print(f"   📦 Размер батча для классификации: {settings.classification_batch_size}")
        print(f"   ⏱️  Задержка между батчами: {settings.rate_limit_delay}с")
        print(f"   🔄 Максимум попыток: {settings.max_retries}")
        print(f"   👷 Количество воркеров: {settings.max_workers}")

        # Предупреждения о настройках
        if settings.classification_batch_size > 20:
            self.warnings.append(
                f"Большой размер батча ({settings.classification_batch_size}) "
                "может привести к rate limits"
            )

        if settings.rate_limit_delay < 5:
            self.warnings.append(
                f"Маленькая задержка ({settings.rate_limit_delay}с) "
                "может привести к rate limits"
            )

        if settings.max_workers > 3:
            self.warnings.append(
                f"Много воркеров ({settings.max_workers}) "
                "может привести к rate limits"
            )

        # Проверка прокси
        if settings.proxy_url:
            print(f"   🌐 Прокси настроен: {settings.proxy_url}")
        else:
            print("   🌐 Прокси не настроен")

        self.checks_passed += 1
        return True

    async def run_all_checks(self):
        """Запустить все проверки"""
        print("🔧 ПРОВЕРКА ГОТОВНОСТИ СИСТЕМЫ OKPD2 CLASSIFIER")
        print("=" * 60)

        # Запускаем все проверки
        await self.check_source_mongodb()
        await self.check_target_mongodb()
        await self.check_redis()
        await self.check_anthropic_api()
        await self.check_api_server()
        await self.check_settings()

        # Итоги
        print("\n" + "=" * 60)
        print("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ:")
        print(f"   ✅ Успешно: {self.checks_passed}")
        print(f"   ❌ Ошибок: {self.checks_failed}")
        print(f"   ⚠️  Предупреждений: {len(self.warnings)}")

        if self.warnings:
            print("\n⚠️  ПРЕДУПРЕЖДЕНИЯ:")
            for warning in self.warnings:
                print(f"   - {warning}")

        if self.checks_failed == 0:
            print("\n✅ СИСТЕМА ГОТОВА К РАБОТЕ!")
            print("\n🚀 Для запуска:")
            print("   1. docker-compose up -d")
            print("   2. make migration-start API_KEY=your-api-key")
            print("   3. make monitor API_KEY=your-api-key")
            return True
        else:
            print("\n❌ СИСТЕМА НЕ ГОТОВА К РАБОТЕ")
            print("   Исправьте ошибки и повторите проверку")
            return False


async def main():
    checker = SystemReadinessChecker()
    success = await checker.run_all_checks()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
