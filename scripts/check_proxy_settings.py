#!/usr/bin/env python3
"""
Проверка настроек прокси и их влияния на MongoDB
"""
import os
import sys
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()

print("=" * 60)
print("ПРОВЕРКА НАСТРОЕК ПРОКСИ")
print("=" * 60)

# Проверяем переменные окружения
proxy_vars = {
    "HTTP_PROXY": os.getenv("HTTP_PROXY", ""),
    "HTTPS_PROXY": os.getenv("HTTPS_PROXY", ""),
    "SOCKS_PROXY": os.getenv("SOCKS_PROXY", ""),
    "NO_PROXY": os.getenv("NO_PROXY", ""),
    "http_proxy": os.getenv("http_proxy", ""),
    "https_proxy": os.getenv("https_proxy", ""),
    "no_proxy": os.getenv("no_proxy", "")
}

print("\nТекущие настройки прокси:")
for var, value in proxy_vars.items():
    if value:
        print(f"  {var}: {value}")

if not any(proxy_vars.values()):
    print("  Прокси не настроен")

# Проверяем NO_PROXY
no_proxy = os.getenv("NO_PROXY", "") or os.getenv("no_proxy", "")
print(f"\nNO_PROXY содержит: {no_proxy}")

if "localhost" not in no_proxy or "127.0.0.1" not in no_proxy:
    print("\n⚠️  ВНИМАНИЕ: localhost и 127.0.0.1 НЕ исключены из прокси!")
    print("   Это может привести к тому, что подключения к локальной MongoDB")
    print("   будут идти через прокси!")

print("\n" + "=" * 60)
print("РЕКОМЕНДАЦИИ:")
print("=" * 60)
print("\n1. Добавьте в .env файл:")
print("   NO_PROXY=localhost,127.0.0.1,mongodb,redis,*.local")
print("\n2. Или временно отключите прокси для теста:")
print("   # HTTP_PROXY=")
print("   # HTTPS_PROXY=")
print("   # SOCKS_PROXY=")
print("\n3. Убедитесь, что в docker-compose.yml есть NO_PROXY:")
print("   environment:")
print("     - NO_PROXY=localhost,127.0.0.1,mongodb,redis")

# Тест подключения без прокси
print("\n" + "=" * 60)
print("ТЕСТ ПОДКЛЮЧЕНИЯ")
print("=" * 60)

# Временно отключаем прокси для теста
test_env = os.environ.copy()
test_env.pop('HTTP_PROXY', None)
test_env.pop('HTTPS_PROXY', None)
test_env.pop('SOCKS_PROXY', None)
test_env.pop('http_proxy', None)
test_env.pop('https_proxy', None)
test_env.pop('socks_proxy', None)

print("\nТестируем подключение к MongoDB БЕЗ прокси...")

import subprocess
result = subprocess.run(
    ["python", "-c", """
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

async def test():
    try:
        client = AsyncIOMotorClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
        await client.admin.command('ping')
        count = await client.okpd_classifier.products_stage_one.count_documents({})
        print(f'✓ Подключение успешно! Товаров в базе: {count}')
        client.close()
    except Exception as e:
        print(f'✗ Ошибка: {e}')

asyncio.run(test())
"""],
    env=test_env,
    capture_output=True,
    text=True
)

print(result.stdout)
if result.stderr:
    print(f"Ошибки: {result.stderr}")