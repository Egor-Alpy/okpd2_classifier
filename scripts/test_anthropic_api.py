#!/usr/bin/env python3
"""
Тестирование Anthropic API и выбор правильной модели
"""
import asyncio
import os
from anthropic import AsyncAnthropic
from dotenv import load_dotenv

load_dotenv()


async def test_anthropic():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    current_model = os.getenv("ANTHROPIC_MODEL", "claude-3-sonnet-20241022")

    print(f"🔑 API Key: {api_key[:10]}...{api_key[-5:] if api_key and len(api_key) > 15 else ''}")
    print(f"🤖 Current model in .env: {current_model}")

    if not api_key:
        print("❌ API key not found in .env")
        return

    if not api_key.startswith("sk-ant-"):
        print("⚠️  Warning: API key should start with 'sk-ant-'")

    client = AsyncAnthropic(api_key=api_key)

    # Список доступных моделей для тестирования
    models_to_try = [
        # Последние модели
        "claude-3-5-sonnet-20241022",  # Если эта модель есть
        "claude-3-5-sonnet-20240620",  # Подтвержденная рабочая модель
        "claude-3-haiku-20240307",  # Быстрая модель
        "claude-3-opus-20240229",  # Мощная модель
        "claude-3-sonnet-20240229",  # Сбалансированная модель
        # Старые модели (на случай если новые недоступны)
        "claude-2.1",
        "claude-2.0"
    ]

    working_models = []

    for test_model in models_to_try:
        print(f"\n🧪 Testing model: {test_model}")
        try:
            response = await client.messages.create(
                model=test_model,
                max_tokens=50,
                messages=[{"role": "user", "content": "Hello, respond with 'API works!' and nothing else"}],
                temperature=0.0
            )
            print(f"✅ Success! Response: {response.content[0].text.strip()}")
            working_models.append(test_model)
        except Exception as e:
            error_msg = str(e)
            if "403" in error_msg and "forbidden" in error_msg:
                print(f"❌ Error 403: Model not accessible with your API key")
            elif "404" in error_msg:
                print(f"❌ Error 404: Model does not exist")
            elif "authentication_error" in error_msg:
                print(f"❌ Authentication error: Check your API key")
            else:
                print(f"❌ Error: {error_msg}")

    print("\n" + "=" * 50)

    if working_models:
        print("\n✨ Working models found:")
        for model in working_models:
            print(f"  - {model}")

        print(f"\n📝 Update your .env file:")
        print(f"ANTHROPIC_MODEL={working_models[0]}")

        # Проверяем, есть ли Sonnet среди рабочих моделей
        sonnet_models = [m for m in working_models if "sonnet" in m]
        if sonnet_models:
            print(f"\n💡 Recommended (Sonnet): ANTHROPIC_MODEL={sonnet_models[0]}")
    else:
        print("\n❌ No working models found. Please check:")
        print("  1. Your API key is valid")
        print("  2. Your account has access to Claude models")
        print("  3. You don't have regional restrictions")
        print("  4. Your account is not rate-limited")

    await client.close()


if __name__ == "__main__":
    asyncio.run(test_anthropic())