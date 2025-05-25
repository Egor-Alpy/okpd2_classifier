#!/usr/bin/env python3
"""
Тестирование Anthropic API через прокси
"""
import asyncio
import os
import httpx
from anthropic import AsyncAnthropic
from dotenv import load_dotenv

load_dotenv()


async def test_direct_connection():
    """Тест прямого подключения"""
    api_key = os.getenv("ANTHROPIC_API_KEY")

    print("🔍 Testing DIRECT connection to Anthropic...")

    try:
        client = AsyncAnthropic(api_key=api_key)
        response = await client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=50,
            messages=[{"role": "user", "content": "Say 'Direct connection works!'"}],
            temperature=0.0
        )
        print(f"✅ Direct connection: {response.content[0].text.strip()}")
        await client.close()
        return True
    except Exception as e:
        print(f"❌ Direct connection failed: {str(e)[:100]}...")
        return False


async def test_proxy_connection(proxy_url: str):
    """Тест подключения через прокси"""
    api_key = os.getenv("ANTHROPIC_API_KEY")

    print(f"\n🔍 Testing connection through proxy: {proxy_url}")

    try:
        # Создаем HTTP клиент с прокси
        http_client = httpx.AsyncClient(
            proxies={
                "http://": proxy_url,
                "https://": proxy_url
            },
            timeout=httpx.Timeout(30.0, connect=10.0)
        )

        client = AsyncAnthropic(
            api_key=api_key,
            http_client=http_client
        )

        response = await client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=50,
            messages=[{"role": "user", "content": "Say 'Proxy connection works!'"}],
            temperature=0.0
        )

        print(f"✅ Proxy connection: {response.content[0].text.strip()}")
        await client.close()
        await http_client.aclose()
        return True
    except httpx.ProxyError as e:
        print(f"❌ Proxy error: {e}")
        print("   Check if proxy is running and accessible")
        return False
    except Exception as e:
        print(f"❌ Proxy connection failed: {str(e)[:100]}...")
        return False


async def test_system_proxy():
    """Тест системных настроек прокси"""
    print("\n📋 System proxy settings:")

    # Проверяем переменные окружения
    http_proxy = os.getenv("HTTP_PROXY", "")
    https_proxy = os.getenv("HTTPS_PROXY", "")
    socks_proxy = os.getenv("SOCKS_PROXY", "")

    print(f"   HTTP_PROXY:  {http_proxy or 'Not set'}")
    print(f"   HTTPS_PROXY: {https_proxy or 'Not set'}")
    print(f"   SOCKS_PROXY: {socks_proxy or 'Not set'}")

    # Определяем какой прокси использовать
    proxy_url = socks_proxy or https_proxy or http_proxy

    if proxy_url:
        return proxy_url
    else:
        print("\n⚠️  No proxy configured in .env")
        return None


async def main():
    api_key = os.getenv("ANTHROPIC_API_KEY")

    print("🔧 Anthropic API Proxy Test")
    print("=" * 50)

    if not api_key:
        print("❌ ANTHROPIC_API_KEY not found in .env")
        return

    print(f"🔑 API Key: {api_key[:10]}...{api_key[-5:] if len(api_key) > 15 else ''}")

    # Тест прямого подключения
    direct_works = await test_direct_connection()

    # Проверяем настройки прокси
    proxy_url = await test_system_proxy()

    # Тест подключения через прокси
    proxy_works = False
    if proxy_url:
        proxy_works = await test_proxy_connection(proxy_url)

    # Рекомендации
    print("\n" + "=" * 50)
    print("📊 Results:")

    if direct_works:
        print("✅ Direct connection works - no proxy needed!")
        print("   You can work without VPN")
    elif proxy_works:
        print("✅ Proxy connection works!")
        print("   The system will use proxy for Anthropic API")
        print("   MongoDB and other services will work locally")
    else:
        print("❌ Neither direct nor proxy connection works")
        print("\n💡 Recommendations:")
        print("1. If you need VPN/proxy for Anthropic:")
        print("   - Set up a proxy server (HTTP/HTTPS/SOCKS5)")
        print("   - Add to .env: HTTPS_PROXY=http://your-proxy:8080")
        print("   - Or use SOCKS: SOCKS_PROXY=socks5://your-proxy:1080")
        print("\n2. Popular proxy options:")
        print("   - Shadowsocks: SOCKS_PROXY=socks5://127.0.0.1:1080")
        print("   - V2Ray: HTTP_PROXY=http://127.0.0.1:10809")
        print("   - Clash: HTTP_PROXY=http://127.0.0.1:7890")
        print("\n3. For authentication:")
        print("   - HTTP_PROXY=http://user:pass@proxy:8080")


if __name__ == "__main__":
    asyncio.run(main())