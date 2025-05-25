#!/usr/bin/env python3
"""
Скрипт для мониторинга прогресса классификации
"""
import asyncio
import sys
import os
import time
from datetime import datetime, timedelta
import aiohttp
import argparse

from dotenv import load_dotenv

load_dotenv()


async def get_stats(api_url: str, api_key: str):
    """Получить статистику через API"""
    url = f"{api_url}/api/v1/monitoring/stats"
    headers = {"X-API-Key": api_key}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    print(f"Error: {await resp.text()}")
                    return None
    except Exception as e:
        print(f"Connection error: {e}")
        return None


def calculate_eta(total, completed, rate_per_minute):
    """Рассчитать предполагаемое время завершения"""
    if rate_per_minute <= 0:
        return "Unknown"

    remaining = total - completed
    minutes_remaining = remaining / rate_per_minute
    eta = datetime.now() + timedelta(minutes=minutes_remaining)

    # Форматируем время
    if minutes_remaining < 60:
        time_str = f"{int(minutes_remaining)} minutes"
    elif minutes_remaining < 1440:  # 24 hours
        hours = minutes_remaining / 60
        time_str = f"{hours:.1f} hours"
    else:
        days = minutes_remaining / 1440
        time_str = f"{days:.1f} days"

    return f"{time_str} (ETA: {eta.strftime('%Y-%m-%d %H:%M')})"


async def monitor_progress(api_url: str, api_key: str, interval: int = 30):
    """Мониторить прогресс классификации"""
    print("📊 Classification Progress Monitor")
    print("=" * 60)

    previous_stats = None
    start_time = time.time()

    try:
        while True:
            stats = await get_stats(api_url, api_key)

            if stats:
                total = stats['total']
                pending = stats['pending']
                processing = stats['processing']
                classified = stats['classified']
                none_classified = stats['none_classified']
                failed = stats['failed']

                completed = classified + none_classified + failed
                progress = (completed / total * 100) if total > 0 else 0

                # Очищаем экран
                os.system('clear' if os.name == 'posix' else 'cls')

                print("📊 Classification Progress Monitor")
                print("=" * 60)
                print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Running for: {timedelta(seconds=int(time.time() - start_time))}")
                print()

                # Статистика
                print(f"Total Products:      {total:,}")
                print(f"Completed:          {completed:,} ({progress:.1f}%)")
                print(f"  - Classified:     {classified:,} ({stats.get('classified_percentage', 0):.1f}%)")
                print(f"  - Not Classified: {none_classified:,} ({stats.get('none_classified_percentage', 0):.1f}%)")
                print(f"  - Failed:         {failed:,} ({stats.get('failed_percentage', 0):.1f}%)")
                print(f"Pending:            {pending:,} ({stats.get('pending_percentage', 0):.1f}%)")
                print(f"Processing:         {processing:,}")
                print()

                # Скорость обработки
                if previous_stats and previous_stats['completed'] != completed:
                    rate = (completed - previous_stats['completed']) / (interval / 60)  # per minute
                    print(f"Processing Rate:    {rate:.1f} products/minute")

                    # ETA
                    if pending > 0:
                        eta_str = calculate_eta(total, completed, rate)
                        print(f"Estimated Time:     {eta_str}")

                # Progress bar
                print()
                bar_length = 50
                filled_length = int(bar_length * progress / 100)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                print(f"Progress: [{bar}] {progress:.1f}%")

                # Warnings
                if failed > 0:
                    print(f"\n⚠️  Warning: {failed} products failed!")
                    print("   Run 'python scripts/reset_failed_products.py' to retry them")

                if processing > 100:
                    print(f"\n⚠️  Warning: {processing} products stuck in processing!")

                # Информация о rate limit
                if stats['total'] > 0:
                    print(f"\n💡 Tips for your rate limit (8,000 tokens/min):")
                    print(f"   - Current batch size: {os.getenv('CLASSIFICATION_BATCH_SIZE', '10')}")
                    print(f"   - Delay between batches: {os.getenv('RATE_LIMIT_DELAY', '10')}s")
                    print(f"   - Using model: {os.getenv('ANTHROPIC_MODEL', 'claude-3-haiku-20240307')}")

                previous_stats = {
                    'completed': completed,
                    'time': time.time()
                }

            else:
                print("Failed to get statistics. Retrying...")

            # Ждем перед следующим обновлением
            await asyncio.sleep(interval)

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")


async def main():
    parser = argparse.ArgumentParser(description='Monitor classification progress')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API URL')
    parser.add_argument('--api-key', required=True, help='API key')
    parser.add_argument('--interval', type=int, default=30, help='Update interval in seconds')

    args = parser.parse_args()

    await monitor_progress(args.api_url, args.api_key, args.interval)


if __name__ == "__main__":
    asyncio.run(main())