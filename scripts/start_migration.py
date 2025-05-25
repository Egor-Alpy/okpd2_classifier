#!/usr/bin/env python3
import asyncio
import argparse
import aiohttp
import json
import sys


async def start_migration(api_url: str, api_key: str):
    """Запустить миграцию через API"""
    url = f"{api_url}/api/v1/classification/migration/start"
    headers = {"X-API-Key": api_key}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers) as resp:
            if resp.status != 200:
                print(f"Error: {await resp.text()}")
                sys.exit(1)

            data = await resp.json()
            print(f"Migration started successfully!")
            print(f"Job ID: {data['job_id']}")
            return data['job_id']


async def monitor_migration(api_url: str, api_key: str, job_id: str):
    """Мониторить прогресс миграции"""
    url = f"{api_url}/api/v1/classification/migration/{job_id}"
    headers = {"X-API-Key": api_key}

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    print(f"Error: {await resp.text()}")
                    break

                data = await resp.json()
                print(f"\rProgress: {data['migrated_products']}/{data['total_products']} "
                      f"({data['progress_percentage']}%) - Status: {data['status']}", end='')

                if data['status'] in ['completed', 'failed']:
                    print()  # Новая строка
                    break

        await asyncio.sleep(5)


async def main():
    parser = argparse.ArgumentParser(description='Start product migration')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API URL')
    parser.add_argument('--api-key', required=True, help='API key')
    parser.add_argument('--monitor', action='store_true', help='Monitor progress')

    args = parser.parse_args()

    job_id = await start_migration(args.api_url, args.api_key)

    if args.monitor:
        print("\nMonitoring migration progress...")
        await monitor_migration(args.api_url, args.api_key, job_id)
        print("\nMigration completed!")


if __name__ == "__main__":
    asyncio.run(main())