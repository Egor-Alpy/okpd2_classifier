#!/usr/bin/env python3
"""
Интерактивный дашборд для мониторинга метрик в реальном времени
"""
import asyncio
import aiohttp
import argparse
from datetime import datetime
import os
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.align import Align
from rich.text import Text
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class MetricsDashboard:
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key
        self.console = Console()
        self.running = True

    async def fetch_data(self):
        """Получить все данные"""
        headers = {"X-API-Key": self.api_key}

        try:
            async with aiohttp.ClientSession() as session:
                # Получаем статистику
                stats_resp = await session.get(
                    f"{self.api_url}/api/v1/monitoring/stats",
                    headers=headers
                )
                stats = await stats_resp.json() if stats_resp.status == 200 else None

                # Получаем метрики
                metrics_resp = await session.get(
                    f"{self.api_url}/api/v1/monitoring/metrics/summary",
                    headers=headers
                )
                metrics = await metrics_resp.json() if metrics_resp.status == 200 else None

                # Получаем здоровье воркеров
                workers_resp = await session.get(
                    f"{self.api_url}/api/v1/monitoring/workers/health",
                    headers=headers
                )
                workers = await workers_resp.json() if workers_resp.status == 200 else None

                return stats, metrics, workers

        except Exception as e:
            self.console.print(f"[red]Error fetching data: {e}[/red]")
            return None, None, None

    def create_stats_panel(self, stats):
        """Панель общей статистики"""
        if not stats:
            return Panel("[red]No stats data[/red]", title="📊 Statistics")

        table = Table(show_header=False, box=None)
        table.add_column(style="cyan", width=20)
        table.add_column(style="white")

        total = stats['total']
        completed = stats['classified'] + stats['none_classified']
        progress = (completed / total * 100) if total > 0 else 0

        table.add_row("Total Products:", f"{total:,}")
        table.add_row("Progress:", f"{progress:.1f}%")
        table.add_row("", "")
        table.add_row("✅ Classified:", f"{stats['classified']:,} ({stats.get('classified_percentage', 0):.1f}%)")
        table.add_row("❌ Not Classified:",
                      f"{stats['none_classified']:,} ({stats.get('none_classified_percentage', 0):.1f}%)")
        table.add_row("⏳ Pending:", f"{stats['pending']:,}")
        table.add_row("🔄 Processing:", f"{stats['processing']:,}")
        table.add_row("⚠️  Failed:", f"{stats['failed']:,}")

        return Panel(table, title="📊 Classification Progress", border_style="blue")

    def create_performance_panel(self, metrics):
        """Панель производительности"""
        if not metrics:
            return Panel("[red]No metrics data[/red]", title="📈 Performance")

        table = Table(show_header=True, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Last Hour", style="white")
        table.add_column("Last 24h", style="dim")

        class_1h = metrics['classification']['last_hour']
        class_24h = metrics['classification']['last_24_hours']

        table.add_row(
            "Throughput",
            f"{class_1h['throughput_per_minute']:.1f}/min",
            f"{class_24h['throughput_per_minute']:.1f}/min"
        )
        table.add_row(
            "Success Rate",
            f"{class_1h['success_rate']:.1f}%",
            f"{class_24h['success_rate']:.1f}%"
        )
        table.add_row(
            "Avg Batch Size",
            f"{class_1h['avg_batch_size']:.0f}",
            f"{class_24h['avg_batch_size']:.0f}"
        )
        table.add_row(
            "Avg Process Time",
            f"{class_1h['avg_processing_time']:.1f}s",
            f"{class_24h['avg_processing_time']:.1f}s"
        )
        table.add_row(
            "Rate Limits Hit",
            f"{class_1h['rate_limits_total']}",
            f"{class_24h['rate_limits_total']}"
        )

        return Panel(table, title="📈 Performance Metrics", border_style="green")

    def create_workers_panel(self, workers_data):
        """Панель воркеров"""
        if not workers_data:
            return Panel("[red]No workers data[/red]", title="👷 Workers")

        table = Table(show_header=True, box=None)
        table.add_column("Worker", style="cyan")
        table.add_column("Status", style="white")
        table.add_column("Active", style="white")
        table.add_column("Last Hour", style="white")
        table.add_column("Success", style="white")

        workers = workers_data.get('workers', {})

        for worker_id, data in sorted(workers.items()):
            status_color = "green" if data['status'] == 'active' else "red"
            status = f"[{status_color}]{data['status']}[/{status_color}]"

            table.add_row(
                worker_id,
                status,
                str(data['active_products']),
                str(data['processed_last_hour']),
                f"{data['success_rate']:.0f}%"
            )

        health_color = "green" if workers_data['health_status'] == 'healthy' else "yellow"
        footer = f"\n[{health_color}]Health: {workers_data['health_status']}[/{health_color}]"

        if workers_data['stuck_products'] > 0:
            footer += f"\n[yellow]⚠️  Stuck products: {workers_data['stuck_products']}[/yellow]"

        return Panel(
            table,
            title=f"👷 Workers ({workers_data['total_active_workers']} active)",
            border_style="yellow",
            subtitle=footer
        )

    def create_migration_panel(self, metrics):
        """Панель миграции"""
        if not metrics or not metrics.get('migration'):
            return Panel("[dim]No migration data[/dim]", title="📦 Migration")

        migration = metrics['migration']['last_hour']

        table = Table(show_header=False, box=None)
        table.add_column(style="cyan", width=20)
        table.add_column(style="white")

        table.add_row("Batches:", str(migration['batches_processed']))
        table.add_row("Throughput:", f"{migration['throughput_per_minute']:.0f}/min")
        table.add_row("Inserted:", f"{migration['total_inserted']:,}")
        table.add_row("Duplicates:", f"{migration['total_duplicates']:,} ({migration['duplicate_rate']:.1f}%)")

        return Panel(table, title="📦 Migration Status", border_style="magenta")

    def create_layout(self):
        """Создать layout дашборда"""
        layout = Layout()

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3)
        )

        layout["body"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )

        layout["left"].split_column(
            Layout(name="stats"),
            Layout(name="migration", size=10)
        )

        layout["right"].split_column(
            Layout(name="performance"),
            Layout(name="workers")
        )

        return layout

    async def run(self):
        """Запустить дашборд"""
        layout = self.create_layout()

        with Live(layout, refresh_per_second=0.5, screen=True) as live:
            while self.running:
                try:
                    # Получаем данные
                    stats, metrics, workers = await self.fetch_data()

                    # Обновляем header
                    header_text = Text()
                    header_text.append("🎯 OKPD2 Classifier Dashboard", style="bold blue")
                    header_text.append(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")
                    layout["header"].update(Panel(Align.center(header_text), border_style="blue"))

                    # Обновляем панели
                    layout["stats"].update(self.create_stats_panel(stats))
                    layout["performance"].update(self.create_performance_panel(metrics))
                    layout["workers"].update(self.create_workers_panel(workers))
                    layout["migration"].update(self.create_migration_panel(metrics))

                    # Footer
                    footer_text = "[dim]Press Ctrl+C to exit | Updates every 5 seconds[/dim]"
                    layout["footer"].update(Panel(Align.center(footer_text), border_style="dim"))

                    # Ждем перед следующим обновлением
                    await asyncio.sleep(5)

                except KeyboardInterrupt:
                    self.running = False
                    break
                except Exception as e:
                    self.console.print(f"[red]Dashboard error: {e}[/red]")
                    await asyncio.sleep(5)


async def main():
    parser = argparse.ArgumentParser(description='Metrics dashboard')
    parser.add_argument('--api-url', default='http://localhost:8000', help='API URL')
    parser.add_argument(
        '--api-key',
        default=os.getenv('API_KEY'),  # Берем из .env если не указан
        help='API key (default: from API_KEY env variable)'
    )

    args = parser.parse_args()

    # Проверяем наличие API ключа
    if not args.api_key:
        print("❌ Error: API key is required!")
        print("   Set it in .env file as API_KEY=your-key")
        print("   Or pass it as: --api-key your-key")
        return

    dashboard = MetricsDashboard(args.api_url, args.api_key)

    try:
        await dashboard.run()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")


if __name__ == "__main__":
    asyncio.run(main())