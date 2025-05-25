"""
Централизованный модуль для сбора метрик
"""
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import asyncio
from collections import defaultdict
from enum import Enum

class MetricType(str, Enum):
    """Типы метрик"""
    CLASSIFICATION = "classification"
    MIGRATION = "migration"
    API_CALL = "api_call"
    RATE_LIMIT = "rate_limit"

@dataclass
class ClassificationMetrics:
    """Метрики классификации"""
    timestamp: datetime
    worker_id: str
    batch_size: int
    processing_time: float
    success_count: int
    failure_count: int
    tokens_used: int = 0

@dataclass
class MigrationMetrics:
    """Метрики миграции"""
    timestamp: datetime
    batch_size: int
    processing_time: float
    inserted_count: int
    duplicate_count: int

class MetricsCollector:
    """Синглтон для сбора метрик"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.classification_metrics: List[ClassificationMetrics] = []
        self.migration_metrics: List[MigrationMetrics] = []
        self.worker_stats = defaultdict(lambda: {
            'total_processed': 0,
            'total_classified': 0,
            'total_failed': 0,
            'avg_processing_time': 0,
            'rate_limits_hit': 0,
            'last_activity': None
        })
        self._lock = asyncio.Lock()
        self._initialized = True

    async def record_classification(self, metric: ClassificationMetrics):
        """Записать метрику классификации"""
        async with self._lock:
            self.classification_metrics.append(metric)

            # Обновляем статистику воркера
            stats = self.worker_stats[metric.worker_id]
            stats['total_processed'] += metric.batch_size
            stats['total_classified'] += metric.success_count
            stats['total_failed'] += metric.failure_count
            stats['last_activity'] = metric.timestamp

            # Скользящее среднее времени обработки
            stats['avg_processing_time'] = (
                stats['avg_processing_time'] * 0.9 + metric.processing_time * 0.1
            )

            # Очищаем старые метрики (храним последние 24 часа)
            await self._cleanup_old_metrics()

    async def record_migration(self, metric: MigrationMetrics):
        """Записать метрику миграции"""
        async with self._lock:
            self.migration_metrics.append(metric)
            await self._cleanup_old_metrics()

    async def record_rate_limit(self, worker_id: str):
        """Записать попадание в rate limit"""
        async with self._lock:
            self.worker_stats[worker_id]['rate_limits_hit'] += 1

    async def _cleanup_old_metrics(self):
        """Очистка старых метрик"""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)

        self.classification_metrics = [
            m for m in self.classification_metrics
            if m.timestamp > cutoff_time
        ]

        self.migration_metrics = [
            m for m in self.migration_metrics
            if m.timestamp > cutoff_time
        ]

    async def get_classification_stats(self, time_window_minutes: int = 60) -> Dict:
        """Получить статистику классификации"""
        async with self._lock:
            cutoff_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
            recent_metrics = [
                m for m in self.classification_metrics
                if m.timestamp > cutoff_time
            ]

            if not recent_metrics:
                return self._empty_classification_stats()

            total_processed = sum(m.batch_size for m in recent_metrics)
            total_classified = sum(m.success_count for m in recent_metrics)
            total_time = sum(m.processing_time for m in recent_metrics)

            # Пропускная способность
            if len(recent_metrics) > 1:
                time_span = (
                    recent_metrics[-1].timestamp - recent_metrics[0].timestamp
                ).total_seconds()
                throughput = (total_processed / time_span * 60) if time_span > 0 else 0
            else:
                throughput = 0

            return {
                'time_window_minutes': time_window_minutes,
                'workers_active': len(self.worker_stats),
                'total_processed': total_processed,
                'total_classified': total_classified,
                'total_failed': total_processed - total_classified,
                'success_rate': (
                    total_classified / total_processed * 100
                ) if total_processed > 0 else 0,
                'avg_batch_size': total_processed / len(recent_metrics),
                'avg_processing_time': total_time / len(recent_metrics),
                'throughput_per_minute': round(throughput, 2),
                'worker_stats': dict(self.worker_stats),
                'rate_limits_total': sum(
                    stats['rate_limits_hit']
                    for stats in self.worker_stats.values()
                )
            }

    async def get_migration_stats(self, time_window_minutes: int = 60) -> Dict:
        """Получить статистику миграции"""
        async with self._lock:
            cutoff_time = datetime.utcnow() - timedelta(minutes=time_window_minutes)
            recent_metrics = [
                m for m in self.migration_metrics
                if m.timestamp > cutoff_time
            ]

            if not recent_metrics:
                return self._empty_migration_stats()

            total_processed = sum(m.batch_size for m in recent_metrics)
            total_inserted = sum(m.inserted_count for m in recent_metrics)
            total_duplicates = sum(m.duplicate_count for m in recent_metrics)
            total_time = sum(m.processing_time for m in recent_metrics)

            # Скорость миграции
            if len(recent_metrics) > 1:
                time_span = (
                    recent_metrics[-1].timestamp - recent_metrics[0].timestamp
                ).total_seconds()
                throughput = (total_processed / time_span * 60) if time_span > 0 else 0
            else:
                throughput = 0

            return {
                'time_window_minutes': time_window_minutes,
                'batches_processed': len(recent_metrics),
                'total_processed': total_processed,
                'total_inserted': total_inserted,
                'total_duplicates': total_duplicates,
                'avg_batch_size': total_processed / len(recent_metrics),
                'avg_processing_time': total_time / len(recent_metrics),
                'throughput_per_minute': round(throughput, 2),
                'duplicate_rate': (
                    total_duplicates / total_processed * 100
                ) if total_processed > 0 else 0
            }

    def _empty_classification_stats(self) -> Dict:
        """Пустая статистика классификации"""
        return {
            'time_window_minutes': 0,
            'workers_active': 0,
            'total_processed': 0,
            'total_classified': 0,
            'total_failed': 0,
            'success_rate': 0,
            'avg_batch_size': 0,
            'avg_processing_time': 0,
            'throughput_per_minute': 0,
            'worker_stats': {},
            'rate_limits_total': 0
        }

    def _empty_migration_stats(self) -> Dict:
        """Пустая статистика миграции"""
        return {
            'time_window_minutes': 0,
            'batches_processed': 0,
            'total_processed': 0,
            'total_inserted': 0,
            'total_duplicates': 0,
            'avg_batch_size': 0,
            'avg_processing_time': 0,
            'throughput_per_minute': 0,
            'duplicate_rate': 0
        }

# Глобальный экземпляр коллектора
metrics_collector = MetricsCollector()