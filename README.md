# OKPD2 Stage One Classifier

Первый этап классификации товаров по ОКПД2.

## 🚀 Быстрый старт

### 1. Подготовка окружения

```bash
# Клонируйте репозиторий
git clone <repository-url>
cd okpd2-stage-one

# Создайте .env файл
cp .env.example .env

# Отредактируйте .env и добавьте ваши ключи:
# - ANTHROPIC_API_KEY - ключ от Anthropic Claude API
# - API_KEY - ваш секретный ключ для API
```

### 2. Запуск для разработки

```bash
# Запустите только инфраструктуру (MongoDB, Redis)
docker-compose -f docker-compose.dev.yml up -d

# Проверьте подключения и создайте индексы
python scripts/init_db.py

# Запустите API сервер локально
python -m uvicorn src.main:app --reload

# В отдельном терминале запустите миграцию
python scripts/start_migration.py --api-key your-key --monitor
```

### 3. Запуск в production

```bash
# Запустите все сервисы
docker-compose up -d

# Проверьте статус
docker-compose ps

# Начните миграцию
python scripts/start_migration.py --api-key your-key --monitor
```

## 📊 Мониторинг

### Просмотр данных в MongoDB:
- Source DB: http://localhost:8081 (admin/admin)
- Target DB: http://localhost:8082 (admin/admin)

### API endpoints:
- Статистика: `GET http://localhost:8000/api/v1/monitoring/stats`
- Health check: `GET http://localhost:8000/health`
- Swagger docs: `http://localhost:8000/docs`

### Проверка логов:
```bash
# Все логи
docker-compose logs -f

# Только classification workers
docker-compose logs -f classification-worker

# Только migration worker
docker-compose logs -f migration-worker
```

## 🏗️ Архитектура

### Компоненты:
- **API Server**: REST API для управления и мониторинга
- **Migration Worker**: Переносит товары из исходной MongoDB в целевую
- **Classification Workers**: Классифицируют товары через Claude API (3 экземпляра)
- **Source MongoDB**: Исходная база данных с товарами (read-only)
- **Target MongoDB**: Наша база для хранения результатов классификации
- **Redis**: Очередь задач и кеширование

### Процесс работы:
1. Migration Worker читает товары из Source MongoDB батчами
2. Товары сохраняются в Target MongoDB со статусом "pending"
3. Classification Workers берут pending товары и отправляют в Claude API
4. Результаты сохраняются обратно в Target MongoDB

### Структура данных в Target MongoDB:
```javascript
{
  collection_name: "products",
  old_mongo_id: "6823aecaa470...",
  title: "Название товара",
  okpd_group: ["17", "32"],  // Может быть несколько групп
  status_stg1: "classified",   // pending, processing, classified, none_classified, failed
  created_at: ISODate(),
  updated_at: ISODate(),
  error_message: null,
  batch_id: "batch_12345"
}
```

## 🛠️ Разработка

### Запуск тестов:
```bash
# Unit тесты
python -m pytest tests/unit

# Integration тесты
python -m pytest tests/integration
```

### Добавление тестовых данных:
```bash
# Данные добавляются автоматически при запуске docker-compose.dev.yml
# Или вручную:
docker exec -it source-mongo mongo /docker-entrypoint-initdb.d/01_insert_sample_products.js
```

### Очистка данных:
```bash
# Остановить и удалить все контейнеры и volumes
docker-compose down -v
```

## 🔍 Troubleshooting

### Ошибка "duplicate key error":
- Товары уже были мигрированы ранее
- Решение: Продолжите с того места где остановились или очистите Target DB

### Classification workers не берут товары:
- Проверьте наличие pending товаров в БД
- Проверьте API ключ Anthropic
- Посмотрите логи workers

### Миграция зависла:
- Проверьте статус через API: `/api/v1/classification/migration/{job_id}`
- Возобновите через: `POST /api/v1/classification/migration/{job_id}/resume`

## 📝 Конфигурация

Основные параметры в `.env`:
- `MIGRATION_BATCH_SIZE` - размер батча для миграции (default: 1000)
- `CLASSIFICATION_BATCH_SIZE` - размер батча для классификации (default: 50)
- `MAX_WORKERS` - количество classification workers (default: 3)

## 📈 Производительность

При настройках по умолчанию:
- Миграция: ~1000 товаров/сек
- Классификация: ~50 товаров за вызов API (~2-3 сек на батч)
- 3 workers = ~150 товаров/10 сек = ~900 товаров/мин

Для 100,000 товаров:
- Миграция: ~2 минуты
- Классификация: ~2 часа