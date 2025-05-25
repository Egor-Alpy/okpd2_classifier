# OKPD2 Stage One Classifier

Первый этап классификации товаров по ОКПД2 с использованием Claude API.

## 📋 Требования

- Python 3.11+
- Docker и Docker Compose
- MongoDB (внешняя для source, локальная или внешняя для target)
- Redis
- Anthropic API ключ

## 🚀 Быстрый старт

### 1. Клонирование и настройка

```bash
# Клонируйте репозиторий
git clone <repository-url>
cd okpd2-stage-one

# Создайте виртуальное окружение
python -m venv venv
source venv/bin/activate  # Linux/Mac
# или
venv\Scripts\activate  # Windows

# Установите зависимости
pip install -r requirements.txt

# Создайте .env файл
cp .env.example .env
```

### 2. Настройка .env

Отредактируйте `.env` файл и укажите параметры подключения:

```bash
# Source MongoDB (внешняя база с товарами)
SOURCE_MONGO_HOST=mongodb.angora-ide.ts.net
SOURCE_MONGO_PORT=27017
SOURCE_MONGO_USER=parser
SOURCE_MONGO_PASS=your_password_here
SOURCE_MONGO_AUTHSOURCE=parser
SOURCE_MONGODB_DATABASE=TenderDB
SOURCE_COLLECTION_NAME=products

# Target MongoDB (локальная или внешняя)
TARGET_MONGO_HOST=localhost
TARGET_MONGO_PORT=27017
TARGET_MONGO_USER=
TARGET_MONGO_PASS=
TARGET_MONGODB_DATABASE=okpd_classifier

# API ключи
ANTHROPIC_API_KEY=your_anthropic_key_here
API_KEY=your_secure_api_key_here
```

### 3. Проверка подключений

```bash
# Проверьте подключение к MongoDB
python scripts/test_mongo_connection.py

# Или используя Make
make test-connection
```

### 4. Инициализация базы данных

```bash
# Создайте индексы и проверьте все компоненты
python scripts/init_db.py

# Или используя Make
make init-db
```

### 5. Запуск системы

#### Вариант 1: Локальная разработка

```bash
# Запустите инфраструктуру (Redis и Target MongoDB если нужна)
docker-compose -f docker-compose.dev.yml up -d

# Запустите API сервер
uvicorn src.main:app --reload

# В отдельных терминалах запустите воркеры:
python -m src.workers.migration_worker
python -m src.workers.classification_worker --worker-id worker_1
python -m src.workers.classification_worker --worker-id worker_2
```

#### Вариант 2: Production через Docker

```bash
# Запустите все сервисы
docker-compose -f docker-compose.prod.yml up -d

# Проверьте статус
docker-compose -f docker-compose.prod.yml ps

# Смотрите логи
docker-compose -f docker-compose.prod.yml logs -f
```

### 6. Запуск миграции

```bash
# Начните миграцию товаров
python scripts/start_migration.py --api-key your-api-key --monitor

# Или используя Make
make migration-start API_KEY=your-api-key
```

## 📊 API Endpoints

### Документация
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Основные endpoints

#### Статистика классификации
```bash
curl http://localhost:8000/api/v1/monitoring/stats \
  -H "X-API-Key: your-api-key"
```

#### Начать миграцию
```bash
curl -X POST http://localhost:8000/api/v1/classification/migration/start \
  -H "X-API-Key: your-api-key"
```

#### Статус миграции
```bash
curl http://localhost:8000/api/v1/classification/migration/{job_id} \
  -H "X-API-Key: your-api-key"
```

## 🏗️ Архитектура

### Компоненты системы

1. **API Server** - REST API для управления процессом
2. **Migration Worker** - Переносит товары из source в target MongoDB
3. **Classification Workers** - Классифицируют товары через Claude API
4. **Source MongoDB** - Исходная база с товарами (read-only)
5. **Target MongoDB** - База для хранения результатов классификации
6. **Redis** - Координация между воркерами

### Процесс работы

1. **Миграция**: Migration Worker читает товары батчами из Source MongoDB и сохраняет в Target MongoDB со статусом "pending"
2. **Классификация**: Classification Workers берут pending товары, отправляют в Claude API и обновляют результаты
3. **Мониторинг**: API Server предоставляет статистику и управление процессом

### Структура данных

```javascript
{
  collection_name: "products",
  old_mongo_id: "6823aecaa470...",
  title: "Название товара",
  okpd_group: ["17", "32"],  // Массив групп ОКПД2
  status_stg1: "classified",   // pending, processing, classified, none_classified, failed
  created_at: ISODate(),
  updated_at: ISODate(),
  batch_id: "batch_12345",
  worker_id: "worker_1"
}
```

## 🛠️ Полезные команды

### Makefile команды

```bash
make help              # Показать все доступные команды
make test-connection   # Проверить подключения
make init-db          # Инициализировать БД
make dev              # Запустить сервер для разработки
make prod-up          # Запустить production
make prod-logs        # Смотреть логи production
make stats            # Получить статистику
```

### Docker команды

```bash
# Пересобрать образы
docker-compose -f docker-compose.prod.yml build

# Перезапустить конкретный сервис
docker-compose -f docker-compose.prod.yml restart classification-worker-1

# Масштабировать воркеры
docker-compose -f docker-compose.prod.yml up -d --scale classification-worker=5
```

## 🔍 Troubleshooting

### Проблема: "Failed to connect to MongoDB"
- Проверьте параметры подключения в .env
- Убедитесь что MongoDB доступна по указанному адресу
- Проверьте права пользователя

### Проблема: "Duplicate key error"
- Товары уже были мигрированы
- Используйте resume для продолжения миграции

### Проблема: Classification workers не работают
- Проверьте ANTHROPIC_API_KEY
- Убедитесь что есть pending товары
- Проверьте логи воркеров

### Проблема: Медленная классификация
- Увеличьте количество воркеров
- Увеличьте CLASSIFICATION_BATCH_SIZE
- Проверьте лимиты Anthropic API

## 📈 Производительность

- **Миграция**: ~1000 товаров/секунду
- **Классификация**: ~50 товаров за вызов API
- **3 воркера**: ~900 товаров/минуту

Для 100,000 товаров:
- Миграция: ~2 минуты
- Классификация: ~2 часа

## 🔒 Безопасность

- Используйте сильные пароли для MongoDB
- Храните API ключи в безопасном месте
- Ограничьте доступ к API через firewall
- Регулярно обновляйте зависимости

## 📝 Лицензия

[Укажите вашу лицензию]