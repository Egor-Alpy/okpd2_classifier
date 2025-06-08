# Классификатор товаров по ОКПД2

Система для автоматической классификации товаров по стандарту ОКПД2 с использованием AI (Claude).

## 🚀 Быстрый старт

### 1. Подготовка окружения

```bash
# Скопируйте и настройте .env файл
cp .env.example .env

# Отредактируйте .env и укажите:
# - Данные подключения к MongoDB (source и target)
# - API ключ Anthropic
# - API ключ для защиты эндпоинтов
```

### 2. Подготовка данных ОКПД2

```bash
# Если у вас есть CSV файл с ОКПД2
python scripts/prepare_okpd2_data.py okpd2.csv

# Файлы будут созданы в src/data/
```

### 3. Запуск системы

```bash
# Запустить API и Redis
make up

# Проверить статус
make stats
```

## 📋 Два режима работы

### 1. Классификация тендера (через API)

Отправьте JSON тендера на эндпоинт `/api/v1/tender/classify-tender`:

```bash
# Пример с файлом paste.txt
make test-tender

# Или через curl
curl -X POST http://localhost:8000/api/v1/tender/classify-tender \
  -H "X-API-Key: your_api_key" \
  -H "Content-Type: application/json" \
  -d @tender.json
```

Система:
- Найдет товары без кода ОКПД2
- Классифицирует их
- Вернет исходный JSON с добавленными кодами

### 2. Массовая классификация из MongoDB

#### Шаг 1: Миграция
```bash
# Запустить миграцию (один раз)
make migration-start API_KEY=your_api_key

# В отдельном терминале запустить воркер
make migration-worker
```

#### Шаг 2: Классификация - Этап 1
```bash
# Запустить воркер первого этапа
make worker-stage1

# Или для конкретной коллекции
make worker-stage1-collection COLLECTION=electronics

# Или несколько воркеров
make worker-stage1-scale
```

#### Шаг 3: Классификация - Этап 2
```bash
# После завершения первого этапа
make worker-stage2

# Мониторинг прогресса
make monitor
```

## 📊 Мониторинг

```bash
# Общая статистика
make stats

# Статистика второго этапа
make stats-stage2

# Статистика по коллекциям
make stats-collections

# Реальный мониторинг (обновляется каждые 5 сек)
make monitor
```

## 🔧 Настройки в .env

### Важные параметры:
```env
# Размер батчей
MIGRATION_BATCH_SIZE=1000      # Для миграции
CLASSIFICATION_BATCH_SIZE=50   # Для классификации

# Rate limiting
RATE_LIMIT_DELAY=10    # Задержка между батчами (сек)
MAX_RETRIES=3          # Количество повторов при ошибках

# Модель AI
ANTHROPIC_MODEL=claude-3-5-sonnet-20241022
```

### Подключение к MongoDB:
```env
# Исходная БД (откуда берем товары)
SOURCE_MONGO_HOST=mongodb.server.com
SOURCE_MONGO_USER=user
SOURCE_MONGO_PASS=password
SOURCE_MONGODB_DATABASE=products
SOURCE_COLLECTION_NAME=    # Пусто = все коллекции

# Целевая БД (куда сохраняем результаты)
TARGET_MONGO_HOST=mongodb.server.com
TARGET_MONGODB_DATABASE=TenderDB
TARGET_COLLECTION_NAME=classified_products
```

## 🐳 Docker команды

```bash
# Собрать образ
make build

# Запустить все
make up

# Остановить
make down

# Логи
make logs

# Shell в контейнере
make shell
```

## ⚡ Производительность

- **Этап 1**: ~50 товаров за батч, ~200-300 товаров в минуту
- **Этап 2**: ~15 товаров за батч, ~60-100 товаров в минуту

Для ускорения:
1. Запустите несколько воркеров: `make worker-stage1-scale`
2. Используйте более быструю модель: `claude-3-haiku-20240307`
3. Увеличьте `CLASSIFICATION_BATCH_SIZE` (но может быть rate limit)

## 🔍 Проверка перед запуском

```bash
# Проверить настройки
make verify-env

# Файлы должны существовать:
# - .env
# - src/data/okpd2_5digit_groups_optimized.txt
# - src/data/okpd2_full_tree.json
```

## 📝 API Endpoints

- `POST /api/v1/tender/classify-tender` - Классификация тендера
- `GET /api/v1/stats` - Общая статистика
- `GET /api/v1/stats/stage2` - Статистика второго этапа
- `GET /api/v1/stats/by-source-collection` - По коллекциям
- `POST /api/v1/migration/start` - Запуск миграции

Документация: http://localhost:8000/docs

## ❗ Частые проблемы

### "No products found for stage 2"
- Убедитесь, что первый этап завершен
- Проверьте статистику: `make stats`

### Rate limit errors
- Увеличьте `RATE_LIMIT_DELAY` в .env
- Уменьшите `CLASSIFICATION_BATCH_SIZE`

### Timeout errors
- Уменьшите размер батча
- Проверьте прокси настройки если используете

## 🛠️ Разработка

```bash
# Локальная установка зависимостей
make install-deps

# Запуск в dev режиме
make dev
```