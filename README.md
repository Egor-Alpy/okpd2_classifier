# OKPD2 Stage One Classifier

Первый этап классификации товаров по ОКПД2.

## Быстрый старт

1. Скопируйте `.env.example` в `.env` и заполните параметры
2. Запустите сервисы: `docker-compose up -d`
3. Начните миграцию: `python scripts/start_migration.py --api-key your-key --monitor`
4. Проверьте статистику: `curl http://localhost:8000/api/v1/monitoring/stats -H "X-API-Key: your-key"`

## Архитектура

- **Migration Worker**: Переносит товары из исходной MongoDB в целевую
- **Classification Workers**: Классифицируют товары через Claude API
- **API Server**: REST API для управления и мониторинга

## Структура данных

```javascript
{
  collection_name: "products",
  old_mongo_id: "6823aecaa470...",
  title: "Название товара",
  okpd_group: ["17", "32"],
  status_stg1: "classified",
  created_at: ISODate(),
  updated_at: ISODate()
}