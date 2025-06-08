# Makefile для управления классификатором ОКПД2

# Переменные по умолчанию
API_KEY ?= your_secure_api_key_here
COLLECTION ?=

# Цветной вывод
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[1;33m
NC=\033[0m

.PHONY: help
help: ## Показать это меню помощи
	@echo "Управление классификатором ОКПД2"
	@echo "================================"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Использование:\n  make \033[36m<команда>\033[0m\n\nКоманды:\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ==================== DOCKER КОМАНДЫ ====================

.PHONY: build
build: ## Собрать Docker образ
	@echo "$(GREEN)Сборка Docker образа...$(NC)"
	docker-compose build

.PHONY: up
up: ## Запустить API и Redis
	@echo "$(GREEN)Запуск API и Redis...$(NC)"
	docker-compose up -d api redis
	@echo "$(YELLOW)API доступно на http://localhost:8000$(NC)"
	@echo "$(YELLOW)Документация: http://localhost:8000/docs$(NC)"

.PHONY: down
down: ## Остановить все сервисы
	@echo "$(RED)Остановка всех сервисов...$(NC)"
	docker-compose down

.PHONY: logs
logs: ## Показать логи API
	docker-compose logs -f api

.PHONY: logs-all
logs-all: ## Показать логи всех сервисов
	docker-compose logs -f

# ==================== МИГРАЦИЯ ====================

.PHONY: migration-start
migration-start: ## Запустить миграцию из MongoDB
	@echo "$(GREEN)Запуск миграции товаров...$(NC)"
	@echo "$(YELLOW)Используйте API_KEY=$(API_KEY)$(NC)"
	curl -X POST http://localhost:8000/api/v1/migration/start \
		-H "X-API-Key: $(API_KEY)" \
		-H "Content-Type: application/json" | jq '.'

.PHONY: migration-worker
migration-worker: ## Запустить воркер миграции
	@echo "$(GREEN)Запуск воркера миграции...$(NC)"
	docker-compose --profile migration up migration-worker

.PHONY: migration-status
migration-status: ## Проверить статус миграции
	@echo "$(YELLOW)Статус миграции:$(NC)"
	curl -X GET http://localhost:8000/api/v1/stats \
		-H "X-API-Key: $(API_KEY)" | jq '.'

# ==================== КЛАССИФИКАЦИЯ ЭТАП 1 ====================

.PHONY: worker-stage1
worker-stage1: ## Запустить воркер первого этапа (все коллекции)
	@echo "$(GREEN)Запуск воркера классификации (этап 1)...$(NC)"
	docker-compose --profile workers up classification-worker-1

.PHONY: worker-stage1-collection
worker-stage1-collection: ## Запустить воркер первого этапа для конкретной коллекции
	@if [ -z "$(COLLECTION)" ]; then \
		echo "$(RED)Ошибка: укажите коллекцию COLLECTION=название$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Запуск воркера для коллекции $(COLLECTION)...$(NC)"
	COLLECTION=$(COLLECTION) docker-compose --profile workers-collection up classification-worker-collection

.PHONY: worker-stage1-scale
worker-stage1-scale: ## Запустить несколько воркеров первого этапа
	@echo "$(GREEN)Запуск 3 воркеров классификации (этап 1)...$(NC)"
	docker-compose --profile workers up --scale classification-worker-1=3

# ==================== КЛАССИФИКАЦИЯ ЭТАП 2 ====================

.PHONY: worker-stage2
worker-stage2: ## Запустить воркер второго этапа (все коллекции)
	@echo "$(GREEN)Запуск воркера классификации (этап 2)...$(NC)"
	docker-compose --profile workers-stage2 up classification-worker-stage2-1

.PHONY: worker-stage2-collection
worker-stage2-collection: ## Запустить воркер второго этапа для конкретной коллекции
	@if [ -z "$(COLLECTION)" ]; then \
		echo "$(RED)Ошибка: укажите коллекцию COLLECTION=название$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Запуск воркера этапа 2 для коллекции $(COLLECTION)...$(NC)"
	COLLECTION=$(COLLECTION) docker-compose --profile workers-stage2-collection up classification-worker-stage2-collection

.PHONY: worker-stage2-scale
worker-stage2-scale: ## Запустить несколько воркеров второго этапа
	@echo "$(GREEN)Запуск 2 воркеров классификации (этап 2)...$(NC)"
	docker-compose --profile workers-stage2 up --scale classification-worker-stage2-1=2

# ==================== СТАТИСТИКА ====================

.PHONY: stats
stats: ## Показать общую статистику
	@echo "$(YELLOW)Общая статистика классификации:$(NC)"
	@curl -s -X GET http://localhost:8000/api/v1/stats \
		-H "X-API-Key: $(API_KEY)" | jq '.'

.PHONY: stats-stage2
stats-stage2: ## Показать статистику второго этапа
	@echo "$(YELLOW)Статистика второго этапа:$(NC)"
	@curl -s -X GET http://localhost:8000/api/v1/stats/stage2 \
		-H "X-API-Key: $(API_KEY)" | jq '.'

.PHONY: stats-collections
stats-collections: ## Показать статистику по коллекциям
	@echo "$(YELLOW)Статистика по коллекциям:$(NC)"
	@curl -s -X GET http://localhost:8000/api/v1/stats/by-source-collection \
		-H "X-API-Key: $(API_KEY)" | jq '.'

.PHONY: monitor
monitor: ## Мониторинг прогресса в реальном времени
	@echo "$(YELLOW)Мониторинг прогресса (обновление каждые 5 сек)...$(NC)"
	@echo "$(YELLOW)Нажмите Ctrl+C для выхода$(NC)"
	@while true; do \
		clear; \
		echo "$(GREEN)========== СТАТИСТИКА КЛАССИФИКАЦИИ ==========$(NC)"; \
		curl -s -X GET http://localhost:8000/api/v1/stats \
			-H "X-API-Key: $(API_KEY)" | jq '.'; \
		echo ""; \
		echo "$(GREEN)========== СТАТИСТИКА ЭТАПА 2 ==========$(NC)"; \
		curl -s -X GET http://localhost:8000/api/v1/stats/stage2 \
			-H "X-API-Key: $(API_KEY)" | jq '.'; \
		sleep 5; \
	done

# ==================== ТЕСТИРОВАНИЕ ТЕНДЕРОВ ====================

.PHONY: test-tender
test-tender: ## Протестировать классификацию тендера из paste.txt
	@echo "$(GREEN)Тестирование классификации тендера...$(NC)"
	@curl -X POST http://localhost:8000/api/v1/tender/classify-tender \
		-H "X-API-Key: $(API_KEY)" \
		-H "Content-Type: application/json" \
		-d @paste.txt | jq '.'

.PHONY: test-tender-save
test-tender-save: ## Классифицировать тендер и сохранить результат
	@echo "$(GREEN)Классификация тендера и сохранение результата...$(NC)"
	@curl -X POST http://localhost:8000/api/v1/tender/classify-tender \
		-H "X-API-Key: $(API_KEY)" \
		-H "Content-Type: application/json" \
		-d @paste.txt | jq '.' > tender_classified_$$(date +%Y%m%d_%H%M%S).json
	@echo "$(YELLOW)Результат сохранен в tender_classified_$$(date +%Y%m%d_%H%M%S).json$(NC)"

# ==================== УПРАВЛЕНИЕ БД ====================

.PHONY: reset-failed
reset-failed: ## Сбросить товары со статусом failed
	@echo "$(YELLOW)Сброс товаров со статусом failed...$(NC)"
	@echo "$(RED)Эта операция изменит статус всех failed товаров на pending$(NC)"
	@echo "Нажмите Enter для продолжения или Ctrl+C для отмены"
	@read confirm
	# Здесь должна быть команда для сброса через API

.PHONY: cleanup
cleanup: ## Очистить контейнеры и volumes
	@echo "$(RED)ВНИМАНИЕ: Это удалит все данные!$(NC)"
	@echo "Нажмите Enter для продолжения или Ctrl+C для отмены"
	@read confirm
	docker-compose down -v

# ==================== БЫСТРЫЙ СТАРТ ====================

.PHONY: quickstart
quickstart: build up ## Быстрый старт (сборка и запуск API)
	@echo "$(GREEN)Система запущена!$(NC)"
	@echo ""
	@echo "$(YELLOW)Дальнейшие шаги:$(NC)"
	@echo "1. Настройте .env файл"
	@echo "2. Запустите миграцию: make migration-start"
	@echo "3. Запустите воркер миграции: make migration-worker"
	@echo "4. Запустите воркеры классификации: make worker-stage1"
	@echo "5. После первого этапа запустите: make worker-stage2"
	@echo ""
	@echo "$(YELLOW)Мониторинг: make monitor$(NC)"

.PHONY: full-cycle
full-cycle: ## Полный цикл классификации (интерактивный)
	@echo "$(GREEN)========== ПОЛНЫЙ ЦИКЛ КЛАССИФИКАЦИИ ==========$(NC)"
	@echo ""
	@echo "$(YELLOW)Шаг 1: Проверка сервисов$(NC)"
	@docker-compose ps
	@echo ""
	@echo "$(YELLOW)Нажмите Enter для запуска миграции или Ctrl+C для отмены$(NC)"
	@read confirm
	@$(MAKE) migration-start
	@echo ""
	@echo "$(YELLOW)Теперь запустите в отдельном терминале:$(NC)"
	@echo "  make migration-worker"
	@echo ""
	@echo "$(YELLOW)После завершения миграции запустите:$(NC)"
	@echo "  make worker-stage1  (в отдельном терминале)"
	@echo "  make worker-stage2  (после завершения первого этапа)"
	@echo ""
	@echo "$(YELLOW)Для мониторинга используйте:$(NC)"
	@echo "  make monitor"

# ==================== РАЗРАБОТКА ====================

.PHONY: dev
dev: ## Запустить в режиме разработки (с автоперезагрузкой)
	@echo "$(GREEN)Запуск в режиме разработки...$(NC)"
	docker-compose up api redis

.PHONY: shell
shell: ## Открыть shell в контейнере API
	docker-compose exec api /bin/bash

.PHONY: redis-cli
redis-cli: ## Открыть Redis CLI
	docker-compose exec redis redis-cli

# ==================== УСТАНОВКА ЗАВИСИМОСТЕЙ ====================

.PHONY: install-deps
install-deps: ## Установить зависимости для локальной разработки
	pip install -r requirements.txt

.PHONY: verify-env
verify-env: ## Проверить настройки окружения
	@echo "$(YELLOW)Проверка настроек окружения...$(NC)"
	@if [ ! -f .env ]; then \
		echo "$(RED)Ошибка: файл .env не найден!$(NC)"; \
		echo "$(YELLOW)Создайте .env из .env.example:$(NC)"; \
		echo "  cp .env.example .env"; \
		exit 1; \
	fi
	@echo "$(GREEN)Файл .env найден$(NC)"
	@if [ ! -f src/data/okpd2_5digit_groups.txt ]; then \
		echo "$(RED)Ошибка: файл okpd2_5digit_groups.txt не найден!$(NC)"; \
		echo "$(YELLOW)Создайте файл с группами ОКПД2$(NC)"; \
		exit 1; \
	fi
	@if [ ! -f src/data/okpd2_full_tree.json ]; then \
		echo "$(RED)Ошибка: файл okpd2_full_tree.json не найден!$(NC)"; \
		echo "$(YELLOW)Создайте файл с полным деревом ОКПД2$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)Все файлы данных найдены$(NC)"