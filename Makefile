.PHONY: help test-connection init-db dev prod-up prod-down prod-logs stats migration-start clean

# Default target
help:
	@echo "Available commands:"
	@echo "  make test-connection  - Test MongoDB connections"
	@echo "  make init-db         - Initialize database and create indexes"
	@echo "  make dev             - Run development server"
	@echo "  make prod-up         - Start production services"
	@echo "  make prod-down       - Stop production services"
	@echo "  make prod-logs       - View production logs"
	@echo "  make stats           - Get classification statistics"
	@echo "  make migration-start - Start product migration (requires API_KEY)"
	@echo "  make clean           - Clean up Docker volumes and containers"

# Test MongoDB connections
test-connection:
	python scripts/test_connection.py

# Test Anthropic API
test-anthropic:
	python scripts/test_anthropic_api.py

# Initialize database
init-db:
	python scripts/init_db.py

# Run development server
dev:
	uvicorn src.main:app --reload --host 0.0.0.0 --port 8000

# Start production services
prod-up:
	docker-compose -f docker-compose.prod.yml up -d

# Stop production services
prod-down:
	docker-compose -f docker-compose.prod.yml down

# View production logs
prod-logs:
	docker-compose -f docker-compose.prod.yml logs -f

# Get statistics
stats:
	@if [ -z "$(API_KEY)" ]; then \
		echo "Error: API_KEY is required. Usage: make stats API_KEY=your-key"; \
		exit 1; \
	fi
	@curl -s http://localhost:8000/api/v1/monitoring/stats \
		-H "X-API-Key: $(API_KEY)" | python -m json.tool

# Start migration
migration-start:
	@if [ -z "$(API_KEY)" ]; then \
		echo "Error: API_KEY is required. Usage: make migration-start API_KEY=your-key"; \
		exit 1; \
	fi
	python scripts/start_migration.py --api-key $(API_KEY) --monitor

# Monitor progress
monitor:
	@if [ -z "$(API_KEY)" ]; then \
		echo "Error: API_KEY is required. Usage: make monitor API_KEY=your-key"; \
		exit 1; \
	fi
	python scripts/monitor_progress.py --api-key $(API_KEY)

# Reset failed products
reset-failed:
	python scripts/reset_failed_products.py

# Clean up
clean:
	docker-compose -f docker-compose.prod.yml down -v
	docker-compose -f docker-compose.dev.yml down -v
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Docker development
dev-up:
	docker-compose -f docker-compose.dev.yml up -d

dev-down:
	docker-compose -f docker-compose.dev.yml down

dev-logs:
	docker-compose -f docker-compose.dev.yml logs -f

# Build Docker images
build:
	docker-compose -f docker-compose.prod.yml build

# Run tests
test:
	@echo "Tests not implemented yet"

# Install dependencies
install:
	pip install -r requirements.txt

# Create .env from example
env:
	cp .env.example .env
	@echo ".env file created from .env.example"
	@echo "Please edit .env and add your configuration"