# Makefile for Docker operations

.PHONY: help build up down logs clean test health

# Default target
help:
	@echo "Available commands:"
	@echo "  build     - Build all Docker images"
	@echo "  up        - Start all services"
	@echo "  down      - Stop all services"
	@echo "  logs      - Show logs from all services"
	@echo "  clean     - Clean up containers, images, and volumes"
	@echo "  test      - Run health checks on all services"
	@echo "  health    - Check health of running services"
	@echo "  dbt       - Run dbt services only"
	@echo "  airflow   - Run Airflow services only"

# Build all images
build:
	docker-compose build --no-cache

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# Show logs
logs:
	docker-compose logs -f

# Clean up everything
clean:
	docker-compose down -v --remove-orphans
	docker system prune -f
	docker volume prune -f

# Run health checks
test:
	@echo "Running health checks..."
	@docker-compose exec -T postgres pg_isready -U airflow || echo "PostgreSQL not ready"
	@docker-compose exec -T redis redis-cli ping || echo "Redis not ready"
	@docker-compose exec -T airflow-webserver curl -f http://localhost:8080/health || echo "Airflow webserver not ready"

# Check service health
health:
	@echo "Checking service health..."
	@docker-compose ps

# Run only dbt services
dbt:
	docker-compose --profile dbt up -d

# Run only Airflow services
airflow:
	docker-compose up -d postgres redis airflow-webserver airflow-scheduler airflow-worker

# Development setup
dev-setup:
	cp .env.example .env
	@echo "Please edit .env file with your configuration"
	@echo "Then run: make build && make up"

# Production deployment
prod-deploy:
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# View service status
status:
	docker-compose ps
	@echo "\nService URLs:"
	@echo "Airflow UI: http://localhost:8080"
	@echo "Monitoring: http://localhost:9090"