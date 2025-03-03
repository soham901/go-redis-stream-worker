WORKER_DIR=backend
API_DIR=api
DOCKER_COMPOSE=deployments/compose.yml

run-worker:
	@echo "Starting Go Worker..."
	cd $(WORKER_DIR) && go run main.go

run-api:
	@echo "Starting Hono API..."
	cd $(API_DIR) && pnpm run dev

build-worker:
	@echo "Building Go Worker..."
	cd $(WORKER_DIR) && go build -o main

build-api:
	@echo "Building Hono API..."
	cd $(API_DIR) && ppnm install && pnpm run build

build:
	@echo "Building everything..."
	make build-worker & make build-api

run:
	@echo "Starting everything..."
	make run-worker & make run-api

docker-up:
	@echo "Starting services with Docker Compose..."
	docker compose -f $(DOCKER_COMPOSE) up --build -d

docker-down:
	@echo "Stopping all services..."
	docker compose -f $(DOCKER_COMPOSE) down


.PHONY: run-worker run-api build-worker build-api run docker-up docker-down build