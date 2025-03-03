GO_WORKER=backend/main.go
API_DIR=api

run-worker:
	@echo "Starting Go Worker..."
	go run $(GO_WORKER)

run-api:
	@echo "Starting Hono API..."
	cd $(API_DIR) && pnpm run dev

build-worker:
	@echo "Building Go Worker..."
	cd backend && go build -o bin/worker main.go

build-api:
	@echo "Building Hono API..."
	cd $(API_DIR) && ppnm install && pnpm run build

run:
	@echo "Starting everything..."
	make run-worker & make run-api


.PHONY: run-worker run-api build-worker build-api run