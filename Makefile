.PHONY: up-db verify-db recover-db init-db init-db-force up-app up-all down ps

PYTHON ?= python3
ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

up-db:
	@echo "[make] Starting db service..."
	@cd $(ROOT_DIR) && docker compose up -d db
	@$(MAKE) --no-print-directory verify-db
	@echo "[make] db is ready."

verify-db:
	@echo "[make] Verifying db image tools..."
	@cd $(ROOT_DIR) && docker compose exec -T db sh -lc "command -v osm2pgsql >/dev/null" || ( \
		echo "DB image is missing osm2pgsql. Run: make recover-db"; \
		exit 1; \
	)
	@echo "[make] osm2pgsql check passed."

recover-db:
	@echo "[make] Recovering db image..."
	@cd $(ROOT_DIR) && docker compose down
	@cd $(ROOT_DIR) && docker image rm -f urban-risk-postgis:16-3.4-local urban-risk-postgis:16-3.4 2>/dev/null || true
	@cd $(ROOT_DIR) && docker compose build --no-cache db
	@cd $(ROOT_DIR) && docker compose up -d db
	@$(MAKE) --no-print-directory verify-db
	@echo "[make] db recovery complete. Please Now run make init-db-force to initialize the database."

init-db:
	@echo "[make] Initializing database (safe mode)..."
	@cd $(ROOT_DIR) && $(PYTHON) backend/scripts/init_database.py

init-db-force:
	@echo "[make] Initializing database (force rebuild)..."
	@cd $(ROOT_DIR) && $(PYTHON) backend/scripts/init_database.py --force

up-app:
	@echo "[make] Starting api + frontend..."
	@cd $(ROOT_DIR) && docker compose up -d api frontend

up-all:
	@echo "[make] Starting db + api + frontend..."
	@cd $(ROOT_DIR) && docker compose up -d db api frontend

ps:
	@cd $(ROOT_DIR) && docker compose ps

down:
	@echo "[make] Stopping stack..."
	@cd $(ROOT_DIR) && docker compose down
