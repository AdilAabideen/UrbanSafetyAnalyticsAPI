.PHONY: up-db init-db init-db-force up-app up-all down ps

up-db:
	docker compose up -d db

init-db:
	python backend/scripts/init_database.py

init-db-force:
	python backend/scripts/init_database.py --force

up-app:
	docker compose up -d api frontend

up-all:
	docker compose up -d db api frontend

ps:
	docker compose ps

down:
	docker compose down
