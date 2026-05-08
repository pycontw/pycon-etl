lint:
	uv run mypy dags/ tests/
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check . --fix
	uv run ruff format .

test:
	uv run pytest

coverage:
	uv run pytest --cov=dags tests

build-dev:
	docker compose -f ./docker-compose-dev.yml build

deploy-dev:
	docker compose -f ./docker-compose-dev.yml up -d

down-dev:
	docker compose -f ./docker-compose-dev.yml down

deploy-prod:
	docker compose -f ./docker-compose.yml up -d

down-prod:
	docker compose -f ./docker-compose.yml down

build-docs:
	uv run --group docs  mkdocs build

serve-docs:
	uv run --group docs mkdocs serve
