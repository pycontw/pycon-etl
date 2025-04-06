lint:
	uv run ruff check .
	uv run ruff format .
	uv run mypy dags/ tests/

format:
	uv run ruff check . --fix
	uv run ruff format .

test:
	PYTHONPATH=./dags uv run pytest

coverage:
	PYTHONPATH=./dags uv run pytest --cov=dags tests

build-dev:
	docker-compose -f ./docker-compose-dev.yml build

deploy-dev:
	docker-compose -f ./docker-compose-dev.yml up -d

down-dev:
	docker-compose -f ./docker-compose-dev.yml down

deploy-prod:
	docker-compose -f ./docker-compose.yml up -d

down-prod:
	docker-compose -f ./docker-compose.yml down
