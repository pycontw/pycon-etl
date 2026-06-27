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

DOCKER_TEST_IMAGE ?= pycon-etl:test

test-docker:
	docker build -t $(DOCKER_TEST_IMAGE) -f Dockerfile.test .
	docker run --rm -w /opt/airflow --entrypoint pytest $(DOCKER_TEST_IMAGE)

# Cooldown is configured in pyproject.toml (uv) and .pre-commit-config.yaml (prek >=0.3.12).
upgrade:
	uv lock --upgrade
	uvx --from "prek>=0.3.12" prek auto-update

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
