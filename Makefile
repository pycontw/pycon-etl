lint:
	black . --check
	isort --check-only .
	flake8 .
	mypy dags/ tests/

format:
	black .
	isort .

test:
	PYTHONPATH=./dags pytest

coverage:
	PYTHONPATH=./dags pytest --cov=dags tests

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
