VENV_PREFIX=poetry run

lint:
	$(VENV_PREFIX) black . --check
	$(VENV_PREFIX) isort --check-only .
	$(VENV_PREFIX) flake8 .
	$(VENV_PREFIX) mypy dags/ tests/

format:
	$(VENV_PREFIX) black .
	$(VENV_PREFIX) isort .

test:
	PYTHONPATH=./dags $(VENV_PREFIX) pytest

coverage:
	PYTHONPATH=./dags $(VENV_PREFIX) pytest --cov=dags tests

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
