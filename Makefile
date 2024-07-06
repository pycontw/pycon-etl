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

deps:
	poetry export -f requirements.txt -o requirements.txt --without-hashes

deploy:
	docker-compose -f ./docker/docker-compose.yml up -d

down:
	docker-compose -f ./docker/docker-compose.yml down
