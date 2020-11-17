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
	$(VENV_PREFIX) pytest
