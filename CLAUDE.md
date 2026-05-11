# pycon-etl

Apache Airflow ETL system for PyCon Taiwan's data infrastructure. Manages event ticketing (KKTIX), social media analytics, finance reconciliation, and AI-assisted user profiling via Google Gemini.

## Stack

- **Airflow 3.x** — Python 3.10 (locked to <3.11), uv for dependency management
- **BigQuery** — `pycontw-225217`; `ods.*` for raw ingestion, `dwd.*` for cleansed transforms (kktix attendees, user profiles). Transform code lives in-place under `dags/ods/.../udfs/`, not a separate package.
- **SQLite** — intentional; this project is also used to test Airflow + SQLite compatibility
- **Docker** — multi-service compose (API Server, DAG Processor, Scheduler, Triggerer)

## Common commands

```bash
make lint      # ruff check + format + mypy
make format    # ruff autofix + format
make test      # pytest (PYTHONPATH=./dags set automatically)
make coverage  # pytest --cov=dags tests
```

## Dag structure

```
dags/
  app/    — notification bots (Discord), AI profiling (Gemini)
  ods/    — raw data ingestion from external APIs (KKTIX, social media, YouTube)
  utils/  — shared helpers: retry logic (tenacity), social media base classes
```

## Key conventions

- **Terminology**: write "Dag" in prose; `DAG` is only the Python class name.
- **Dag ID**: the `@dag`-decorated function name becomes the Dag ID when no explicit `dag_id=` is passed. Renaming the function renames the Dag in Airflow's metadata DB.
- **Airflow Variables**: runtime secrets (API keys, webhook URLs) are stored as Airflow Variables, not env vars. See `.env.template` for the static env config.
- **Dependency updates**: after changing `pyproject.toml`, run `uv lock` to update `uv.lock`. The pre-commit hook verifies that the Airflow version in `pyproject.toml` matches the `Dockerfile`.
- **Typing**: mypy strict mode is enforced on `dags/` and `tests/`. Run `make lint` before pushing.

## Local dev setup

```bash
cp .env.template .env   # fill in secrets
uv sync --group dev
uv run airflow standalone   # spins up all Airflow services locally
```

Or with Docker:

```bash
make deploy-dev   # docker-compose-dev.yml
```

## Testing

`pyproject.toml` adds `.` and `dags/` to `pythonpath` so both `from dags.x import y` and `from ods.x import y` (the in-Airflow style) resolve. `tests/conftest.py` sets `AIRFLOW_TEST_MODE=True` before any airflow import. Just run `make test`.

## Deployment

See `docs/DEPLOYMENT.md`. In short: `git pull` → `docker pull` → `docker compose up -d`.
