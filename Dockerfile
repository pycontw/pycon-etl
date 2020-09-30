FROM puckel/docker-airflow:1.10.9
USER root
ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/pypoetry' \
    GOOGLE_APPLICATION_CREDENTIALS='/usr/local/airflow/service-account.json'

COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN pip install --no-cache-dir poetry==1.0.5 \
    && python -m poetry install --no-interaction --no-ansi --no-dev \
    # Cleaning poetry installation's cache for production:
    && rm -rf "$POETRY_CACHE_DIR" \
    && pip uninstall -yq poetry
USER airflow
COPY dags /usr/local/airflow/dags