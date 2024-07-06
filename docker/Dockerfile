ARG AIRFLOW_VERSION=1.10.13
ARG PYTHON_VERSION=3.8
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
USER root
ENV POETRY_CACHE_DIR='/var/cache/pypoetry' \
    GOOGLE_APPLICATION_CREDENTIALS='/opt/airflow/service-account.json'

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29 \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B7B3B788A8D3785C \
    && apt-get update \
    && apt-get install -y --no-install-recommends git \
    # 1. if you don't need postgres, remember to remove postgresql-dev and sqlalchemy
    # 2. libglib2.0-0 libsm6 libxext6 libxrender-dev libgl1-mesa-dev are required by opencv
    # 3. git is required by pip install git+https
    && pip install --no-cache-dir -U poetry==1.6.1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml pyproject.toml
COPY poetry.toml poetry.toml
COPY poetry.lock poetry.lock

RUN python -m poetry install --no-interaction --no-ansi --no-dev \
    # Cleaning poetry installation's cache for production:
    && rm -rf "$POETRY_CACHE_DIR" \
    && pip uninstall -yq poetry

USER airflow
COPY dags /opt/airflow/dags
COPY airflow.cfg airflow.cfg
