FROM apache/airflow:slim-2.7.2-python3.10
USER root
ENV POETRY_CACHE_DIR='~/.cache/pypoetry' \
    GOOGLE_APPLICATION_CREDENTIALS='/usr/local/airflow/service-account.json'

RUN apt-get update \
    && apt-get install -y --no-install-recommends git gcc \
    # 1. if you don't need postgres, remember to remove postgresql-dev and sqlalchemy
    # 2. libglib2.0-0 libsm6 libxext6 libxrender-dev libgl1-mesa-dev are required by opencv
    # 3. git is required by pip install git+https

    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY pyproject.toml pyproject.toml
COPY poetry.lock poetry.lock

RUN pip install --no-cache-dir -U pip poetry==1.6.1 \
    && python -m poetry export -o requirements.txt \
    && python -m pip uninstall poetry -y \
    && pip install -r requirements.txt

COPY dags /opt/airflow/dags
# COPY airflow.cfg airflow.cfg

CMD ["airflow"]
