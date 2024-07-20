ARG AIRFLOW_VERSION=1.10.13
ARG PYTHON_VERSION=3.8

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29 \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B7B3B788A8D3785C \
    && apt-get update \
    && apt-get install -y --no-install-recommends git \
    # 1. if you don't need postgres, remember to remove postgresql-dev and sqlalchemy
    # 2. libglib2.0-0 libsm6 libxext6 libxrender-dev libgl1-mesa-dev are required by opencv
    # 3. git is required by pip install git+https
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER airflow

COPY ./requirements.txt ${AIRFLOW_HOME}/requirements.txt
COPY ./constraints-3.8.txt ${AIRFLOW_HOME}/constraints-3.8.txt

RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt --constraint constraints-3.8.txt

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

COPY --chown=airflow:root dags ${AIRFLOW_HOME}/dags

ENTRYPOINT ["/entrypoint.sh"]
