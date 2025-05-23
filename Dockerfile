ARG AIRFLOW_VERSION=1.10.15
ARG PYTHON_VERSION=3.8
ARG PLATFORM=linux/amd64

FROM --platform=${PLATFORM} ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

RUN apt-get update && \
    apt-get install -y gcc libc6-dev --no-install-recommends

WORKDIR /app

COPY ./pyproject.toml .
COPY ./uv.lock .

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev


FROM --platform=${PLATFORM} apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

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

COPY --from=builder --chown=airflow:airflow /app /app
ENV PATH="/app/.venv/bin:$PATH"

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY --chown=airflow:root dags ${AIRFLOW_HOME}/dags

ENTRYPOINT ["/entrypoint.sh"]
