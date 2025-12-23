ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.10
ARG PLATFORM=linux

FROM --platform=${PLATFORM} ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

COPY ./pyproject.toml .
COPY ./uv.lock .

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev


FROM --platform=${PLATFORM} apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER airflow

COPY --from=builder --chown=airflow:airflow /app /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="${AIRFLOW_HOME}:$PYTHONPATH"

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY --chown=airflow:root dags ${AIRFLOW_HOME}/dags

ENTRYPOINT ["/entrypoint.sh"]
