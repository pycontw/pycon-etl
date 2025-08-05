# PyCon TW ETL

![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)
![Docker Image CI](https://github.com/pycontw/PyCon-ETL/workflows/Docker%20Image%20CI/badge.svg)

Using Airflow to implement our ETL pipelines.

[TOC]

## Prerequisites

- [Python 3.10](https://www.python.org/downloads/release/python-31018/)
- [Docker](https://docs.docker.com/get-docker/)
- [Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)
- [uv]

## Installation

We use [uv] to manage dependencies and virtual environment.

Below are the steps to create a virtual environment using [uv]:

### 1. Create a Virtual Environment with Dependencies Installed

To create a virtual environment, run the following command:

```bash
uv sync
```

By default, [uv] sets up the virtual environment in `.venv`

### 2. Activate the Virtual Environment

After creating the virtual environment, activate it using the following command:

```bash
source .venv/bin/activate
```

### 3. Deactivate the Virtual Environment

When you're done working in the virtual environment, you can deactivate it with:

```bash
deactivate
```

## Configuration

1. For development or testing, run `cp .env.template .env.staging`. For production, run `cp .env.template .env.production`.
2. Follow the instructions in `.env.<staging|production>` and fill in your secrets. If you are running the staging instance for development as a sandbox and do not need to access any specific third-party services, leaving `.env.staging` as-is should be fine.

> Contact the maintainer if you don't have these secrets.
>
> **⚠ WARNING: About .env**
> Please do not use the .env file for local development, as it might affect the production tables.

### BigQuery (Optional)

- Set up the Authentication for GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
  - After running `gcloud auth application-default login`, you will get a credentials.json file located at `$HOME/.config/gcloud/application_default_credentials.json`.
  - Run `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
- `service-account.json`: Please contact @david30907d via email or Discord. You do not need this json file if you are running the sandbox staging instance for development.

## Running the Project

If you are a developer 👨‍💻, please check the [Contributing Guide](./CONTRIBUTING.md).

If you are a maintainer 👨‍🔧, please check the [Maintenance Guide](./MAINTENANCE.md).

### Local Development with uv

```bash
# point the database to local "sqlite/airflow.db"
# Run "uv run airflow db migrate" if the file does not exist
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=`pwd`/sqlite/airflow.db

# point the airflow home to current directory
export AIRFLOW_HOME=`pwd`

# Run standalone airflow
# Note that there may be slight differences between using this command and running through docker compose
# However, the difference should not be noticeable in most cases.
uv run airflow standalone
```

### Local Development with docker-compose

```bash
# Build the local dev/test image
make build-dev

# Start dev/test services
make deploy-dev

# Stop dev/test services
make down-dev
```

> The difference between production and dev/test compose files is that the dev/test compose file uses a locally built image, while the production compose file uses the image from Docker Hub.

#### Use images from Artifacts

If you are an authorized maintainer, you can pull the image from the [GCP Artifact Registry].

Docker client must be configured to use the [GCP Artifact Registry].

```bash
gcloud auth configure-docker asia-east1-docker.pkg.dev
```

Then, pull the image:

```bash
docker pull asia-east1-docker.pkg.dev/pycontw-225217/data-team/pycon-etl:{tag}
```

Available tags:

- `cache`: cache the image for faster deployment
- `test`: for testing purposes, including the test dependencies
- `staging`: when pushing to the staging environment
- `latest`: when pushing to the production environment

## Contact

[PyCon TW Volunteer Data Team - Discord](https://discord.com/channels/752904426057892052/900721883383758879)

[uv]: https://docs.astral.sh/uv/
