# PyConTW ETL

![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)
![Docker Image CI](https://github.com/pycontw/PyCon-ETL/workflows/Docker%20Image%20CI/badge.svg)

Using Airflow to implement our ETL pipelines.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [BigQuery (Optional)](#bigquery-optional)
- [Running the Project](#running-the-project)
  - [Local Environment with Docker](#local-environment-with-docker)
  - [Production](#production)
- [Contact](#contact)

## Prerequisites

- [Python 3.8+](https://www.python.org/downloads/release/python-3811/)
- [Docker](https://docs.docker.com/get-docker/)
- [Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)
- [Poetry](https://python-poetry.org/docs/#installation) (Optional, only for creating virtual environments during development)

## Installation

Install the local environment for development:

```bash
# Use poetry to create a virtual environment
poetry install

# Or use pip to install in your existing Python environment
# If you encounter any Airflow errors, check constraints-3.8.txt and reinstall Airflow dependencies
pip install -r requirements.txt
```

## Configuration

1. For development or testing, run `cp .env.template .env.staging`. For production, run `cp .env.template .env.production`.

2. Follow the instructions in `.env.<staging|production>` and fill in your secrets.
    If you are running the staging instance for development as a sandbox and do not need to access any specific third-party services, leaving `.env.staging` as-is should be fine.

> Contact the maintainer if you don't have these secrets.

> **⚠ WARNING: About .env**
> Please do not use the .env file for local development, as it might affect the production tables.

### BigQuery (Optional)

Set up the Authentication for GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
    * After running `gcloud auth application-default login`, you will get a credentials.json file located at `$HOME/.config/gcloud/application_default_credentials.json`. Run `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
    * service-account.json: Please contact @david30907d via email, Telegram, or Discord. You do not need this json file if you are running the sandbox staging instance for development.

## Running the Project

If you are a developer 👨‍💻, please check the [Contributing Guide](./docs/CONTRIBUTING.md).

If you are a maintainer 👨‍🔧, please check the [Maintenance Guide](./docs/MAINTENANCE.md).

### Local Environment with Docker

For development/testing:

```bash
# Build the local dev/test image
make build-dev

# Create the Airflow DB volume during the first setup
docker volume create --name=airflow-db-volume

# Start dev/test services
make deploy-dev

# Stop dev/test services
make down-dev
```

> The difference between production and dev/test compose files is that the dev/test compose file uses a locally built image, while the production compose file uses the image from Docker Hub.

### Production

Please check the [Production Deployment Guide](./docs/DEPLOYMENT.md).

## Contact

[PyCon TW Volunteer Data Team - Discord](https://discord.com/channels/752904426057892052/900721883383758879)