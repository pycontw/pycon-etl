# PyConTW ETL

![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)
![Docker Image CI](https://github.com/pycontw/PyCon-ETL/workflows/Docker%20Image%20CI/badge.svg)

Using Airflow to implement our ETL pipelines

## Table of Contents

- [Prerequisites](#prerequisites)
- [Install](#install)
- [Configuration](#configuration)
- [BigQuery (Optional)](#bigquery-optional)
- [Run](#run)
  - [Local environment with Docker](#local-environment-with-docker)
  - [Local environment with Docker (Windows)](#local-environment-with-docker-windows)
  - [Production](#production)
- [Contact](#contact)

## Prerequisites

* [Python 3.8+](https://www.python.org/downloads/release/python-3811/)
* [Poetry](https://python-poetry.org/docs/#installation)
* [Docker](https://docs.docker.com/get-docker/)
* [Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)

## Install

Install local environment for development:

```bash
poetry install
```

## Configuration

1. `cp .env.template .env.staging` for dev/test. `cp .env.template .env.production` instead if you are going to start a production instance.

2. Follow the instructions in `.env.<staging|production>` and fill in your secrets.
    If you are running the staging instance for development as a sandbox and not going to access any specific third-party service, leave the `.env.staging` as-is should be fine.

> Find the maintainer if you don't have those secrets.

> **⚠ WARNING: About .env**
> Please don't use the .env for local development, or it might screw up the production tables.

### BigQuery (Optional)

Setup the Authentication of GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
    *After invoking `gcloud auth application-default login`, you'll get a credentials.json resides in `$HOME/.config/gcloud/application_default_credentials.json`. Invoke `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
    * service-account.json: Please contact @david30907d using email, telegram, or discord. No worry about this json if you are running the sandbox staging instance for development.

## Run

If you are a developer 👨‍💻, please check [Contributing Guide](./docs/CONTRIBUTING.md).

If you are a mantainer 👨‍🔧, please check [Maintenance Guide](./docs/MAINTENANCE.md).

### Local environment with Docker

dev/test environment:

```bash
# build the dev/test local image
make build-dev

# start dev/test services
make deploy-dev

# stop dev/test services
make down-dev
```

> Difference between production and dev/test compose files is dev/test compose file use local build image, and production compose file use the image from docker hub.

### Local environment with Docker (Windows)

Same as above, bit do not use Windows Powershell; please use Command Prompt instead.

### Production

Please check [Production Deployment Guide](./docs/DEPLOYMENT.md).

## Contact

[PyCon TW Volunteer Data Team - Discord](https://discord.com/channels/752904426057892052/900721883383758879)
