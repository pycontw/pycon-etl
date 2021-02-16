# PyConTW ETL
![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)
![Docker Image CI](https://github.com/pycontw/PyCon-ETL/workflows/Docker%20Image%20CI/badge.svg)

Using Airflow to implement our ETL pipelines

## Dags

1. ods/opening_crawler: Crawlers written by @Rain. Those openings can be used for recuitment board, which was implemented by @tai271828 and @stacy.
2. ods/survey_cake: A manually triggered uploader which would upload questionnaire to bigquery. The uploader should be invoked after we recieved the surveycake questionnaire.

## Prerequisites

1. [Install Python 3.7](https://www.python.org/downloads/release/python-379/)
2. [Get Docker](https://docs.docker.com/get-docker/)
3. [Install Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)
4. [Get npm](https://www.npmjs.com/get-npm)

## Install

1. `docker pull puckel/docker-airflow:1.10.9`
2. Python dependencies:
    1. `virtualenv venv; . venv/bin/activate`
    2. `pip install poetry`
    3. `poetry install`
3. Npm dependencies, for linter, formatter and commit linter (optional):
    1. `brew install npm`
    2. `npm ci`

## Commit

1. `git add <files>`
2. `npm run check`: Apply all the linter and formatter
3. `npm run commit`

## Run
### Local environment

> Would need to setup Snowflake Connection manually, find @davidtnfsh if you don't have those secrets

> **⚠ WARNING: About .env**  
> Please don't use the .env for local development, or it might screw up the production tables.

1. Build docker image:
    1. First, build a prod image (for prod): `docker build -t davidtnfsh/pycon_etl:prod --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile .`
    2. Build dev/test image (for dev/test): `docker build -t davidtnfsh/pycon_etl:test --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile.test .`
2. Fill in some secrets:
    1. `cp .env.template .env.staging`
    2. Follow the instruction in `.env.staging` and fill in your secrets
3. Start the Airflow server:
    * dev/test: `docker run --rm -p 80:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags -v $(pwd)/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env davidtnfsh/pycon_etl:test webserver`
    * prod: `docker run --rm -p 80:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags -v $(pwd)/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env davidtnfsh/pycon_etl:prod webserver`
    * service-account.json: Please contact @david30907d using email, telegram or discord.
4. Setup the Authentication of GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
    * After invoking `gcloud auth application-default login`, you'll get a credentials.json resides in `/Users/<xxx>/.config/gcloud/application_default_credentials.json`. Invoke `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
5. Give [Toy-Examples](#Toy-Examples) a try

## Deployment

1. 
### CI/CD

Please check [.github/workflows](.github/workflows) for details

## Tutorials

BigQuery Example:

```python
from google.cloud import bigquery

client = bigquery.Client(project='pycontw-225217')

# Perform a query.
QUERY = '''
    SELECT scenario.day2checkin.attr.diet FROM `pycontw-225217.ods.ods_opass_attendee_timestamp`
'''
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.diet)
```