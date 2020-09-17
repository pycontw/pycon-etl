# PyConTW ETL
![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)

Using Airflow to implement our ETL pipelines

## Prerequisites

1. [Get Docker](https://docs.docker.com/get-docker/)
2. [Install Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)
3. [Get npm](https://www.npmjs.com/get-npm)

## Install

`docker pull puckel/docker-airflow:1.10.9`

## Before you commit

1. `git add <files>`
2. `npm run check`: Apply all the linter and formatter
3. `npm run commit`

## Run in local env

1. Start the Airflow server: `docker run  --rm -p 8080:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags puckel/docker-airflow:1.10.9 webserver`
2. Setup the Authentication of GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
    * After invoking `gcloud auth application-default login`, you'll get a credentials.json resides in `/Users/<xxx>/.config/gcloud/application_default_credentials.json`. Invoke `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
3. Give [Toy-Examples](#Toy-Examples) a try

## CI/CD

Please check [.github/workflows](.github/workflows) for details

## Toy Examples

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