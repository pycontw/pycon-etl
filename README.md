# PyConTW ETL
![Python CI](https://github.com/pycontw/PyCon-ETL/workflows/Python%20CI/badge.svg)
![Docker Image CI](https://github.com/pycontw/PyCon-ETL/workflows/Docker%20Image%20CI/badge.svg)

Using Airflow to implement our ETL pipelines

## Dags

* Dag 的命名規則請看這篇 [阿里巴巴大數據實戰](https://medium.com/@davidtnfsh/%E5%A4%A7%E6%95%B0%E6%8D%AE%E4%B9%8B%E8%B7%AF-%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%AE%9E%E8%B7%B5-%E8%AE%80%E6%9B%B8%E5%BF%83%E5%BE%97-54e795c2b8c)
* Please refer to [this article](https://medium.com/@davidtnfsh/%E5%A4%A7%E6%95%B0%E6%8D%AE%E4%B9%8B%E8%B7%AF-%E9%98%BF%E9%87%8C%E5%B7%B4%E5%B7%B4%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%AE%9E%E8%B7%B5-%E8%AE%80%E6%9B%B8%E5%BF%83%E5%BE%97-54e795c2b8c) for naming guidline

1. ods/opening_crawler: Crawlers written by @Rain. Those openings can be used for recuitment board, which was implemented by @tai271828 and @stacy.
2. ods/survey_cake: A manually triggered uploader which would upload questionnaire to bigquery. The uploader should be invoked after we recieved the surveycake questionnaire.

## Prerequisites

1. [Install Python 3.8+](https://www.python.org/downloads/release/python-3811/)
2. [Get Docker](https://docs.docker.com/get-docker/)
3. [Install Git](https://git-scm.com/book/zh-tw/v2/%E9%96%8B%E5%A7%8B-Git-%E5%AE%89%E8%A3%9D%E6%95%99%E5%AD%B8)
4. [Get npm](https://www.npmjs.com/get-npm)

## Install

1. `docker pull puckel/docker-airflow:1.10.9`
2. Python dependencies:
    1. `virtualenv venv`
        * `. venv/bin/activate`
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

### Local environment Python Script

1. `. venv/bin/activate`
2. `. ./.env.sh`
3. `cd contrib`
4. Check its command in [contrib/README.md](contrib/README.md)
5. `python xxx.py`


### Local environment Docker

> Find @davidtnfsh if you don't have those secrets.

> **⚠ WARNING: About .env**  
> Please don't use the .env for local development, or it might screw up the production tables.

1. Build docker image:
    * Build a production image (for production): `docker build -t davidtnfsh/pycon_etl:prod --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile .`
      If you want to build dev/test image, you also need to build this docker image first because dev/test image is on top of this production image. See below.
    * Build dev/test image (for dev/test): `docker build -t davidtnfsh/pycon_etl:test --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile.test .`
2. Fill in some secrets:
    1. `cp .env.template .env.staging` for dev/test. `cp .env.template .env.production` instead if you are going to start a production instance.
    2. Follow the instruction in `.env.<staging|production>` and fill in your secrets.
       If you are just running the staging instance for development as a sandbox, and not going to access any specific thrid-party service, leave the `.env.staging` as-is should be fine.
3. Start the Airflow server:
    * production: `docker run -p 80:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags -v $(pwd)/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env.production davidtnfsh/pycon_etl:prod webserver`
    * dev/test: `docker run -p 80:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags -v $(pwd)/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env.staging davidtnfsh/pycon_etl:test webserver`
    * Note the difference are just the env file name and the image cache.
4. Portforward compute instance to your local and then navigate to <http://localhost:8080/admin/>:
   1. `gcloud beta compute ssh --zone "asia-east1-b" "data-team" --project "pycontw-225217" -- -NL 8080:localhost:8080`
   2. If Port 8080 is already in use. You need to stop the service occupied 8080 port on your local first.

![image](./docs/airflow.png)

### Local environment Docker(windows)
> Do not use Windows Powershell, please use Comman Prompt instead.

> Find @davidtnfsh if you don't have those secrets.

> **⚠ WARNING: About .env**  
> Please don't use the .env for local development, or it might screw up the production tables.

1. Build docker image:
    * Build a production image (for production): `docker build -t davidtnfsh/pycon_etl:prod --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile .`
      If you want to build dev/test image, you also need to build this docker image first because dev/test image is on top of this production image. See below.
    * Build dev/test image (for dev/test): `docker build -t davidtnfsh/pycon_etl:test --cache-from davidtnfsh/pycon_etl:prod -f Dockerfile.test .`
2. Fill in some secrets:
    1. `copy .env.template .env.staging` for dev/test. `copy .env.template .env.production` instead if you are going to start a production instance.
    2. Follow the instruction in `.env.<staging|production>` and fill in your secrets.
       If you are just running the staging instance for development as a sandbox, and not going to access any specific thrid-party service, leave the `.env.staging` as-is should be fine.
3. Start the Airflow server:
    * production: `docker run -p 80:8080 --name airflow -v "/$(pwd)"/dags:/usr/local/airflow/dags -v "/$(pwd)"/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env.production davidtnfsh/pycon_etl:prod webserver`
    * dev/test: `docker run -p 80:8080 --name airflow  -v "/$(pwd)"/dags:/usr/local/airflow/dags -v "/$(pwd)"/service-account.json:/usr/local/airflow/service-account.json --env-file=./.env.staging davidtnfsh/pycon_etl:test webserver`
    * Note the difference are just the env file name and the image cache.
4. Portforward compute instance to your local and then navigate to <http://localhost/admin/>:
   1. `gcloud beta compute ssh --zone "asia-east1-b" "data-team" --project "pycontw-225217" -- -N -L 8080:localhost:8080`
   2. If Port 8080 is already in use. You need to stop the service occupied 8080 port on your local first.


![image](./docs/airflow.png)
#### BigQuery (Optional)
1. Setup the Authentication of GCP: <https://googleapis.dev/python/google-api-core/latest/auth.html>
    * After invoking `gcloud auth application-default login`, you'll get a credentials.json resides in `$HOME/.config/gcloud/application_default_credentials.json`. Invoke `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/keyfile.json"` if you have it.
    * service-account.json: Please contact @david30907d using email, telegram or discord. No worry about this json if you are just running the sandbox staging instance for development.
2. Give [Toy-Examples](#Toy-Examples) a try

## Deployment

1. Manually deploy to Google compute instance
2. Fill out `airflow.cfg` with Google OAuth ID and credential (Ref: [setting-up-google-authentication](https://airflow.apache.org/docs/apache-airflow/1.10.1/security.html#setting-up-google-authentication))

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

### Conventions

* table name convention:
    ![img](https://miro.medium.com/max/1400/1*bppuEKMnL9gFnvoRHUO8CQ.png)
