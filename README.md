# PyConTW ETL

A ETL pipeline built on GCP cloud composer

## Install

1. Python dependencies:
    1. `virtualenv venv; . venv/bin/activate`
    2. `pip install poetry`
    3. `poetry install`
2. Npm dependencies, for linter, formatter and commit linter (optional):
    1. `brew install npm`
    2. `npm ci`

## Before you commit

1. `git add <files>`
2. `npm run check`: Apply all the linter and formatter
3. `npm run commit`

## Run in local env

Not implemented yet.

## Deploy

Put your dags in dags folder and upload to GCP storage

<https://console.cloud.google.com/storage/browser/asia-east2-pycon-data-team-ba6275e0-bucket/dags?project=pycontw-225217>