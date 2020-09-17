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

`docker run  --rm -p 8080:8080 --name airflow  -v $(pwd)/dags:/usr/local/airflow/dags puckel/docker-airflow:1.10.9 webserver`

## CI/CD

Please check [.github/workflows](.github/workflows) for details