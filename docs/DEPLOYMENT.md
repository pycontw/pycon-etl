# Deployment Guide

## Start Deploying

### 1. Login to the data team's GCE server

```bash
gcloud compute ssh --zone "asia-east1-b" "data-team" --project "pycontw-225217"
```

* Location of the Services:
    * ETL (airflow): `/srv/pycon-etl`
    * Metabase: `/mnt/disks/data-team-additional-disk/pycontw-infra-scripts/data_team/metabase_server`

### 2. Pull the latest codebase and image to this server

```bash
git checkout prod
git pull origin prod

docker pull asia-east1-docker.pkg.dev/pycontw-225217/data-team/pycon-etl:latest
```

### 3. Add credentials to the `.env.production` file (only needs to be done once).

### 4. Restart the services:

```bash
# Start production services
docker-compose -f ./docker-compose.yml up

# Stop production services
# docker-compose -f ./docker-compose.yml down
```

### 5. Check whether the services are up

```bash
# For Airflow, the following services should be included: 
# * airflow-api-server
# * airflow-dag-processor
# * airflow-scheduler
# * airflow-triggerer
docker ps

# Check the resource usage if needed
docker stats
```

### 6. Login to the service
For security reasons, our Airflow instance is not publicly accessible. You will need an authorized GCP account to perform port forwarding for the webserver and an authorized Airflow account to access it.