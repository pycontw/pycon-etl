# Deployment Guide

1. Login to the data team's server:
    1. Run: `gcloud compute ssh --zone "asia-east1-b" "data-team" --project "pycontw-225217"`
    2. Services:
        * ETL: `/srv/pycon-etl`
        * Metabase is located here: `/mnt/disks/data-team-additional-disk/pycontw-infra-scripts/data_team/metabase_server`

2. Pull the latest codebase to this server: `git pull`

3. Add credentials to the `.env` file (only needs to be done once).

4. Start the services:

```bash
# Start production services
make deploy-prod

# Stop production services
# make down-prod
```