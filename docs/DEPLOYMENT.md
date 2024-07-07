# Deployment Guide

1. Login to the data team's server:
    1. `gcloud compute ssh --zone "asia-east1-b" "data-team" --project "pycontw-225217"`
    2. service:
        * ETL: `/home/zhangtaiwei/pycon-etl`
        * btw, metabase is located here: `/mnt/disks/data-team-additional-disk/pycontw-infra-scripts/data_team/metabase_server`

2. Pull the latest codebase to this server: `sudo git pull`

3. Add Credentials (only need to do once):
    * Airflow:
        * Connections:
            * kktix_api: `conn_id=kktix_api`, `host` and `extra(header)` are confidential since its KKTIX's private endpoint. Please DM @GTB or data team's teammembers for these credentials.
                * extra: `{"Authorization": "bearer xxx"}`
            * klaviyo_api: `conn_id=klaviyo_api`, `host` is <https://a.klaviyo.com/api>
        * Variables:
            * KLAVIYO_KEY: Create from <https://www.klaviyo.com/account#api-keys-tab>
            * KLAVIYO_LIST_ID: Create from <https://www.klaviyo.com/lists>
            * KLAVIYO_CAMPAIGN_ID: Create from <https://www.klaviyo.com/campaigns>
            * kktix_events_endpoint: url path of kktix's `hosting_events`, ask @gtb for details!

4. Start the services:

```bash
# start production services
make deploy-prod

# stop production services
# make down-prod
```
