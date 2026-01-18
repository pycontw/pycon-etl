"""
Create bot network traffic
"""
import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import Variable, dag, task

DEFAULT_ARGS = {
    "owner": "Henry Lee",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

logger = logging.getLogger(__name__)


@dag(
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    max_active_runs=1,
    catchup=False,
)
def PYCONTW_ETL_BOT_v1():

    @task
    def GET_TOP_WEBSITES() -> list[str]:
        """Call Cloudflare Radar and return a list of the top-100 domains.

        Docs: https://developers.cloudflare.com/api/resources/radar/subresources/ranking/methods/top/
        """
        token = Variable.get("CLOUDFLARE_RADAR_API_TOKEN")

        url = "https://api.cloudflare.com/client/v4/radar/ranking/top"
        params = {"limit": 100}  # 100 is the maximum allowed
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Radar API response: {"result":{"top_0":[{"domain":"google.com", ...}, ...]}}
        domains = [item["domain"] for item in data.get("result", {}).get("top_0", [])]

        logger.info("Fetched %d domains from Cloudflare Radar", len(domains))
        return domains

    @task
    def REQUEST_EACH_WEBSITE(domains: list[str]):
        """Iterate through each domain and fire a GET request."""
        for domain in domains:
            site_url = f"https://www.{domain}"  # request to the www subdomain
            try:
                headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
                    "User-Agent": "PYCONTWETL Bot",
                }
                resp = requests.get(site_url, headers=headers, timeout=5, allow_redirects=True)
                logger.info("GET %s -> %s", site_url, resp.status_code)
            except Exception as exc:
                logger.warning("Failed to reach %s: %s", site_url, exc)

    top_domains = GET_TOP_WEBSITES()
    REQUEST_EACH_WEBSITE(top_domains)


dag_obj = PYCONTW_ETL_BOT_v1()

if __name__ == "__main__":
    dag_obj.test()
