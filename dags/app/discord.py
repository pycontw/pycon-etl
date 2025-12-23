import logging
from datetime import datetime

import requests
import tenacity
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Asset, Context, Variable, dag, task

# get the airflow.task logger

logger = logging.getLogger(__name__)


@dag(
    schedule=(
        Asset(name="finance_report_diff")
        | Asset(name="proposal_count")
        | Asset(name="registration_statistics")
    ),
    start_date=datetime(2025, 12, 23),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Wei Lee",
        "depends_on_past": False,
    },
    tags=["discord"],
)
def discord_message_notification():
    """Send Discord Message."""

    @task
    def send_discord_message(**context: Context) -> None:
        for asset_like_obj, asset_events in context["triggering_asset_events"].items():
            logger.info(f"Receive asset event from {asset_like_obj}")

            http_hook = HttpHook(method="POST", http_conn_id="discord_webhook")
            for asset_event in asset_events:  # type: ignore[attr-defined]
                if not (details := asset_event.extra):
                    logger.error(
                        f"Detail {details} cannot be empty. It's required to send discord message."
                    )
                    continue

                logger.info("Start sending Discord message")
                http_hook.run_with_advanced_retry(
                    endpoint=Variable.get(details.get("webhook_endpoint_key")),
                    data={
                        "username": details.get("username"),
                        "content": details.get("content"),
                    },
                    _retry_args=dict(
                        wait=tenacity.wait_random(min=1, max=10),
                        stop=tenacity.stop_after_attempt(10),
                        retry=tenacity.retry_if_exception_type(
                            requests.exceptions.ConnectionError
                        ),
                    ),
                )
                logger.info("Discord message sent")

    send_discord_message()


dag_obj = discord_message_notification()

if __name__ == "__main__":
    dag_obj.test()
