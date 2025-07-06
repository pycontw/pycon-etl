import logging
from datetime import datetime

import requests
import tenacity
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Asset, AssetWatcher, Context, Variable, dag, task

from triggers.finance_report import FinanceReportTrigger

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")


finance_report_asset = Asset(
    name="finance_report",
    watchers=[
        AssetWatcher(
            name="finance_report_watcher",
            trigger=FinanceReportTrigger(
                poke_interval=86400,  # 60*60*24
            ),
        )
    ],
)


@dag(
    schedule=(
        finance_report_asset
        | Asset(name="proposal_count")
        | Asset(name="registration_statistics")
    ),
    start_date=datetime(2025, 6, 28),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Wei Lee",
        "depends_on_past": False,
    },
)
def discord_message_notification():
    """Send Discord Message."""

    @task
    def send_discord_message(**context: Context) -> None:
        triggering_asset_events = context["triggering_asset_events"]
        for asset_uri, asset_events in triggering_asset_events.items():
            task_logger.info(f"Receive asset event from Asset uri={asset_uri}")

            http_hook = HttpHook(method="POST", http_conn_id="discord_webhook")
            for asset_event in asset_events:  # type: ignore[attr-defined]
                if asset_event.extra.get("from_trigger", False):
                    details = asset_event.extra["payload"]
                else:
                    details = asset_event.extra

                if not details:
                    task_logger.error(
                        f"Detail {details} cannot be empty. It's required to send discord message."
                    )
                    continue

                task_logger.info("Start sending discord message")
                endpoint = Variable.get(details.get("webhook_endpoint_key"))
                http_hook.run_with_advanced_retry(
                    endpoint=endpoint,
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
                task_logger.info("Discord message sent")

    send_discord_message()


dag_obj = discord_message_notification()

if __name__ == "__main__":
    dag_obj.test()
