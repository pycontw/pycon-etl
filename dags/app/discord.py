import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import Asset, AssetWatcher, Context, dag, task

from triggers.finance_report import FinanceReportTrigger

logger = logging.getLogger(__name__)

finance_report_asset = Asset(
    name="finance_report",
    watchers=[
        AssetWatcher(
            name="finance_report_watcher",
            trigger=FinanceReportTrigger(
                # poke_interval=86400,  # 60*60*24
                poke_interval=5,  # 60*60*24
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
    """Send Discord Message"""

    @task(
        retries=10,
        retry_delay=timedelta(seconds=10),
    )
    def send_discord_message(**context: Context) -> None:
        triggering_asset_events = context["triggering_asset_events"]
        session = requests.session()
        logger.info(f"Receive asset events {triggering_asset_events}")
        for asset_uri, asset_events in triggering_asset_events.items():
            logger.info(f"Receive asset event from Asset uri={asset_uri}")
            for asset_event in asset_events:  # type: ignore[attr-defined]
                if asset_event.extra.get("from_trigger", False):
                    details = asset_event.extra["payload"]
                else:
                    details = asset_event.extra

                session.post(
                    details.get("webhook_url"),
                    json={
                        "username": details.get("username"),
                        "content": details.get("content"),
                    },
                )

    send_discord_message()


dag_obj = discord_message_notification()

if __name__ == "__main__":
    dag_obj.test()
