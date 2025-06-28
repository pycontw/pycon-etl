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
                poke_interval=86400,  # 60*60*24
            ),
        )
    ],
)


@dag(
    schedule=(
        finance_report_asset
        | Asset.ref(name="CFP_summary")
        | Asset.ref(name="kktix_order_report")
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
        for asset_uri, asset_event in triggering_asset_events.items():
            logger.info(f"Receive asset event from Asset uri={asset_uri}")
            if asset_event.extra.get("from_trigger", False):  # type: ignore[attr-defined]
                details = asset_event.extra["payload"]  # type: ignore[attr-defined]
            else:
                details = asset_event.extra  # type: ignore[attr-defined]

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
