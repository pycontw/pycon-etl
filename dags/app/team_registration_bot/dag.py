"""
Send daily ordering metrics to discord channel
"""

from datetime import datetime, timedelta

from airflow.sdk import Asset, Metadata, Variable, dag, task
from app.team_registration_bot.udf import (
    _compose_discord_msg,
    _get_statistics_from_bigquery,
)

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 4),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


kktix_order_report_asset = Asset(name="kktix_order_report")


@dag(
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
)
def KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION():
    @task(outlets=[kktix_order_report_asset])
    def LOAD_TO_DISCORD():
        statistics = _get_statistics_from_bigquery()
        yield Metadata(
            kktix_order_report_asset,
            extra={
                "webhook_url": Variable.get("discord_webhook_registration_endpoint"),
                "username": "KKTIX order report",
                "content": _compose_discord_msg(statistics),
            },
        )

    LOAD_TO_DISCORD()


dag_obj = KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION()


if __name__ == "__main__":
    dag_obj.test()
