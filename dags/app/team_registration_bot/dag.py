"""
Send daily ordering metrics to discord channel
"""

from datetime import datetime, timedelta

from app import discord
from app.team_registration_bot.udf import (
    _compose_discord_msg,
    _get_statistics_from_bigquery,
)
from airflow.sdk import dag
from airflow.sdk import task
from airflow.sdk import Variable

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 4),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
)
def KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION():
    @task
    def LOAD_TO_DISCORD():
        webhook_url = Variable.get("discord_webhook_registration_endpoint")
        statistics = _get_statistics_from_bigquery()
        discord.send_webhook_message(
            webhook_url=webhook_url,
            usernmae="KKTIX order report",
            msg=_compose_discord_msg(statistics),
        )

    LOAD_TO_DISCORD()


dag_obj = KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION()


if __name__ == "__main__":
    dag_obj.test()
