from airflow.sdk import Metadata, asset
from app.team_registration_bot.udf import (
    compose_discord_msg,
    get_statistics_from_bigquery,
)


@asset(
    schedule="@daily",
    tags=["discord"],
)
def registration_statistics(self):
    statistics = get_statistics_from_bigquery()
    yield Metadata(
        self,
        extra={
            "webhook_endpoint_key": "discord_webhook_registration_endpoint",
            "username": "KKTIX order report",
            "content": compose_discord_msg(statistics),
        },
    )
