"""
Send daily ordering metrics to discord channel
"""

from airflow.sdk import Metadata, asset
from app.team_registration_bot.udf import (
    compose_discord_msg,
    get_statistics_from_bigquery,
)

# DEFAULT_ARGS = {
#     "owner": "David Jr.",
#     "depends_on_past": False,
#     "start_date": datetime(2022, 7, 4),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
#     "on_failure_callback": lambda x: "Need to send notification to Discord!",
# }


@asset(schedule="@daily")
def registration_statistics(self):
    # KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION
    statistics = get_statistics_from_bigquery()
    yield Metadata(
        self,
        extra={
            "webhook_endpoint_key": "discord_webhook_registration_endpoint",
            "username": "KKTIX order report",
            "content": compose_discord_msg(statistics),
        },
    )
