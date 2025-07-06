"""
Send daily ordering metrics to discord channel
"""

from airflow.sdk import Metadata, Variable, asset
from app.team_registration_bot.udf import (
    _compose_discord_msg,
    _get_statistics_from_bigquery,
)

# DEFAULT_ARGS = {
#     "owner": "David Jr.",
#     "depends_on_past": False,
#     "start_date": datetime(2022, 7, 4),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
#     "on_failure_callback": lambda x: "Need to send notification to Discord!",
# }


@asset(
    name="registration_statistics",
    dag_id="registration_statistics",
    schedule="@daily",
)
def KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION(self):
    statistics = _get_statistics_from_bigquery()
    yield Metadata(
        self,
        extra={
            "webhook_url": Variable.get("discord_webhook_registration_endpoint"),
            "username": "KKTIX order report",
            "content": _compose_discord_msg(statistics),
        },
    )
