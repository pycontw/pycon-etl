"""
Send Proposal Summary to Discord
"""

from airflow.sdk import Metadata, Variable, asset
from app.proposal_reminder.udf import get_proposal_summary

# DEFAULT_ARGS = {
#     "owner": "Henry Lee",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 2, 25),
#     "end_date": datetime(2025, 4, 9),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
# }


@asset(
    name="proposal_count",
    dag_id="proposal_count",
    schedule="0 16 * * *",  # At 16:00 (00:00 +8)
)
def DISCORD_PROPOSAL_REMINDER_v3(self):
    summary = get_proposal_summary()
    n_talk = summary["num_proposed_talk"]
    n_tutorial = summary["num_proposed_tutorial"]

    yield Metadata(
        self,
        extra={
            "webhook_url": Variable.get("DISCORD_PROGRAM_REMINDER_WEBHOOK"),
            "username": "Program talk reminder",
            "content": f"目前投稿議程數: {n_talk}; 課程數: {n_tutorial}",
        },
    )
