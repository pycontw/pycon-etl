"""
Send Proposal Summary to Discord
"""

from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task
from app import discord
from app.proposal_reminder.udf import get_proposal_summary

DEFAULT_ARGS = {
    "owner": "Henry Lee",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 25),
    "end_date": datetime(2025, 4, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="0 16 * * *",  # At 16:00 (00:00 +8)
    max_active_runs=1,
    catchup=False,
)
def DISCORD_PROPOSAL_REMINDER_v3():
    @task
    def SEND_PROPOSAL_SUMMARY():
        webhook_url = Variable.get("DISCORD_PROGRAM_REMINDER_WEBHOOK")

        summary = get_proposal_summary()
        n_talk = summary["num_proposed_talk"]
        n_tutorial = summary["num_proposed_tutorial"]
        msg = f"目前投稿議程數: {n_talk}; 課程數: {n_tutorial}"

        discord.send_webhook_message(
            webhook_url=webhook_url,
            username="Program talk reminder",
            msg=msg,
        )

    SEND_PROPOSAL_SUMMARY()


dag_obj = DISCORD_PROPOSAL_REMINDER_v3()

if __name__ == "__main__":
    dag_obj.test()
