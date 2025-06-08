"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from app import discord

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="@yearly",
    max_active_runs=1,
    catchup=False,
)
def DISCORD_CHORES_REMINDER() -> None:
    @task
    def REMINDER_OF_THIS_TEAM() -> None:
        webhook_url = Variable.get("DISCORD_CHORES_REMINDER_WEBHOOK")
        discord.send_webhook_message(
            webhook_url=webhook_url,
            username="Data Team Airflow reminder",
            msg=(
                "<@&790739794148982796> <@&755827317904769184> <@&791157626099859487>\n"
                "記得大會結束後，要有一個人負責去取消 Klaviyo 的訂閱，不然我們每個月會一直繳 $NTD2000 喔！"
            ),
        )

    REMINDER_OF_THIS_TEAM()


dag_obj = DISCORD_CHORES_REMINDER()

if __name__ == "__main__":
    dag_obj.test()
