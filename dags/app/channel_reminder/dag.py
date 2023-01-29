"""
Send Google Search Report to Discord
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.channel_reminder.udfs import discord

DEFAULT_ARGS = {
    "owner": "davidtnfsh",
    "depends_on_past": False,
    "start_date": datetime(2022, 9, 15),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}
dag = DAG(
    "DISCORD_CHORES_REMINDER",
    default_args=DEFAULT_ARGS,
    schedule_interval="@yearly",
    max_active_runs=1,
    catchup=False,
)
with dag:
    REMINDER_OF_THIS_TEAM = PythonOperator(
        task_id="KLAIVYO_REMINDER",
        python_callable=discord.main,
        op_kwargs={
            "msg": """
<@&790739794148982796> <@&755827317904769184> <@&791157626099859487>
記得大會結束後，要有一個人負責去取消 Klaviyo 的訂閱，不然我們每個月會一直繳 $NTD2000 喔！
""",
        },
    )

if __name__ == "__main__":
    dag.cli()
