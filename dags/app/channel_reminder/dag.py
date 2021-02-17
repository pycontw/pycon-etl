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
    "start_date": datetime(2020, 12, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}
dag = DAG(
    "DISCORD_REMINDER",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(weeks=2),
    max_active_runs=1,
    catchup=False,
)
with dag:
    for team in ("DATATEAM",):
        REMINDER_OF_THIS_TEAM = PythonOperator(
            task_id=f"REMINDER_OF_TEAM_{team}",
            python_callable=discord.main,
            op_kwargs={
                "team": team,
                "msg": """
1. Data Team 這一季最重要的目標是讓大家多 social 以及多舉辦 sharing session
2. 其次是建好 PyCon TW 的 Data Infra
3. 雙周會前請在 Hackmd 上更新自己的近況：[Hackmd](https://hackmd.io/LLcaglX0Szed-AsHUdC3bg)
4. 認領任務：[Trello](https://trello.com/b/Rxtrpqxi/data-squad)""",
            },
        )

if __name__ == "__main__":
    dag.cli()
