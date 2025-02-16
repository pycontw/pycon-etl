"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.channel_reminder import udf

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
        task_id="KLAIVYO_REMINDER", python_callable=udf.main
    )

if __name__ == "__main__":
    dag.cli()
