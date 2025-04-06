"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.finance_bot import udf

DEFAULT_ARGS = {
    "owner": "qchwan",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 27),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}
dag = DAG(
    "DISCORD_FINANCE_REMINDER",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
)
with dag:
    REMINDER_OF_THIS_TEAM = PythonOperator(
        task_id="FINANCE_REMINDER", python_callable=udf.main
    )

if __name__ == "__main__":
    dag.cli()
