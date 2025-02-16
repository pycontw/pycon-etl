"""
send daily ordering metrics to discord channel
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.team_registration_bot import udf

DEFAULT_ARGS = {
    "owner": "davidtnfsh@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 4),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KKTIX_DISCORD_BOT_FOR_TEAM_REGISTRATION",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
)
with dag:
    SEND_MSG_TO_DISCORD = PythonOperator(
        task_id="LOAD_TO_DISCORD",
        python_callable=udf.main,
    )

if __name__ == "__main__":
    dag.cli()
