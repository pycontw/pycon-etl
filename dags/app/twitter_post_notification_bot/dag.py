"""
Send Proposal Summary to Discord
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from app.twitter_post_notification_bot import udf

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "TWITTER_POST_NOTIFICATION_BOT_V2",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="SEND_TWITTER_POST_NOTIFICATION",
        python_callable=udf.main,
    )

if __name__ == "__main__":
    dag.cli()
