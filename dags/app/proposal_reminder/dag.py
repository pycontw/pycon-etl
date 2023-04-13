"""
Send Proposal Summary to Discord
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    "owner": "Henry Lee",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 1),
    "end_date": datetime(2023, 4, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "DISCORD_PROPOSAL_REMINDER_v2",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 16 * * *",  # At 16:00 (00:00 +8)
    max_active_runs=1,
    catchup=False,
) as dag:
    from app.proposal_reminder import udf

    PythonOperator(
        task_id="SEND_PROPOSAL_SUMMARY", python_callable=udf.main,
    )

if __name__ == "__main__":
    dag.cli()
