"""
Save view, like count these kind of metrics into BigQuery
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.kktix_ticket_orders.udfs import discord_bot

DEFAULT_ARGS = {
    "owner": "davidtnfsh@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 4),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KKTIX_TICKET_ORDERS",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=True,
)
with dag:
    SEND_MSG_TO_DISCORD = PythonOperator(
        task_id="LOAD_TO_DISCORD", python_callable=discord_bot.send,
    )

if __name__ == "__main__":
    dag.cli()
