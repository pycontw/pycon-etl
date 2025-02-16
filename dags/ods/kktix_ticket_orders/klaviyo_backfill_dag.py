"""
Ingest KKTIX's daily data and load them to Mailer
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.kktix_ticket_orders.udfs import batch_kktix2mailer

DEFAULT_ARGS = {
    "owner": "henry410213028@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 29),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KLAVIYO_SEND_MAIL_V3",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=True,
)
with dag:
    GET_ATTENDEE_INFOS = PythonOperator(
        task_id="GET_ATTENDEE_INFOS",
        python_callable=batch_kktix2mailer.main,
        provide_context=True,
    )

    GET_ATTENDEE_INFOS

if __name__ == "__main__":
    dag.cli()
