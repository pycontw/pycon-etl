"""
Update KKTIX's data if attendee has been refunded
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.kktix_ticket_orders.udfs import kktix_refund

DEFAULT_ARGS = {
    "owner": "henry410213028@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 18, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KKTIX_TICKET_REFUND_V3",
    default_args=DEFAULT_ARGS,
    schedule_interval="50 23 * * *",  # At 23:50 (everyday)
    max_active_runs=1,
    catchup=True,
)
with dag:
    UPDATE_REFUNDED_ATTENDEE_IDS = PythonOperator(
        task_id="UPDATE_REFUNDED_ATTENDEE_IDS",
        python_callable=kktix_refund.main,
    )

    UPDATE_REFUNDED_ATTENDEE_IDS

if __name__ == "__main__":
    dag.cli()
