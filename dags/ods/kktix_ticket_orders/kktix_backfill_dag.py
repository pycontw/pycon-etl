"""
Ingest KKTIX's data and load them to BigQuery every 5mins
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.kktix_ticket_orders.udfs import bigquery_loader, gather_town_loader, kktix_api

DEFAULT_ARGS = {
    "owner": "bc842017@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2022, 10, 30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KKTIX_TICKET_BACKFILL_ORDERS_V1",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=True,
)
with dag:
    CREATE_TABLE_IF_NEEDED = PythonOperator(
        task_id="CREATE_TABLE_IF_NEEDED",
        python_callable=bigquery_loader.create_table_if_needed,
    )

    GET_ATTENDEE_INFOS = PythonOperator(
        task_id="GET_ATTENDEE_INFOS",
        python_callable=kktix_api.main,
        provide_context=True,
        op_kwargs={"backfill": True}
    )

    CREATE_TABLE_IF_NEEDED >> GET_ATTENDEE_INFOS

if __name__ == "__main__":
    dag.cli()
