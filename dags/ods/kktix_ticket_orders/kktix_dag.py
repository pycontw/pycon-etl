"""
Ingest KKTIX's data and load them to BigQuery every 5mins
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.kktix_ticket_orders.udfs import bigquery_loader, gather_town_loader, kktix_api

DEFAULT_ARGS = {
    "owner": "davidtnfsh@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 14, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "KKTIX_TICKET_ORDERS_V6",
    default_args=DEFAULT_ARGS,
    schedule_interval="50 * * * *",
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
    )

    ADD_USER_TO_GATHER_TOWN_WHITELIST = PythonOperator(
        task_id="ADD_USER_TO_GATHER_TOWN_WHITELIST",
        python_callable=gather_town_loader.load,
        provide_context=True,
    )

    CREATE_TABLE_IF_NEEDED >> GET_ATTENDEE_INFOS >> ADD_USER_TO_GATHER_TOWN_WHITELIST

if __name__ == "__main__":
    dag.cli()
