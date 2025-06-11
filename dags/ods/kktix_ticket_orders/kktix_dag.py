"""
Ingest KKTIX's data and load them to BigQuery every 5mins
"""

from datetime import datetime, timedelta

from ods.kktix_ticket_orders.udfs import bigquery_loader, kktix_api
from airflow.sdk import dag
from airflow.sdk import task

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 16, 15),  # 23 (+8)
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="50 * * * *",
    max_active_runs=1,
    catchup=True,
)
def KKTIX_TICKET_ORDERS_V10():
    @task
    def CREATE_TABLE_IF_NEEDED():
        bigquery_loader.create_table_if_needed()

    @task
    def GET_ATTENDEE_INFOS(**context):
        kktix_api.main(**context)

    CREATE_TABLE_IF_NEEDED() >> GET_ATTENDEE_INFOS()


dag_obj = KKTIX_TICKET_ORDERS_V10()

if __name__ == "__main__":
    dag_obj.test()
