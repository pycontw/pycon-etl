"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.google_search_console.udfs.google_search import GoogleSearchConsoleReporter

DEFAULT_ARGS = {
    "owner": "davidtnfsh",
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}
dag = DAG(
    "GOOGLE_SEARCH_REPORT",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(weeks=2),
    max_active_runs=1,
    catchup=False,
)
GOOGLE_SEARCH_REPORTER = GoogleSearchConsoleReporter()
with dag:
    GET_AND_SEND_REPORT = PythonOperator(
        task_id="GET_AND_SEND_REPORT",
        python_callable=GOOGLE_SEARCH_REPORTER.main,
    )

if __name__ == "__main__":
    dag.cli()
