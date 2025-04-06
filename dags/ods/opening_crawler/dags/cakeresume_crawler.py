"""
A crawler which would crawl the openings
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.opening_crawler.udfs.crawlers import CakeResumeCrawler

DEFAULT_ARGS = {
    "owner": "davidtnfsh",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Telegrame",
}
dag = DAG(
    "OPENING_CRAWLER_V1",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
)
with dag:
    CRAWLER = PythonOperator(
        task_id="CRAWLER",
        python_callable=CakeResumeCrawler.crawl,
        provide_context=True,
        op_kwargs={},
    )

if __name__ == "__main__":
    dag.cli()
