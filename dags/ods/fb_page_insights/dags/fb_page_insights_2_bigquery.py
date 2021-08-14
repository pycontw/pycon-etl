"""
Send PyconTW Facebook Page Insights Data to BigQuery
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.fb_page_insights.udfs.fb_page_insights_data_uploader import (
    download_fb_insight_data_upload_to_bigquery,
)


DEFAULT_ARGS = {
    "owner": "Grimmer",
    "start_date": datetime(2021, 8, 21),
    "schedule_interval": "@weekly",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "catchup": False,
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}


with DAG("FB_PAGE_INSIGHTS_2_BIGQUERY", default_args=DEFAULT_ARGS) as dag:
    superman_task = PythonOperator(
        task_id="superman_task",
        python_callable=download_fb_insight_data_upload_to_bigquery,
    )


if __name__ == "__main__":
    dag.cli()
