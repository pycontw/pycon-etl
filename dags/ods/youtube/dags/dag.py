"""
Save view, like count these kind of metrics into BigQuery
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.youtube.udfs import youtube_api

DEFAULT_ARGS = {
    "owner": "davidtnfsh",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 19),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Telegram",
}
dag = DAG(
    "ODS_YOUTUBE_2_BIGQUERY",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
)
with dag:
    CREATE_TABLE_IF_NEEDED = PythonOperator(
        task_id="CREATE_TABLE_IF_NEEDED",
        python_callable=youtube_api.create_table_if_needed,
    )

    GET_VIDEO_IDS = PythonOperator(
        task_id="GET_VIDEO_IDS",
        python_callable=youtube_api.get_video_ids,
        provide_context=True,
    )

    SAVE_STATISTICS_DATA_2_BQ = PythonOperator(
        task_id="SAVE_STATISTICS_DATA_2_BQ",
        python_callable=youtube_api.save_video_data_2_bq,
        provide_context=True,
        op_kwargs={"datatype": "statistics"},
    )
    CREATE_TABLE_IF_NEEDED >> GET_VIDEO_IDS >> SAVE_STATISTICS_DATA_2_BQ

    SAVE_INFO_DATA_2_BQ = PythonOperator(
        task_id="SAVE_INFO_DATA_2_BQ",
        python_callable=youtube_api.save_video_data_2_bq,
        provide_context=True,
        op_kwargs={"datatype": "info"},
    )
    GET_VIDEO_IDS >> SAVE_INFO_DATA_2_BQ

if __name__ == "__main__":
    dag.cli()
