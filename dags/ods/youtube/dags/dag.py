"""
Save view, like count these kind of metrics into BigQuery
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from ods.youtube.udfs import youtube_api

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 19),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Telegram",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
)
def ODS_YOUTUBE_2_BIGQUERY():
    @task
    def CREATE_TABLE_IF_NEEDED():
        youtube_api.create_table_if_needed()

    @task
    def GET_VIDEO_IDS(**context):
        youtube_api.get_video_ids(**context)

    @task
    def SAVE_DATA_2_BQ(datatype: str, **context):
        youtube_api.save_video_data_2_bq(datatype=datatype, **context)

    (
        CREATE_TABLE_IF_NEEDED()
        >> GET_VIDEO_IDS()
        >> SAVE_DATA_2_BQ.expand(datatype=["statistics", "info"])
    )


dag_obj = ODS_YOUTUBE_2_BIGQUERY()

if __name__ == "__main__":
    dag_obj.test()
