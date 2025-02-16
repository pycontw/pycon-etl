from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.twitter_post_insights import udfs

DEFAULT_ARGS = {
    "owner": "Henry Lee",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 14, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}
dag = DAG(
    "TWITTER_POST_INSIGHTS_V1",
    default_args=DEFAULT_ARGS,
    schedule_interval="5 8 * * *",
    max_active_runs=1,
    catchup=False,
)
with dag:
    CREATE_TABLE_IF_NEEDED = PythonOperator(
        task_id="CREATE_TABLE_IF_NEEDED",
        python_callable=udfs.create_table_if_needed,
    )

    SAVE_TWITTER_POSTS_AND_INSIGHTS = PythonOperator(
        task_id="SAVE_TWITTER_POSTS_AND_INSIGHTS",
        python_callable=udfs.save_twitter_posts_and_insights,
    )

    CREATE_TABLE_IF_NEEDED >> SAVE_TWITTER_POSTS_AND_INSIGHTS


if __name__ == "__main__":
    dag.cli()
