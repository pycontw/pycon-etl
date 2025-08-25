"""
Query user profile catagory
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from dags.app.user_profile.udf import (
    get_gemini_api_key,
    create_user_profile_table,
    process_table,
)

DEFAULT_ARGS = {
    "owner": "Xch1",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 20),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "need to query updates ",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule=None,
    max_active_runs=1,
    catchup=False,
)
def user_profile_catagory_query():
    @task
    def process_catagory_by_gemini():
        get_gemini_api_key()
        tasktypes = ["organization", "job_title"]
        create_user_profile_table()
        for tasktype in tasktypes:
            process_table("gemini-2.0-flash", 8192, 100, tasktype)

    process_catagory_by_gemini()


dag_obj = user_profile_catagory_query()

if __name__ == "__main__":
    dag_obj.test()
