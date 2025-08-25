"""
Query user profile catagory
"""
from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task

from dags.app.user_profile.udf import (
    process_table,
    create_user_profile_table,
    check_gemini_api_key
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
def USER_PROFILE_CATAGORY_QUERY():
    @task
    def PROCESS_CATAGORY_BY_GEMINI():
        check_gemini_api_key()
        tasktypes = ["organization", "job_title"]
        create_user_profile_table()
        for tasktype in tasktypes:
            process_table("gemini-2.0-flash", 8192, 100, tasktype)

    PROCESS_CATAGORY_BY_GEMINI()


dag_obj = USER_PROFILE_CATAGORY_QUERY()

if __name__ == "__main__":
    dag_obj.test()