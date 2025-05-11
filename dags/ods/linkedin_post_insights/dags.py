from datetime import datetime, timedelta

from airflow.decorators import dag, task
from ods.linkedin_post_insights import udfs

DEFAULT_ARGS = {
    "owner": "Angus Yang",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 14, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="5 8 */2 * *",
    max_active_runs=1,
    catchup=False,
)
def LINKEDIN_POST_INSIGHTS_V2():
    @task
    def CREATE_TABLE_IF_NEEDED():
        udfs.create_table_if_needed()

    @task
    def SAVE_LINKEDIN_POSTS_AND_INSIGHTS():
        udfs.save_posts_and_insights()

    CREATE_TABLE_IF_NEEDED() >> SAVE_LINKEDIN_POSTS_AND_INSIGHTS()


dag_obj = LINKEDIN_POST_INSIGHTS_V2()

if __name__ == "__main__":
    dag_obj.test()
