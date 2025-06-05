"""
Scrape Facebook posts and insights data, save to BigQuery
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from utils.posts_insights.facebook import FacebookPostsInsightsParser

DEFAULT_ARGS = {
    "owner": "CHWan",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 14, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="5 8 * * *",
    max_active_runs=1,
    catchup=False,
)
def FB_POST_INSIGHTS_V1():
    @task
    def CREATE_TABLE_IF_NEEDED():
        FacebookPostsInsightsParser().create_tables_if_not_exists()

    @task
    def SAVE_FB_POSTS_AND_INSIGHTS():
        FacebookPostsInsightsParser().save_posts_and_insights()

    CREATE_TABLE_IF_NEEDED() >> SAVE_FB_POSTS_AND_INSIGHTS()


dag_obj = FB_POST_INSIGHTS_V1()

if __name__ == "__main__":
    dag_obj.test()
