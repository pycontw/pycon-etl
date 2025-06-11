"""
Scrape X (Twitter) posts and insights data, save to BigQuery
"""
from datetime import datetime, timedelta

from utils.posts_insights.twitter import TwitterPostsInsightsParser
from airflow.sdk import dag
from airflow.sdk import task

DEFAULT_ARGS = {
    "owner": "Henry Lee",
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
def TWITTER_POST_INSIGHTS_V1():
    @task
    def CREATE_TABLE_IF_NEEDED():
        TwitterPostsInsightsParser().create_tables_if_not_exists()

    @task
    def SAVE_TWITTER_POSTS_AND_INSIGHTS():
        TwitterPostsInsightsParser().save_posts_and_insights()

    CREATE_TABLE_IF_NEEDED() >> SAVE_TWITTER_POSTS_AND_INSIGHTS()


dag_obj = TWITTER_POST_INSIGHTS_V1()

if __name__ == "__main__":
    dag_obj.test()
