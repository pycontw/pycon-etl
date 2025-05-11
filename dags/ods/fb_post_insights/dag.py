import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from google.cloud import bigquery
from ods.fb_post_insights.udfs import (
    convert_fb_time,
    dump_posts_insights_to_bigquery,
    dump_posts_to_bigquery,
    query_last_post,
    request_posts_data,
)

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
    schedule_interval="5 8 * * *",
    max_active_runs=1,
    catchup=False,
)
def FB_POST_INSIGHTS_V1():
    @task
    def CREATE_TABLE_IF_NEEDED():
        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        post_sql = """
        CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_fb_posts` (
            id STRING,
            created_at TIMESTAMP,
            message STRING
        )
        """
        client.query(post_sql)
        insights_sql = """
        CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_fb_posts_insights` (
            post_id STRING,
            query_time TIMESTAMP,
            comments INTEGER,
            reactions INTEGER,
            share INTEGER
        )
        """
        client.query(insights_sql)

    @task
    def SAVE_FB_POSTS_AND_INSIGHTS():
        posts = request_posts_data()

        last_post = query_last_post()
        if last_post is None:
            new_posts = posts
        else:
            new_posts = [
                post
                for post in posts
                if datetime.strptime(
                    post["created_time"], "%Y-%m-%dT%H:%M:%S%z"
                ).timestamp()
                > last_post["created_at"].timestamp()
            ]

        if not dump_posts_to_bigquery(
            [
                {
                    "id": post["id"],
                    "created_at": convert_fb_time(post["created_time"]),
                    "message": post.get("message", "No message found"),
                }
                for post in new_posts
            ]
        ):
            raise RuntimeError("Failed to dump posts to BigQuery")

        if not dump_posts_insights_to_bigquery(
            [
                {
                    "post_id": post["id"],
                    "query_time": datetime.now().timestamp(),
                    "comments": post["comments"]["summary"]["total_count"],
                    "reactions": post["reactions"]["summary"]["total_count"],
                    "share": post.get("shares", {}).get("count", 0),
                }
                for post in posts
            ]
        ):
            raise RuntimeError("Failed to dump posts insights to BigQuery")

    CREATE_TABLE_IF_NEEDED() >> SAVE_FB_POSTS_AND_INSIGHTS()


dag_obj = FB_POST_INSIGHTS_V1()

if __name__ == "__main__":
    dag_obj.test()
