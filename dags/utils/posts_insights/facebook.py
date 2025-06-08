import logging
from datetime import datetime

import requests
from airflow.models import Variable
from google.cloud import bigquery
from utils.posts_insights.base import (
    CREATE_POST_TABLE_SQL_TEMPLATE,
    BasePostsInsightsParser,
)

logger = logging.getLogger(__name__)


class FacebookPostsInsightsParser(BasePostsInsightsParser):
    POST_TABLE_NAME: str = "ods_pycontw_fb_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_fb_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_fb_posts"
    )
    CREATE_INSIGHTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_fb_posts_insights` (
        post_id STRING,
        query_time TIMESTAMP,
        comments INTEGER,
        reactions INTEGER,
        share INTEGER
    )
    """

    def _request_posts_data(self) -> list[dict]:
        url = "https://graph.facebook.com/v20.0/160712400714277/feed/"
        # 160712400714277 is PyConTW's fb id
        access_token = Variable.get("FB_ACCESS_KEY")
        headers = {"Content-Type": "application/json"}
        params = {
            "fields": "id,created_time,message,comments.summary(true),reactions.summary(true),shares",
            "access_token": access_token,
        }
        response = requests.get(url, headers=headers, params=params)
        if response.ok:
            return response.json()["data"]
        raise RuntimeError(f"Failed to fetch posts data: {response.text}")

    def _filter_new_posts(self, posts: list[dict], last_post: dict) -> list[dict]:
        return [
            post
            for post in posts
            if datetime.strptime(
                post["created_time"], "%Y-%m-%dT%H:%M:%S%z"
            ).timestamp()
            > last_post["created_at"].timestamp()
        ]

    def _process_posts(self, posts: list[dict]) -> list[dict]:
        return [
            {
                "id": post["id"],
                "created_at": convert_fb_time(post["created_time"]),
                "message": post.get("message", "No message found"),
            }
            for post in posts
        ]

    def _dump_posts_to_bigquery(self, posts: list[dict]) -> None:
        self._dump_to_bigquery(
            posts=posts,
            dump_type="posts",
            bq_schema_fields=[
                bigquery.SchemaField(field_name, field_type, mode="REQUIRED")
                for field_name, field_type in [
                    ("id", "STRING"),
                    ("created_at", "TIMESTAMP"),
                    ("message", "STRING"),
                ]
            ],
        )

    def _process_posts_insights(self, posts: list[dict]) -> list[dict]:
        return [
            {
                "post_id": post["id"],
                "query_time": datetime.now().timestamp(),
                "comments": post["comments"]["summary"]["total_count"],
                "reactions": post["reactions"]["summary"]["total_count"],
                "share": post.get("shares", {}).get("count", 0),
            }
            for post in posts
        ]

    def _dump_posts_insights_to_bigquery(self, posts: list[dict]) -> None:
        self._dump_to_bigquery(
            posts=posts,
            dump_type="posts insights",
            bq_schema_fields=[
                bigquery.SchemaField("post_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("query_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("reactions", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("share", "INTEGER", mode="NULLABLE"),
            ],
        )


def convert_fb_time(time_string: str) -> str:
    return (
        datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S%z")
        .strftime("%Y-%m-%d %H:%M:%S%z")
        .replace("+0000", "UTC")
    )
