import logging
import os
from datetime import datetime
from typing import List, Optional

import requests
from airflow.models import Variable
from google.cloud import bigquery

logger = logging.getLogger(__name__)


def create_table_if_needed() -> None:
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


def save_fb_posts_and_insights() -> None:
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


def query_last_post() -> Optional[dict]:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql = """
    SELECT
        created_at
    FROM
        `pycontw-225217.ods.ods_pycontw_fb_posts`
    ORDER BY
        created_at DESC
    LIMIT 1
    """
    result = client.query(sql)
    data = list(result)
    return data[0] if data else None


def request_posts_data() -> List[dict]:
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


def dump_posts_to_bigquery(posts: List[dict]) -> bool:
    if not posts:
        logger.info("No posts to dump!")
        return True

    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("message", "STRING", mode="REQUIRED"),
        ],
        write_disposition="WRITE_APPEND",
    )
    try:
        job = client.load_table_from_json(
            posts,
            "pycontw-225217.ods.ods_pycontw_fb_posts",
            job_config=job_config,
        )
        job.result()
        return True
    except Exception as e:
        logger.error(f"Failed to dump posts to BigQuery: {e}", exc_info=True)
        return False


def dump_posts_insights_to_bigquery(posts: List[dict]) -> bool:
    if not posts:
        logger.info("No post insights to dump!")
        return True

    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("post_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("query_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reactions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("share", "INTEGER", mode="NULLABLE"),
        ],
        write_disposition="WRITE_APPEND",
    )
    try:
        job = client.load_table_from_json(
            posts,
            "pycontw-225217.ods.ods_pycontw_fb_posts_insights",
            job_config=job_config,
        )
        job.result()
        return True
    except Exception as e:
        logger.error(f"Failed to dump posts insights to BigQuery: {e}", exc_info=True)
        return False


def convert_fb_time(time_string: str) -> str:
    return (
        datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S%z")
        .strftime("%Y-%m-%d %H:%M:%S%z")
        .replace("+0000", "UTC")
    )
