import logging
import os
import requests
from typing import List, Optional
from airflow.models import Variable
from google.cloud import bigquery
from datetime import datetime


logger = logging.getLogger(__name__)


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    post_sql = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_twitter_posts` (
        id STRING,
        created_at TIMESTAMP,
        message STRING
    )
    """
    client.query(post_sql)
    insights_sql = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_twitter_posts_insights` (
        post_id STRING,
        query_time TIMESTAMP,
        period STRING,
        favorite INTEGER,
        reply INTEGER,
        retweet INTEGER,
        views INTEGER
    )
    """
    client.query(insights_sql)


def save_twitter_posts_and_insights() -> None:
    posts = request_posts_data()

    last_post = query_last_post()
    if last_post is None:
        new_posts = posts
    else:
        new_posts = [
            post
            for post in posts
            if post["timestamp"] > last_post["created_at"].timestamp()
        ]

    if not dump_posts_to_bigquery(
        [
            {
                "id": post["tweet_id"],
                "created_at": post["timestamp"],
                "message": post["text"],
            }
            for post in new_posts
        ]
    ):
        raise RuntimeError("Failed to dump posts to BigQuery")

    if not dump_posts_insights_to_bigquery(
        [
            {
                "post_id": post["tweet_id"],
                "query_time": datetime.now().timestamp(),
                "period": "lifetime",
                "favorite": post["favorite_count"],
                "reply": post["reply_count"],
                "retweet": post["retweet_count"],
                "views": post["views"],
            }
            for post in posts
        ]
    ):
        raise RuntimeError("Failed to dump posts insights to BigQuery")


def query_last_post() -> Optional[dict]:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql = """
    SELECT
        *
    FROM
        `pycontw-225217.ods.ods_pycontw_twitter_posts`
    ORDER BY
        created_at DESC
    LIMIT 1
    """
    result = client.query(sql)
    data = list(result)
    return data[0] if data else None


def request_posts_data() -> List[dict]:
    url = "https://twitter154.p.rapidapi.com/user/tweets"
    # 499339900 is PyConTW's twitter id
    querystring = {
        "username": "pycontw",
        "user_id": "96479162",
        "limit": "40",
        "include_replies": "false",
        "include_pinned": "false",
    }
    headers = {
        "X-RapidAPI-Key": Variable.get("RAPIDAPIAPI_KEY"),
        "X-RapidAPI-Host": "twitter154.p.rapidapi.com",
    }
    response = requests.get(url, headers=headers, params=querystring)
    if response.ok:
        return response.json()["results"]
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
            "pycontw-225217.ods.ods_pycontw_twitter_posts",
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
            bigquery.SchemaField("period", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("favorite", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("reply", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("retweet", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("views", "INTEGER", mode="NULLABLE"),
        ],
        write_disposition="WRITE_APPEND",
    )
    try:
        job = client.load_table_from_json(
            posts,
            "pycontw-225217.ods.ods_pycontw_twitter_posts_insights",
            job_config=job_config,
        )
        job.result()
        return True
    except Exception as e:
        logger.error(f"Failed to dump posts insights to BigQuery: {e}", exc_info=True)
        return False
