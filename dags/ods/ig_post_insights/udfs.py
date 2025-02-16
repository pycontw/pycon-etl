import logging
import os
from datetime import datetime
from typing import List, Optional

import requests
from airflow.models import Variable
from google.cloud import bigquery

logger = logging.getLogger(__name__)


# IG API docs
# https://developers.facebook.com/docs/instagram-api/reference/ig-user/media?locale=zh_TW
# https://developers.facebook.com/docs/instagram-api/reference/ig-media

# // get list of media-id
# GET /v20.0/{page-id}/media/?access_token={access_token}

# // get media detail
# GET /v20.0/{media-id}?access_token={access_token}&fields=id,media_type,caption,timestamp,comments_count,like_count

# PyConTW IG page-id: 17841405043609765
# ps. IG api 目前不提供分享數, 所以只有點讚數和留言數

# Access Token
# Check Henry


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    post_sql = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_ig_posts` (
        id STRING,
        created_at TIMESTAMP,
        message STRING
    )
    """
    client.query(post_sql)
    insights_sql = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_ig_posts_insights` (
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


def save_posts_and_insights() -> None:
    posts = request_posts_data()

    last_post = query_last_post()
    new_posts = (
        [
            post
            for post in posts
            if post["timestamp"] > last_post["created_at"].timestamp()
        ]
        if last_post
        else posts
    )

    if not dump_posts_to_bigquery(
        [
            {
                "id": post["id"],
                "created_at": post["timestamp"],
                "message": post["caption"],
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
                "period": "lifetime",
                "favorite": post["like_count"],
                "reply": post["comments_count"],
                "retweet": "0",  # API not supported
                "views": "0",  # API not supported
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
        `pycontw-225217.ods.ods_pycontw_ig_posts`
    ORDER BY
        created_at DESC
    LIMIT 1
    """
    result = client.query(sql)
    data = list(result)
    return data[0] if data else None


def request_posts_data() -> List[dict]:
    media_list_url = "https://graph.facebook.com/v20.0/17841405043609765/media"
    querystring = {"access_token": Variable.get("IG_ACCESS_TOKEN"), "limit": "0"}
    headers = {"Content-Type": "application/json"}

    response = requests.get(
        media_list_url, headers=headers, params=querystring, timeout=180
    )
    if not response.ok:
        raise RuntimeError(f"Failed to fetch posts data: {response.text}")
    media_list = response.json()["data"]

    media_insight_list = []

    for media in media_list:
        media_insight_url = f"https://graph.facebook.com/v20.0/{media['id']}"
        querystring = {
            "access_token": Variable.get("IG_ACCESS_TOKEN"),
            "fields": "id,media_type,caption,timestamp,comments_count,like_count",
        }
        response = requests.get(
            media_insight_url, headers=headers, params=querystring, timeout=180
        )
        if not response.ok:
            raise RuntimeError(f"Failed to fetch posts data: {response.text}")

        media_insight = {}
        media_res: dict = response.json()
        # Error handling, the response may not include the required fields, media id: 17889558458829258, no "caption"
        media_insight["id"] = media_res.get("id", "0")
        media_insight["timestamp"] = datetime.strptime(
            media_res.get("timestamp", "0"), "%Y-%m-%dT%H:%M:%S%z"
        ).timestamp()
        media_insight["caption"] = media_res.get("caption", "No Content")
        media_insight["comments_count"] = media_res.get("comments_count", "0")
        media_insight["like_count"] = media_res.get("like_count", "0")
        media_insight["media_type"] = media_res.get("media_type", "No Content")

        # print(media_insight)
        media_insight_list.append(media_insight)

    return media_insight_list


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
            "pycontw-225217.ods.ods_pycontw_ig_posts",
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
            "pycontw-225217.ods.ods_pycontw_ig_posts_insights",
            job_config=job_config,
        )
        job.result()
        return True
    except Exception as e:
        logger.error(f"Failed to dump posts insights to BigQuery: {e}", exc_info=True)
        return False


def test_main():
    create_table_if_needed()

    save_posts_and_insights()


if __name__ == "__main__":
    test_main()
