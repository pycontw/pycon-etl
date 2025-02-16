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
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_linkedin_posts` (
        id STRING,
        created_at TIMESTAMP,
        message STRING
    )
    """
    client.query(post_sql)
    insights_sql = """
    CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.ods_pycontw_linkedin_posts_insights` (
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

    # Example output from the Rapid API, not all fields will exists for a specific post
    #
    # {
    #   "text": "For your kids in senior high.",
    #   "totalReactionCount": 6,
    #   "likeCount": 6,
    #   "repostsCount": 1,
    #   "empathyCount": 1,
    #   "commentsCount": 20,
    #   repostsCount:1,
    #   "postUrl": "https://www.linkedin.com/feed/update/urn:li:activity:6940542340960763905/",
    #   "postedAt": "1yr",
    #   "postedDate": "2022-06-09 05:57:23.126 +0000 UTC",
    #   "postedDateTimestamp": 1654754243126,
    #   "urn": "6940542340960763905",
    #   "author": {
    #     "firstName": "Angus",
    #     "lastName": "Yang",
    #     "username": "angus-yang-8885279a",
    #     "url": "https://www.linkedin.com/in/angus-yang-8885279a"
    #   },
    #   "company": {},
    #   "article": {
    #     "title": "2022 AWS STEM Summer Camp On The Cloud",
    #     "subtitle": "pages.awscloud.com • 2 min read",
    #     "link": "https://pages.awscloud.com/tw-2022-aws-stem-summer-camp-on-the-cloud_registration.html"
    #   }
    # },


def save_posts_and_insights() -> None:
    posts = request_posts_data()

    last_post = query_last_post()
    new_posts = (
        [
            post
            for post in posts
            if post["postedDateTimestamp"] > last_post["created_at"].timestamp()
        ]
        if last_post
        else posts
    )

    if not dump_posts_to_bigquery(
        [
            {
                "id": post["urn"],
                "created_at": post["postedDateTimestamp"],
                "message": post["text"],
            }
            for post in new_posts
        ]
    ):
        raise RuntimeError("Failed to dump posts to BigQuery")

    if not dump_posts_insights_to_bigquery(
        [
            {
                "post_id": post["urn"],
                "query_time": datetime.now().timestamp(),
                "period": "lifetime",
                "favorite": post["likeCount"],
                "reply": post["commentsCount"],
                "retweet": post["repostsCount"],
                "views": "0",  # not support by RapidAPI
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
        `pycontw-225217.ods.ods_pycontw_linkedin_posts`
    ORDER BY
        created_at DESC
    LIMIT 1
    """
    result = client.query(sql)
    data = list(result)
    return data[0] if data else None


def request_posts_data() -> List[dict]:
    # Define the request options
    # url = 'https://linkedin-data-api.p.rapidapi.com/get-profile-posts' # for user
    url = "https://linkedin-data-api.p.rapidapi.com/get-company-posts"
    querystring = {"username": "pycontw"}
    headers = {
        "X-RapidAPI-Key": Variable.get("LINKEDIN_RAPIDAPI_KEY"),
        "X-RapidAPI-Host": "linkedin-data-api.p.rapidapi.com",
    }

    response = requests.get(url, headers=headers, params=querystring, timeout=180)
    if not response.ok:
        raise RuntimeError(f"Failed to fetch posts data: {response.text}")

    media_insight_list = []
    media_res_list = response.json()["data"]
    # format handling, the response may not include the required fields
    for media_res in media_res_list:
        media_insight = {}
        media_insight["urn"] = media_res.get("urn", "0")
        media_insight["postedDateTimestamp"] = (
            media_res.get("postedDateTimestamp", "0") / 1000
        )
        media_insight["text"] = media_res.get("text", "No Content")
        media_insight["likeCount"] = media_res.get("totalReactionCount", "0")
        media_insight["commentsCount"] = media_res.get("commentsCount", "0")
        media_insight["repostsCount"] = media_res.get("repostsCount", "0")
        # logger.info(media_insight)
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
            "pycontw-225217.ods.ods_pycontw_linkedin_posts",
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
            "pycontw-225217.ods.ods_pycontw_linkedin_posts_insights",
            job_config=job_config,
        )
        job.result()
        return True
    except Exception as e:
        logger.error(f"Failed to dump posts insights to BigQuery: {e}", exc_info=True)
        return False


def test_main():
    create_table_if_needed()

    # request_posts_data()

    save_posts_and_insights()


if __name__ == "__main__":
    test_main()
