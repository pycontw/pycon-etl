import logging
from datetime import datetime

import requests
from airflow.sdk import Variable
from utils.posts_insights.base import (
    CREATE_INSIGHT_TABLE_SQL_TEMPLATE,
    CREATE_POST_TABLE_SQL_TEMPLATE,
    BasePostsInsightsParser,
)

logger = logging.getLogger(__name__)


class TwitterPostsInsightsParser(BasePostsInsightsParser):
    # Sharing LinkedIn tables also shares its incremental watermark, skipping posts.
    POST_TABLE_NAME: str = "ods_pycontw_twitter_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_twitter_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(POST_TABLE_NAME)
    CREATE_INSIGHTS_TABLE_SQL = CREATE_INSIGHT_TABLE_SQL_TEMPLATE.format(
        INSIGHT_TABLE_NAME
    )

    def _request_posts_data(self) -> list[dict]:
        url = "https://twitter154.p.rapidapi.com/user/tweets"
        # 499339900 is PyConTW's twitter id
        querystring = {
            "username": "pycontw",
            "user_id": "499339900",
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

    def _filter_new_posts(self, posts: list[dict], last_post: dict) -> list[dict]:
        return [
            post
            for post in posts
            if post["timestamp"] > last_post["created_at"].timestamp()
        ]

    def _process_posts(self, posts: list[dict]) -> list[dict]:
        return [
            {
                "id": post["tweet_id"],
                "created_at": post["timestamp"],
                "message": post["text"],
            }
            for post in posts
        ]

    def _process_posts_insights(self, posts: list[dict]) -> list[dict]:
        return [
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
