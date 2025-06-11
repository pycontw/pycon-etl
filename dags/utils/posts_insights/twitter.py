import logging
from datetime import datetime

import requests
from utils.posts_insights.base import (
    CREATE_INSIGHT_TABLE_SQL_TEMPLATE,
    CREATE_POST_TABLE_SQL_TEMPLATE,
    BasePostsInsightsParser,
)
from airflow.sdk import Variable

logger = logging.getLogger(__name__)


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


class TwitterPostsInsightsParser(BasePostsInsightsParser):
    POST_TABLE_NAME: str = "ods_pycontw_linkedin_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_linkedin_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_linkedin_posts"
    )
    CREATE_INSIGHTS_TABLE_SQL = CREATE_INSIGHT_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_linkedin_posts_insights"
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
