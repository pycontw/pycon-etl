import logging
from datetime import datetime

import requests
from airflow.models import Variable
from utils.posts_insights.base import (
    CREATE_INSIGHT_TABLE_SQL_TEMPLATE,
    CREATE_POST_TABLE_SQL_TEMPLATE,
    BasePostsInsightsParser,
)

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


class LinkedinPostsInsightsParser(BasePostsInsightsParser):
    POST_TABLE_NAME: str = "ods_pycontw_linkedin_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_linkedin_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_linkedin_posts"
    )
    CREATE_INSIGHTS_TABLE_SQL = CREATE_INSIGHT_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_linkedin_posts_insights"
    )

    def _request_posts_data(self) -> list[dict]:
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

    def _filter_new_posts(self, posts: list[dict], last_post: dict) -> list[dict]:
        return [
            post
            for post in posts
            if post["postedDateTimestamp"] > last_post["created_at"].timestamp()
        ]

    def _process_posts(self, posts: list[dict]) -> list[dict]:
        return [
            {
                "id": post["urn"],
                "created_at": post["postedDateTimestamp"],
                "message": post["text"],
            }
            for post in posts
        ]

    def _process_posts_insights(self, posts: list[dict]) -> list[dict]:
        return [
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
