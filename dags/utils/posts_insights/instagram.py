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


class InstagramPostsInsightsParser(BasePostsInsightsParser):
    POST_TABLE_NAME: str = "ods_pycontw_ig_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_ig_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_ig_posts"
    )
    CREATE_INSIGHTS_TABLE_SQL = CREATE_INSIGHT_TABLE_SQL_TEMPLATE.format(
        "ods_pycontw_ig_posts_insights"
    )

    def _request_posts_data(self) -> list[dict]:
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

            media_insight_list.append(media_insight)

        return media_insight_list

    def _filter_new_posts(self, posts: list[dict], last_post: dict) -> list[dict]:
        return [
            post
            for post in posts
            if post["timestamp"] > last_post["created_at"].timestamp()
        ]

    def _process_posts(self, posts: list[dict]) -> list[dict]:
        return [
            {
                "id": post["id"],
                "created_at": post["timestamp"],
                "message": post["caption"],
            }
            for post in posts
        ]

    def _process_posts_insights(self, posts: list[dict]) -> list[dict]:
        return [
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
