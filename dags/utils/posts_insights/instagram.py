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


# Instagram media references (Facebook Login):
# https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-user/media
# https://developers.facebook.com/docs/instagram-platform/instagram-graph-api/reference/ig-media


class InstagramPostsInsightsParser(BasePostsInsightsParser):
    POST_TABLE_NAME: str = "ods_pycontw_ig_posts"
    INSIGHT_TABLE_NAME: str = "ods_pycontw_ig_posts_insights"
    CREATE_POSTS_TABLE_SQL = CREATE_POST_TABLE_SQL_TEMPLATE.format(POST_TABLE_NAME)
    CREATE_INSIGHTS_TABLE_SQL = CREATE_INSIGHT_TABLE_SQL_TEMPLATE.format(
        INSIGHT_TABLE_NAME
    )

    API_BASE_URL = "https://graph.facebook.com/v24.0"

    @staticmethod
    def _request_json(url: str, params: dict | None = None) -> dict:
        response = requests.get(url, params=params, timeout=180)
        if not response.ok:
            raise RuntimeError(
                f"Instagram API request failed (HTTP {response.status_code})"
            )
        payload: dict = response.json()
        if "error" in payload:
            raise RuntimeError("Instagram API returned an error")
        return payload

    def _request_posts_data(self) -> list[dict]:
        access_token = Variable.get("IG_ACCESS_TOKEN")
        next_url = f"{self.API_BASE_URL}/17841405043609765/media"
        params: dict | None = {"access_token": access_token, "limit": "100"}
        media_insight_list = []
        seen_pages: set[str] = set()
        seen_media: set[str] = set()
        while next_url:
            if next_url in seen_pages:
                raise RuntimeError("Instagram API returned a pagination cycle")
            seen_pages.add(next_url)
            payload = self._request_json(next_url, params)
            media_list = payload["data"]
            if not media_list:
                logger.info("Instagram media page returned no posts")
            for media in media_list:
                media_id = media.get("id")
                if not self._is_valid_id(media_id):
                    logger.warning("Skipping Instagram media with invalid id")
                    continue
                if media_id in seen_media:
                    continue
                seen_media.add(media_id)
                media_res = self._request_json(
                    f"{self.API_BASE_URL}/{media_id}",
                    {
                        "access_token": access_token,
                        "fields": "id,media_type,caption,timestamp,comments_count,like_count",
                    },
                )
                if not self._is_valid_id(media_res.get("id")):
                    logger.warning("Skipping Instagram media detail with invalid id")
                    continue
                try:
                    timestamp = datetime.strptime(
                        media_res["timestamp"], "%Y-%m-%dT%H:%M:%S%z"
                    ).timestamp()
                except (KeyError, TypeError, ValueError):
                    logger.warning("Skipping Instagram media with invalid timestamp")
                    continue
                media_insight_list.append(
                    {
                        "id": media_res["id"],
                        "timestamp": timestamp,
                        # Media without captions are valid; keep them for insights.
                        "caption": media_res.get("caption", "No Content"),
                        "comments_count": media_res.get("comments_count"),
                        "like_count": media_res.get("like_count"),
                        "media_type": media_res.get("media_type", "No Content"),
                    }
                )
            next_url = payload.get("paging", {}).get("next", "")
            params = None
        if not media_insight_list:
            logger.info("Instagram returned no usable posts")
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
