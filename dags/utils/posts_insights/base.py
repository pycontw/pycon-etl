import logging
import os
from abc import ABC, abstractmethod

from google.cloud import bigquery

logger = logging.getLogger(__name__)


CREATE_POST_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.{}` (
    id STRING,
    created_at TIMESTAMP,
    message STRING
)
"""

CREATE_INSIGHT_TABLE_SQL_TEMPLATE = """
CREATE TABLE IF NOT EXISTS `pycontw-225217.ods.{}` (
    post_id STRING,
    query_time TIMESTAMP,
    period STRING,
    favorite INTEGER,
    reply INTEGER,
    retweet INTEGER,
    views INTEGER
)
"""


class BasePostsInsightsParser(ABC):
    # TODO: combine table name with table creation sql
    POST_TABLE_NAME: str = ""
    INSIGHT_TABLE_NAME: str = ""
    CREATE_POSTS_TABLE_SQL: str = ""
    CREATE_INSIGHTS_TABLE_SQL: str = ""

    def create_tables_if_not_exists(self) -> None:
        if not self.CREATE_POSTS_TABLE_SQL and not self.CREATE_INSIGHTS_TABLE_SQL:
            raise ValueError(
                "Both the SQLs to create table for posts and insights must be set"
            )

        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT", ""))
        for sql in [self.CREATE_POSTS_TABLE_SQL, self.CREATE_INSIGHTS_TABLE_SQL]:
            client.query(sql)

    def save_posts_and_insights(self) -> None:
        posts = self._request_posts_data()
        last_post = self._query_last_post()
        new_posts = (
            self._filter_new_posts(posts, last_post) if last_post is not None else posts
        )

        posts_data = self._process_posts(new_posts)
        self._dump_posts_to_bigquery(posts_data)

        posts_insights_data = self._process_posts_insights(posts)
        self._dump_posts_insights_to_bigquery(posts_insights_data)

    @abstractmethod
    def _request_posts_data(self) -> list[dict]: ...

    @abstractmethod
    def _filter_new_posts(self, posts: list[dict], last_post: dict) -> list[dict]: ...

    def _query_last_post(self) -> dict | None:
        client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
        sql = f"""
        SELECT
            created_at
        FROM
            `pycontw-225217.ods.{self.POST_TABLE_NAME}`
        ORDER BY
            created_at DESC
        LIMIT 1
        """
        result = client.query(sql)
        data = list(result)
        return data[0] if data else None

    @abstractmethod
    def _process_posts(self, posts: list[dict]) -> list[dict]: ...

    def _dump_posts_to_bigquery(self, posts: list[dict]) -> None:
        if not posts:
            logger.info("No posts to dump!")
            return

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
                f"pycontw-225217.ods.{self.POST_TABLE_NAME}",
                job_config=job_config,
            )
            job.result()
        except Exception:
            logger.exception("Failed to dump posts to BigQuery: ")
            raise RuntimeError("Failed to dump posts insights to BigQuery")

    @abstractmethod
    def _process_posts_insights(self, posts: list[dict]) -> list[dict]: ...

    def _dump_posts_insights_to_bigquery(self, posts: list[dict]) -> None:
        if not posts:
            logger.info("No post insights to dump!")
            return

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
                f"pycontw-225217.ods.{self.INSIGHT_TABLE_NAME}",
                job_config=job_config,
            )
            job.result()
        except Exception:
            logger.exception("Failed to dump posts insights to BigQuery: ")
            raise RuntimeError("Failed to dump posts insights to BigQuery")
