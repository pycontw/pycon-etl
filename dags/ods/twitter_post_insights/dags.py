"""
Scrape X (Twitter) posts and insights data, save to BigQuery

PoC: this Dag tracks its ingestion watermark via the AIP-103 asset state store
(Airflow 3.3) instead of re-querying the BigQuery sink for the latest stored
post on every run. The watermark (newest ``created_at`` seen) lives on the
``pycontw_twitter_posts`` asset and survives across Dag runs.
"""

from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task
from utils.posts_insights.twitter import TwitterPostsInsightsParser

DEFAULT_ARGS = {
    "owner": "Henry Lee",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 14, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Logical asset whose state store carries the ingestion watermark.
TWITTER_POSTS = Asset(
    name="pycontw_twitter_posts",
    uri="bigquery://pycontw-225217/ods/ods_pycontw_twitter_posts",
)


@dag(
    default_args=DEFAULT_ARGS,
    schedule="5 8 * * *",
    max_active_runs=1,
    catchup=False,
)
def TWITTER_POST_INSIGHTS_V1():
    @task
    def CREATE_TABLE_IF_NEEDED():
        TwitterPostsInsightsParser().create_tables_if_not_exists()

    @task(inlets=[TWITTER_POSTS], outlets=[TWITTER_POSTS])
    def SAVE_TWITTER_POSTS_AND_INSIGHTS(asset_state_store=None):
        state = asset_state_store[TWITTER_POSTS]

        # Read the watermark the previous run advanced to (ISO 8601 UTC).
        watermark = state.get("watermark")
        last_post = (
            {"created_at": datetime.fromisoformat(watermark)} if watermark else None
        )

        new_watermark = TwitterPostsInsightsParser().save_posts_and_insights(
            last_post=last_post
        )

        # Only advance when this run actually ingested newer posts.
        if new_watermark:
            state.set("watermark", new_watermark)

    CREATE_TABLE_IF_NEEDED() >> SAVE_TWITTER_POSTS_AND_INSIGHTS()


dag_obj = TWITTER_POST_INSIGHTS_V1()

if __name__ == "__main__":
    dag_obj.test()
