from python_fb_page_insights_client import (
    FBPageInsight,
    PageWebInsightData,
    PostsWebInsightData,
    DatePreset,
    Period,
)
from pydantic import BaseSettings
from typing import List, Dict, Union

from google.cloud import bigquery
from google.oauth2 import service_account


class Settings(BaseSettings):
    GOOGLE_APPLICATION_CREDENTIALS = ""
    BIGQUERY_PROJECT = ""


def write_data_to_bigquery(
    table_id: str, insight_data: Union[PageWebInsightData, PostsWebInsightData]
):
    page_insight_dict = insight_data.dict()
    rows_to_insert = page_insight_dict["data"]
    json_schema_properties = insight_data.json_schema.properties

    # init bigquery
    dataset_id = "ods"
    settings = Settings()
    credentials = service_account.Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS
    )
    # TODO: avoid duplicate instantiate twice
    client = bigquery.Client(credentials=credentials, project=settings.BIGQUERY_PROJECT)

    # convert json schema to bigquery schema
    bigquery_schema: List[bigquery.SchemaField] = []
    for key, property in json_schema_properties.items():
        type = property["type"]
        bigquery_type = ""
        if type == "string":
            bigquery_type = "STRING"
        elif type == "integer":
            bigquery_type = "INTEGER"
        else:
            raise TypeError("not handle this type conversion yet")
        schema = bigquery.SchemaField(key, bigquery_type)
        bigquery_schema.append(schema)
    # upload to bigquery
    complete_table_id = f"{settings.BIGQUERY_PROJECT}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        schema=bigquery_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    ]
    # batch write
    load_job = client.load_table_from_json(
        rows_to_insert, complete_table_id, job_config=job_config,
    )
    load_job.result()


def download_fb_insight_data_upload_to_bigquery():

    # NOTE: currently you need to specify below in .env or fill them as function parameters
    # fb_user_access_token=
    # fb_app_id=
    # fb_app_secret=
    # fb_default_page_id=
    # GOOGLE_APPLICATION_CREDENTIALS=
    # BIGQUERY_PROJECT=

    # init FBPageInsight
    fb = FBPageInsight()

    page_insight: PageWebInsightData = fb.get_page_default_web_insight()
    page_table_id = "pycontw_fb_page_summary_insight"
    write_data_to_bigquery(page_table_id, page_insight)

    ## NOTE: until_date can be omitted but it will query data since (2020, 9, 7) until now.
    #  Remove it for production, use it to speed up for developing
    posts_insight: PostsWebInsightData = fb.get_post_default_web_insight(
        until_date=(2020, 11, 15)
    )
    post_table_id = "pycontw_fb_posts_insight"
    write_data_to_bigquery(post_table_id, posts_insight)


if __name__ == "__main__":
    download_fb_insight_data_upload_to_bigquery()

