from typing import Dict, List

from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseSettings
from python_fb_page_insights_client import (
    FBPageInsight,
    PageWebInsightData,
    PostsWebInsightData,
)


class Settings(BaseSettings):
    GOOGLE_APPLICATION_CREDENTIALS = ""
    BIGQUERY_PROJECT = ""


def write_data_to_bigquery(
    table_id: str,
    rows_to_insert: List[Dict[str, str]],
    json_schema: Dict[str, str],
    is_truncate=False,
):
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
    for key, property in json_schema.items():
        type = property["type"]
        bigquery_type = ""
        if type == "string":
            format = property.get("format")
            if format is not None and format == "date-time":
                bigquery_type = "TIMESTAMP"  # TIMESTAMP
            else:
                bigquery_type = "STRING"
        elif type == "integer":
            bigquery_type = "INTEGER"
        else:
            raise TypeError("not handle this type conversion yet")
        schema = bigquery.SchemaField(key, bigquery_type)
        bigquery_schema.append(schema)
    # upload to bigquery
    complete_table_id = f"{settings.BIGQUERY_PROJECT}.{dataset_id}.{table_id}"
    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if is_truncate
        else bigquery.WriteDisposition.WRITE_APPEND,
    )
    if is_truncate:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        schema_update_options = []
    else:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    job_config = bigquery.LoadJobConfig(
        schema=bigquery_schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=schema_update_options,
    )
    # batch write
    load_job = client.load_table_from_json(
        rows_to_insert, complete_table_id, job_config=job_config,
    )
    load_job.result()


def download_fb_page_insight_data_upload_to_bigquery():
    fb = FBPageInsight()

    page_insight: PageWebInsightData = fb.get_page_default_web_insight()
    write_data_to_bigquery(
        "ods_pycontw_fb_page_summary_insights",
        page_insight.dict()["insight_list"],
        page_insight.insight_json_schema.properties,
    )


def download_fb_post_insight_data_upload_to_bigquery():
    fb = FBPageInsight()

    posts_insight: PostsWebInsightData = fb.get_post_default_web_insight()
    write_data_to_bigquery(
        "ods_pycontw_fb_posts_insights",
        posts_insight.dict()["insight_list"],
        posts_insight.insight_json_schema.properties,
    )
    write_data_to_bigquery(
        "ods_pycontw_fb_posts",
        posts_insight.dict()["post_list"],
        posts_insight.post_json_schema.properties,
        True,
    )


if __name__ == "__main__":
    download_fb_page_insight_data_upload_to_bigquery()
    download_fb_post_insight_data_upload_to_bigquery()
