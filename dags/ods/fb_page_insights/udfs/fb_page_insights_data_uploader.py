from enum import auto
from typing import Dict, List, Set

from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseSettings
from python_fb_page_insights_client import (
    FBPageInsight,
    FBPageInsightConst,
    PageWebInsightData,
    Period,
    PostsWebInsightData,
)

# Python 3.10 will have enum.StrEnum built-in. Similar
from strenum import StrEnum


class FBPageInsightKey(StrEnum):
    insight_list = auto()
    post_list = auto()


class BigQueryConst(StrEnum):
    PAGE_INSIGHT_TABLE = "ods_pycontw_fb_page_summary_insights"
    POSTS_TABLE = "ods_pycontw_fb_posts"
    POSTS_INSIGHTS_TABLE = "ods_pycontw_fb_posts_insights"
    DATASET_ODS = "ods"


class Settings(BaseSettings):
    GOOGLE_APPLICATION_CREDENTIALS = ""
    BIGQUERY_PROJECT = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def extract_added_posts(
    client: bigquery.Client,
    rows_to_insert: List[Dict[str, str]],
    complete_table_id: str,
):
    # add 1 in case some "Off-by-one error" happens
    days = FBPageInsightConst.default_between_days + 2
    query_job = client.query(
        f"""
    SELECT id FROM `{complete_table_id}`
    WHERE created_time BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -{days} DAY) AND CURRENT_TIMESTAMP()
    """
    )

    results = query_job.result()
    stored_id_set: Set[str] = set()
    for _, row in enumerate(results):
        id = row["id"]
        stored_id_set.add(id)
    rows_to_insert_new: List[Dict[str, str]] = []
    for row in rows_to_insert:
        new_id = row["id"]
        if new_id not in stored_id_set:
            rows_to_insert_new.append(row)
    return rows_to_insert_new


def convert_json_schema_to_bigquery_schema(json_schema: Dict[str, str]):
    """ for posts' schema, should at least have id and created_time to align with extract_added_posts """
    bigquery_schema: List[bigquery.SchemaField] = []
    for key, property in json_schema.items():
        type = property["type"]
        bigquery_type = ""
        if type == "string":
            format = property.get("format")
            if format is not None and format == "date-time":
                bigquery_type = "TIMESTAMP"
            else:
                bigquery_type = "STRING"
        elif type == "integer":
            bigquery_type = "INTEGER"
        else:
            raise TypeError("not handle this type conversion yet")
        schema = bigquery.SchemaField(key, bigquery_type)
        bigquery_schema.append(schema)
    return bigquery_schema


def init_bigquery_client():
    settings = Settings()
    credentials = service_account.Credentials.from_service_account_file(
        settings.GOOGLE_APPLICATION_CREDENTIALS
    )
    client = bigquery.Client(credentials=credentials, project=settings.BIGQUERY_PROJECT)

    return client


def get_complete_table_id(table_id: str):
    settings = Settings()
    complete_table_id = (
        f"{settings.BIGQUERY_PROJECT}.{BigQueryConst.DATASET_ODS}.{table_id}"
    )
    return complete_table_id


def write_data_to_bigquery(
    client: bigquery.Client,
    table_id: str,
    rows_to_insert: List[Dict[str, str]],
    json_schema: Dict[str, str],
):
    bigquery_schema: List[
        bigquery.SchemaField
    ] = convert_json_schema_to_bigquery_schema(json_schema)
    complete_table_id = get_complete_table_id(table_id)
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

    job_config = bigquery.LoadJobConfig(
        schema=bigquery_schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=schema_update_options,
    )
    # batch write
    if len(rows_to_insert) > 0:
        load_job = client.load_table_from_json(
            rows_to_insert, complete_table_id, job_config=job_config,
        )
        load_job.result()
    else:
        print("uploading data row is empty")


def download_fb_page_insight_data_upload_to_bigquery():
    fb = FBPageInsight()
    client = init_bigquery_client()

    page_insight: PageWebInsightData = fb.get_page_default_web_insight()
    write_data_to_bigquery(
        client,
        BigQueryConst.PAGE_INSIGHT_TABLE,
        page_insight.dict()[FBPageInsightKey.insight_list],
        page_insight.insight_json_schema.properties,
    )

    page_insight: PageWebInsightData = fb.get_page_default_web_insight(
        period=Period.day
    )
    write_data_to_bigquery(
        client,
        BigQueryConst.PAGE_INSIGHT_TABLE,
        page_insight.dict()[FBPageInsightKey.insight_list],
        page_insight.insight_json_schema.properties,
    )


def download_fb_post_insight_data_upload_to_bigquery():
    fb = FBPageInsight()
    posts_insight: PostsWebInsightData = fb.get_post_default_web_insight()

    client = init_bigquery_client()

    post_list_rows = posts_insight.dict()[FBPageInsightKey.post_list]
    complete_table_id = get_complete_table_id(BigQueryConst.POSTS_TABLE)
    filtered_post_list_rows = extract_added_posts(
        client, post_list_rows, complete_table_id
    )

    write_data_to_bigquery(
        client,
        BigQueryConst.POSTS_TABLE,
        filtered_post_list_rows,
        posts_insight.post_json_schema.properties,
    )


if __name__ == "__main__":
    download_fb_page_insight_data_upload_to_bigquery()
    download_fb_post_insight_data_upload_to_bigquery()
