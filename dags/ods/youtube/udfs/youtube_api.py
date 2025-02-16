import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import macros
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from google.cloud import bigquery
from utils.hook_related import RETRY_ARGS

# channel id of YouTube is public to everyone, so it's okay to commit this ID into git
CHANNEL_ID = "UCHLnNgRnfGYDzPCCH8qGbQw"
MAX_RESULTS = 50
PROJECT = os.getenv("BIGQUERY_PROJECT")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


def create_table_if_needed():
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql_filepath = Path(AIRFLOW_HOME) / "dags/ods/youtube/sqls/create_table.sql"
    sql = sql_filepath.read_text().format(PROJECT)
    client.query(sql)


def get_video_ids(**context) -> None:
    video_metadatas = []
    execution_date = context["execution_date"].replace(tzinfo=None)
    last_year = execution_date - macros.timedelta(days=365)
    last_year_RFC_3339_format = f"{last_year.date()}T00:00:00Z"
    http_conn = HttpHook(method="GET", http_conn_id="youtube")
    base_url = f"/youtube/v3/search?key={Variable.get('YOUTUBE_KEY')}&channelId={CHANNEL_ID}&part=snippet,id&order=date&maxResults={MAX_RESULTS}&publishedAfter={last_year_RFC_3339_format}"

    response_json = http_conn.run_with_advanced_retry(
        endpoint=base_url,
        _retry_args=RETRY_ARGS,
        headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
    ).json()
    video_metadatas += [
        {"videoId": item["id"]["videoId"], "title": item["snippet"]["title"]}
        for item in response_json["items"]
        if "videoId" in item["id"]
    ]
    while response_json.get("nextPageToken"):
        next_page_token = response_json["nextPageToken"]
        response_json = http_conn.run_with_advanced_retry(
            endpoint=f"{base_url}&pageToken={next_page_token}",
            _retry_args=RETRY_ARGS,
            headers={"Content-Type": "application/json", "Cache-Control": "no-cache"},
        ).json()
        video_metadatas += [
            {"videoId": item["id"]["videoId"], "title": item["snippet"]["title"]}
            for item in response_json["items"]
            if "videoId" in item["id"]
        ]
    task_instance = context["task_instance"]
    task_instance.xcom_push("GET_VIDEO_IDS", video_metadatas)


def save_video_data_2_bq(**context):
    def _init():
        client = bigquery.Client(project=PROJECT)
        http_conn = HttpHook(method="GET", http_conn_id="youtube")
        execution_date = context["execution_date"].replace(tzinfo=None)
        task_instance = context["task_instance"]
        datatype = context["datatype"]
        video_metadatas = task_instance.xcom_pull("GET_VIDEO_IDS", key="GET_VIDEO_IDS")
        result = []
        return (
            client,
            http_conn,
            execution_date,
            task_instance,
            datatype,
            video_metadatas,
            result,
        )

    def _get_statistics():
        for video_metadata in video_metadatas:
            video_id = video_metadata["videoId"]
            title = video_metadata["title"]
            response_json = http_conn.run_with_advanced_retry(
                endpoint=f"/youtube/v3/videos?id={video_id}&key={Variable.get('YOUTUBE_KEY')}&part=statistics",
                _retry_args=RETRY_ARGS,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                },
            ).json()
            print(response_json["items"][0]["statistics"].keys())
            result.append(
                (
                    execution_date,
                    video_id,
                    title,
                    int(response_json["items"][0]["statistics"]["viewCount"]),
                    int(response_json["items"][0]["statistics"]["likeCount"]),
                    0,  # dislikeCount field is not available in statistics API since 2021!
                    int(response_json["items"][0]["statistics"]["favoriteCount"]),
                    int(response_json["items"][0]["statistics"]["commentCount"]),
                )
            )
        return result

    def _get_info():
        for video_metadata in video_metadatas:
            video_id = video_metadata["videoId"]
            title = video_metadata["title"]
            response_json = http_conn.run_with_advanced_retry(
                endpoint=f"/youtube/v3/videos?id={video_id}&key={Variable.get('YOUTUBE_KEY')}&part=snippet",
                _retry_args=RETRY_ARGS,
                headers={
                    "Content-Type": "application/json",
                    "Cache-Control": "no-cache",
                },
            ).json()
            result.append(
                (
                    execution_date,
                    video_id,
                    title,
                    response_json["items"][0]["snippet"]["thumbnails"]["default"][
                        "url"
                    ],
                    response_json["items"][0]["description"],
                    datetime.strptime(
                        response_json["items"][0]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    f"https://www.youtube.com/watch?v={response_json['items'][0]['id']}",
                )
            )
        return result

    def _transform_to_pandas_dataframe(result):
        df = pd.DataFrame(
            result,
            columns=[
                "created_at",
                "videoId",
                "title",
                "viewCount",
                "likeCount",
                "dislikeCount",
                "favoriteCount",
                "commentCount",
            ],
        )
        return df

    def _insert_to_bq(df, tablename):
        TABLE = f"PROJECT.{tablename}"
        job = client.load_table_from_dataframe(df, TABLE)
        job.result()

    (
        client,
        http_conn,
        execution_date,
        task_instance,
        datatype,
        video_metadatas,
        result,
    ) = _init()

    if datatype == "statistics":
        tablename = "ods.ods_youtubeStatistics_videoId_datetime"
        result = _get_statistics()
    elif datatype == "info":
        tablename = "ods.ods_youtubeInfo_videoId_datetime"
        result = _get_info()
    else:
        raise RuntimeError(f"Unsupported datatype: {datatype}")

    df = _transform_to_pandas_dataframe(result)
    _insert_to_bq(df, tablename)
