import os

import numpy as np
import pandas as pd
import pygsheets
import requests
from airflow.models import Variable
from app import discord
from google.cloud import bigquery

session = requests.session()


def main() -> None:
    # read xls from google doc to df.
    df_xls = read_google_xls_to_df()
    # read bigquery to df.
    df_bigquery = read_bigquery_to_df()
    # check difference between 2 df
    df_diff = df_difference(df_xls, df_bigquery)
    # link to bigquery and write xls file
    write_to_bigquery(df_diff)
    # push to discord
    kwargs = {
        "webhook_url": Variable.get("discord_data_stratagy_webhook"),
        "username": "財務機器人",
        "msg": refine_diff_df_to_string(df_diff),
    }
    if kwargs["msg"] != "no data":
        discord.send_webhook_message(**kwargs)


def df_difference(df_xls, df_bigquery) -> pd.DataFrame:
    merged = pd.merge(df_xls, df_bigquery, how="outer", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)


def read_bigquery_to_df() -> pd.DataFrame:
    client = bigquery.Client()
    query = """
    SELECT *
    FROM `pycontw-225217.ods.pycontw_finance`
    """
    query_job = client.query(query)
    results = query_job.result()
    schema = results.schema
    column_names = [field.name for field in schema]
    data = [list(row.values()) for row in results]
    df = pd.DataFrame(data=data, columns=column_names)

    return df


def read_google_xls_to_df() -> pd.DataFrame:
    gc = pygsheets.authorize(service_file=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    sheet = gc.open_by_url(Variable.get("finance_xls_path"))
    wks = sheet.sheet1
    df = wks.get_as_df(include_tailing_empty=False)
    df.replace("", np.nan, inplace=True)
    df.dropna(inplace=True)
    df = df.astype(str)
    df.columns = [
        "Reason",
        "Price",
        "Remarks",
        "Team_name",
        "Details",
        "To_who",
        "Yes_or_No",
    ]
    return df


def write_to_bigquery(df) -> None:
    project_id = "pycontw-225217"
    dataset_id = "ods"
    table_id = "pycontw_finance"
    client = bigquery.Client(project=project_id)
    table = client.dataset(dataset_id).table(table_id)
    schema = [
        bigquery.SchemaField("Reason", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Price", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Remarks", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Team_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Details", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("To_who", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Yes_or_No", "STRING", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()


def refine_diff_df_to_string(df) -> str:
    msg = ""
    if df.empty:
        return "no data"
    else:
        for row in df.itertuples(index=False):
            msg += f"{row[0]}, 花費: {row[1]}, {row[3]}, {row[4]}\n"
        return msg
