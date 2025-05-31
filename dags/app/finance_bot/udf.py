import os

import numpy as np
import pandas as pd
import pygsheets
from airflow.models import Variable
from google.cloud import bigquery


def df_difference(df_xls: pd.DataFrame, df_bigquery: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(df_xls, df_bigquery, how="outer", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)


def read_bigquery_to_df() -> pd.DataFrame:
    client = bigquery.Client()
    query = "SELECT * FROM `pycontw-225217.ods.pycontw_finance`"
    query_job = client.query(query)
    results = query_job.result()
    schema = results.schema
    column_names = [field.name for field in schema]
    data = [list(row.values()) for row in results]
    df = pd.DataFrame(data=data, columns=column_names)
    return df


def read_google_xls_to_df() -> pd.DataFrame:
    service_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    finance_xls_path = Variable.get("finance_xls_path")

    gc = pygsheets.authorize(service_file=service_file)
    sheet = gc.open_by_url(finance_xls_path)
    df = sheet.sheet1.get_as_df(include_tailing_empty=False)
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
        bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
        for field_name in [
            "Reason",
            "Price",
            "Remarks",
            "Team_name",
            "Details",
            "To_who",
            "Yes_or_No",
        ]
    ]
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()


def refine_diff_df_to_string(df: pd.DataFrame) -> str:
    if df.empty:
        return "no data"

    return "\n".join(
        f"{row[0]}, 花費: {row[1]}, {row[3]}, {row[4]}"
        for row in df.itertuples(index=False)
    )
