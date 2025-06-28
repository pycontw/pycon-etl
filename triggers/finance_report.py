from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import pygsheets
from airflow.sdk import Variable
from airflow.triggers.base import BaseEventTrigger, TriggerEvent
from google.cloud import bigquery

# from datetime import datetime, timedelta
#
# DEFAULT_ARGS = {
#     "owner": "CHWan",
#     "depends_on_past": False,
#     "start_date": datetime(2023, 8, 27),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=5),
#     "on_failure_callback": lambda x: "Need to send notification to Discord",
# }
#


FINANCE_REPORT_COUMNS = [
    "Reason",
    "Price",
    "Remarks",
    "Team_name",
    "Details",
    "To_who",
    "Yes_or_No",
]


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
    df.columns = FINANCE_REPORT_COUMNS
    return df


def df_difference(df_xls: pd.DataFrame, df_bigquery: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(df_xls, df_bigquery, how="outer", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop("_merge", axis=1)


def write_to_bigquery(df: pd.DataFrame) -> None:
    project_id = "pycontw-225217"
    dataset_id = "ods"
    table_id = "pycontw_finance"
    client = bigquery.Client(project=project_id)
    table = client.dataset(dataset_id).table(table_id)
    schema = [
        bigquery.SchemaField(field_name, "STRING", mode="REQUIRED")
        for field_name in FINANCE_REPORT_COUMNS
    ]
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()


def format_diff_df_as_message(diff_df: pd.DataFrame) -> str:
    return "\n".join(
        f"{row[0]}, 花費: {row[1]}, {row[3]}, {row[4]}"
        for row in diff_df.itertuples(index=False)
    )


class FinanceReportTrigger(BaseEventTrigger):
    def __init__(self, poke_interval: float) -> None:
        super().__init__()
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FinanceReportWatcher arguments and classpath."""
        return (
            "triggers.finance_report.FinanceReportTrigger",
            {"poke_interval": self.poke_interval},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the finance report is updated."""

        # TODO: improve it with asycn code
        while True:
            self.log.info("Checking Finance Report")
            # read xls from google doc to df.
            df_xls = read_google_xls_to_df()

            # read bigquery to df.
            df_bigquery = read_bigquery_to_df()

            # check difference between 2 df
            df_diff = df_difference(df_xls, df_bigquery)

            if not df_diff.empty:
                # link to bigquery and write xls file
                write_to_bigquery(df_diff)

                # push to discord
                msg = format_diff_df_as_message(df_diff)
                yield TriggerEvent(
                    {
                        "webhook_url": Variable.get("discord_data_stratagy_webhook"),
                        "username": "財務機器人",
                        "content": msg,
                    }
                )
            self.log.info(
                "Finish checking Finance Report. "
                f"The next poke will happen in {datetime.now() + timedelta(seconds=self.poke_interval)}"
            )
            await asyncio.sleep(self.poke_interval)
