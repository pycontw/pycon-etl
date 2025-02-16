import collections
import json
import os
from typing import Dict, List

import pandas as pd
from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs import kktix_bq_dwd_etl
from ods.kktix_ticket_orders.udfs.bigquery_loader import TABLE

SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("attendee_info", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("refunded", "BOOLEAN", mode="REQUIRED"),
]
JOB_CONFIG = bigquery.LoadJobConfig(schema=SCHEMA)


def load(event_raw_data_array: List):
    """
    load data into bigquery!
    """
    # data quality check
    if len(event_raw_data_array) == 0:
        print("Nothing to load, skip!")
        return
    payload = []
    for event_raw_data in event_raw_data_array:
        sanitized_event_raw_data = _sanitize_payload(event_raw_data)
        payload.append(sanitized_event_raw_data)
    _load_to_bigquery(payload)
    _load_to_bigquery_dwd(payload)


def _load_to_bigquery(payload: List[Dict]) -> None:
    """
    load data to BigQuery's `TABLE`
    """
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    df = pd.DataFrame(
        payload,
        columns=["id", "name", "attendee_info"],
    )
    # for now, these attendees haven't refunded our ticket, yet...
    # we don't know if they would refund down the road
    df["refunded"] = [False] * len(payload)
    job = client.load_table_from_dataframe(df, TABLE, job_config=JOB_CONFIG)
    job.result()


def _load_to_bigquery_dwd(payload: List[Dict]) -> None:
    """
    load data to BigQuery's `TABLE`
    """
    # Spilt payload to dict lists by ticket group
    ticket_groups = ["corporate", "individual", "reserved"]
    dol = collections.defaultdict(list)
    for d in payload:
        for tg in ticket_groups:
            if tg in d["name"].lower():
                dol[tg].append(d)

    print(dol[tg])
    project_id = os.getenv("BIGQUERY_PROJECT")
    dataset_id = "dwd"
    for tg in ticket_groups:
        if len(dol[tg]) > 0:
            _, sanitized_df = kktix_bq_dwd_etl.load_to_df_from_list(dol[tg])
            table_id = f"kktix_ticket_{tg}_attendees"
            kktix_bq_dwd_etl.upload_dataframe_to_bigquery(
                sanitized_df, project_id, dataset_id, table_id
            )


def _sanitize_payload(event_raw_data) -> Dict:
    """
    BigQuery has some constraints for nested data type
    So we put out sanitization/data cleansing logic here!
    """
    event_raw_data["attendee_info"] = json.dumps(event_raw_data["attendee_info"])
    return event_raw_data
