import json
import os
from typing import Dict, List

import pandas as pd
from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs.bigquery_loader import TABLE

SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("attendee_infos", "STRING", mode="REQUIRED"),
]
JOB_CONFIG = bigquery.LoadJobConfig(schema=SCHEMA)


def load(event_raw_data_array: List):
    """
    load data into bigquery!
    """
    # data quality check
    for event_raw_data in event_raw_data_array:
        if len(event_raw_data["attendee_infos"]) == 0:
            print("Nothing to load, skip!")
            return
    _load_to_bigquery(event_raw_data_array)
    _load_to_klaviyo()


def _load_to_bigquery(event_raw_data_array: List[Dict]) -> None:
    """
    load data to BigQuery's `TABLE`
    """
    sanitized_event_raw_data_array = _sanitize_payload(event_raw_data_array)
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    df = pd.DataFrame(
        sanitized_event_raw_data_array, columns=["id", "name", "attendee_infos"],
    )
    job = client.load_table_from_dataframe(df, TABLE, job_config=JOB_CONFIG)
    job.result()


def _load_to_klaviyo():
    """
    Henry to implement!
    """
    pass


def _sanitize_payload(event_raw_data_array) -> List:
    """
    BigQuery has some constraints for nested data type
    So we put out sanitization/data cleansing logic here!
    """
    for event_raw_data in event_raw_data_array:
        event_raw_data["attendee_infos"] = json.dumps(event_raw_data["attendee_infos"])
    return event_raw_data_array
