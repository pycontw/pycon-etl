import collections
import json
import os

import pandas as pd
from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs import kktix_bq_dwd_etl

SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("attendee_info", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("refunded", "BOOLEAN", mode="REQUIRED"),
]
JOB_CONFIG = bigquery.LoadJobConfig(
    schema=SCHEMA, create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
)


def load(event_raw_data_array: list):
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

    project_id = os.getenv("BIGQUERY_PROJECT")
    credential_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    load_to_bigquery_ods(payload, project_id, credential_file)
    load_to_bigquery_dwd(payload, project_id, credential_file)


from typing import Optional

def load_to_bigquery_ods(
    payload: list[dict], project_id: Optional[str], credential_file: Optional[str]
) -> None:
    """
    Load data to BigQuery's ods table

    Args:
        payload: List of dictionaries containing the data to load
        project_id: GCP project ID
        credential_file: Path to GCP credential JSON file
    """
    if project_id is None or credential_file is None:
        raise ValueError("project_id and credential_file must not be None")
    client = bigquery.Client.from_service_account_json(
        credential_file, project=project_id
    )
    df = pd.DataFrame(
        payload,
        columns=["id", "name", "attendee_info"],
    )
    # for now, these attendees haven't refunded our ticket, yet...
    # we don't know if they would refund down the road
    df["refunded"] = [False] * len(payload)
    job = client.load_table_from_dataframe(
        df, f"{project_id}.ods.ods_kktix_attendeeId_datetime", job_config=JOB_CONFIG
    )
    job.result()


def load_to_bigquery_dwd(
    payload: list[dict], project_id: Optional[str], credential_file: Optional[str], ticket_group: Optional[str] = None
) -> None:
    """
    Load data to BigQuery's DWD tables

    Args:
        payload: List of dictionaries containing the data to load
        project_id: GCP project ID
        credential_file: Path to GCP credential JSON file
        ticket_group: Type of ticket group (corporate, individual, reserved)
        year: Year of the event
    """
    if project_id is None or credential_file is None:
        raise ValueError("project_id and credential_file must not be None")
    # Split payload to dict lists by ticket group if not specified
    if ticket_group is None:
        ticket_groups = ["corporate", "individual", "reserved"]
        dol: dict[str, list[dict]] = {tg: [] for tg in ticket_groups}
        for d in payload:
            for tg in ticket_groups:
                if tg in d["name"].lower():
                    dol[tg].append(d)
    else:
        dol = {ticket_group: payload}
        ticket_groups = [ticket_group]

    dataset_id = "dwd"
    for tg in ticket_groups:
        if len(dol[tg]) > 0:
            _, sanitized_df = kktix_bq_dwd_etl.load_to_df_from_list(dol[tg])
            table_id = f"kktix_ticket_{tg}_attendees"
            kktix_bq_dwd_etl.upload_dataframe_to_bigquery(
                sanitized_df, project_id, dataset_id, table_id, credential_file
            )


def _sanitize_payload(event_raw_data: dict) -> dict:
    """
    BigQuery has some constraints for nested data type
    So we put out sanitization/data cleansing logic here!
    """
    event_raw_data["attendee_info"] = json.dumps(
        event_raw_data["attendee_info"], ensure_ascii=False
    )
    return event_raw_data
