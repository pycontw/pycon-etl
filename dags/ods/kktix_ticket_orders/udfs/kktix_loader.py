import json
import os
from typing import Dict, List

import pandas as pd
from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs.bigquery_loader import TABLE


def load(event_raw_data_array: List):
    """
    load data into bigquery!
    """
    if len(event_raw_data_array) == 0:
        print("Nothing to load, skip!")
        return
    _load_to_bigquery(event_raw_data_array)
    _load_to_klaviyo()
    _load_to_discord()


def _load_to_bigquery(event_raw_data_array: List[Dict]) -> None:
    sanitized_event_raw_data_array = _sanitize_payload(event_raw_data_array)
    import json

    json.dump(
        sanitized_event_raw_data_array, open("sanitized_event_raw_data_array.json", "w")
    )
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    df = pd.DataFrame(
        sanitized_event_raw_data_array, columns=["id", "name", "attendee_infos"],
    )
    print(df)
    job = client.load_table_from_dataframe(df, TABLE)
    job.result()


def _load_to_klaviyo():
    pass


def _load_to_discord():
    pass


def _sanitize_payload(event_raw_data_array) -> List:
    """
    docstring
    """
    for event_raw_data in event_raw_data_array:
        event_raw_data["attendee_infos"] = json.dumps(event_raw_data["attendee_infos"])
    return event_raw_data_array
