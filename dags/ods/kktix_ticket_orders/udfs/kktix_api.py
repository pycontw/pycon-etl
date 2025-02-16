import copy
from typing import Callable, Dict, List

import requests
import tenacity
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from dateutil.parser import parse
from ods.kktix_ticket_orders.udfs import kktix_loader, kktix_transformer

SCHEDULE_INTERVAL_SECONDS: int = 3600
HTTP_HOOK = HttpHook(http_conn_id="kktix_api", method="GET")
RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)


def main(**context):
    """
    ETL pipeline should consists of extract, transform and load
    """
    schedule_interval = context["dag"].schedule_interval
    # If we change the schedule_interval, we need to update the logic in condition_filter_callback
    assert schedule_interval == "50 * * * *"  # nosec
    ts_datetime_obj = parse(context["ts"])
    year = ts_datetime_obj.year
    timestamp = ts_datetime_obj.timestamp()
    event_raw_data_array = _extract(
        year=year,
        timestamp=timestamp,
    )
    transformed_event_raw_data_array = kktix_transformer.transform(
        copy.deepcopy(event_raw_data_array)
    )
    kktix_loader.load(transformed_event_raw_data_array)
    print(f"Loaded {len(transformed_event_raw_data_array)} rows to BigQuery!")

    # pass these unhashed data through xcom to next airflow task
    return kktix_transformer._extract_sensitive_unhashed_raw_data(
        copy.deepcopy(event_raw_data_array)
    )


def _extract(year: int, timestamp: float) -> List[Dict]:
    """
    get data from KKTIX's API
    1. condition_filter_callb: use this callbacl to filter out unwanted event!
    2. right now schedule_interval_seconds is a hardcoded value!
    """
    event_raw_data_array: List[Dict] = []

    def _condition_filter_callback(event):
        return str(year) in event["name"] and "registration" in event["name"].lower()

    event_metadatas = get_event_metadatas(_condition_filter_callback)
    for event_metadata in event_metadatas:
        event_id = event_metadata["id"]
        for attendee_info in get_attendee_infos(event_id, timestamp):
            event_raw_data_array.append(
                {
                    "id": event_id,
                    "name": event_metadata["name"],
                    "attendee_info": attendee_info,
                }
            )
    return event_raw_data_array


def get_attendee_infos(event_id: int, timestamp: float) -> List:
    """
    it's a public wrapper for people to get attendee infos!
    """
    attendance_book_id = _get_attendance_book_id(event_id)
    attendee_ids = _get_attendee_ids(event_id, attendance_book_id)
    attendee_infos = _get_attendee_infos(event_id, attendee_ids, timestamp)
    return attendee_infos


def get_event_metadatas(condition_filter: Callable) -> List[Dict]:
    """
    Fetch all the ongoing events
    """
    event_list_resp = HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"{Variable.get('kktix_events_endpoint')}?only_not_ended_event=true",
        _retry_args=RETRY_ARGS,
    ).json()
    event_metadatas: List[dict] = []
    for event in event_list_resp["data"]:
        if condition_filter(event):
            event_metadatas.append(event)
    return event_metadatas


def _get_attendance_book_id(event_id: int) -> int:
    """
    Fetch attendance books
    """
    attendance_books_resp = HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"{Variable.get('kktix_events_endpoint')}/{event_id}/attendance_books?only_not_ended_event=true",
        _retry_args=RETRY_ARGS,
    ).json()
    return attendance_books_resp[0]["id"]


def _get_attendee_ids(event_id: int, attendance_book_id: int) -> List[int]:
    """
    get all attendee ids!
    """
    attendee_ids = []
    attendees_resp = HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"{Variable.get('kktix_events_endpoint')}/{event_id}/attendance_books/{attendance_book_id}",
        _retry_args=RETRY_ARGS,
    ).json()
    for signin_status_tuple in attendees_resp["signin_status"]:
        attendee_ids.append(signin_status_tuple[0])
    return attendee_ids


def _get_attendee_infos(
    event_id: int, attendee_ids: List[int], timestamp: float
) -> List:
    """
    get attendee infos, e.g. email, phonenumber, name and etc
    """
    print(
        f"Fetching attendee infos between {timestamp} and {timestamp + SCHEDULE_INTERVAL_SECONDS}"
    )
    attendee_infos = []
    for attendee_id in attendee_ids:
        attendee_info = HTTP_HOOK.run_with_advanced_retry(
            endpoint=f"{Variable.get('kktix_events_endpoint')}/{event_id}/attendees/{attendee_id}",
            _retry_args=RETRY_ARGS,
        ).json()
        if not attendee_info["is_paid"]:
            continue
        if (
            timestamp
            < attendee_info["updated_at"]
            < timestamp + SCHEDULE_INTERVAL_SECONDS
        ):
            attendee_infos.append(attendee_info)
    return attendee_infos
