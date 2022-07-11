import datetime
from typing import Dict, List

import requests
from dateutil.parser import parse
from ods.kktix_ticket_orders.udfs import kktix_loader, kktix_transformer

# SCHEDULE_INTERVAL_SECONDS : int = 300
SCHEDULE_INTERVAL_SECONDS: int = 300000


def main(**context):
    """
    ETL stands for extract, transform and load
    """
    schedule_interval = context["dag"].schedule_interval
    # If we change the schedule_interval, we need to update the logic in condition_filter_callback
    assert schedule_interval == "*/5 * * * *"
    ts_datetime_obj = parse(context["ts"])
    year = ts_datetime_obj.year
    timestamp = ts_datetime_obj.timestamp()
    event_raw_data_array = _extract(year=year, timestamp=timestamp)
    transformed_event_raw_data_array = kktix_transformer.transform(event_raw_data_array)
    kktix_loader.load(transformed_event_raw_data_array)


def _extract(year: int, timestamp: datetime.datetime) -> Dict:
    """
    get data from KKTIX's API
    1. condition_filter_callb: use this callbacl to filter out unwanted event!
    2. right now schedule_interval_seconds is a hardcoded value!
    """
    event_raw_data_array: List[Dict] = []
    condition_filter_callback = (
        lambda event: str(year) in event["name"]
        and "registration" in event["name"].lower()
    )
    event_metadatas = get_event_metadatas(condition_filter_callback)
    for event_metadata in event_metadatas:
        event_id = event_metadata["id"]
        event_raw_data_array.append(
            {
                "id": event_id,
                "name": event_metadata["name"],
                "attendee_infos": get_attendee_infos(event_id, timestamp),
            }
        )
    return event_raw_data_array


def get_attendee_infos(event_id: int, timestamp: float) -> List:
    attendance_book_id = _get_attendance_book_id(event_id)
    attendee_ids = _get_attendee_ids(event_id, attendance_book_id, timestamp)
    attendee_infos = _get_attendee_infos(event_id, attendee_ids, timestamp)
    return attendee_infos


def get_event_metadatas(condition_filter) -> List[int]:
    # use Airflow's Varaible to store this URI!
    event_list_resp = requests.get(
        "https://kktix.com/api/v2/hosting_events?only_not_ended_event=true",
        headers={"Authorization": "bearer xxx"},
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
    attendance_books_resp = requests.get(
        f"https://kktix.com/api/v2/hosting_events/{event_id}/attendance_books",
        headers={"Authorization": "bearer xxx"},
    ).json()
    return attendance_books_resp[0]["id"]


def _get_attendee_ids(
    event_id: int, attendance_book_id: int, timestamp: float
) -> List[int]:
    """
    get all attendees!
    """
    attendee_ids = []
    attendees_resp = requests.get(
        f"https://kktix.com/api/v2/hosting_events/{event_id}/attendance_books/{attendance_book_id}",
        headers={"Authorization": "bearer xxx"},
    ).json()
    for signin_status_tuple in attendees_resp["signin_status"]:
        attendee_ids.append(signin_status_tuple[0])
    return attendee_ids


def _get_attendee_infos(
    event_id: int, attendee_ids: List[int], timestamp: float
) -> List:
    """
    get attendee infos
    """
    attendee_infos = []
    for attendee_id in attendee_ids:
        attendee_info = requests.get(
            f"https://kktix.com/api/v2/hosting_events/{event_id}/attendees/{attendee_id}",
            headers={"Authorization": "bearer xxx"},
        ).json()
        if (
            timestamp
            < attendee_info["updated_at"]
            < timestamp + SCHEDULE_INTERVAL_SECONDS
        ):
            attendee_infos.append(attendee_info)
    return attendee_infos
