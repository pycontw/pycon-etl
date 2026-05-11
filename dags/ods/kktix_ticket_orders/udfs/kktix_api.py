import copy
import logging
from collections.abc import Callable

import requests
import tenacity
from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Variable
from ods.kktix_ticket_orders.udfs import kktix_loader, kktix_transformer

logger = logging.getLogger(__name__)

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
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]
    event_raw_data_array = _extract(
        year=interval_start.year,
        start_timestamp=interval_start.timestamp(),
        end_timestamp=interval_end.timestamp(),
    )
    transformed_event_raw_data_array = kktix_transformer.transform(
        copy.deepcopy(event_raw_data_array)
    )
    kktix_loader.load(transformed_event_raw_data_array)
    logger.info("Loaded %d rows to BigQuery!", len(transformed_event_raw_data_array))

    # pass these unhashed data through xcom to next airflow task
    return kktix_transformer._extract_sensitive_unhashed_raw_data(
        copy.deepcopy(event_raw_data_array)
    )


def _extract(year: int, start_timestamp: float, end_timestamp: float) -> list[dict]:
    """
    get data from KKTIX's API
    1. condition_filter_callback: use this callback to filter out unwanted event!
    """
    event_raw_data_array: list[dict] = []

    def _condition_filter_callback(event):
        return str(year) in event["name"] and "registration" in event["name"].lower()

    event_metadatas = get_event_metadatas(_condition_filter_callback)
    for event_metadata in event_metadatas:
        event_id = event_metadata["id"]
        for attendee_info in get_attendee_infos(
            event_id, start_timestamp, end_timestamp
        ):
            event_raw_data_array.append(
                {
                    "id": event_id,
                    "name": event_metadata["name"],
                    "attendee_info": attendee_info,
                }
            )
    return event_raw_data_array


def get_attendee_infos(
    event_id: int, start_timestamp: float, end_timestamp: float
) -> list:
    """
    it's a public wrapper for people to get attendee infos!
    """
    attendance_book_id = _get_attendance_book_id(event_id)
    attendee_ids = _get_attendee_ids(event_id, attendance_book_id)
    attendee_infos = _get_attendee_infos(
        event_id, attendee_ids, start_timestamp, end_timestamp
    )
    return attendee_infos


def get_event_metadatas(condition_filter: Callable) -> list[dict]:
    """
    Fetch all the ongoing events
    """
    event_list_resp = HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"{Variable.get('kktix_events_endpoint')}?only_not_ended_event=true",
        _retry_args=RETRY_ARGS,
    ).json()
    event_metadatas: list[dict] = []
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


def _get_attendee_ids(event_id: int, attendance_book_id: int) -> list[int]:
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
    event_id: int,
    attendee_ids: list[int],
    start_timestamp: float,
    end_timestamp: float,
) -> list:
    """
    get attendee infos, e.g. email, phonenumber, name and etc
    """
    logger.info(
        "Fetching attendee infos between %s and %s",
        start_timestamp,
        end_timestamp,
    )
    attendee_infos = []
    for attendee_id in attendee_ids:
        attendee_info = HTTP_HOOK.run_with_advanced_retry(
            endpoint=f"{Variable.get('kktix_events_endpoint')}/{event_id}/attendees/{attendee_id}",
            _retry_args=RETRY_ARGS,
        ).json()
        if not attendee_info["is_paid"]:
            continue
        if start_timestamp < attendee_info["updated_at"] < end_timestamp:
            attendee_infos.append(attendee_info)
    return attendee_infos
