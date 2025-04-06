import os
from collections import defaultdict
from typing import List

from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs.bigquery_loader import TABLE
from ods.kktix_ticket_orders.udfs.kktix_api import (
    _get_attendance_book_id,
    _get_attendee_ids,
)

CLIENT = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))


def main() -> None:
    refunded_attendee_ids = _check_if_refunded_ticket_exists()
    if refunded_attendee_ids:
        _mark_tickets_as_refunded(refunded_attendee_ids)


def _check_if_refunded_ticket_exists() -> List[int]:
    """
    1. iterate through all unrefunded tickets
    2. build up a hash map
    3. get the latest attendance book
    4. compare the difference, the diff would be refunded attendee ids
    """
    refunded_attendee_ids: List[int] = []
    query_job = CLIENT.query(
        f"""
            SELECT
              ID,
              CAST(REPLACE(JSON_EXTRACT(ATTENDEE_INFO,
                  '$.id'), '"', '') AS INT64) AS ATTENDEE_ID
            FROM
              `{TABLE}`
            WHERE
              REFUNDED IS NULL OR REFUNDED = FALSE
        """  # nosec
    )
    event_ids_and_attendee_ids = query_job.result()

    bigquery_side_event_attendee_id_dict = defaultdict(list)
    for event_id, attendee_id in event_ids_and_attendee_ids:
        bigquery_side_event_attendee_id_dict[event_id].append(attendee_id)
    for (
        event_id,
        outdated_latest_attendee_ids,
    ) in bigquery_side_event_attendee_id_dict.items():
        attendance_book_id = _get_attendance_book_id(event_id)
        latest_attendee_ids = _get_attendee_ids(event_id, attendance_book_id)
        refunded_attendee_ids_in_this_event = set(
            outdated_latest_attendee_ids
        ).difference(set(latest_attendee_ids))
        refunded_attendee_ids += list(refunded_attendee_ids_in_this_event)
    return refunded_attendee_ids


def _mark_tickets_as_refunded(refunded_attendee_ids: List[int]) -> None:
    """
    set these attendee info to refunded=true, if we cannot find its attendee_info right now by using KKTIX's API!
    """
    query_job = CLIENT.query(
        f"""
    UPDATE
      `{TABLE}`
    SET
      refunded=TRUE
    WHERE
      CAST(REPLACE(JSON_EXTRACT(ATTENDEE_INFO,
          '$.id'), '"', '') AS INT64) in ({",".join(str(i) for i in refunded_attendee_ids)})
    """
    )
    result = query_job.result()
    print(f"Result of _mark_tickets_as_refunded: {result}")
