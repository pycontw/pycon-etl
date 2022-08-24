import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Text

import requests
import tenacity
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from google.cloud import bigquery
from ods.kktix_ticket_orders.udfs.bigquery_loader import TABLE
from ods.kktix_ticket_orders.udfs.kktix_api import (
    _get_attendance_book_id,
    _get_attendee_ids,
)

DISCORD_HOOK = HttpHook(http_conn_id="discord_registration", method="POST")
HTTP_HOOK = HttpHook(http_conn_id="kktix_api", method="GET")
RETRY_ARGS = dict(
    wait=tenacity.wait_random(min=1, max=10),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
CLIENT = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))


def send() -> None:
    refunded_attendee_ids = _check_if_refunded_ticket_exists()
    if refunded_attendee_ids:
        _mark_tickets_as_refunded(refunded_attendee_ids)
    statistics = _get_statistics_from_bigquery()
    msg = _compose_discord_msg(statistics)
    _send_webhook_to_discord(msg)


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
          '$.id'), '"', '') AS INT64) in ({','.join(str(i) for i in refunded_attendee_ids)}
    """
    )
    result = query_job.result()
    print(f"Result of _mark_tickets_as_refunded: {result}")


def _get_statistics_from_bigquery() -> Dict:
    query_job = CLIENT.query(
        f"""
        SELECT
          NAME,
          REPLACE(JSON_EXTRACT(ATTENDEE_INFO, '$.ticket_name'), '"', '') AS TICKET_NAME,
          COUNT(1) AS COUNTS
        FROM
          `{TABLE}`
        WHERE
          REFUNDED IS NULL OR REFUNDED = FALSE
        GROUP BY
          NAME, TICKET_NAME;
    """  # nosec
    )
    result = query_job.result()
    return result


def _send_webhook_to_discord(payload: Text) -> None:
    DISCORD_HOOK.run_with_advanced_retry(
        endpoint=Variable.get("discord_webhook_registration_endpoint"),
        _retry_args=RETRY_ARGS,
        data=json.dumps({"content": payload}),
    )


def _compose_discord_msg(payload) -> Text:
    msg = f"Hi 這是今天 {datetime.now().date()} 的票種統計資料，售票期結束後，請 follow README 的 `gcloud` 指令進去把 Airflow DAG 關掉\n\n"
    total = 0
    msg_dict = defaultdict(list)
    for name, ticket_name, counts in payload:
        msg_dict[name].append((ticket_name, counts))
    for name, ticket_name_counts_tuples in sorted(msg_dict.items(), key=lambda x: x[0]):
        msg += f"{name}\n"
        for ticket_name, counts in sorted(
            ticket_name_counts_tuples, key=lambda x: x[0]
        ):
            msg += f"  * 票種：{ticket_name}\t{counts}張\n"
            total += counts
    msg += "dashboard: https://metabase.pycon.tw/question/142\n"
    msg += f"總共賣出 {total} 張喔～"
    return msg
