import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, Text

import requests
import tenacity
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from google.cloud import bigquery

DISCORD_HOOK = HttpHook(http_conn_id="discord_registration", method="POST")
RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)


def send() -> None:
    statistics = _get_statistics_from_bigquery()
    msg = _compose_discord_msg(statistics)
    _send_webhook_to_discord(msg)


def _get_statistics_from_bigquery() -> Dict:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    query_job = client.query(
        """
        SELECT
          NAME,
          REPLACE(JSON_EXTRACT(ATTENDEE_INFO, '$.ticket_name'), '"', '') AS TICKET_NAME,
          COUNT(1) AS COUNTS
        FROM
          `pycontw-225217.ods.ods_kktix_attendeeId_datetime`
        GROUP BY
          NAME, TICKET_NAME;
    """
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
    for name, ticket_name_counts_tuples in msg_dict.items():
        msg += f"{name}\n"
        for ticket_name, counts in ticket_name_counts_tuples:
            msg += f"  * 票種：{ticket_name}\t{counts}張\n"
            total += counts
    msg += f"總共賣出 {total} 張喔～"
    return msg
