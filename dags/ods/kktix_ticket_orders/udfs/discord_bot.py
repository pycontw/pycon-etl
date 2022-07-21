import json
import os
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
          REPLACE(JSON_EXTRACT(ATTENDEE_INFOS, '$[0].ticket_name'), '"', '') AS TICKET_NAME,
          COUNT(1) AS COUNTS
        FROM
          `pycontw-225217.ods.ods_kktix_attendeeId_datetime`
        GROUP BY
          NAME, TICKET_NAME;
    """
    )
    result = dict(query_job.result())
    return result


def _send_webhook_to_discord(payload: Text) -> None:
    DISCORD_HOOK.run_with_advanced_retry(
        endpoint=Variable.get("discord_webhook_registration_endpoint"),
        _retry_args=RETRY_ARGS,
        data=json.dumps({"content": payload}),
    )


def _compose_discord_msg(payload: Dict) -> Text:
    msg = "Hi 這是今天的票種統計資料，售票期結束後，請 follow README 的 `gcloud` 指令進去把 Airflow DAG 關掉\n"
    for key, value in payload.items():
        msg += f"票種：{key}\t{value}張\n"
    return msg
