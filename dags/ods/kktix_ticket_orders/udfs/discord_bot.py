import requests, os
import tenacity, json
from typing import Text, Dict
from google.cloud import bigquery
from airflow.models import Variable
from airflow.hooks.http_hook import HttpHook
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
    query_job = client.query('''
        SELECT
            name,
            COUNT(1) as counts
        FROM
            `pycontw-225217.ods.ods_kktix_attendeeId_datetime`
        GROUP BY
            name;
    ''')
    result = dict(query_job.result())
    return result



def _send_webhook_to_discord(payload: Text) -> None:
    DISCORD_HOOK.run_with_advanced_retry(
        endpoint=Variable.get('discord_webhook_registration_endpoint'),
        _retry_args=RETRY_ARGS,
        data=json.dumps({"content": payload})
    )

def _compose_discord_msg(payload: Dict) -> Text:
    msg = 'Hi 這是今天的票種統計資料，售票期結束後，請 follow README 的 \`gcloud\` 指令進去把 Airflow DAG 關掉\n'
    for key, value in payload.items():
        msg += f"票種：{key}\t{value}張\n"
    return msg