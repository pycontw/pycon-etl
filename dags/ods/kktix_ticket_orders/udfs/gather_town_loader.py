"""
load user's name, email etc into gather town whitelist
please refer to this document for details: https://hackmd.io/PM_sWO5USo6dxMqT1uCrCQ?view
"""

import requests
import tenacity
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable

RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)

GATHERTOWN_HTTP_HOOK = HttpHook(http_conn_id="gathertown_api", method="POST")


def load(**context):
    event_raw_data_array = context["ti"].xcom_pull(task_ids="GET_ATTENDEE_INFOS")
    for event_raw_data in event_raw_data_array:
        resp = GATHERTOWN_HTTP_HOOK.run_with_advanced_retry(
            endpoint="/api/setEmailGuestlist",
            _retry_args=RETRY_ARGS,
            json={
                "spaceId": Variable.get("gather_town_space_id"),
                "apiKey": Variable.get("gather_town_api_key"),
                "guestlist": {
                    event_raw_data["聯絡人 Email"]: {
                        "name": "",
                        "role": "guest",
                        "affiliation": "Attendee",
                    }
                },
            },
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        ).json()
        print(resp)
