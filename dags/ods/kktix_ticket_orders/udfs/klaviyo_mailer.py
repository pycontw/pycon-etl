"""Send a mail via Klaviyo

Requirements:

1. Create a [Klaviyo List](https://www.klaviyo.com/lists)

2. Create a template [campaign](https://www.klaviyo.com/campaigns) and set the previous List as target recipients list

"""

from datetime import datetime
from typing import List

import requests
import tenacity
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable

SCHEDULE_INTERVAL_SECONDS: int = 300
RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(3),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)


def main(
    list_id: str,
    campaign_id: str,
    campaign_name: str,
    datas: List[dict],
):
    """
    Args:
        list_id (str): Klaviyo list id, that will be save your target recipients
        campaign_id (str): A existed campaign you want to copy from
        campaign_name (str): A new campaign name
        datas (List[dict]): Recipient profile, example like below

    [
        {
            "email": "foo@example.com",
            "name": "Foo",
            "property1": "value1",
            "property2": "value2",
        },
        {
            "email": "bar@example.com",
            "name": "Bar",
            "property1": "value1",
            "property2": "value2",
        },
    ]
    """
    # check list and compaign existed
    assert _klaviyo_get_list_info(list_id)
    assert _klaviyo_get_campaign_info(campaign_id)

    # update list members
    existed_members = _klaviyo_get_list_members(list_id)["records"]
    if existed_members:
        _klaviyo_remove_list_members(
            list_id, body={"emails": list(map(lambda x: x["email"], existed_members))}
        )

    _klaviyo_add_list_members(list_id, body={"profiles": datas})
    new_members = _klaviyo_get_list_members(list_id)["records"]
    assert new_members

    # create a new compaign and send mail immediately
    campaign_suffix = f"{datetime.now():%Y-%m-%d_%H:%M:%S}"
    response = _klaviyo_clone_campaign(
        campaign_id,
        name=f"{campaign_name}_{campaign_suffix}",
        list_id=list_id,
    )
    new_campaign_id = response["id"]
    _klaviyo_send_campaign(new_campaign_id)
    print(f"Send {len(new_members)} Mails")


def _klaviyo_get_list_info(list_id: str) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="GET")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v2/list/{list_id}?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        headers={"Accept": "application/json"},
    ).json()


def _klaviyo_get_list_members(list_id: str) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="GET")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v2/group/{list_id}/members/all?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        headers={"Accept": "application/json"},
    ).json()


def _klaviyo_remove_list_members(list_id: str, body: dict) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="DELETE")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v2/list/{list_id}/members?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        json=body,
        headers={"Content-Type": "application/json"},
    )


def _klaviyo_add_list_members(list_id: str, body: dict) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="POST")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v2/list/{list_id}/members?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        json=body,
        headers={"Accept": "application/json", "Content-Type": "application/json"},
    ).json()


def _klaviyo_get_campaign_info(campaign_id: str) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="GET")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v1/campaign/{campaign_id}?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        headers={"Accept": "application/json"},
    ).json()


def _klaviyo_clone_campaign(campaign_id: str, name: str, list_id: str) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="POST")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v1/campaign/{campaign_id}/clone?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        data={"name": name, "list_id": list_id},
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    ).json()


def _klaviyo_send_campaign(campaign_id: str) -> dict:
    HTTP_HOOK = HttpHook(http_conn_id="klaviyo_api", method="POST")
    API_KEY = Variable.get("KLAVIYO_KEY")
    return HTTP_HOOK.run_with_advanced_retry(
        endpoint=f"/v1/campaign/{campaign_id}/send?api_key={API_KEY}",
        _retry_args=RETRY_ARGS,
        headers={"Accept": "application/json"},
    ).json()
