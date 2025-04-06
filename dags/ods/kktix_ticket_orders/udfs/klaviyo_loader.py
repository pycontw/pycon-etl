from typing import Iterable, List

from airflow.models import Variable
from ods.kktix_ticket_orders.udfs import klaviyo_mailer


def _load_raw_data(event_raw_data_array: List) -> Iterable:
    for event in event_raw_data_array:
        attendee_info = event["attendee_info"]
        # search string contains personal information and it's unstructured. Therefore just drop it!
        del attendee_info["search_string"]
        tmp = {
            key: value
            for index, (key, value) in enumerate(attendee_info["data"])
            if key in ("聯絡人 Email", "聯絡人 姓名")
        }
        tmp.update({"qrcode": attendee_info["qrcode"]})
        yield tmp


def load(event_raw_data_array: List) -> None:
    """
    Send a notify mail for all participants via third-party service
    """
    try:
        list_id = Variable.get("KLAVIYO_LIST_ID")
        campaign_id = Variable.get("KLAVIYO_CAMPAIGN_ID")
    except KeyError:
        print(
            "Skip klaviyo mailer, 'KLAVIYO_LIST_ID' or 'KLAVIYO_CAMPAIGN_ID' variable not found"
        )
        return

    datas = [
        {
            "email": item["聯絡人 Email"],
            "name": item["聯絡人 姓名"],
            "qrcode": item["qrcode"],
        }
        for item in _load_raw_data(event_raw_data_array)
    ]
    if not datas:
        print("Skip klaviyo mailer, no user profiles")
        return

    klaviyo_mailer.main(
        list_id=list_id,
        campaign_id=campaign_id,
        campaign_name="隨買即用",
        datas=datas,
    )
