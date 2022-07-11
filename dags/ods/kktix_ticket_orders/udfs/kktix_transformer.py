import hashlib
from typing import Dict, List


def transform(event_raw_data_array: List) -> List[Dict]:
    """
    de-identify user's email in this block!
    """
    for event in event_raw_data_array:
        for attendee_info in event["attendee_infos"]:
            # search string contains personal information and it's unstructured. Therefore just drop it!
            del attendee_info["search_string"]
            for index, (key, value) in enumerate(attendee_info["data"]):
                if key in {"聯絡人 姓名", "聯絡人 Email", "聯絡人 手機"}:
                    hashed_value = hashlib.sha256(value.encode("utf-8")).hexdigest()
                    attendee_info["data"][index][1] = hashed_value
    return event_raw_data_array
