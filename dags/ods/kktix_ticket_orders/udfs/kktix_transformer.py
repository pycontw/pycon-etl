import hashlib
from typing import Dict, List


def transform(event_raw_data_array: List) -> List[Dict]:
    """
    de-identify user's email in this block!
    """
    for event in event_raw_data_array:
        attendee_info = event["attendee_info"]
        # search string contains personal information and it's unstructured. Therefore just drop it!
        del attendee_info["search_string"]
        for index, (key, value) in enumerate(attendee_info["data"]):
            for key_should_be_hashed in {
                "聯絡人 姓名",
                "聯絡人 Email",
                "聯絡人 手機",
                "Address",
            }:
                if key_should_be_hashed in key:
                    hashed_value = hashlib.sha256(value.encode("utf-8")).hexdigest()
                    attendee_info["data"][index][1] = hashed_value
                else:
                    continue
    return event_raw_data_array
