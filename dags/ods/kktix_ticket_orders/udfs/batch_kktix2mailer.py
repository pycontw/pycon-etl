from dateutil.parser import parse
from ods.kktix_ticket_orders.udfs import kktix_api, klaviyo_loader


def main(**context):
    """
    Extract user info from kktix api and load to mailer
    """
    schedule_interval = context["dag"].schedule_interval
    # If we change the schedule_interval, we need to update the logic in condition_filter_callback
    assert schedule_interval == "0 * * * *"  # nosec
    ts_datetime_obj = parse(context["ts"])
    year = ts_datetime_obj.year
    timestamp = ts_datetime_obj.timestamp()
    event_raw_data_array = kktix_api._extract(
        year=year,
        timestamp=timestamp,
    )
    # load name and email to mailer before data has been hashed
    klaviyo_loader.load(event_raw_data_array)
    print(f"Batch load {len(event_raw_data_array)} data to downstream task")
