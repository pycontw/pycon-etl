from unittest.mock import patch

from ods.kktix_ticket_orders.udfs import klaviyo_loader


def fake_airflow_variable():
    return {
        "KLAVIYO_LIST_ID": "abc",
        "KLAVIYO_CAMPAIGN_ID": "123",
    }


@patch("ods.kktix_ticket_orders.udfs.klaviyo_mailer.main")
@patch(
    "ods.kktix_ticket_orders.udfs.klaviyo_loader.Variable",
    new_callable=fake_airflow_variable,
)
def test_klaviyo_loader(variable, mailer, kktix_api_data):
    klaviyo_loader.load(kktix_api_data)
    mailer.assert_called_once_with(
        list_id="abc",
        campaign_id="123",
        campaign_name="隨買即用",
        datas=[{"email": "xxx@gmail.com", "name": "李xx"}],
    )
