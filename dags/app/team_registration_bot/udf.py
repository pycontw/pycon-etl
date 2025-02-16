import os
from datetime import datetime
from typing import Dict

from airflow.models import Variable
from app import discord
from google.cloud import bigquery

YEAR = datetime.now().year

TABLE = f"{os.getenv('BIGQUERY_PROJECT', 'pycontw-225217')}.ods.ods_kktix_attendeeId_datetime"

CLIENT = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))


def main() -> None:
    statistics = _get_statistics_from_bigquery()
    msg = _compose_discord_msg(statistics)
    kwargs = {
        "webhook_url": Variable.get("discord_webhook_registration_endpoint"),
        "username": "KKTIX order report",
        "msg": msg,
    }
    discord.send_webhook_message(**kwargs)


def _get_statistics_from_bigquery() -> Dict:
    query_job = CLIENT.query(
        f"""
        WITH UNIQUE_RECORDS AS (
          SELECT DISTINCT
            NAME,
            JSON_EXTRACT(ATTENDEE_INFO, '$.id') AS ORDER_ID,
            REPLACE(JSON_EXTRACT(ATTENDEE_INFO, '$.ticket_name'), '"', '') AS TICKET_NAME,
          FROM
            `{TABLE}`
          WHERE
            ((REFUNDED IS NULL) OR (REFUNDED = FALSE)) AND (NAME LIKE "PyCon TW {YEAR} Registration%")
        )

        SELECT
          NAME,
          TICKET_NAME,
          COUNT(1) AS COUNTS
        FROM UNIQUE_RECORDS
        GROUP BY
          NAME, TICKET_NAME;
    """  # nosec
    )
    result = query_job.result()
    return result


ticket_price = {
    # please update the price for target year
    "企業票 - 一般階段 / Corporate - Regular Stage": 5800,
    "企業票 - 晚鳥階段 / Corporate - Final Stage": 6500,
    "企業團體票 (歡迎申請) / Group-Buy Corporate (Free to Apply)": 5220,
    "優惠票 (含紀念衣服) / Reserved - Community (with T-Shirt)": 2590,
    "貢獻者票 (含紀念衣服) / Reserved - Contributor (with T-Shirt)": 1290,
    "財務補助票 / Reserved - Financial Aid": 0,
    "邀請票 (含紀念衣服) / Reserved - Invited (with T-Shirt)": 0,
    "個人贊助票 (含紀念衣服) / Individual - Sponsor (with T-Shirt)": 5500,
    "個人票 - 早鳥 (含紀念衣服) / Individual - Early Stage (with T-Shirt)": 2790,
    "個人票 - 一般 (含紀念衣服)/ Individual - Regular Stage (with T-Shirt)": 3790,
    "個人票 - 晚鳥階段 / Individual - Final Stage": 4290,
    "愛心優待票 (含紀念衣服)/ Individual - Concession": 1895,
}


def _compose_discord_msg(payload) -> str:
    msg = f"Hi 這是今天 {datetime.now().date()} 的票種統計資料，售票期結束後，請 follow README 的 `gcloud` 指令進去把 Airflow DAG 關掉\n\n"
    total = 0
    total_income = 0
    for name, ticket_name, counts in payload:
        msg += f"  * 票種：{ticket_name}\t{counts}張\n"
        total += counts
        total_income += ticket_price.get(ticket_name, 0) * counts
    total_income = f"{total_income:,}"
    msg += f"dashboard: https://metabase.pycon.tw/question/142?year={YEAR}\n"
    msg += f"總共賣出 {total} 張喔～ (總收入 TWD${total_income})"
    return msg
