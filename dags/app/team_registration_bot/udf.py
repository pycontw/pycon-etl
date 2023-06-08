import os
from datetime import datetime
from typing import Dict, Text

from airflow.models import Variable
from app import discord
from google.cloud import bigquery

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
            ((REFUNDED IS NULL) OR (REFUNDED = FALSE)) AND (NAME LIKE "PyCon TW 2023 Registration%")
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
    "企業票 - 早鳥階段 (含紀念衣服) / Corporate - Early Stage (with T-Shirt)": 5500,
    "企業票 - 一般階段 / Corporate - Regular Stage": 5800,
    "企業票 - 晚鳥階段 / Corporate - Final Stage": 6500,
    "社群優惠票 (含紀念衣服) / Community Discount (with T-Shirt)": 2790,
    "OSCVPass (含紀念衣服) / OSCVPass Discount (with T-Shirt)": 2790,
    "超級 Py 粉票 (含紀念衣服) / PyCon TW Stans (with T-Shirt)": 1990,
    "講者票 (含紀念衣服) / Speaker (with T-Shirt)": 1290,
    "投稿者票 / Submitter": 1990,
    "貢獻者票 (含紀念衣服) / Contributor (with T-Shirt)": 1290,
    "大會贊助票 (歡迎申請) / Sponsorship from PyCon TW (Free to Apply)": 2490,
    "邀請票 (含紀念衣服) / Invited (with T-Shirt)": 0,
    "獎品預留票 (含紀念衣服) / Prize Reserved (with T-Shirt)": 1895,
    "企業獎品預留票 (含紀念衣服) / Corporate Prize Reserved (with T-Shirt)": 2900,
    "個人尊榮票 (含紀念衣服) / Premium (with T-Shirt)": 5500,
    "團體票 (歡迎申請) / Group-Buy Individual (Free to Apply)": 0,
    "個人票 - 早鳥階段 (含紀念衣服) / Individual - Early Stage (with T-Shirt)": 2990,
    "個人票 - 一般階段 / Individual - Regular Stage": 3790,
    "個人票 - 晚鳥階段 / Individual - Final Stage": 4290,
    "社群票 / Community": 3390,
    "愛心優待票 / Concession": 1895,
}


def _compose_discord_msg(payload) -> Text:
    msg = f"Hi 這是今天 {datetime.now().date()} 的票種統計資料，售票期結束後，請 follow README 的 `gcloud` 指令進去把 Airflow DAG 關掉\n\n"
    total = 0
    total_income = 0
    for name, ticket_name, counts in payload:
        msg += f"  * 票種：{ticket_name}\t{counts}張\n"
        total += counts
        total_income += ticket_price.get(ticket_name, 0) * counts
    total_income = "{:,}".format(total_income)
    msg += "dashboard: https://metabase.pycon.tw/question/142\n"
    msg += f"總共賣出 {total} 張喔～ (總收入 TWD${total_income})"
    return msg
