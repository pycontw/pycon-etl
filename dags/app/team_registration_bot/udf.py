import os
from datetime import datetime
from typing import Dict, Text

import requests
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
        "msg": msg
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


def _compose_discord_msg(payload) -> Text:
    msg = f"Hi 這是今天 {datetime.now().date()} 的票種統計資料，售票期結束後，請 follow README 的 `gcloud` 指令進去把 Airflow DAG 關掉\n\n"
    total = 0
    for name, ticket_name, counts in payload:
        msg += f"  * 票種：{ticket_name}\t{counts}張\n"
        total += counts
    msg += "dashboard: https://metabase.pycon.tw/question/142\n"
    msg += f"總共賣出 {total} 張喔～"
    return msg
