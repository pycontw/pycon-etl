import os
from pathlib import Path

from google.cloud import bigquery

TABLE = f"{os.getenv('BIGQUERY_PROJECT')}.ods.ods_kktix_attendeeId_datetime"


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql = (
        Path("dags/ods/kktix_ticket_orders/sqls/create_table.sql")
        .read_text()
        .format(TABLE)
    )
    client.query(sql)
