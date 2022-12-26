import os
from pathlib import Path

from google.cloud import bigquery

TABLE = f"{os.getenv('BIGQUERY_PROJECT')}.ods.ods_kktix_attendeeId_datetime_copy2"
# since backfill would insert duplicate records, we need this dedupe to make it idempotent
DEDUPE_SQL = f"""
CREATE OR REPLACE TABLE
  `{TABLE}` AS
SELECT
  DISTINCT *
FROM
  `{TABLE}`
"""  # nosec


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql = (
        Path("dags/ods/kktix_ticket_orders/sqls/create_table.sql")
        .read_text()
        .format(TABLE)
    )
    client.query(sql)
    client.query(DEDUPE_SQL)
