import os
from pathlib import Path

from google.cloud import bigquery

TABLE = f"{os.getenv('BIGQUERY_PROJECT', 'pycontw-225217')}.ods.ods_kktix_attendeeId_datetime"
# since backfill would insert duplicate records, we need this dedupe to make it idempotent
DEDUPE_SQL = f"""
CREATE OR REPLACE TABLE
  `{TABLE}` AS
SELECT
  DISTINCT *
FROM
  `{TABLE}`
"""  # nosec
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    sql_filepath = (
        Path(AIRFLOW_HOME) / "dags/ods/kktix_ticket_orders/sqls/create_table.sql"
    )
    sql = sql_filepath.read_text().format(TABLE)
    client.query(sql)
    client.query(DEDUPE_SQL)
