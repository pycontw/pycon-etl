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


def create_table_if_needed() -> None:
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    base_path = Path(__file__).parent.parent
    sql_path = base_path / "sql" / "create_table.sql"
    sql = sql_path.read_text().format(TABLE)
    client.query(sql)
    client.query(DEDUPE_SQL)
