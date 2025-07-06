"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow.sdk import Variable, dag, task

from dags.app import discord
from dags.app.finance_bot.udf import (
    df_difference,
    read_bigquery_to_df,
    read_google_xls_to_df,
    refine_diff_df_to_string,
    write_to_bigquery,
)

DEFAULT_ARGS = {
    "owner": "CHWan",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 27),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
)
def DISCORD_FINANCE_REMINDER():
    @task
    def REMINDER_OF_THIS_TEAM():
        # read xls from google doc to df.
        df_xls = read_google_xls_to_df()

        # read bigquery to df.
        df_bigquery = read_bigquery_to_df()

        # check difference between 2 df
        df_diff = df_difference(df_xls, df_bigquery)

        # link to bigquery and write xls file
        write_to_bigquery(df_diff)

        # push to discord
        webhook_url = Variable.get("discord_data_stratagy_webhook")
        msg = refine_diff_df_to_string(df_diff)
        if msg != "no data":
            discord.send_webhook_message(
                webhook_url=webhook_url, username="財務機器人", msg=msg
            )

    REMINDER_OF_THIS_TEAM()


dag_obj = DISCORD_FINANCE_REMINDER()

if __name__ == "__main__":
    dag_obj.test()
