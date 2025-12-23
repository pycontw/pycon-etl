from datetime import datetime, timedelta

from airflow.sdk import Asset, AssetAlias, Metadata, dag, task

from dags.app.finance_bot.udf import (
    df_difference,
    read_bigquery_to_df,
    read_google_xls_to_df,
    refine_diff_df_to_string,
    write_to_bigquery,
)

DEFAULT_ARGS = {
    "owner": "Xch1",
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
    tags=["discord"],
)
def DISCORD_FINANCE_REMINDER():
    @task(outlets=[AssetAlias("finance_report_diff_notification")])
    def REMINDER_OF_THIS_TEAM():
        # read xls from google doc to df.
        df_xls = read_google_xls_to_df()

        # read bigquery to df.
        df_bigquery = read_bigquery_to_df()

        # check difference between 2 df
        df_diff = df_difference(df_xls, df_bigquery)
        if not df_diff.empty:
            # link to bigquery and write xls file
            write_to_bigquery(df_diff)

            # push to discord
            yield Metadata(
                Asset(name="finance_report_diff"),
                extra={
                    "webhook_endpoint_key": "discord_data_stratagy_webhook",
                    "username": "財務機器人",
                    "content": refine_diff_df_to_string(df_diff),
                },
                alias=AssetAlias("finance_report_diff_notification"),
            )

    REMINDER_OF_THIS_TEAM()


dag_obj = DISCORD_FINANCE_REMINDER()

if __name__ == "__main__":
    dag_obj.test()
