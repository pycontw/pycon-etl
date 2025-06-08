"""
Send Google Search Report to Discord
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from ods.google_search_console.udfs.google_search import GoogleSearchConsoleReporter

DEFAULT_ARGS = {
    "owner": "David Jr.",
    "depends_on_past": False,
    "start_date": datetime(2020, 12, 9),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}


@dag(
    "GOOGLE_SEARCH_REPORT",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(weeks=2),
    max_active_runs=1,
    catchup=False,
)
def GOOGLE_SEARCH_REPORT():
    @task
    def GET_AND_SEND_REPORT():
        google_search_reporter = GoogleSearchConsoleReporter()
        google_search_reporter.main()

    GET_AND_SEND_REPORT()


dag_obj = GOOGLE_SEARCH_REPORT()

if __name__ == "__main__":
    dag_obj.test()
