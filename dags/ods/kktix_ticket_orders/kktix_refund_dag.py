"""
Update KKTIX's data if attendee has been refunded
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from ods.kktix_ticket_orders.udfs import kktix_refund

DEFAULT_ARGS = {
    "owner": "henry410213028@gmail.com",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 18, 0),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord!",
}


@dag(
    default_args=DEFAULT_ARGS,
    schedule_interval="50 23 * * *",  # At 23:50 (everyday)
    max_active_runs=1,
    catchup=True,
)
def KKTIX_TICKET_REFUND_V3():
    @task
    def UPDATE_REFUNDED_ATTENDEE_IDS():
        kktix_refund.main()

    UPDATE_REFUNDED_ATTENDEE_IDS()


dag_obj = KKTIX_TICKET_REFUND_V3()

if __name__ == "__main__":
    dag_obj.test()
