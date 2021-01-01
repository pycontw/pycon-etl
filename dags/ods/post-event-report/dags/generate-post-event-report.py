"""
Airflow DAG to generate PyCon TW post event reports
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

DEFAULT_ARGS = {
    "owner": "tai271828",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Discord",
}

dag = DAG(
    "GENERATE_POST_EVENT_REPORT",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)

file_in_dir = os.path.dirname(os.path.realpath(__file__))
trigger_command_dir = os.path.realpath(os.path.join(file_in_dir, "../scripts/"))
trigger_command = os.path.join(trigger_command_dir, "trigger-report-generator.sh")

with dag:
    if os.path.exists(trigger_command):
        run_this = BashOperator(
            task_id="QUERY_AND_GENERATE_REPORT",
            # the trailing space is necessary for an airflow pitfall
            # ref: https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html
            bash_command=trigger_command + " ",
            dag=dag,
        )
    else:
        raise Exception(f"{trigger_command} can not be found.")


if __name__ == "__main__":
    dag.cli()
