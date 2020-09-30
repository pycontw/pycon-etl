"""
A crawler which would crawl the openings
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from ods.survey_cake.udfs.survey_cake_csv_uploader import SurveyCakeCSVUploader

DEFAULT_ARGS = {
    "owner": "davidtnfsh",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda x: "Need to send notification to Telegrame",
}
dag = DAG(
    "QUESTIONNAIRE_2_BIGQUERY",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)
SURVEY_CAKE_CSV_UPLOADER = SurveyCakeCSVUploader(filename="data_questionnaire.csv")
with dag:
    UPLOADER = PythonOperator(
        task_id="UPLOADER",
        python_callable=SURVEY_CAKE_CSV_UPLOADER.run_dag,
        provide_context=True,
        op_kwargs={},
    )

if __name__ == "__main__":
    dag.cli()
