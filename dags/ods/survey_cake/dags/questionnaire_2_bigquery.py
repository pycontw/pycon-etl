"""
A crawler which would crawl the openings
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ods.survey_cake.udfs.survey_cake_csv_uploader import SurveyCakeCSVUploader

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

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
with dag:
    if bool(os.getenv("AIRFLOW_TEST_MODE")):
        filepath = Path(AIRFLOW_HOME) / "dags/fixtures/data_questionnaire.csv"
        FILENAMES: dict[str, dict] = {str(filepath): {}}
    else:
        FILENAMES = {
            "data_questionnaire.csv": {
                "data_domain": "questionnaire",
                "primary_key": "ip",
                "time_dimension": "datetime",
            },
            "data_sponsor_questionnaire.csv": {
                "data_domain": "sponsorQuestionnaire",
                "primary_key": "ip",
                "time_dimension": "datetime",
            },
        }
    for filename, metadata in FILENAMES.items():
        FILENAME_STEM = Path(filename).stem
        SURVEY_CAKE_CSV_UPLOADER = SurveyCakeCSVUploader(filename=filename)
        TRANSFORM = PythonOperator(
            task_id=f"TRANSFORM_{FILENAME_STEM}",
            python_callable=SURVEY_CAKE_CSV_UPLOADER.transform,
            provide_context=True,
        )

        if not bool(os.getenv("AIRFLOW_TEST_MODE")):
            UPLOAD_FACTTABLE = PythonOperator(
                task_id=f"UPLOAD_FACTTABLE_{FILENAME_STEM}",
                python_callable=SURVEY_CAKE_CSV_UPLOADER.upload,
                op_kwargs={
                    "facttable_or_dimension_table": "fact",
                    "data_layer": "ods",
                    "data_domain": metadata["data_domain"],
                    "primary_key": metadata["primary_key"],
                    "time_dimension": metadata["time_dimension"],
                },
            )
            UPLOAD_DIMENSION_TABLE = PythonOperator(
                task_id=f"UPLOAD_DIMENSION_TABLE_{FILENAME_STEM}",
                python_callable=SURVEY_CAKE_CSV_UPLOADER.upload,
                op_kwargs={
                    "facttable_or_dimension_table": "dim",
                    "data_layer": "dim",
                    "data_domain": metadata["data_domain"],
                    "primary_key": "questionId",
                    "time_dimension": "year",
                },
            )
            TRANSFORM >> UPLOAD_FACTTABLE
            TRANSFORM >> UPLOAD_DIMENSION_TABLE


if __name__ == "__main__":
    dag.cli()
