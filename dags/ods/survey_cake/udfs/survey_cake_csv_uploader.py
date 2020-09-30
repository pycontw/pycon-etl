import os
import csv
from pathlib import Path

from google.cloud import bigquery


class SurveyCakeCSVUploader:
    def __init__(self, filename):
        self.filename = filename
        self.year = None
        # Construct a BigQuery client object.
        self.project = "pycontw-225217"
        if not bool(os.getenv("AIRFLOW_TEST_MODE")):
            self.client = bigquery.Client(project=self.project)

        self.facttable_filepath = str(Path(self.filename).parent / "facttable.csv")
        self.dimension_table_filepath = str(
            Path(self.filename).parent / "dimension.csv"
        )

    def run_dag(self, **context):
        self.year = context["execution_date"].year
        self.transform()
        if not bool(os.getenv("AIRFLOW_TEST_MODE")):
            self.upload()

    def transform(self):
        def _export_facttable(header_of_fact_table):
            with open(self.facttable_filepath, "w") as target:
                writer = csv.writer(target)
                writer.writerow(header_of_fact_table)
                for row in rows_of_fact_table:
                    writer.writerow(row)

        def _export_dimension_table(question_id_dienstion_table):
            with open(self.dimension_table_filepath, "w") as target:
                writer = csv.writer(target)
                writer.writerow(("question_id", "question", "year"))
                for question_id, question in question_id_dienstion_table.items():
                    writer.writerow((question_id, question, self.year))

        with open(self.filename, "r", encoding="utf-8-sig") as csvfile:
            rows = csv.reader(csvfile)
            # skip header
            header = next(iter(rows))
            question_id_dienstion_table = self._generate_question_id_dimension_table(
                header
            )
            question_ids = sorted(question_id_dienstion_table.keys())
            header_of_fact_table = ("ip", "question_id", "answer")
            rows_of_fact_table = self._transform_raw_data_to_fact_table_format(
                rows, question_id_dienstion_table, question_ids
            )

        _export_facttable(header_of_fact_table)
        _export_dimension_table(question_id_dienstion_table)

    def upload(self):
        self._upload_2_bigquery(
            self.facttable_filepath,
            f"{self.project}.ods.ods_questionnaire_ip_datetime",
        )
        self._upload_2_bigquery(
            self.dimension_table_filepath,
            f"{self.project}.dim.dim_questionnaire_questionId_year",
        )

    def _upload_2_bigquery(self, file_path, table_id):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
        )
        with open(file_path, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )

        job.result()  # Waits for the job to complete.

        table = self.client.get_table(table_id)  # Make an API request.
        print(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        )

    def _generate_question_id_dimension_table(self, header):
        question_id_dim_table = {}
        for index, column in enumerate(header):
            column = column.strip()
            question_id_dim_table[
                index if column != "其他" else self._get_index_of_else_column(index)
            ] = column
        return question_id_dim_table

    @staticmethod
    def _get_index_of_else_column(index):
        """
        use 0.1 to represent "其他"
        """
        return index - 1 + 0.1

    @staticmethod
    def _transform_raw_data_to_fact_table_format(
        rows, question_id_dienstion_table, question_ids
    ):
        result = []
        for row in rows:
            row_dict = dict(zip(question_ids, row))
            question_id_of_primary_key = [
                key
                for key, value in question_id_dienstion_table.items()
                if value == "IP紀錄"
            ][0]
            primary_key = row_dict[question_id_of_primary_key]
            for question_id, answer in row_dict.items():
                result.append((primary_key, question_id, answer))
        return result


if __name__ == "__main__":
    survey_cake_csv_uploader = SurveyCakeCSVUploader(filename="data_questionnaire.csv")
    survey_cake_csv_uploader.run_dag()
