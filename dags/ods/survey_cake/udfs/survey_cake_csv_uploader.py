import csv
import os
from pathlib import Path

from google.cloud import bigquery

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


class SurveyCakeCSVUploader:
    def __init__(self, filename):
        self.filename = Path(filename)
        self.year = None
        if not bool(os.getenv("AIRFLOW_TEST_MODE")):
            self.client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))

        self.facttable_filepath = (
            self.filename.parent / f"{self.filename.stem}_facttable.csv"
        )
        self.dimension_table_filepath = (
            self.filename.parent / f"{self.filename.stem}_dimension.csv"
        )

    @property
    def bigquery_project(self):
        return os.getenv("BIGQUERY_PROJECT")

    def transform(self, **context):
        self.year = context["execution_date"].year
        self._transform()

    def _transform(self):
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

        filepath = Path(AIRFLOW_HOME) / "dags" / self.filename
        with open(filepath, encoding="utf-8-sig") as csvfile:
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

    def upload(
        self,
        facttable_or_dimension_table,
        data_layer,
        data_domain,
        primary_key,
        time_dimension,
    ):
        if facttable_or_dimension_table == "fact":
            print(self.facttable_filepath)
            print(self.facttable_filepath)
            print(self.facttable_filepath)
            print(self.facttable_filepath)
            self._upload_2_bigquery(
                self.facttable_filepath,
                f"{self.bigquery_project}.{data_layer}.{data_layer}_{data_domain}_{primary_key}_{time_dimension}",
            )
        elif facttable_or_dimension_table == "dim":
            self._upload_2_bigquery(
                self.dimension_table_filepath,
                f"{self.bigquery_project}.{data_layer}.{data_layer}_{data_domain}_{primary_key}_{time_dimension}",
            )

    def _upload_2_bigquery(self, file_path, table_id):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            allow_quoted_newlines=True,
            write_disposition="WRITE_TRUNCATE",
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
