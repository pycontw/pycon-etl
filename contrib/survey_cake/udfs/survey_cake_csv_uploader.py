import csv
import os
from pathlib import Path
from typing import Dict, List

from google.cloud import bigquery


class SurveyCakeCSVUploader:
    USELESS_COLUMNS = {
        "額滿結束註記",
        "使用者紀錄",
        "會員時間",
        "會員編號",
        "自訂ID",
        "備註",
    }

    def __init__(self, year: int, filename: str):
        self._year = year
        self.filename = Path(filename)
        if not bool(os.getenv("AIRFLOW_TEST_MODE")):
            self.client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
            self.existing_question_and_id_dict = self._get_existing_question_and_id()
        else:
            self.existing_question_and_id_dict = {"placeholder": 1}
        self.facttable_filepath = (
            self.filename.parent / f"{self.filename.stem}_facttable.csv"
        )
        self.dimension_table_filepath = (
            self.filename.parent / f"{self.filename.stem}_dimension.csv"
        )

    @property
    def year(self):
        return self._year

    @property
    def bigquery_project(self):
        return os.getenv("BIGQUERY_PROJECT")

    def _get_existing_question_and_id(self):
        query = """
            SELECT
                question, question_id
            FROM
                dim.dim_questionnaire_questionId_year;
        """
        query_job = self.client.query(query)
        return {row["question"]: row["question_id"] for row in query_job}

    def transform(self):
        def _export_facttable(header_of_fact_table):
            with open(self.facttable_filepath, "w") as target:
                writer = csv.writer(target)
                writer.writerow(header_of_fact_table)
                for row in rows_of_fact_table:
                    row_with_year = row + (self.year,)
                    writer.writerow(row_with_year)

        def _export_dimension_table(question_id_dimension_table):
            with open(self.dimension_table_filepath, "w") as target:
                writer = csv.writer(target)
                writer.writerow(("question_id", "question", "year"))
                for question_id, question in question_id_dimension_table.items():
                    # need to filter out existing question_id, otherwise we would end up having duplicate question_id in BigQuery
                    if question not in self.existing_question_and_id_dict.keys():
                        writer.writerow((question_id, question, self.year))

        def _get_question_ids_of_this_year(
            header: List, question_id_dimension_table: Dict
        ) -> List:
            reversed_question_id_dimension_table = {
                question: question_id
                for question_id, question in question_id_dimension_table.items()
            }
            return [
                reversed_question_id_dimension_table[column]
                for column in header
                if column not in self.USELESS_COLUMNS
            ]

        with open(Path(self.filename), encoding="utf-8-sig") as csvfile:
            rows = csv.reader(csvfile)
            # skip header
            header = [column.strip() for column in next(iter(rows))]
            question_id_dimension_table = self._generate_question_id_dimension_table(
                header
            )

            question_ids = _get_question_ids_of_this_year(
                header, question_id_dimension_table
            )
            header_of_fact_table = ("ip", "question_id", "answer", "year")
            rows_of_fact_table = self._transform_raw_data_to_fact_table_format(
                rows, question_id_dimension_table, question_ids
            )

        _export_facttable(header_of_fact_table)
        _export_dimension_table(question_id_dimension_table)

    def upload(
        self,
        facttable_or_dimension_table,
        data_layer,
        data_domain,
        primary_key,
        time_dimension,
    ):
        if facttable_or_dimension_table == "fact":
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
            write_disposition="WRITE_APPEND",
            schema_update_options="ALLOW_FIELD_ADDITION",
        )
        with open(file_path, "rb") as source_file:
            job = self.client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )

        job.result()  # Waits for the job to complete.

        table = self.client.get_table(table_id)  # Make an API request.
        print(
            f"There's {table.num_rows} rows and {len(table.schema)} columns in {table_id} now!"
        )

    def _generate_question_id_dimension_table(self, header):
        max_existing_question_id = int(max(self.existing_question_and_id_dict.values()))
        question_id_dim_table = {}
        for index, column in enumerate(header, start=max_existing_question_id):
            if column in self.USELESS_COLUMNS:
                continue
            if column in self.existing_question_and_id_dict:
                question_id_dim_table[self.existing_question_and_id_dict[column]] = (
                    column
                )
            else:
                question_id_dim_table[float(index)] = column
        return question_id_dim_table

    @staticmethod
    def _transform_raw_data_to_fact_table_format(
        rows, question_id_dimension_table, question_ids
    ):
        result = []
        for row in rows:
            row_dict = dict(zip(question_ids, row))
            question_id_of_primary_key = [
                key
                for key, value in question_id_dimension_table.items()
                if value == "IP紀錄"
            ][0]
            primary_key = row_dict[question_id_of_primary_key]
            for question_id, answer in row_dict.items():
                result.append((primary_key, question_id, answer))
        return result
