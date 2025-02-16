import csv
from pathlib import Path

from contrib.survey_cake.udfs.survey_cake_csv_uploader import SurveyCakeCSVUploader


def test_cakeresume_uploader() -> None:
    fixtures = {
        "tests/data_questionnaire.csv": {
            "data_domain": "questionnaire",
            "primary_key": "ip",
            "time_dimension": "datetime",
        }
    }

    for filename, metadata in fixtures.items():
        SURVEY_CAKE_CSV_UPLOADER = SurveyCakeCSVUploader(year=2146, filename=filename)
        SURVEY_CAKE_CSV_UPLOADER.transform()
    with open(
        Path("tests/data_questionnaire_dimension.csv")
    ) as data_questionnaire_dimension:
        rows = csv.reader(data_questionnaire_dimension)
        header = next(iter(rows))
        if __debug__:
            if header != ["question_id", "question", "year"]:
                raise AssertionError("wrong header!")

    with open(
        Path("tests/data_questionnaire_facttable.csv")
    ) as data_questionnaire_facttable:
        rows = csv.reader(data_questionnaire_facttable)
        header = next(iter(rows))
        if __debug__:
            if header != ["ip", "question_id", "answer", "year"]:
                raise AssertionError("wrong header!")
