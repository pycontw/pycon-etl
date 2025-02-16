"""
A crawler which would crawl the openings
"""

import argparse

from udfs.survey_cake_csv_uploader import SurveyCakeCSVUploader

TEST_DATA_LAYER = "test"
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
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("-y", "--year", type=int, required=True)
    PARSER.add_argument(
        "-c",
        "--contributor",
        type=str,
        help="input your name please! You'll find a table with your name in Bigquery.test dataset",
        required=True,
    )
    PARSER.add_argument("-p", "--prod", action="store_true")
    ARGS = PARSER.parse_args()
    print(
        "HINT: the default mode would load data to dataset `test`. To load data to bigquery's `ods` dataset, please add `--prod` flag!"
    )
    for filename, metadata in FILENAMES.items():
        SURVEY_CAKE_CSV_UPLOADER = SurveyCakeCSVUploader(
            year=ARGS.year, filename=filename
        )
        SURVEY_CAKE_CSV_UPLOADER.transform()
        SURVEY_CAKE_CSV_UPLOADER.upload(
            facttable_or_dimension_table="fact",
            data_layer="ods" if ARGS.prod else TEST_DATA_LAYER,
            data_domain=metadata["data_domain"]
            if ARGS.prod
            else f"{ARGS.contributor}_{metadata['data_domain']}",
            primary_key=metadata["primary_key"],
            time_dimension=metadata["time_dimension"],
        )
        SURVEY_CAKE_CSV_UPLOADER.upload(
            facttable_or_dimension_table="dim",
            data_layer="dim" if ARGS.prod else TEST_DATA_LAYER,
            data_domain=metadata["data_domain"]
            if ARGS.prod
            else f"{ARGS.contributor}_{metadata['data_domain']}",
            primary_key="questionId",
            time_dimension="year",
        )
