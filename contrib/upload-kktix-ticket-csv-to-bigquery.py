#!/usr/bin/env python3
import argparse
import hashlib
import logging
import re
import unittest
from typing import Dict, Set

import pandas as pd
from google.cloud import bigquery

CANONICAL_COLUMN_NAMES_CORE = {
    "paid_date",
    "area_of_interest",
    "payment_status",
    "country_or_region",
    "job_title",
    "ticket_type",
    "email",
    "organization",
    "price",
    "dietary_habit",
    "gender",
    "years_of_using_python",
    "registration_no",
}


CANONICAL_COLUMN_NAMES_2020_CORE = {
    "ticket_type",
    "payment_status",
    "tags",
    "paid_date",
    "price",
    "dietary_habit",
    "years_of_using_python",
    "area_of_interest",
    "organization",
    "job_title",
    "country_or_region",
    "departure_from_region",
    "gender",
    "email_from_sponsor",
    "email_to_sponsor",
    "ive_already_read_and_i_accept_the_epidemic_prevention_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "email",
    "registration_no",
    "attendance_book",
}

CANONICAL_COLUMN_NAMES_2020_EXTRA_CORPORATE = {
    "invoice_policy",
    "invoiced_company_name",
    "unified_business_no",
    "pynight_attendee_numbers",
    "know_financial_aid",
    "have_you_ever_attended_pycon_tw",
    "pynight_attending_or_not",
    "how_did_you_know_pycon_tw",
}

CANONICAL_COLUMN_NAMES_2020_EXTRA_INDIVIDUAL = {
    "pynight_attendee_numbers",
    "know_financial_aid",
    "have_you_ever_attended_pycon_tw",
    "pynight_attending_or_not",
    "how_did_you_know_pycon_tw",
}

CANONICAL_COLUMN_NAMES_2020_EXTRA_RESERVED: Set = set()


CANONICAL_COLUMN_NAMES_2019_CORE = {
    "ticket_type",
    "payment_status",
    "tags",
    "paid_date",
    "price",
    "dietary_habit",
    "need_shuttle_bus_service",
    "size_of_tshirt",
    "years_of_using_python",
    "area_of_interest",
    "organization",
    "job_title",
    "country_or_region",
    "gender",
    "email",
    "registration_no",
    "attendance_book",
}

CANONICAL_COLUMN_NAMES_2019_EXTRA_CORPORATE = {
    "invoice_policy",
    "invoiced_company_name",
    "unified_business_no",
}

CANONICAL_COLUMN_NAMES_2019_EXTRA_INDIVIDUAL: Set = set()

CANONICAL_COLUMN_NAMES_2019_EXTRA_RESERVED = {
    "invoice_policy",
}


CANONICAL_COLUMN_NAMES_2018_CORE = {
    "registration_no",
    "ticket_type",
    "payment_status",
    "paid_date",
    "price",
    "invoice_policy",
    "dietary_habit",
    "need_shuttle_bus_service",
    "size_of_tshirt",
    "years_of_using_python",
    "area_of_interest",
    "organization",
    "job_title",
    "country_or_region",
    "gender",
    "email",
    "tags",
    "attendance_book",
}

CANONICAL_COLUMN_NAMES_2018_EXTRA_CORPORATE = {
    "invoiced_company_name",
    "unified_business_no",
}

CANONICAL_COLUMN_NAMES_2018_EXTRA_INDIVIDUAL: Set = set()

CANONICAL_COLUMN_NAMES_2018_EXTRA_RESERVED = {
    "invoiced_company_name",
    "unified_business_no",
}


HEURISTIC_COMPATIBLE_MAPPING_TABLE = {
    # from 2020 reformatted column names
    "years_of_using_python_python": "years_of_using_python",
    "company_for_students_or_teachers_fill_in_the_school_department_name": "organization",
    "invoiced_company_name_optional": "invoiced_company_name",
    "unified_business_no_optional": "unified_business_no",
    "job_title_if_you_are_a_student_fill_in_student": "job_title",
    "come_from": "country_or_region",
    "departure_from_regions": "departure_from_region",
    "how_did_you_find_out_pycon_tw_pycon_tw": "how_did_you_know_pycon_tw",
    "have_you_ever_attended_pycon_tw_pycon_tw": "have_you_ever_attended_pycon_tw",
    "privacy_policy_of_pycon_tw_2020": "privacy_policy_of_pycon_tw",
    "privacy_policy_of_pycon_tw_2020_pycon_tw_2020_bitly3eipaut": "privacy_policy_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycontw_2020_pycon_tw_2020": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw_2020_pycon_tw_2020": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "ive_already_read_and_i_accept_the_epidemic_prevention_of_pycon_tw_2020_pycon_tw_2020_covid19": "ive_already_read_and_i_accept_the_epidemic_prevention_of_pycon_tw",
    "do_you_know_we_have_financial_aid_this_year": "know_financial_aid",
    "contact_email": "email",
    # from 2020 reformatted column names which made it duplicate
    "PyNight 參加意願僅供統計人數，實際是否舉辦需由官方另行公告": "pynight_attendee_numbers",
    "PyNight 參加意願": "pynight_attending_or_not",
    "是否願意收到贊助商轉發 Email 訊息": "email_from_sponsor",
    "是否願意提供 Email 給贊助商": "email_to_sponsor",
    # from 2018 reformatted column names
    "size_of_tshirt_t": "size_of_tshirt",
    # from 2021 reformatted column names
    "Ive_already_read and_I_accept_the_Privacy_Policy_of_PyCon_TW_2021": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "privacy_policy_of_pycon_tw_2021_pycon_tw_2021_httpsbitly2qwl0am": "privacy_policy_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw_2021_pycon_tw_2021": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
}

UNWANTED_DATA_TO_UPLOAD = (
    # raw column names
    "Id",
    "Order Number",
    "Checkin Code",
    "QR Code Serial No.",
    "Nickname / 暱稱 (Shown on Badge)",
    "Contact Name",
    "Contact Mobile",
    "Epidemic Prevention of PyCon TW 2020 / PyCon TW 2020 COVID-19 防疫守則 bit.ly/3fcnhu2",
    "Epidemic Prevention of PyCon TW 2020",
    "Privacy Policy of PyCon TW 2020 / PyCon TW 2020 個人資料保護聲明 bit.ly/3eipAut",
    "Privacy Policy of PyCon TW 2020",
    "If you buy the ticket with PySafe, remember to fill out correct address and size of t-shirt for us to send the parcel. if you fill the wrong information to cause missed delivery, we will not resend th",
    "請務必填寫正確之「Address / 收件地址」和「Size of T-shirt / T恤尺寸 」（僅限台灣及離島區域）以避免 PySafe 無法送達，如因填寫錯誤致未收到 PySafe，報名人須自行負責，大會恕不再另行補寄",
    "購買含 PySafe 票卷者，請務必填寫正確之「Address / 收件地址」和「Size of T-shirt / T恤尺寸 」（僅限台灣及離島區域），以避免 PySafe 無法送達，如因填寫錯誤致未收到 PySafe，報名人須自行負責，大會恕不再另行補寄",
    "Address / 收件地址 EX: 115台北市南港區研究院路二段128號",
)


logging.basicConfig(level=logging.INFO)


def upload_dataframe_to_bigquery(
    df: pd.DataFrame, project_id: str, dataset_name: str, table_name: str
) -> None:
    client = bigquery.Client(project=project_id)

    dataset_ref = bigquery.dataset.DatasetReference(project_id, dataset_name)
    table_ref = bigquery.table.TableReference(dataset_ref, table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    if "vat_number" in df.columns:
        df["vat_number"] = df["vat_number"].astype("string")
    # dump the csv into bigquery

    job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
    )
    job.result()

    logging.info(
        "Loaded %s rows into %s:%s.", job.output_rows, dataset_name, table_name
    )


def reserved_alphabet_space_underscore(string_as_is: str) -> str:
    regex = re.compile("[^a-zA-Z 0-9_]")
    return regex.sub("", string_as_is)


def reserved_only_one_space_between_words(string_as_is: str) -> str:
    string_as_is = string_as_is.strip()
    # two or more space between two words
    # \w : word characters, a.k.a. alphanumeric and underscore
    match = re.search(r"\w[ ]{2,}\w", string_as_is)

    if not match:
        return string_as_is

    regex = re.compile(r"\s+")
    string_as_is = regex.sub(" ", string_as_is)

    return string_as_is


def get_reformatted_style_columns(columns: dict) -> dict:
    reformatted_columns = {}
    for key, column_name in columns.items():
        reformatted_column_name = reserved_alphabet_space_underscore(column_name)
        reformatted_column_name = reserved_only_one_space_between_words(
            reformatted_column_name
        )
        reformatted_column_name = reformatted_column_name.replace(" ", "_")
        reformatted_column_name = reformatted_column_name.lower()

        reformatted_columns[key] = reformatted_column_name

    return reformatted_columns


def find_reformat_none_unique(columns: dict) -> list:
    # reverse key-value of original dict to be value-key of reverse_dict
    reverse_dict: Dict = {}

    for key, value in columns.items():
        reverse_dict.setdefault(value, set()).add(key)

    result = [key for key, values in reverse_dict.items() if len(values) > 1]

    return result


def apply_compatible_mapping_name(columns: dict) -> dict:
    """Unify names with a heuristic hash table"""
    updated_columns = apply_heuristic_name(columns)

    return updated_columns


def apply_heuristic_name(columns: dict) -> dict:
    updated_columns = dict(columns)

    for candidate in HEURISTIC_COMPATIBLE_MAPPING_TABLE.keys():
        for key, value in columns.items():
            if candidate == value:
                candidate_value = HEURISTIC_COMPATIBLE_MAPPING_TABLE[candidate]
                updated_columns[key] = candidate_value

    return updated_columns


def init_rename_column_dict(columns_array: pd.core.indexes.base.Index) -> dict:
    columns_dict = {}

    for item in columns_array:
        columns_dict[item] = item

    return columns_dict


def strip_unwanted_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_strip = [
        column_name
        for column_name in UNWANTED_DATA_TO_UPLOAD
        if column_name in df.columns
    ]

    if not columns_to_strip:
        return df

    return df.drop(columns=columns_to_strip)


def sanitize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-process the column names of raw data

    Pre-checking rules of column name black list and re-formatting if necessary.

    The sanitized pre-process of data should follow the following rules:
        1. style of column name (which follows general SQL conventions)
        1-1. singular noun
        1-2. lower case
        1-3. snake-style (underscore-separated words)
        1-4. full word (if possible) except common abbreviations
        2. a column name SHOULD be unique
        3. backward compatible with column names in the past years
    """
    df_stripped_unwanted = strip_unwanted_columns(df)
    rename_column_dict = init_rename_column_dict(df_stripped_unwanted.columns)

    # apply possible heuristic name if possible
    # this is mainly meant to resolve style-reformatted names duplicate conflicts
    applied_heuristic_columns = apply_heuristic_name(rename_column_dict)

    # pre-process of style of column name
    style_reformatted_columns = get_reformatted_style_columns(applied_heuristic_columns)
    df.rename(columns=style_reformatted_columns)

    # pre-process of name uniqueness
    duplicate_column_names = find_reformat_none_unique(style_reformatted_columns)
    if duplicate_column_names:
        logging.error(
            "Found the following duplicate column names: %s", duplicate_column_names
        )

    # pre-process of backward compatibility
    compatible_columns = apply_compatible_mapping_name(style_reformatted_columns)

    return df_stripped_unwanted.rename(columns=compatible_columns)


def hash_string(string_to_hash: str) -> str:
    sha = hashlib.sha256()
    sha.update(str(string_to_hash).encode("utf-8"))
    string_hashed = sha.hexdigest()

    return string_hashed


def hash_privacy_info(df: pd.DataFrame) -> None:
    df["email"] = df["email"].apply(hash_string)


def main():
    """
    Commandline entrypoint
    """
    parser = argparse.ArgumentParser(
        description="Sanitize ticket CSV and upload to BigQuery"
    )

    parser.add_argument(
        "csv_file",
        type=str,
        help="Ticket CSV file",
    )

    parser.add_argument("-p", "--project-id", help="BigQuery project ID")

    parser.add_argument(
        "-d", "--dataset-name", help="BigQuery dataset name to create or append"
    )

    parser.add_argument(
        "-t", "--table-name", help="BigQuery table name to create or append"
    )

    parser.add_argument(
        "--upload",
        action="store_true",
        help="Parsing the file but not upload it",
        default=False,
    )

    args = parser.parse_args()

    # load the csv into bigquery
    df = pd.read_csv(args.csv_file)
    if "Email" in df.columns:
        # BUG: theoretically, column name should be `Contact Email` not `Email`
        # hope registration team would remove `Email` column in 2023
        df = df.drop(columns=["Email"])
    sanitized_df = sanitize_column_names(df)
    hash_privacy_info(sanitized_df)

    if args.upload:
        upload_dataframe_to_bigquery(
            sanitized_df, args.project_id, args.dataset_name, args.table_name
        )
    else:
        logging.info("Dry-run mode. Data will not be uploaded.")
        logging.info("Column names (as-is):")
        logging.info(df.columns)
        logging.info("")
        logging.info("Column names (to-be):")
        logging.info(sanitized_df.columns)

    return sanitized_df.columns


class TestCrossYear(unittest.TestCase):
    """python -m unittest upload-kktix-ticket-csv-to-bigquery.py"""

    def test_columns_intersection(self):
        set_extra = {"tags", "attendance_book"}

        set_2020 = CANONICAL_COLUMN_NAMES_2020_CORE
        set_2019 = CANONICAL_COLUMN_NAMES_2019_CORE
        set_2018 = CANONICAL_COLUMN_NAMES_2018_CORE
        set_intersection_cross_year = set_2020.intersection(
            set_2019.intersection(set_2018.difference(set_extra))
        )

        set_core = CANONICAL_COLUMN_NAMES_CORE

        assert not set_intersection_cross_year.difference(set_core)
        assert not set_core.difference(set_intersection_cross_year)


class Test2020Ticket(unittest.TestCase):
    """python -m unittest upload-kktix-ticket-csv-to-bigquery.py"""

    @classmethod
    def setUpClass(cls):
        cls.df_corporate = pd.read_csv("./data/corporate-attendees-2020.csv")
        cls.sanitized_df_corporate = sanitize_column_names(cls.df_corporate)

        cls.df_individual = pd.read_csv("./data/individual-attendees-2020.csv")
        cls.sanitized_df_individual = sanitize_column_names(cls.df_individual)

        cls.df_reserved = pd.read_csv("./data/reserved-attendees-2020.csv")
        cls.sanitized_df_reserved = sanitize_column_names(cls.df_reserved)

    def compare_column_set(self, set_actual, set_expected):
        set_union = set_actual.union(set_expected)

        assert not set_union.difference(set_actual)
        assert not set_union.difference(set_expected)

    def test_column_number_corporate(self):
        assert 28 == len(self.sanitized_df_corporate.columns)

    def test_column_number_individual(self):
        assert 25 == len(self.sanitized_df_individual.columns)

    def test_column_number_reserved(self):
        assert 20 == len(self.sanitized_df_reserved.columns)

    def test_column_title_content_all(self):
        assert len(self.sanitized_df_corporate.columns) == len(
            CANONICAL_COLUMN_NAMES_2020_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2020_EXTRA_CORPORATE)
        assert len(self.sanitized_df_individual.columns) == len(
            CANONICAL_COLUMN_NAMES_2020_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2020_EXTRA_INDIVIDUAL)
        assert len(self.sanitized_df_reserved.columns) == len(
            CANONICAL_COLUMN_NAMES_2020_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2020_EXTRA_RESERVED)

    def test_column_title_content_corporate(self):
        self.compare_column_set(
            set(self.sanitized_df_corporate.columns),
            CANONICAL_COLUMN_NAMES_2020_CORE.union(
                CANONICAL_COLUMN_NAMES_2020_EXTRA_CORPORATE
            ),
        )

    def test_column_title_content_individual(self):
        self.compare_column_set(
            set(self.sanitized_df_individual.columns),
            CANONICAL_COLUMN_NAMES_2020_CORE.union(
                CANONICAL_COLUMN_NAMES_2020_EXTRA_INDIVIDUAL
            ),
        )

    def test_column_title_content_reserved(self):
        self.compare_column_set(
            set(self.sanitized_df_reserved.columns),
            CANONICAL_COLUMN_NAMES_2020_CORE.union(
                CANONICAL_COLUMN_NAMES_2020_EXTRA_RESERVED
            ),
        )

    def test_column_content_corporate(self):
        assert "Regular 原價" == self.sanitized_df_corporate["ticket_type"][1]

    def test_column_content_individual(self):
        assert "Discount 優惠價" == self.sanitized_df_individual["ticket_type"][1]

    def test_column_content_reserved(self):
        assert "Contributor 貢獻者票" == self.sanitized_df_reserved["ticket_type"][1]

    def test_hash(self):
        string_hashed = hash_string("1234567890-=qwertyuiop[]")

        assert (
            "aefefa43927b374a9af62ab60e4512e86f974364919d1b09d0013254c667e512"
            == string_hashed
        )

    def test_hash_email_corporate(self):
        hash_privacy_info(self.sanitized_df_corporate)

        assert (
            "7fcedd1de57031e2ae316754ff211088a1b08c4a9112676478ac5a6bf0f95131"
            == self.sanitized_df_corporate["email"][1]
        )

    def test_hash_email_individual(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "7fcedd1de57031e2ae316754ff211088a1b08c4a9112676478ac5a6bf0f95131"
            == self.sanitized_df_individual["email"][1]
        )

    def test_hash_email_reserved(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "fc5008329367fe025e138088e9ae5b316d91e8c1939158133f6d2bc937003877"
            == self.sanitized_df_individual["email"][1]
        )


class Test2019Ticket(unittest.TestCase):
    """python -m unittest upload-kktix-ticket-csv-to-bigquery.py"""

    @classmethod
    def setUpClass(cls):
        cls.df = pd.read_csv("./data/corporate-attendees-2019.csv")
        cls.sanitized_df_corporate = sanitize_column_names(cls.df)

        cls.df_individual = pd.read_csv("./data/individual-attendees-2019.csv")
        cls.sanitized_df_individual = sanitize_column_names(cls.df_individual)

        cls.df_reserved = pd.read_csv("./data/reserved-attendees-2019.csv")
        cls.sanitized_df_reserved = sanitize_column_names(cls.df_reserved)

    def compare_column_set(self, set_actual, set_expected):
        set_union = set_actual.union(set_expected)

        assert not set_union.difference(set_actual)
        assert not set_union.difference(set_expected)

    def test_column_number_corporate(self):
        assert 20 == len(self.sanitized_df_corporate.columns)

    def test_column_number_individual(self):
        assert 17 == len(self.sanitized_df_individual.columns)

    def test_column_number_reserved(self):
        assert 18 == len(self.sanitized_df_reserved.columns)

    def test_column_title_content_all(self):
        assert len(self.sanitized_df_corporate.columns) == len(
            CANONICAL_COLUMN_NAMES_2019_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2019_EXTRA_CORPORATE)
        assert len(self.sanitized_df_individual.columns) == len(
            CANONICAL_COLUMN_NAMES_2019_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2019_EXTRA_INDIVIDUAL)
        assert len(self.sanitized_df_reserved.columns) == len(
            CANONICAL_COLUMN_NAMES_2019_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2019_EXTRA_RESERVED)

    def test_column_title_content_corporate(self):
        self.compare_column_set(
            set(self.sanitized_df_corporate.columns),
            CANONICAL_COLUMN_NAMES_2019_CORE.union(
                CANONICAL_COLUMN_NAMES_2019_EXTRA_CORPORATE
            ),
        )

    def test_column_title_content_individual(self):
        self.compare_column_set(
            set(self.sanitized_df_individual.columns),
            CANONICAL_COLUMN_NAMES_2019_CORE.union(
                CANONICAL_COLUMN_NAMES_2019_EXTRA_INDIVIDUAL
            ),
        )

    def test_column_title_content_reserved(self):
        self.compare_column_set(
            set(self.sanitized_df_reserved.columns),
            CANONICAL_COLUMN_NAMES_2019_CORE.union(
                CANONICAL_COLUMN_NAMES_2019_EXTRA_RESERVED
            ),
        )

    def test_column_content(self):
        assert "Regular 原價" == self.sanitized_df_corporate["ticket_type"][1]

    def test_column_content_individual(self):
        assert "Discount 優惠價" == self.sanitized_df_individual["ticket_type"][1]

    def test_column_content_reserved(self):
        assert "Invited 邀請票" == self.sanitized_df_reserved["ticket_type"][1]

    def test_hash(self):
        string_hashed = hash_string("1234567890-=qwertyuiop[]")

        assert (
            "aefefa43927b374a9af62ab60e4512e86f974364919d1b09d0013254c667e512"
            == string_hashed
        )

    def test_hash_email_corporate(self):
        hash_privacy_info(self.sanitized_df_corporate)

        assert (
            "7fcedd1de57031e2ae316754ff211088a1b08c4a9112676478ac5a6bf0f95131"
            == self.sanitized_df_corporate["email"][1]
        )

    def test_hash_email_individual(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "6f197622cc2f46bf56f961489d98e67a116fa058126578a5f37bcb5b16c719e5"
            == self.sanitized_df_individual["email"][1]
        )

    def test_hash_email_reserved(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "15e5151c59563f8e6159239048ea2dba1e3554684ef813916129e0981fd82737"
            == self.sanitized_df_individual["email"][1]
        )


class Test2018Ticket(unittest.TestCase):
    """python -m unittest upload-kktix-ticket-csv-to-bigquery.py"""

    @classmethod
    def setUpClass(cls):
        cls.df = pd.read_csv("./data/corporate-attendees-2018.csv")
        cls.sanitized_df_corporate = sanitize_column_names(cls.df)

        cls.df_individual = pd.read_csv("./data/individual-attendees-2018.csv")
        cls.sanitized_df_individual = sanitize_column_names(cls.df_individual)

        cls.df_reserved = pd.read_csv("./data/reserved-attendees-2018.csv")
        cls.sanitized_df_reserved = sanitize_column_names(cls.df_reserved)

    def compare_column_set(self, set_actual, set_expected):
        set_union = set_actual.union(set_expected)

        assert not set_union.difference(set_actual)
        assert not set_union.difference(set_expected)

    def test_column_number_corporate(self):
        assert 20 == len(self.sanitized_df_corporate.columns)

    def test_column_number_individual(self):
        assert 18 == len(self.sanitized_df_individual.columns)

    def test_column_number_reserved(self):
        assert 20 == len(self.sanitized_df_reserved.columns)

    def test_column_title_content_all(self):
        assert len(self.sanitized_df_corporate.columns) == len(
            CANONICAL_COLUMN_NAMES_2018_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2018_EXTRA_CORPORATE)
        assert len(self.sanitized_df_individual.columns) == len(
            CANONICAL_COLUMN_NAMES_2018_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2018_EXTRA_INDIVIDUAL)
        assert len(self.sanitized_df_reserved.columns) == len(
            CANONICAL_COLUMN_NAMES_2018_CORE
        ) + len(CANONICAL_COLUMN_NAMES_2018_EXTRA_RESERVED)

    def test_column_title_content_corporate(self):
        self.compare_column_set(
            set(self.sanitized_df_corporate.columns),
            CANONICAL_COLUMN_NAMES_2018_CORE.union(
                CANONICAL_COLUMN_NAMES_2018_EXTRA_CORPORATE
            ),
        )

    def test_column_title_content_individual(self):
        self.compare_column_set(
            set(self.sanitized_df_individual.columns),
            CANONICAL_COLUMN_NAMES_2018_CORE.union(
                CANONICAL_COLUMN_NAMES_2018_EXTRA_INDIVIDUAL
            ),
        )

    def test_column_title_content_reserved(self):
        self.compare_column_set(
            set(self.sanitized_df_reserved.columns),
            CANONICAL_COLUMN_NAMES_2018_CORE.union(
                CANONICAL_COLUMN_NAMES_2018_EXTRA_RESERVED
            ),
        )

    def test_column_content_corporate(self):
        assert "Regular 原價" == self.sanitized_df_corporate["ticket_type"][1]

    def test_column_content_individual(self):
        assert (
            "EarlyBird, Discount 優惠價"
            == self.sanitized_df_individual["ticket_type"][1]
        )

    def test_column_content_reserved(self):
        assert "Sponsor 贊助夥伴" == self.sanitized_df_reserved["ticket_type"][1]

    def test_hash(self):
        string_hashed = hash_string("1234567890-=qwertyuiop[]")

        assert (
            "aefefa43927b374a9af62ab60e4512e86f974364919d1b09d0013254c667e512"
            == string_hashed
        )

    def test_hash_email_corporate(self):
        hash_privacy_info(self.sanitized_df_corporate)

        assert (
            "7fcedd1de57031e2ae316754ff211088a1b08c4a9112676478ac5a6bf0f95131"
            == self.sanitized_df_corporate["email"][1]
        )

    def test_hash_email_individual(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "6f197622cc2f46bf56f961489d98e67a116fa058126578a5f37bcb5b16c719e5"
            == self.sanitized_df_individual["email"][1]
        )

    def test_hash_email_reserved(self):
        hash_privacy_info(self.sanitized_df_individual)

        assert (
            "15e5151c59563f8e6159239048ea2dba1e3554684ef813916129e0981fd82737"
            == self.sanitized_df_individual["email"][1]
        )


if __name__ == "__main__":
    main()
