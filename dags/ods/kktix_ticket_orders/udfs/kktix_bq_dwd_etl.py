#!/usr/bin/env python3
import argparse
import hashlib
import json
import logging
import re
from datetime import datetime
from typing import Dict, Set, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame

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
    # from 2022 reformatted column names
    "how_did_you_know_pycon_apac_2022_pycon_apac_2022": "how_did_you_know_pycon_tw",
    "Ive_already_read and_I_accept_the_Privacy_Policy_of_PyCon_TW_2022": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "privacy_policy_of_pycon_apac_2022_pycon_apac_2022_httpsreurlcc1zxzxw": "privacy_policy_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_apac_2022_pycon_apac_2022": "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw",
    "would_you_like_to_receive_an_email_from_sponsors_email": "email_from_sponsor",
    "would_you_like_to_receive_emails_from_the_sponsors_email": "email_from_sponsor",
    "pyckage_address_size_of_tshirt_t_pyckage_pyckage": "address_size_of_tshirt_t",
    "pyckage_address_size_of_tshirt_t": "address_size_of_tshirt_t",
    "privacy_policy_of_pycon_apac_2022_pycon_apac_2022": "privacy_policy_of_pycon_tw",
    "privacy_policy_of_pycon_apac_2022": "privacy_policy_of_pycon_tw",
    # from 2023 reformatted column names
    "attend_first_time_how_did_you_know_pycon_tw_2023_pycon_tw_2023": "how_did_you_know_pycon_tw",
    "for_submitter_how_did_you_know_the_cfp_information_of_pycon_taiwan_pycon_taiwan_if_you_are_not_submitter_fill_in_nonsubmitter": "how_did_you_know_cfp_of_pycon_tw",
    "ive_already_read_and_i_accept_the_privacy_policy_of_pycon_tw_2023_pycon_tw_2023": "privacy_policy_of_pycon_tw",
    "size_of_tshirt_for_tickets_with_tshirt_should_fill_in_this_field": "size_of_tshirt",
    "ticket_with_tshirt_size_of_tshirt": "size_of_tshirt",
    "job_title_if_you_are_a_student_fill_in_student_student": "job_title",
    "would_you_like_to_receive_an_email_from_sponsors": "email_from_sponsor",
    "Size of T-shirt / 衣服尺寸 (For tickets with t-shirt should fill in this field / 票種有含紀念衣服需填寫)": "size_of_tshirt",
    "Would you like to receive an email from sponsors? / 是否願意收到贊助商轉發的電子郵件？": "email_from_sponsor",
    "Would you like to receive an email from sponsors？/ 是否願意收到贊助商轉發的電子郵件？": "email_from_sponsor",  # ? is different
    "I would like to donate invoice to Open Culture Foundation / 我願意捐贈發票給開放文化基金會 (ref: https://reurl.cc/ZQ6VY6)": "i_would_like_to_donate_invoice_to_open_culture_foundation",
    "I would like to donate invoice to Open Culture Foundation / 我願意捐贈發票給開放文化基金會 (Ref: https://reurl.cc/ZQ6VY6)": "i_would_like_to_donate_invoice_to_open_culture_foundation",  # ref vs Ref
    "Have you ever been a PyCon TW volunteer？ / 是否曾擔任過 PyCon TW 志工？": "have_you_ever_been_a_pycontw_volunteer_pycontw",
    "Have you ever been a PyCon TW volunteer? / 是否曾擔任過 PyCon TW 志工？": "have_you_ever_been_a_pycontw_volunteer_pycontw",
    "How did you know PyCon TW 2023？ / 如何得知 PyCon TW 2023？": "how_did_you_know_pycon_tw",
    "(初次參與) How did you know PyCon TW 2023？ / (Attend first time) 如何得知 PyCon TW 2023？": "how_did_you_know_pycon_tw",
    '(投稿者) 你是怎麼得知 PyCon Taiwan 投稿資訊？/ (For Submitter) How did you know the CfP information of PyCon Taiwan? (If you are NOT submitter, fill in "non-submitter"/ 如果您沒有投稿，請填寫「非投稿者」)': "how_did_you_know_cfp_of_pycon_tw",
    # form 2024 reformatted column names
    "來自國家、地區或縣市": "country_or_region",
    "(Attend first time) How did you know PyCon TW 2024? / (初次參與) 如何得知 PyCon TW？": "how_did_you_know_pycon_tw",
    "Dietary Preference / 飲食偏好": "dietary_habit",
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
    # For 2022
    "Address / 收件地址  Ex: No. 128, Sec. 2, Academia Rd., Nangang Dist., Taipei City 115201, Taiwan (R.O.C.) / 115台北市南港區研究院路二段128號",
    "Address/ 收件地址 Ex: No. 128, Sec. 2, Academia Rd., Nangang Dist., Taipei City 115201, Taiwan (R.O.C.) / 115台北市南港區研究院路二段128號",
    "聯絡人 姓名",
    "Email",  # duplicated with 聯絡人 Email
    "聯絡人 手機",
    "標籤",
    "姓名",
    "手機",
    # For 2023, just for notes
    "提醒填寫正確的衣服尺寸",
    "徵稿資訊管道",
    "個人資料保護聲明",
    "行為準則",
    "注意事項",
    "企業票種將提供報帳收據",
    "衣服尺寸注意事項",
    "PyCon TW 2023 個人資料保護聲明",
    "PyCon TW 2023 行為準則",
    # For 2023, duplicated or unwanted
    # "I would like to donate invoice to Open Culture Foundation / 我願意捐贈發票給開放文化基金會 (Ref: https://reurl.cc/ZQ6VY6)",
    # "Size of T-shirt / 衣服尺寸 (For tickets with t-shirt should fill in this field / 票種有含紀念衣服需填寫)",
    """I'm willing to comply with the PyCon TW 2023 CoC / 我願意遵守 PyCon TW 2023 行為準則""",
    # "Have you ever been a PyCon TW volunteer？ / 是否曾擔任過 PyCon TW 志工？",
    # For 2024, just for notes
    "地址",
    "聯絡人 地址",
    "注意事項",
    "I would like to donate invoice to Open Culture Foundation / 我願意捐贈發票給開放文化基金會 (Ref: https://reurl.cc/ZQ6VY6)",
    "I’ve already read and I accept the Privacy Policy of PyCon TW 2024 / 我已閱讀並同意 PyCon TW 2024 個人資料保護聲明",
    "I'm willing to comply with the PyCon TW 2024 CoC / 我願意遵守 PyCon TW 2024 行為準則",
    "我願意收到未來 PyCon TW 贊助機會相關信件",
    "願意收到未來 PyCon TW 活動訊息",
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
        logging.info(
            "Found the following duplicate column names: %s, will be grouped",
            duplicate_column_names,
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
    if "email" in df.columns:
        df["email"] = df["email"].apply(hash_string)


TABLE = "pycontw-225217.ods.ods_kktix_attendeeId_datetime"


SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("attendee_info", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("refunded", "BOOLEAN", mode="REQUIRED"),
]
JOB_CONFIG = bigquery.LoadJobConfig(schema=SCHEMA)


def _load_row_df_from_dict(json_dict, update_after_ts) -> DataFrame:
    df_dict = pd.DataFrame([json_dict])

    # We don't have paid_date column from ATTENDEE_INFO, convert the updated_at timestamp
    timestamp = df_dict.loc[0, "updated_at"]
    # filtering with timestamp if set
    if update_after_ts:
        if timestamp < update_after_ts:
            return None
    # print(timestamp)
    dt_object = datetime.fromtimestamp(timestamp)
    transc_date = dt_object.strftime("%Y-%m-%d")
    # print(transc_date)
    df_dict["paid_date"] = transc_date

    # ticket_type value is always "qrcode" here, and conflict with old table,
    # kyc, id_number, slot value is always empty
    # 'id','ticket_id','state','checkin_code', 'qrcode', updated_at, order_no no exist in old table
    # data will be handle later
    useless_columns = [
        "id",
        "ticket_id",
        "state",
        "checkin_code",
        # "qrcode",
        "currency",
        "data",
        "ticket_type",
        "kyc",
        # "updated_at",
        "id_number",
        "slot",
        "order_no",
    ]
    df_dict = df_dict.drop(columns=useless_columns)
    df_dict = df_dict.rename(
        columns={
            "reg_no": "registration_no",
            "ticket_name": "ticket_type",
            "is_paid": "payment_status",
        }
    )
    if "payment_status" in df_dict.columns:
        if df_dict.loc[0, "payment_status"]:
            # df_dict['payment_status'] = df_dict['payment_status'].astype("string")
            df_dict.loc[0, "payment_status"] = "paid"
        else:
            df_dict.loc[0, "payment_status"] = "not"
    # json_dict['data'] in format of [[item_name, item_value],[],...]
    data_list = json_dict["data"]
    column_list = [dl[0] for dl in data_list]
    value_list = [dl[1] for dl in data_list]
    # print(len(column_list))
    # print(value_list)
    df_data = pd.DataFrame()
    df_data[column_list] = [value_list]

    df_dict = pd.concat([df_dict, df_data], axis=1)
    return df_dict


def load_to_df_from_list(
    results, source="dag", update_after_ts=0
) -> Tuple[DataFrame, DataFrame]:
    # Use DataFrame for table transform operations
    df = pd.DataFrame()
    attendee_info_str = "attendee_info"
    if source == "bigquery":
        attendee_info_str = attendee_info_str.upper()
    for row in results:
        json_dict = json.loads(
            row[attendee_info_str]
        )  # The name will be uppercase if the results was from bg
        df_dict = _load_row_df_from_dict(json_dict, update_after_ts)
        if df_dict is not None:
            df = pd.concat([df, df_dict], ignore_index=True)

    if len(df.index) == 0:
        return df, df

    # print(df.columns)
    sanitized_df = sanitize_column_names(df)
    # The privacy info in "pycontw-225217.ods.ods_kktix_attendeeId_datetime" had already hashed
    # hash_privacy_info(sanitized_df)

    # Group the columns with the same purposes and names
    sanitized_df = (
        sanitized_df.replace("null", np.nan)
        .groupby(sanitized_df.columns, axis=1, sort=False)
        .first()
    )

    # For columns with FLOAT type in BigQuery table schema, do pd.to_numeric to handle empty string (convert to NaN)
    num_columns = [
        "price",
        "invoice_policy",
        "if_you_buy_the_ticket_with_pyckage",
        "vat_number_optional",
    ]
    for col in num_columns:
        if col in sanitized_df.columns:
            sanitized_df[[col]] = sanitized_df[[col]].apply(pd.to_numeric)

    # Drop privacy, duplicated or temp columns
    priv_columns = [
        "nickname",
        "address_size_of_tshirt_t",
    ]
    for col in priv_columns:
        if col in sanitized_df.columns:
            sanitized_df = sanitized_df.drop(columns=[col])

    # print(sanitized_df.iloc[:, :5])
    # Keep the latest update record with registration_no as the unique key
    sanitized_df = sanitized_df.sort_values(by=["updated_at"])
    sanitized_df = sanitized_df.drop_duplicates(subset=["registration_no"], keep="last")
    sanitized_df = sanitized_df.set_index("registration_no")

    return df, sanitized_df


def main():
    # Set the default project ID and dataset ID
    project_id = "pycontw-225217"
    dataset_id = "ods"
    table_id = "your-table-id"
    ticket_type: str = "corporate"
    ticket_year: str = "2023"
    update_after_ts = 0
    """
    Commandline entrypoint
    """
    parser = argparse.ArgumentParser(
        description="Deserialize Attendee info with JSON format from KKTIX and load to legcy 3 tables corporate, individual, reserved"
    )

    parser.add_argument("-p", "--project-id", help="BigQuery project ID")

    parser.add_argument(
        "-d", "--dataset-name", help="BigQuery dataset name to create or append"
    )

    parser.add_argument(
        "-t", "--table-name", help="BigQuery table name to create or append"
    )

    parser.add_argument(
        "-k",
        "--ticket-type",
        help="Attendee ticket-type name in corporate, individual, reserved",
    )

    parser.add_argument("-y", "--ticket-year", help="PyConTW year")

    parser.add_argument("-f", "--update-after", help="TimeStamp filter")

    parser.add_argument(
        "--upload",
        action="store_true",
        help="Parsing the file but not upload it",
        default=False,
    )

    args = parser.parse_args()

    if args.project_id:
        project_id = args.project_id
    if args.dataset_name:
        dataset_id = args.dataset_name
    if args.table_name:
        table_id = args.table_name
    if args.ticket_type:
        ticket_type = args.ticket_type
    if args.ticket_year:
        ticket_year = args.ticket_year
    if args.update_after:
        # update_after = args.update_after
        # dt_string = "2018-1-1 09:15:32"
        # Considering date is in dd/mm/yyyy format
        dt_object = datetime.strptime(args.update_after, "%Y-%m-%d %H:%M:%S")
        ticket_year = dt_object.year
        update_after_ts = datetime.timestamp(dt_object)

    # Set up the client object
    client = bigquery.Client(project=project_id)

    # Set up the job config
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    # Build the SQL query to extract the data
    # Use  parameterized queries to prevent SQL inject
    job_config.query_parameters = [
        bigquery.ScalarQueryParameter(
            "t_year_type", "STRING", f"%{ticket_year}%{ticket_type}%"
        ),
        #    bigquery.ScalarQueryParameter("t_type", "STRING", f'%{ticket_type}%'),
    ]
    query = f"SELECT * FROM `{TABLE}` WHERE lower( NAME ) LIKE @t_year_type"
    # Execute the query and extract the data
    # bigquery depends on db_dtypes for to_dataframe(), depends on apache-arrow, cannot be installed (build failed) on my old MAC
    # use the raw result from bigQuery is good for this use case
    job = client.query(query, job_config=job_config)
    results = job.result()

    # Don't iterate results before load_to_df_from_list, need copy first
    # res_clone = copy.copy(results)
    # print(res_clone)

    print(f"Extracted from {TABLE}  successful.")

    # load to DataFrame for later bigquery upload from the extracted results
    df, sanitized_df = load_to_df_from_list(results, "bigquery", update_after_ts)
    # print(sanitized_df.columns)
    # print(sanitized_df.head())
    # df_null = sanitized_df.isnull()
    # print(sanitized_df.iloc[:, :5])

    # TODO
    # Loop for the 3 tables by set the table ID and upload_dataframe_to_bigquery
    if args.upload:
        upload_dataframe_to_bigquery(sanitized_df, project_id, dataset_id, table_id)
    else:
        logging.info("Dry-run mode. Data will not be uploaded.")
        logging.info("Column names (as-is):")
        logging.info(df.columns)
        # print(df[['paid_date', 'payment_status', 'email']])
        logging.info("")
        logging.info("Column names (to-be):")
        logging.info(sanitized_df.columns)
        print(sanitized_df.iloc[:, :5])
        # print(sanitized_df[['paid_date', 'payment_status', 'email']])

    return sanitized_df.columns


if __name__ == "__main__":
    main()
