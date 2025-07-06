import argparse
import os
import json
import csv
from typing import List, Dict, Any

# Import existing loader functions and schema
# Adjust the import path as needed based on your project structure
from ods.kktix_ticket_orders.udfs.kktix_loader import SCHEMA, load_to_bigquery, load_to_bigquery_dwd

# TODO: use logging instead of print statements

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load offline CSV ticket data into BigQuery"
    )
    parser.add_argument(
        "--gcp_credential_file", type=str, required=True,
        help="Path to GCP credential JSON file"
    )
    parser.add_argument(
        "--gcp_project_id", type=str, required=True,
        help="GCP project ID"
    )
    parser.add_argument(
        "--year", type=int, required=True,
        help="Year of the event (must be > 2020)"
    )
    parser.add_argument(
        "--ticket_group", type=str, choices=["corporate", "individual", "reserved"],
        required=True, help="Type of ticket group"
    )
    parser.add_argument(
        "--meta_field_mapping_file", type=str, required=True,
        default="default_meta_field_mapping.json",
        help="Path to JSON file mapping ATTENDEE_INFO fields to CSV columns"
    )
    parser.add_argument(
        "--data_field_names_file", type=str, required=True,
        default="default_data_field_names.txt",
        help="Path to TXT file listing data field names (one per line)"
    )
    parser.add_argument(
        "--attendees_csv_file", type=str, required=True,
        help="Path to attendees CSV file"
    )
    parser.add_argument(
        "--orders_csv_file", type=str, required=True,
        help="Path to orders CSV file"
    )
    parser.add_argument(
        "--is_dry_run", action="store_true",
        help="If set, prints transformed payload without loading to BigQuery"
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """
    Validate command-line arguments for correctness.
    """
    # Check file paths
    for path_attr in [
        'gcp_credential_file', 'meta_field_mapping_file',
        'data_field_names_file', 'attendees_csv_file', 'orders_csv_file'
    ]:
        path = getattr(args, path_attr)
        if not os.path.isfile(path):  # TODO: use pathlib for better path handling
            raise FileNotFoundError(f"File not found: {path_attr} ({path})")

    # Year validation
    if args.year <= 2020:
        raise ValueError("The 'year' argument must be greater than 2020.")

    # ticket_group is enforced by argparse choices


def load_meta_field_mapping(file_path: str) -> Dict[str, str]:
    """
    Load JSON mapping of ATTENDEE_INFO fields to CSV columns.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        mapping = json.load(f)
    # TODO: validate mapping structure, make sure one to one mapping
    return mapping


def load_data_field_names(file_path: str) -> List[str]:
    """
    Load list of data field names from a text file.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        names = [line.strip() for line in f if line.strip()]
    return names


def read_csv(file_path: str) -> List[Dict[str, Any]]:
    """
    Read CSV file into a list of dictionaries.
    """
    # TODO: use pandas for better CSV handling if needed
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


def transform_to_payload(
    attendees: List[Dict[str, Any]],
    orders: List[Dict[str, Any]],
    meta_mapping: Dict[str, str],
    data_fields: List[str],
    args: argparse.Namespace
) -> List[Dict[str, Any]]:
    """
    Convert raw CSV rows into payload matching SCHEMA for BigQuery load.

    - Merge attendees and orders on order number
    - Map CSV fields to ATTENDEE_INFO keys
    - Generate unique event ID for offline imports
    - Assemble SCHEMA payload list
    """
    # TODO: call a function
    # implement merging logic and mapping according to meta_mapping

    # TODO: call a function
    # generate or derive SCHEMA['ID'] and 'NAME'
    # ticket_group: {corporate: 0, individual: 1, reserved: 2}
    # generated_id = f"999000{year}0{ticket_group}"

    # TODO: construct 'ATTENDEE_INFO' dict including 'data' array
    # this is example ATTENDEE_INFO structure
    # {
    #    "id":84948085,
    #    "ticket_id":449148,
    #    "ticket_name":"Group Buy 企業團體票（with PyCkage）",
    #    "reg_no":107,
    #    "state":"activated",
    #    "checkin_code":"98G9",
    #    "qrcode":"98g9c2b67e6a1239gfb912bc77b4e5a4",
    #    "is_paid":true,
    #    "price":0.0,
    #    "currency":"TWD",
    #    "payment_method":"FREE",
    #    "data":[
    #       [
    #          "Nickname / 暱稱 (Shown on Badge)",
    #          "Jerry Lin"
    #       ],
    #       [
    #          "Gender / 生理性別",
    #          "Male / 男性"
    #       ],
    #       ...
    #    ],
    #    "kyc":{},
    #    "id_number":null,
    #    "updated_at":1658817767.85132,
    #    "ticket_type":"qrcode",
    #    "slot":{},
    #    "order_no":127773545
    # }

    payload: List[Dict[str, Any]] = []
    # Example placeholder:
    # payload.append({
    #     'ID': generated_id,
    #     'NAME': args.event_name,
    #     'ATTENDEE_INFO': attendee_info_dict
    # })
    return payload


def main():
    args = parse_args()
    try:
        validate_args(args)
    except Exception as e:
        print(f"Argument validation error: {e}")
        return

    meta_mapping = load_meta_field_mapping(args.meta_field_mapping_file)
    data_field_names = load_data_field_names(args.data_field_names_file)

    attendees_rows = read_csv(args.attendees_csv_file)
    orders_rows = read_csv(args.orders_csv_file)

    # TODO: validate that attendees and orders table can be merged
    ...

    # TODO: validate that attendees and orders table have necessary meta and data fields
    ...

    payload = transform_to_payload(
        attendees_rows,
        orders_rows,
        meta_mapping,
        data_field_names,
        args
    )

    # TODO: check if all meta fields are present in the payload, if not warning

    # TODO: check if all data fields are present in the payload, if not warning

    # TODO: ref. dags/ods/kktix_ticket_orders/udfs/kktix_loader.py call _sanitize_payload to mask sensitive data before loading
    ...

    if args.is_dry_run:
        print("Dry run enabled. Transformed payload preview:")
        # TODO: pretty-print or log first few records
        for record in payload[:5]:
            print(record)
    else:
        # Load into BigQuery
        print("Loading to BigQuery: ODS table")
        load_to_bigquery(
            payload,
            project_id=args.gcp_project_id,
            credential_file=args.gcp_credential_file
        )
        print("Loading to BigQuery: DWD tables")
        load_to_bigquery_dwd(
            payload,
            project_id=args.gcp_project_id,
            credential_file=args.gcp_credential_file,
            ticket_group=args.ticket_group,
        )


if __name__ == "__main__":
    main()
