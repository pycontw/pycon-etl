import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from ods.kktix_ticket_orders.udfs.kktix_loader import (
    _sanitize_payload,
    load_to_bigquery_ods,
    load_to_bigquery_dwd,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_args() -> argparse.Namespace:
    """解析命令列參數"""
    parser = argparse.ArgumentParser(
        description="從離線 CSV 檔案載入 KKTIX 票務資料到 BigQuery"
    )
    parser.add_argument(
        "--gcp_credential_file",
        type=Path,
        required=True,
        help="GCP 憑證 JSON 檔案的路徑",
    )
    parser.add_argument("--gcp_project_id", type=str, required=True, help="GCP 專案 ID")
    parser.add_argument(
        "--year", type=int, required=True, help="活動年份 (必須 > 2020)"
    )
    parser.add_argument(
        "--event_name",
        type=str,
        required=True,
        help="活動名稱 (例如 'PyCon APAC 2022 Registration')",
    )
    parser.add_argument(
        "--ticket_group",
        type=str,
        choices=["corporate", "individual", "reserved"],
        required=True,
        help="票種群組",
    )
    parser.add_argument(
        "--meta_field_mapping_file",
        type=Path,
        required=True,
        help="定義 ATTENDEE_INFO 欄位與 CSV 欄位對應的 JSON 檔案路徑",
    )
    parser.add_argument(
        "--data_field_names_file",
        type=Path,
        required=True,
        help="列出 data 欄位名稱的 TXT 檔案路徑 (每行一個)",
    )
    parser.add_argument(
        "--attendees_csv_file",
        type=Path,
        required=True,
        help="attendees.csv 檔案的路徑",
    )
    parser.add_argument(
        "--orders_csv_file", type=Path, required=True, help="orders.csv 檔案的路徑"
    )
    parser.add_argument(
        "--is_dry_run",
        action="store_true",
        help="若設定此旗標，將只會印出轉換後的 payload 而不載入到 BigQuery",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    """驗證命令列參數的正確性"""
    logging.info("Validating arguments...")
    for path_attr in [
        "gcp_credential_file",
        "meta_field_mapping_file",
        "data_field_names_file",
        "attendees_csv_file",
        "orders_csv_file",
    ]:
        path = getattr(args, path_attr)
        if not path.is_file():
            raise FileNotFoundError(f"檔案不存在: {path_attr} ({path})")

    if args.year <= 2020:
        raise ValueError("參數 'year' 必須大於 2020.")
    logging.info("Argument validation successful.")


def load_meta_field_mapping(file_path: Path) -> dict[str, str]:
    """載入 ATTENDEE_INFO 欄位到 CSV 欄位的 JSON 對應檔"""
    logging.info(f"Loading meta field mapping from {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        mapping = json.load(f)

    # 驗證對應關係是否為一對一
    if len(mapping.values()) != len(set(mapping.values())):
        raise ValueError(
            f"在 {file_path} 中的 CSV 欄位名稱有重複，對應必須是一對一的。"
        )
    return mapping


def load_data_field_names(file_path: Path) -> list[str]:
    """從文字檔載入 data 欄位名稱列表"""
    logging.info(f"Loading data field names from {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        names = [line.strip() for line in f if line.strip()]
    return names


def read_csv_to_df(file_path: Path) -> pd.DataFrame:
    """使用 pandas 讀取 CSV 檔案為 DataFrame"""
    logging.info(f"Reading CSV file: {file_path}")
    return pd.read_csv(file_path)


def _merge_dataframes(
    attendees_df: pd.DataFrame, orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    以 '訂單編號' 為 key 合併兩個 DataFrame。
    使用左合併 (left merge) 以 attendees_df 為主。
    """
    # 留下票券狀態為 activated 的訂單
    if "票券狀態" not in orders_df.columns:
        raise ValueError("orders.csv 必須包含 '票券狀態' 欄位。")

    activated_orders_df = orders_df[
        orders_df["票券狀態"] == "activated"
    ].copy()
    if activated_orders_df.empty:
        logging.warning(
            "沒有找到任何 '票券狀態' 為 'activated' 的參與者。請檢查 orders.csv 檔案。"
        )
        return pd.DataFrame()

    logging.info("Merging attendees and orders dataframes on '訂單編號'.")
    if "訂單編號" not in attendees_df.columns or "訂單編號" not in orders_df.columns:
        raise ValueError("兩個 CSV 檔案都必須包含 '訂單編號' 欄位。")

    # 為了避免合併後產生重複欄位，先移除 orders_df 中除了 key 以外與 attendees_df 重複的欄位
    order_cols_to_keep = ["訂單編號"] + [
        col for col in orders_df.columns if col not in attendees_df.columns
    ]

    merged_df = pd.merge(
        attendees_df, orders_df[order_cols_to_keep], on="訂單編號", how="left"
    )

    # 檢查是否有 attendees 的訂單在 orders.csv 中找不到
    if merged_df.isnull().values.any():
        # 這裡只記錄警告，因為某些欄位本來就可能為空
        logging.warning(
            "Merged dataframe contains null values. Some orders might be missing in orders.csv or have empty fields."
        )

    return merged_df


def _validate_columns(
    df: pd.DataFrame, meta_mapping: dict[str, str], data_fields: list[str]
) -> None:
    """驗證 DataFrame 是否包含所有必要的欄位"""
    logging.info("Validating required columns in the merged dataframe.")
    required_csv_columns = set(meta_mapping.values()) | set(data_fields)
    missing_columns = required_csv_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            "合併後的 CSV 資料缺少以下必要欄位:\n" + "\n".join([f"- '{col}'" for col in missing_columns])
        )
    logging.info("All required columns are present.")


def _generate_event_id(year: int, ticket_group: str) -> int:
    """為離線匯入產生唯一的活動 ID"""
    group_map = {"corporate": 0, "individual": 1, "reserved": 2}
    group_code = group_map.get(ticket_group)
    generated_id = int(f"999000{year}0{group_code}")
    logging.info(
        f"Generated Event ID: {generated_id} for group '{ticket_group}' in year {year}."
    )
    return generated_id


def _to_unix_timestamp(time_str: str | None) -> float | None:
    """將 'YYYY/MM/DD HH:MM:SS' 格式的字串轉為 Unix timestamp"""
    if pd.isna(time_str) or not time_str:
        return None
    try:
        # KKTIX CSV 的時間格式
        dt_object = datetime.strptime(str(time_str), "%Y/%m/%d %H:%M:%S")
        return dt_object.timestamp()
    except (ValueError, TypeError):
        logging.warning(f"無法解析時間格式: '{time_str}'. 將其設為 None.")
        return None


def transform_to_raw_array(
    merged_df: pd.DataFrame,
    meta_mapping: dict[str, str],
    data_fields: list[str],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    """將原始 CSV 行轉換為符合 SCHEMA 的 data_array 以便載入 BigQuery"""
    logging.info("Starting transformation to data_array...")

    event_id = _generate_event_id(args.year, args.ticket_group)
    data_array = []

    for _, row in merged_df.iterrows():
        attendee_info = {}

        # 根據 meta_mapping 填入欄位
        for key, csv_col in meta_mapping.items():
            if csv_col not in row or pd.isna(row[csv_col]):
                logging.warning(
                    f"在訂單編號 {row.get('訂單編號')} 中找不到或為空的 meta 欄位: '{csv_col}' for key '{key}'"
                )
                attendee_info[key] = None
                continue

            val = row[csv_col]
            try:
                # 根據 key 進行資料類型轉換
                if key in ["id", "reg_no", "order_no"]:
                    attendee_info[key] = int(val)
                elif key == "is_paid":
                    # '已繳費' 表示 True
                    attendee_info[key] = str(val).strip() == "已繳費"
                elif key == "price":
                    attendee_info[key] = float(val)
                elif key == "updated_at":
                    attendee_info[key] = _to_unix_timestamp(val)
                else:
                    attendee_info[key] = val
            except (ValueError, TypeError) as e:
                logging.error(f"轉換欄位 '{csv_col}' (值: {val}) 時發生錯誤: {e}")
                attendee_info[key] = None

        # 處理固定的或不存在於 CSV 的欄位
        attendee_info.setdefault("ticket_id", None)  # ticket_id 不存在於 CSV
        attendee_info.setdefault("currency", "TWD")
        attendee_info.setdefault("kyc", {})
        attendee_info.setdefault("id_number", None)
        attendee_info.setdefault("slot", {})

        # 建立 'data' 欄位
        data_list = []
        for field_name in data_fields:
            if field_name in row and not pd.isna(row[field_name]):
                data_list.append([field_name, row[field_name]])
            else:
                logging.warning(
                    f"在訂單編號 {row.get('訂單編號')} 中找不到或為空的 data 欄位: '{field_name}'"
                )
                data_list.append([field_name, None])  # 即使找不到也保留欄位結構
        attendee_info["data"] = data_list

        raw_data = {
            "id": event_id,
            "name": args.event_name,
            "attendee_info": attendee_info,
        }
        data_array.append(raw_data)

    logging.info(f"Transformation complete. Generated {len(data_array)} records.")
    return data_array


def main():
    """主執行函式"""
    args = parse_args()
    try:
        validate_args(args)
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"參數驗證錯誤: {e}")
        return

    try:
        meta_mapping = load_meta_field_mapping(args.meta_field_mapping_file)
        data_field_names = load_data_field_names(args.data_field_names_file)

        attendees_df = read_csv_to_df(args.attendees_csv_file)
        orders_df = read_csv_to_df(args.orders_csv_file)

        merged_df = _merge_dataframes(attendees_df, orders_df)

        _validate_columns(merged_df, meta_mapping, data_field_names)

        event_raw_data_array = transform_to_raw_array(merged_df, meta_mapping, data_field_names, args)

        payload = []
        for event_raw_data in event_raw_data_array:
            sanitized_event_raw_data = _sanitize_payload(event_raw_data)
            payload.append(sanitized_event_raw_data)


        if not payload:
            logging.warning("沒有產生任何資料，流程即將結束。")
            return

        if args.is_dry_run:
            logging.info("Dry run 模式已啟用。以下是前 5 筆轉換後的 payload 預覽:")
            for record in payload[:5]:
                print(json.dumps(record, indent=2, ensure_ascii=False))

                # TODO: save to payload.json
                ...
        else:
            logging.info("開始載入資料到 BigQuery...")
            # 載入到 ODS 表
            logging.info("正在載入到 BigQuery: ODS table")
            load_to_bigquery_ods(
                payload,
                project_id=args.gcp_project_id,
                credential_file=str(args.gcp_credential_file),
            )
            # 載入到 DWD 表
            logging.info("正在載入到 BigQuery: DWD tables")
            load_to_bigquery_dwd(
                payload,
                project_id=args.gcp_project_id,
                credential_file=str(args.gcp_credential_file),
                ticket_group=args.ticket_group,
            )
            logging.info("資料成功載入 BigQuery。")

    except Exception as e:
        logging.error(f"執行過程中發生未預期的錯誤: {e}")


if __name__ == "__main__":
    main()
