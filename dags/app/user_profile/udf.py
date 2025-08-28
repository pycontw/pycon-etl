import json
import logging
import time
from collections.abc import Generator
from itertools import islice

import pandas as pd
import requests
from airflow.sdk import Variable
from google import genai
from google.api_core.exceptions import (
    BadRequest,
    DeadlineExceeded,
    Forbidden,
    GoogleAPICallError,
    NotFound,
    ServiceUnavailable,
)
from google.cloud import bigquery
from google.genai import types

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Define headers and requirements outside the main function
ORG_REQUIREMENT = """
資料中的每筆記錄都是一個公司/組織名稱, 幫我對每筆記錄進行標記產業類別, 如果還是無法判斷請給 0 表示未知, 最終以 json 的格式輸出

產業類別對照表:
```
0,未知
1,批發／零售／傳直銷業
2,文教相關業
3,大眾傳播相關業
4,旅遊／休閒／運動業
5,一般服務業
6,電子資訊／軟體／半導體相關業
7,一般製造業
8,農林漁牧水電資源業
9,運輸物流及倉儲
10,政治宗教及社福相關業
11,金融投顧及保險業
12,法律／會計／顧問／研發／設計業
13,建築營造及不動產相關業
14,醫療保健及環境衛生業
15,礦業及土石採取業
16,住宿／餐飲服務業
```

請根據提供的公司/組織名稱列表，為每一項產生對應的 json 格式輸出 (公司名稱,類別編號)。例如，如果輸入是 
[
    {"organization":"台積電", "organization":"台灣高鐵"}
]，輸出應該類似這樣：
```
[
    {"organization":"台積電", "category":"6"},
    {"organization":"台灣高鐵", "category":"9"}

]
```
請確保輸出的**每個項目**都嚴格遵循 "公司名稱,類別編號" 的格式，且只輸出這些資料行。
"""

JOB_REQUIREMENT = """
檔案中的每筆記錄都是一個職位名稱, 幫我對每筆記錄進行標記職位類別, 如果還是無法判斷請給 0 表示未知, 最終以 json 的方式輸出

職位類別對照表:
```
0. 其他 (Others): 一些較特殊的職位
1. 後端工程師 (Backend Engineer): 專注於伺服器端邏輯, 資料庫和 API 開發
2. 前端工程師 (Frontend Engineer): 專注於使用者介面和使用者體驗的開發
3. 全端工程師 (Full-Stack Engineer): 同時具備後端和前端開發能力
4. 行動應用開發工程師 (Mobile App Developer): 專注於 Android 和 iOS 平台的應用程式開發
5. 資料工程師 (Data Engineer): 負責資料的收集, 儲存, 處理和管道建設
6. 資料分析師/科學家 (Data Analyst/Scientist): 負責資料的分析, 洞察提取和模型建立
7. AI/機器學習工程師/研究員 (AI/ML Engineer/Researcher): 專注於人工智慧, 機器學習演算法的開發, 應用和研究
8. DevOps/SRE 工程師 (DevOps/SRE Engineer): 負責開發流程的自動化, 基礎設施的管理和系統的穩定性
9. 系統/網路工程師 (System/Network Engineer): 負責系統架設, 維護和網路管理
10. 嵌入式/韌體工程師 (Embedded/Firmware Engineer): 專注於硬體相關的軟體開發
11. 測試/品質保證工程師 (QA/Test Engineer): 負責軟體的測試, 品質保證和自動化測試
12. 技術主管/經理 (Technical Lead/Manager): 負責技術團隊的管理, 領導和技術方向的制定
13. 專案經理/產品經理 (Project Manager/Product Manager): 負責專案的規劃, 執行和產品的定義, 開發
14. 資料庫管理員 (DBA): 負責資料庫的設計, 維護和管理
15. 顧問/架構師 (Consultant/Architect): 提供技術諮詢, 解決方案設計和系統架構規劃
16. 研究人員/學者 (Researcher/Academic): 在學術界或研究機構進行研究工作
17. 業務/行銷/客戶成功 (Sales/Marketing/Customer Success): 負責業務拓展, 行銷推廣和客戶關係維護
18. 行政/管理 (Administrative/Management): 負責行政事務, 部門管理和組織運營
19. 設計師/藝術家 (Designer/Artist): 負責視覺設計, 使用者介面設計和藝術創作
20. 一般工程師: 難以歸類的工程師職位
21. 學生 (Intern): 實習生或學生職位
```

請根據提供的職位名稱列表，為每一項產生對應的 json 格式輸出 (職位名稱,類別編號)。例如，如果輸入是 
[
    {"job_title":"ai工程師", "job_title":"golang rd"}
]
，輸出應該類似這樣：
[
    {"job_title":"ai工程師", "category":"7"},
    {"job_title":"golang rd", "category":"1"}

]

請確保輸出的**每個項目**都嚴格遵循 "職位名稱,類別編號" 的格式，且只輸出這些資料行。
"""


BIGQUERY_PROJECT = "pycontw-225217"
BIGQUERY_DATASET = "dwd"
USER_PROFILE_TABLE = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.kktix_ticket_user_profile"
SOURCE_TABLES = [
    f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.kktix_ticket_individual_attendees",
    f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.kktix_ticket_reserved_attendees",
    f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.kktix_ticket_corporate_attendees",
]


def create_user_profile_table() -> None:
    client = bigquery.Client()
    try:
        client.get_table(USER_PROFILE_TABLE)  # 檢查 table 是否存在
    except NotFound:
        # table 不存在 → 建立空表
        schema = [
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("organization", "STRING"),
            bigquery.SchemaField("job_title", "STRING"),
            bigquery.SchemaField("country_or_region", "STRING"),
            bigquery.SchemaField("age_range", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("update_time", "TIMESTAMP"),
        ]
        table = bigquery.Table(USER_PROFILE_TABLE, schema=schema)
        client.create_table(table)
        logging.info(f"已建立 table: {USER_PROFILE_TABLE}")

    union_all_sql = " UNION ALL ".join(
        [
            f"""
        SELECT
            EXTRACT(YEAR FROM PARSE_DATE('%F', paid_date)) AS year,
            email,
            organization,
            job_title,
            country_or_region,
            age_range,
            gender
        FROM `{table}`
        """
            for table in SOURCE_TABLES
        ]
    )

    query = f"""
            MERGE {USER_PROFILE_TABLE} AS tgt
            USING (
                SELECT *
                FROM (
                    {union_all_sql}
                ) 
                WHERE EXTRACT(YEAR FROM PARSE_DATE('%F', paid_date)) IS NOT NULL
            ) AS src
            ON tgt.email = src.email AND tgt.year = EXTRACT(YEAR FROM PARSE_DATE('%F', paid_date))
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.organization = src.organization,
                    tgt.job_title = src.job_title,
                    tgt.country_or_region = src.country_or_region,
                    tgt.age_range = src.age_range,
                    tgt.gender = src.gender,
                    tgt.update_time = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (year, email, organization, job_title, country_or_region, age_range, gender, update_time)
                VALUES (
                    EXTRACT(YEAR FROM PARSE_DATE('%F', paid_date)),
                    src.email,
                    src.organization,
                    src.job_title,
                    src.country_or_region,
                    src.age_range,
                    src.gender,
                    CURRENT_TIMESTAMP()
                )
            """
    job = client.query(query)
    job.result()


def get_task_config(task_type: str) -> str:
    if task_type == "organization":
        return ORG_REQUIREMENT
    elif task_type == "job_title":
        return JOB_REQUIREMENT
    raise ValueError("不支援的工作類型。請選擇 'organization' 或 'job_title'。")


def get_gemini_api_key() -> str:
    GEMINI_API_KEY = Variable.get("GOOGLE_GEMINI_KEY")
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY 未設定")
    return GEMINI_API_KEY


def read_kktix_ticket_user_profile(task_type: str) -> list[str]:
    client = bigquery.Client()
    try:
        query = f"""
            SELECT DISTINCT {task_type}
            FROM `{USER_PROFILE_TABLE}`
            WHERE {task_type} IS NOT NULL
            AND update_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        """
        rows = client.query(query).result()  # 等 query 完成
        # 將每一 row 的 task_type 放到 list
        data_list = [row[task_type] for row in rows]
    except (NotFound, BadRequest, Forbidden, GoogleAPICallError) as err:
        if isinstance(err, NotFound):
            err_msg = "找不到資料表: "
        elif isinstance(err, BadRequest):
            err_msg = "SQL 語法錯誤或欄位不存在: "
        elif isinstance(err, BadRequest):
            err_msg = "BigQuery 權限不足: "
        elif isinstance(err, GoogleAPICallError):
            err_msg = "BigQuery API 呼叫錯誤: "  
        logging.exception(err_msg)
        raise
    return data_list


def write_result_to_bigquery(df: pd.DataFrame, task_type: str) -> None:
    client = bigquery.Client()
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.kktix_ticket_{task_type}_updates"  # 要建立的暫存表

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 將資料寫入舊表，如果表不存在則建立
        schema=[
            bigquery.SchemaField(task_type, "STRING"),
            bigquery.SchemaField("category", "INT64"),
        ],
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def chunk_data(data: list[str], batch_size: int) -> Generator[list[str], None, None]:
    it = iter(data)
    while batch := list(islice(it, batch_size)):
        yield batch


def get_gemini_response(
    model: str, max_output_tokens_on_model: int, prompt: str
) -> str | None:
    client = genai.Client(api_key=get_gemini_api_key())
    response = client.models.generate_content(
        model=model,
        contents=[prompt],
        config=types.GenerateContentConfig(
            max_output_tokens=max_output_tokens_on_model
        ),
    )
    return response.text


def process_table(
    model: str, max_output_tokens_on_model: int, batch_size: int, task_type: str
) -> None:
    # Read the input file
    data_list = read_kktix_ticket_user_profile(task_type)
    results_to_update = []

    logging.info(
        f"共讀取到 {len(data_list)} 筆記錄，將分成約 {len(data_list) // batch_size + (1 if len(data_list) % batch_size > 0 else 0)} 個批次處理..."
    )

    # Process in batches
    for index, chunk in enumerate(chunk_data(data_list, batch_size), start=1):
        batch_text = json.dumps(chunk, ensure_ascii=False)
        batch_prompt = f"{get_task_config(task_type)}\n資料列表:\n{batch_text}"

        logging.info(f"正在處理批次 {index}...")

        # Check batch size against model limit (optional, but good practice)
        # Note: Token count is not a simple character count, this is a rough check
        if len(batch_text) > max_output_tokens_on_model * 0.8:  # Use a buffer
            logging.warning(
                f"警告：批次 {index} 的大小可能接近或超過模型的最大輸入限制 ({max_output_tokens_on_model} tokens)。"
            )

        try:
            response = get_gemini_response(
                model, max_output_tokens_on_model, batch_prompt
            )
            if response:
                try:
                    clean_text = response.strip()
                    clean_text = clean_text[len("```json") :].strip()
                    clean_text = clean_text[:-3].strip()
                    result_json = json.loads(clean_text)
                except json.JSONDecodeError:
                    raise ValueError(f"批次 {index} JSON解析錯誤：{clean_text}")
            else:
                result_json = []
            results_to_update.extend(result_json)

        except (GoogleAPICallError, DeadlineExceeded, ServiceUnavailable):
            logging.exception(f"批次 {index} API 呼叫錯誤: ")
        except requests.exceptions.RequestException:
            logging.exception(f"批次 {index} 網路請求錯誤: ")
        except ValueError:
            logging.exception(f"批次 {index} 資料格式錯誤: ")
        except Exception:
            logging.exception(f"批次 {index} 未知錯誤")
        # Add a small delay between batches to avoid hitting rate limits
        time.sleep(1)

    df = pd.DataFrame(results_to_update)
    df["category"] = df["category"].astype(int)
    logging.info("--- 所有批次處理完成 ---")
    write_result_to_bigquery(df, task_type)
