from google import genai
from google.genai import types
import time
import os
import logging
import click

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Define headers and requirements outside the main function
ORG_HEADER = "organization,category"
ORG_REQUIREMENT = """
資料中的每筆記錄都是一個公司/組織名稱, 幫我對每筆記錄進行標記產業類別, 如果還是無法判斷請給 0 表示未知, 最終以 csv 的格式輸出

輸出的 csv 中**只包含資料列，不包含標頭**

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

請根據提供的公司/組織名稱列表，為每一項產生對應的 CSV 格式輸出 (公司名稱,類別編號)。例如，如果輸入是 "台積電\n台灣高鐵"，輸出應該類似這樣：
```
台積電,6
台灣高鐵,9
```
請確保輸出的**每一行**都嚴格遵循 "公司名稱,類別編號" 的格式，且只輸出這些資料行。
"""

JOB_HEADER = "job_title,category"
JOB_REQUIREMENT = """
檔案中的每筆記錄都是一個職位名稱, 幫我對每筆記錄進行標記職位類別, 如果還是無法判斷請給 0 表示未知, 最終以 csv 的方式輸出

輸出的 csv 中**只包含資料列，不包含標頭**

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

請根據提供的職位名稱列表，為每一項產生對應的 CSV 格式輸出 (職位名稱,類別編號)。例如，如果輸入是 "ai工程師\ngolang rd"，輸出應該類似這樣：

```
ai工程師,7
golang rd,1
```

請確保輸出的**每一行**都嚴格遵循 "職位名稱,類別編號" 的格式，且只輸出這些資料行。
"""


def get_task_config(task_type: str):
    """
    根據任務類型獲取對應的標頭和要求。

    Args:
        task_type: 任務類型 ('organization' 或 'job_title')。

    Returns:
        包含標頭和要求的元組 (header, requirement)。

    Raises:
        click.BadParameter: 如果任務類型不支援。
    """
    if task_type == "organization":
        return ORG_HEADER, ORG_REQUIREMENT
    elif task_type == "job_title":
        return JOB_HEADER, JOB_REQUIREMENT
    else:
        raise click.BadParameter("不支援的工作類型。請選擇 'organization' 或 'job_title'。")


def process_file(file_path: str, output_filename: str, model: str, max_output_tokens_on_model: int, batch_size: int, task_type: str):
    """
    讀取檔案，根據任務類型處理內容，並將結果寫入 CSV 檔案。

    Args:
        file_path: 輸入檔案的路徑。
        output_filename: 輸出 CSV 檔案的名稱。
        model: 要使用的 Gemini 模型名稱。
        max_output_tokens_on_model: 模型允許的最大輸出 token 數。
        batch_size: 每次處理的記錄數。
        task_type: 任務類型 ('organization' 或 'job_title')。
    """
    # Check for API key
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    if not GEMINI_API_KEY:
        logging.error("錯誤：請設置環境變數 GEMINI_API_KEY")
        # Exit the script if API key is not set
        exit(1) # Use exit(1) to indicate an error

    client = genai.Client(api_key=GEMINI_API_KEY)

    # Get task configuration
    header, requirement = get_task_config(task_type)

    # Read the input file
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            lines = file.readlines()
    except FileNotFoundError:
        logging.error(f"錯誤：找不到檔案 {file_path}")
        exit(1)
    except Exception as e:
        logging.error(f"讀取檔案時發生錯誤: {e}")
        exit(1)

    # Clean and filter empty lines
    lines = [line.strip().replace(",", "") for line in lines]
    all_results = []

    logging.info(f"共讀取到 {len(lines)} 筆記錄，將分成約 {len(lines) // batch_size + (1 if len(lines) % batch_size > 0 else 0)} 個批次處理...")

    # Process in batches
    for index, i in enumerate(range(0, len(lines), batch_size)):
        batch = lines[i:i + batch_size]
        batch_text = "\n".join(batch)
        batch_prompt = f"{requirement}\n---\n\n```\n{batch_text}\n```"

        logging.info(f"正在處理批次 {index + 1}...")

        # Check batch size against model limit (optional, but good practice)
        # Note: Token count is not a simple character count, this is a rough check
        if len(batch_text) > max_output_tokens_on_model * 0.8: # Use a buffer
             logging.warning(f"警告：批次 {index + 1} 的大小可能接近或超過模型的最大輸入限制 ({max_output_tokens_on_model} tokens)。")

        try:
            response = client.models.generate_content(
                model=model,
                contents=batch_prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_output_tokens_on_model,
                )
            )

            if response and response.text:
                response_text = response.text.strip()
                csv_lines = []
                # Split response into lines, handling potential markdown code blocks
                lines_from_response = response_text.split('\n')
                # Filter lines to only include valid CSV format (e.g., "name,category")
                for line in lines_from_response:
                    line = line.strip()
                    # Basic validation: check for comma and exactly two parts
                    if ',' in line and len(line.split(',', 1)) == 2: # Use split(',', 1) to handle commas within names
                        csv_lines.append(line)
                    elif line.startswith("```"):  # Ignore markdown block lines
                        pass
                    else:
                        logging.warning(f"警告：批次 {index + 1} 中發現格式不正確的行，已跳過: '{line}'")

                all_results.extend(csv_lines)
                logging.info(f"批次 {index + 1} 處理完成，新增 {len(csv_lines)} 筆結果.")
            else:
                logging.warning(f"警告：本次批次 {index + 1} 呼叫 API 未收到有效回應。")

        except Exception as e:
            logging.error(f"處理批次 {index + 1} 時發生錯誤: {e}")

        # Add a small delay between batches to avoid hitting rate limits
        time.sleep(1)

    logging.info("--- 所有批次處理完成 ---")

    # Prepare final output
    final_csv_output = header + "\n" + "\n".join(all_results)

    # Write results to output file
    try:
        with open(output_filename, "w", encoding='utf-8') as outfile:
            outfile.write(final_csv_output)
        logging.info(f"結果已儲存至 {output_filename}")
    except Exception as e:
        logging.error(f"儲存檔案時發生錯誤: {e}")
        exit(1)


@click.command()
@click.option('--file_path', type=click.Path(exists=True), required=True, help='輸入檔案的路徑，每行一個記錄。')
@click.option('--output_filename', type=str, required=True, help='輸出 CSV 檔案的名稱。')
@click.option('--task_type', type=click.Choice(['organization', 'job_title']), required=True, help='任務類型：可以是 "organization" 或 "job_title"。')
@click.option('--model', type=str, default="gemini-2.0-flash", help='要使用的 Gemini 模型名稱。')
@click.option('--max_output_tokens_on_model', type=int, default=8192, help='模型允許的最大輸出 token 數。')
@click.option('--batch_size', type=int, default=100, help='每次處理的記錄數。')
def main(file_path, output_filename, task_type, model, max_output_tokens_on_model, batch_size):
    """
    使用 Gemini 模型處理檔案中的組織或職位名稱並進行分類。
    """
    logging.info("程式開始執行...")
    process_file(file_path, output_filename, model, max_output_tokens_on_model, batch_size, task_type)
    logging.info("程式執行結束。")

# --- Entry Point ---
if __name__ == '__main__':
    main()
