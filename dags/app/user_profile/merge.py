import pandas as pd

def clean_string_column(series: pd.Series) -> pd.Series:
    """
    將 pandas Series 中的字串轉換為小寫，去除首尾空白，並移除逗號。

    Args:
    series: 要處理的 pandas Series。

    Returns:
    處理後的 pandas Series。
    """
    return series.str.lower().str.strip().str.replace(",", "", regex=False)

mapping_org = {
    0: "未知",
    1: "批發／零售／傳直銷業",
    2: "文教相關業",
    3: "大眾傳播相關業",
    4: "旅遊／休閒／運動業",
    5: "一般服務業",
    6: "電子資訊／軟體／半導體相關業",
    7: "一般製造業",
    8: "農林漁牧水電資源業",
    9: "運輸物流及倉儲",
    10: "政治宗教及社福相關業",
    11: "金融投顧及保險業",
    12: "法律／會計／顧問／研發／設計業",
    13: "建築營造及不動產相關業",
    14: "醫療保健及環境衛生業",
    15: "礦業及土石採取業",
    16: "住宿／餐飲服務業",
}

mapping_job = {
    0: "其他",
    1: "後端工程師",
    2: "前端工程師",
    3: "全端工程師",
    4: "行動應用開發工程師",
    5: "資料工程師",
    6: "資料分析師/科學家",
    7: "AI/機器學習工程師/研究員",
    8: "DevOps/SRE 工程師",
    9: "系統/網路工程師",
    10: "嵌入式/韌體工程師",
    11: "測試/品質保證工程師",
    12: "技術主管/經理",
    13: "專案經理/產品經理",
    14: "資料庫管理員",
    15: "顧問/架構師",
    16: "研究人員/學者",
    17: "業務/行銷/客戶成功",
    18: "行政/管理",
    19: "設計師/藝術家",
    20: "一般工程師",
    21: "學生",
}

audience_df = pd.read_csv('pycontw_audience.csv')
organization_category_df = pd.read_csv('output_organization.csv')
job_title_category_df = pd.read_csv('output_job_title.csv')

audience_df['job_title'] = clean_string_column(audience_df['job_title'])
audience_df['organization'] = clean_string_column(audience_df['organization'])
organization_category_df['organization'] = clean_string_column(organization_category_df['organization'])
job_title_category_df['job_title'] = clean_string_column(job_title_category_df['job_title'])

merged_df = pd.merge(audience_df, organization_category_df, on='organization', how='left')
merged_df.rename(columns={'category': 'org_category_id'}, inplace=True)

merged_df = pd.merge(merged_df, job_title_category_df, on='job_title', how='left')
merged_df.rename(columns={'category': 'job_category_id'}, inplace=True)

merged_df['org_category'] = merged_df['org_category_id'].map(mapping_org)
merged_df['job_category'] = merged_df['job_category_id'].map(mapping_job)

merged_df.drop(columns=['org_category_id', 'job_category_id'], inplace=True)

merged_df.to_csv('output_pycontw_audience.csv', index=False)

print("檔案合併完成，並已輸出至 output_pycontw_audience.csv")