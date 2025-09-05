# data_handler.py (v5.2 - AI Diagnostics)
import requests
import pandas as pd
import akshare as ak
import re
import json
import os
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from thefuzz import process as fuzz_process

# --- 辅助函数 ---
def find_best_column_name(available_columns, target_keywords, min_score=80):
    """
    在一组可用的列名中，为一组目标关键词找到最佳匹配的列名。
    """
    best_match = None
    highest_score = 0
    for keyword in target_keywords:
        match, score = fuzz_process.extractOne(keyword, available_columns)
        if score > highest_score:
            highest_score = score
            best_match = match
    if highest_score >= min_score:
        return best_match
    return None

# --- 核心功能：数据抓取、解析与信息提取 ---
def scrape_and_normalize_akshare(core_keywords, modifier_keywords, start_date, end_date):
    """
    抓取、模糊匹配列名、标准化并使用精准关键词筛选。
    """
    all_raw_dfs = []
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    for single_date in date_list:
        date_str = single_date.strftime('%Y%m%d')
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_raw_dfs.append(daily_notices_df)
        except Exception as e:
            print(f"  - AkShare: 在 {date_str} 获取数据时发生错误: {e}")
        time.sleep(0.5)

    if not all_raw_dfs:
        return pd.DataFrame()

    raw_df = pd.concat(all_raw_dfs, ignore_index=True)

    available_cols = raw_df.columns.tolist()
    column_mapping = {
        '股票代码': find_best_column_name(available_cols, ['代码', '股票代码']),
        '公司名称': find_best_column_name(available_cols, ['简称', '公司名称', '股票简称']),
        '公告标题': find_best_column_name(available_cols, ['标题', '公告标题']),
        '公告日期': find_best_column_name(available_cols, ['日期', '公告日期']),
        'PDF链接': find_best_column_name(available_cols, ['链接', '公告链接', 'url']),
    }

    normalized_df = pd.DataFrame()
    for std_name, found_name in column_mapping.items():
        if found_name:
            normalized_df[std_name] = raw_df[found_name]
        else:
            normalized_df[std_name] = None
    
    if '公告标题' in normalized_df.columns and not normalized_df['公告标题'].isnull().all():
        core_pattern = '|'.join(core_keywords)
        modifier_pattern = '|'.join(modifier_keywords)
        
        contains_core = normalized_df['公告标题'].str.contains(core_pattern, regex=True, na=False)
        contains_modifier = normalized_df['公告标题'].str.contains(modifier_pattern, regex=True, na=False)
        
        filtered_df = normalized_df[contains_core & contains_modifier].copy()
        return filtered_df
    else:
        return pd.DataFrame()

def _do_pdf_extraction(pdf_url, timeout=30):
    """下载PDF并提取前3页文本的核心逻辑。"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for i, page in enumerate(reader.pages) if i < 3 and page.extract_text())
        return re.sub(r'\s+', ' ', text)
    except Exception as e:
        print(f"  ! PDF提取失败 ({pdf_url}): {e}")
        return ""

async def extract_details_from_pdf(pdf_link):
    """
    【AI版本】从PDF文本中智能提取交易的关键信息。
    """
    text = _do_pdf_extraction(pdf_link)
    if not text:
        return ("信息提取失败", "待解析", "待解析", "待解析", "未能成功解析PDF文件。")

    # --- 【核心改进】增加API密钥的诊断检查 ---
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("  - \033[91mFATAL\033[0m: 环境变量 'GEMINI_API_KEY' 未设置。AI解析功能已禁用。")
        return ("AI配置缺失", "待解析", "待解析", "待解析", "由于缺少API密钥，AI解析功能无法使用。")

    api_url = f"[https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key=](https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-05-20:generateContent?key=){api_key}"
    
    system_prompt = """
    你是一位专业的金融分析师。你的任务是阅读一份上市公司公告的文本，然后以JSON格式返回以下关键信息：
    - transaction_type: 交易类型（例如："公司被收购", "资产购买", "资产出售"）
    - acquirer: 收购方的公司全名。如果公告方是收购方，请填写“公告方”。
    - target: 标的方（被收购的公司或资产）的全名。
    - transaction_price: 交易对价，包含数字和单位（例如："5.2亿元"）。
    - summary: 用一句话总结这次交易的核心内容。
    如果某项信息在文本中没有明确提及，请返回 "信息未披露"。
    """
    
    payload = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"parts": [{"text": text[:30000]}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT", "properties": {
                    "transaction_type": {"type": "STRING"}, "acquirer": {"type": "STRING"},
                    "target": {"type": "STRING"}, "transaction_price": {"type": "STRING"},
                    "summary": {"type": "STRING"}
                }
            }
        }
    }
    
    try:
        response = requests.post(api_url, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        result = response.json()
        
        content_part = result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', '{}')
        parsed_json = json.loads(content_part)

        return (
            parsed_json.get("transaction_type", "解析失败"),
            parsed_json.get("acquirer", "解析失败"),
            parsed_json.get("target", "解析失败"),
            parsed_json.get("transaction_price", "解析失败"),
            parsed_json.get("summary", "AI未能生成概要。")
        )
    except Exception as e:
        print(f"  ! 调用AI解析时发生错误: {e}")
        return ("AI调用失败", "待解析", "待解析", "待解析", "调用AI解析时发生网络或API错误。")

def get_company_profiles(stock_codes):
    """获取公司的基本信息（行业、主营业务），增加了备用数据源。"""
    profiles = {}
    for code in stock_codes:
        try:
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df.loc[profile_df['item'] == '行业', 'value'].iloc[0]
            main_business = profile_df.loc[profile_df['item'] == '主营业务范围', 'value'].iloc[0]
            profiles[code] = {'industry': industry, 'main_business': main_business}
        except Exception:
            try:
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em.loc[profile_df_em['item'] == '行业', 'value'].iloc[0]
                main_business = profile_df_em.loc[profile_df_em['item'] == '主营业务', 'value'].iloc[0]
                profiles[code] = {'industry': industry, 'main_business': main_business}
            except Exception:
                profiles[code] = {'industry': '查询失败', 'main_business': '查询失败'}
        time.sleep(0.3)
    return profiles
