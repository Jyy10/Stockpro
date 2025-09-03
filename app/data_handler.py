# data_handler.py (v1.8 - 智能模糊匹配版)

import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from thefuzz import process as fuzz_process

def find_best_column_name(available_columns, target_keywords, min_score=85):
    """在一组可用的列名中，为一组目标关键词找到最佳匹配的列名。"""
    best_match = None
    highest_score = 0
    for keyword in target_keywords:
        match, score = fuzz_process.extractOne(keyword, available_columns)
        if score > highest_score:
            highest_score = score
            best_match = match
    if highest_score >= min_score:
        return best_match
    else:
        return None

def get_stock_financial_data(stock_codes):
    # ... (此函数保持原样，此处省略)
    pass

def _do_pdf_extraction(pdf_url):
    # ... (此函数保持原样，此处省略)
    pass

def extract_details_from_pdf(pdf_url):
    # ... (此函数保持原样，此处省略)
    pass

def scrape_cninfo(keywords, start_date, end_date):
    # ... (此函数保持原样，此处省略)
    pass

def scrape_akshare(keywords, start_date, end_date, placeholder):
    """备用数据源：使用 AkShare，并增加了对列名变化的模糊匹配容错处理。"""
    all_results_df_list = []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        placeholder.info(f"⏳ 正在反向检索备用源: {single_date.strftime('%Y-%m-%d')} ({i+1}/{total_days})...")
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
        except Exception as e:
            print(f"AkShare: Failed to fetch data for {date_str}, error: {e}")
        time.sleep(0.3)

    if not all_results_df_list:
        return pd.DataFrame()
    
    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"AkShare返回的原始列名: {all_results_df.columns.tolist()}")

    title_col = find_best_column_name(all_results_df.columns, ['公告标题', '标题'])
    if not title_col:
        print("警告: 在AkShare返回数据中找不到可识别的'标题'列，无法筛选。")
        return pd.DataFrame()

    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()

    if filtered_df.empty:
        return pd.DataFrame()

    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['股票代码', '代码'],
        '公司名称': ['股票简称', '公司名称', '简称'],
        '公告标题': ['公告标题', '标题'],
        '公告日期': ['公告日期', '日期'],
        'PDF链接': ['公告链接', '链接']
    }

    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name:
            final_df[target_col] = filtered_df[best_col_name]
        else:
            final_df[target_col] = "N/A"
            print(f"警告: 找不到列 '{target_col}' 的任何匹配项。")

    if '公告日期' in final_df.columns and pd.api.types.is_datetime64_any_dtype(final_df['公告日期']):
         final_df['公告日期'] = final_df['公告日期'].dt.strftime('%Y-%m-%d')

    final_df.drop_duplicates(inplace=True)
    if '公告日期' in final_df.columns:
        final_df.sort_values(by='公告日期', ascending=False, inplace=True)
        
    return final_df
