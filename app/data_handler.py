# data_handler.py (v3.0 - Enhanced Logging)
import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from thefuzz import process as fuzz_process

# -------------------- 辅助函数 --------------------

def find_best_column_name(available_columns, target_keywords, min_score=80):
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
    return None

# -------------------- 核心数据抓取与处理函数 --------------------

def scrape_akshare(keywords, start_date, end_date):
    """
    使用 AkShare 批量获取公告，并根据关键词筛选。
    【诊断增强版】: 增加原始标题和匹配标题的日志打印。
    """
    all_results_df_list = []
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    total_days = len(date_list)

    print(f"AkShare: 准备从 {start_date} 到 {end_date} 检索 {total_days} 天的数据...")

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        print(f"⏳ 正在检索数据源: {single_date.strftime('%Y-%m-%d')} ({i+1}/{total_days})...")
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
        except Exception as e:
            print(f"  ! AkShare: 在为 {date_str} 获取数据时发生错误: {e}")
        time.sleep(0.5)

    if not all_results_df_list:
        print("AkShare: 在指定时间范围内未返回任何公告数据。")
        return pd.DataFrame()

    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"抓取完成：在应用筛选前共找到 {len(all_results_df)} 条公告。")

    title_col = find_best_column_name(all_results_df.columns, ['title', '公告标题'])
    if not title_col:
        print("警告: 在AkShare返回数据中找不到可识别的'标题'列，无法进行筛选。")
        return pd.DataFrame()
        
    # --- 【新增】打印原始标题 ---
    print("\n" + "-"*20 + " 原始公告标题预览 (最多50条) " + "-"*20)
    for title in all_results_df[title_col].head(50):
        print(f"  - {title}")
    print("-"*65)

    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()
    
    if filtered_df.empty:
        print("筛选完成：未能匹配到任何相关的公告。")
        return pd.DataFrame()

    # --- 列名标准化 ---
    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['code', '股票代码'], '公司名称': ['stock_name', '股票简称'],
        '公告标题': ['title', '公告标题'], '公告日期': ['notice_date', '公告日期'],
        'PDF链接': ['url', '链接']
    }
    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name:
            final_df[target_col] = filtered_df[best_col_name]
        else:
            final_df[target_col] = "N/A"
    
    if '公告日期' in final_df.columns:
        final_df['公告日期'] = pd.to_datetime(final_df['公告日期']).dt.date

    final_df.drop_duplicates(subset=['公告日期', '公告标题'], inplace=True)
    final_df.sort_values(by='公告日期', ascending=False, inplace=True)
        
    return final_df
