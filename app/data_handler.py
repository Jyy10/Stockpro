# data_handler.py (v1.9 - Resilient API Fix)

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
        # Use fuzzy matching to find the best column name
        match, score = fuzz_process.extractOne(keyword, available_columns)
        if score > highest_score:
            highest_score = score
            best_match = match
    if highest_score >= min_score:
        return best_match
    else:
        return None

def get_company_profiles(stock_codes):
    """
    获取公司的基本信息（行业、主营业务），增加了备用数据源以提高稳定性。
    首先尝试从巨潮资讯获取，失败则尝试从东方财富获取。
    """
    profiles = {}
    valid_codes = [code for code in stock_codes if code and code != 'N/A']
    if not valid_codes:
        return profiles

    print(f"准备为 {len(valid_codes)} 家公司获取基本信息...")
    for code in valid_codes:
        profile_data = {'industry': '查询失败', 'main_business': '查询失败'}
        try:
            # Primary Source: cninfo (more official)
            print(f"  - 尝试从 巨潮资讯 获取 {code} 的信息...")
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profile_data = {'industry': industry, 'main_business': main_business}
            print(f"  - [巨潮] 成功获取 {code} ({industry}) 的信息")
        except Exception as e_cninfo:
            print(f"  ! [巨潮] 获取 {code} 信息失败: {e_cninfo}. 尝试备用源...")
            try:
                # Fallback Source: East Money
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em[profile_df_em['item'] == '行业']['value'].iloc[0]
                main_business = profile_df_em[profile_df_em['item'] == '主营业务']['value'].iloc[0]
                profile_data = {'industry': industry, 'main_business': main_business}
                print(f"  - [东财] 成功获取 {code} ({industry}) 的信息")
            except Exception as e_em:
                print(f"  ! [东财] 备用源也获取 {code} 信息失败: {e_em}")
        
        profiles[code] = profile_data
        time.sleep(0.5)  # Add a polite delay between requests
    return profiles

def scrape_akshare(keywords, start_date, end_date, placeholder):
    """
    备用数据源：使用 AkShare 从巨潮资讯网获取公告，增加了对列名变化的模糊匹配容错处理。
    原有的 stock_notice_report 接口(新浪财经源)可能不稳定，切换到更可靠的 stock_notice_cninfo 接口。
    """
    all_results_df_list = []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y-%m-%d') # cninfo API requires YYYY-MM-DD format
        placeholder.info(f"⏳ 正在反向检索数据源: {date_str} ({i+1}/{total_days})...")
        try:
            # Switched to the more stable cninfo API
            daily_notices_df = ak.stock_notice_cninfo(date=date_str)
            if daily_notices_df is not None and not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
            else:
                print(f"AkShare-cninfo: {date_str} did not return any data.")
        except Exception as e:
            print(f"AkShare-cninfo: Failed to fetch data for {date_str}, error: {e}")
        time.sleep(0.5) # Increased delay to prevent being blocked

    if not all_results_df_list:
        return pd.DataFrame()
    
    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"AkShare-cninfo raw columns returned: {all_results_df.columns.tolist()}")

    title_col = find_best_column_name(all_results_df.columns, ['公告标题', '标题', 'title'])
    if not title_col:
        print("Warning: Could not find a recognizable 'title' column in AkShare data. Filtering is not possible.")
        return pd.DataFrame()

    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()

    if filtered_df.empty:
        return pd.DataFrame()

    # The cninfo API may not return stock code and name directly, so we add them if missing.
    if 'code' not in filtered_df.columns and '股票代码' not in filtered_df.columns:
        filtered_df['股票代码'] = filtered_df[title_col].str.extract(r'(\d{6})').fillna('N/A')
    
    if 'name' not in filtered_df.columns and '股票简称' not in filtered_df.columns:
        filtered_df['公司名称'] = 'N/A' # Hard to parse reliably, defaulting to N/A

    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['股票代码', '代码', 'code'],
        '公司名称': ['股票简称', '公司名称', '简称', 'name'],
        '公告标题': ['公告标题', '标题', 'title'],
        '公告日期': ['公告日期', '日期', 'display_time', 'time'],
        'PDF链接': ['公告链接', '链接', 'url']
    }
    
    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name:
            final_df[target_col] = filtered_df[best_col_name]
        else:
            final_df[target_col] = "N/A"
            print(f"Warning: Could not find any match for column '{target_col}'.")

    # Ensure the 'PDF链接' column contains a full URL
    pdf_link_col = 'PDF链接'
    if pdf_link_col in final_df.columns and not final_df[pdf_link_col].empty:
        # Check if the first non-null URL is not absolute
        first_link = final_df[pdf_link_col].dropna().iloc[0]
        if not str(first_link).startswith('http'):
             final_df[pdf_link_col] = 'http://static.cninfo.com.cn/' + final_df[pdf_link_col].astype(str)

    # Standardize date format
    date_col = '公告日期'
    if date_col in final_df.columns:
        final_df[date_col] = pd.to_datetime(final_df[date_col]).dt.strftime('%Y-%m-%d')

    final_df.drop_duplicates(inplace=True)
    if date_col in final_df.columns:
        final_df.sort_values(by=date_col, ascending=False, inplace=True)
        
    return final_df

# --- Other functions like _do_pdf_extraction, extract_details_from_pdf can remain the same ---

def get_stock_financial_data(stock_codes):
    # This function was not in the original file but is referenced in app.py. 
    # Adding a placeholder implementation.
    all_data = []
    for code in stock_codes:
        try:
            # Using a comprehensive API to get various metrics
            data_df = ak.stock_quote_single_pl_em(symbol=code)
            all_data.append(data_df)
        except Exception as e:
            print(f"Could not fetch financial data for {code}: {e}")
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)


def _do_pdf_extraction(pdf_url):
    try:
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:5])
        return text
    except Exception as e:
        print(f"Failed to extract text from PDF {pdf_url}: {e}")
        return ""

def extract_details_from_pdf(pdf_url):
    # This function can remain as is, its logic is internal text processing
    text = _do_pdf_extraction(pdf_url)
    if not text:
        return "信息提取失败", "信息提取失败", "信息提取失败"

    # Simplified extraction logic for demonstration
    target_company = re.search(r"标的公司\s*[:：为]?\s*([^\s，。(（]+)", text)
    transaction_price = re.search(r"交易作价\s*[:：为]?\s*([\d,.]+\s*[万元亿元]+)", text)
    shareholders = re.search(r"交易对方\s*[:：为]?\s*([^\s。(（]+)", text)

    return (
        target_company.group(1) if target_company else "未明确",
        transaction_price.group(1) if transaction_price else "未明确",
        shareholders.group(1) if shareholders else "未明确",
    )
