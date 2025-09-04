# data_handler.py (v2.4 - Alternative Function Fix)

import requests
import pandas as pd
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from thefuzz import process as fuzz_process
import akshare as ak

def find_best_column_name(available_columns, target_keywords, min_score=85):
    """根据模糊匹配从可用列名中找到最佳匹配项"""
    best_match = None
    highest_score = 0
    for keyword in target_keywords:
        try:
            match, score = fuzz_process.extractOne(keyword, available_columns)
            if score > highest_score:
                highest_score = score
                best_match = match
        except Exception:
            continue
    if highest_score >= min_score:
        return best_match
    else:
        return None

def get_company_profiles(stock_codes):
    """获取公司档案（行业、主营业务），包含备用数据源以确保稳定性"""
    profiles = {}
    valid_codes = [code for code in stock_codes if code and code != 'N/A']
    if not valid_codes: return profiles

    print(f"准备为 {len(valid_codes)} 家公司获取档案...")
    for code in valid_codes:
        profile_data = {'industry': '查询失败', 'main_business': '查询失败'}
        try:
            print(f"  - 尝试从 cninfo 获取 {code} 的信息...")
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profile_data = {'industry': industry, 'main_business': main_business}
            print(f"  - [cninfo] 成功获取 {code} ({industry}) 的档案")
        except Exception as e_cninfo:
            print(f"  ! [cninfo] 获取 {code} 档案失败: {e_cninfo}。正在尝试备用源...")
            try:
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em[profile_df_em['item'] == '行业']['value'].iloc[0]
                main_business = profile_df_em[profile_df_em['item'] == '主营业务']['value'].iloc[0]
                profile_data = {'industry': industry, 'main_business': main_business}
                print(f"  - [East Money] 成功获取 {code} ({industry}) 的档案")
            except Exception as e_em:
                print(f"  ! [East Money] 备用源也获取 {code} 失败: {e_em}")
        
        profiles[code] = profile_data
        time.sleep(0.5)
    return profiles

def scrape_akshare(keywords, start_date, end_date, placeholder):
    """使用 ak.stock_notice_report 函数从巨潮资讯网抓取公告"""
    all_results_df_list = []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)
    markets = {"sh": "上海证券交易所", "sz": "深圳证券交易所"}

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        placeholder.info(f"⏳ 正在反向检索数据源: {date_str} ({i+1}/{total_days})...")
        for market_code, market_name in markets.items():
            try:
                # --- FIX: Use a more stable, alternative function ---
                daily_notices_df = ak.stock_notice_report(market=market_name, date=date_str)
                if daily_notices_df is not None and not daily_notices_df.empty:
                    all_results_df_list.append(daily_notices_df)
            except Exception as e:
                print(f"AkShare-{market_code}: 在为 {date_str} 获取数据时发生错误: {e}")
            time.sleep(0.5)

    if not all_results_df_list: return pd.DataFrame()
    
    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"抓取完成：在应用筛选前共找到 {len(all_results_df)} 条公告。")

    title_col = find_best_column_name(all_results_df.columns, ['公告标题', '标题'])
    if not title_col:
        print("警告：无法识别出'标题'列，无法进行关键词筛选。"); return pd.DataFrame()

    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()
    print(f"筛选完成：匹配到 {len(filtered_df)} 条相关的公告。")

    if filtered_df.empty: return pd.DataFrame()

    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['股票代码', '代码'], '公司名称': ['股票简称', '公司名称'],
        '公告标题': ['公告标题', '标题'], '公告日期': ['公告日期', '日期'],
        'PDF链接': ['公告链接', '链接']
    }
    
    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name: final_df[target_col] = filtered_df[best_col_name]
        else: final_df[target_col] = "N/A"; print(f"警告：找不到列 '{target_col}' 的任何匹配项。")

    pdf_link_col = 'PDF链接'
    if pdf_link_col in final_df.columns and not final_df[pdf_link_col].empty:
        base_url = 'http://static.cninfo.com.cn'
        final_df[pdf_link_col] = final_df[pdf_link_col].apply(
            lambda url: base_url + url if isinstance(url, str) and url.startswith('/') else url
        )

    date_col = '公告日期'
    if date_col in final_df.columns:
        final_df[date_col] = pd.to_datetime(final_df[date_col]).dt.strftime('%Y-%m-%d')

    final_df.drop_duplicates(subset=['PDF链接'], inplace=True)
    if date_col in final_df.columns:
        final_df.sort_values(by=date_col, ascending=False, inplace=True)
        
    return final_df

def get_stock_financial_data(stock_codes):
    """为 app.py 提供财务数据获取功能"""
    all_data = []
    for code in stock_codes:
        try:
            data_df = ak.stock_quote_single_pl_em(symbol=code)
            all_data.append(data_df)
        except Exception as e:
            print(f"无法获取 {code} 的财务数据: {e}")
    if not all_data: return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def _do_pdf_extraction(pdf_url):
    """下载PDF并提取前几页的文本"""
    try:
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:5] if page.extract_text())
        return text
    except Exception as e:
        print(f"从PDF {pdf_url} 提取文本失败: {e}"); return ""

def extract_details_from_pdf(pdf_url):
    """使用正则表达式从PDF文本中提取关键交易细节"""
    text = _do_pdf_extraction(pdf_url)
    if not text: return "提取失败", "提取失败", "提取失败"

    target_company = re.search(r"标的公司\s*[:：为]?\s*([^\s，。(（]+)", text)
    transaction_price = re.search(r"交易作价\s*[:：为]?\s*([\d,.]+\s*[万元亿元]+)", text)
    shareholders = re.search(r"交易对方\s*[:：为]?\s*([^\s。(（]+)", text)

    return (
        target_company.group(1).strip() if target_company else "未找到",
        transaction_price.group(1).strip() if transaction_price else "未找到",
        shareholders.group(1).strip() if shareholders else "未找到",
    )

