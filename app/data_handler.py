# data_handler.py (v1.5 - 增强了scrape_akshare的健壮性)

import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# ... (get_stock_financial_data, extract_details_from_pdf, scrape_cninfo 函数保持不变，此处省略) ...
def get_stock_financial_data(stock_codes):
    data_df = pd.DataFrame()
    if not stock_codes: return data_df
    try:
        quote_df = ak.stock_zh_a_spot_em()
        quote_df = quote_df[quote_df['代码'].isin(stock_codes)]; quote_df = quote_df[['代码', '总市值', '市盈率-动态']]; quote_df.rename(columns={'代码': '股票代码'}, inplace=True); data_df = quote_df
    except Exception as e: print(f"获取行情数据时出错: {e}")
    try:
        temp_financials = []
        for code in stock_codes:
            try:
                df = ak.stock_financial_analysis_indicator(stock_code=code); latest_financials = df.iloc[0][['资产负债率(%)']]; latest_financials['股票代码'] = code; temp_financials.append(latest_financials)
            except: continue
        if temp_financials:
            financial_df = pd.DataFrame(temp_financials)
            if not data_df.empty: data_df = pd.merge(data_df, financial_df, on='股票代码', how='left')
            else: data_df = financial_df
    except Exception as e: print(f"获取财务指标时出错: {e}")
    try:
        industry_df = ak.stock_board_concept_name_em()
        temp_concepts = []
        for code in stock_codes:
            try:
                concepts = industry_df[industry_df['代码'] == code]['概念名称'].str.cat(sep=', '); temp_concepts.append({'股票代码': code, '行业题材': concepts})
            except: continue
        if temp_concepts:
            concept_df = pd.DataFrame(temp_concepts)
            if not data_df.empty: data_df = pd.merge(data_df, concept_df, on='股票代码', how='left')
            else: data_df = concept_df
    except Exception as e: print(f"获取行业题材时出错: {e}")
    return data_df

def _do_pdf_extraction(pdf_url):
    target_company, transaction_price, shareholders = "未提取到", "未提取到", "未提取到"
    headers = {'User-Agent': 'Mozilla/5.0'}; response = requests.get(pdf_url, headers=headers, timeout=20); response.raise_for_status()
    with BytesIO(response.content) as pdf_file:
        reader = PdfReader(pdf_file); text = ""
        num_pages_to_read = min(len(reader.pages), 10)
        for i in range(num_pages_to_read):
            page = reader.pages[i]
            if page.extract_text(): text += page.extract_text()
    text = text.replace("\n", "").replace(" ", "")
    match_target = re.search(r'(?:标的公司|标的资产|交易标的)为(\w+?公司)', text)
    if match_target: target_company = match_target.group(1)
    else:
        match_target = re.search(r'拟购买(\w+?公司)', text)
        if match_target: target_company = match_target.group(1)
    match_price_text = re.search(r'(?:交易作价|交易价格|交易对价)(?:暂定为|为|合计为)([\d,.\s]+(?:元|万元|亿元))', text)
    if match_price_text: transaction_price = match_price_text.group(1)
    match_shareholders = re.search(r'交易对方为([\w、，,（）()]+?)[，。]', text)
    if match_shareholders: shareholders = match_shareholders.group(1).strip('，').strip('。')
    return target_company, transaction_price, shareholders

def extract_details_from_pdf(pdf_url):
    PDF_PARSE_TIMEOUT = 60
    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            future = executor.submit(_do_pdf_extraction, pdf_url); result = future.result(timeout=PDF_PARSE_TIMEOUT); return result
        except TimeoutError:
            print(f"PDF解析超时: {pdf_url}"); return "解析超时", "解析超时", "解析超时"
        except Exception as e:
            print(f"处理PDF时发生未知错误: {pdf_url}, 原因: {e}"); return "解析出错", "解析出错", "解析出错"

def scrape_cninfo(keywords, start_date, end_date):
    api_url, headers = "http://www.cninfo.com.cn/new/hisAnnouncement/query", {'User-Agent': 'Mozilla/5.0'}
    str_start_date, str_end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
    all_announcements = []
    for keyword in keywords:
        page_num = 1
        while True:
            params = {'pageNum': page_num, 'pageSize': 30, 'column': 'sse,szse,hk', 'tabName': 'fulltext', 'searchkey': keyword, 'seDate': f'{str_start_date}~{str_end_date}', 'isHLtitle': 'true'}
            try:
                response = requests.post(api_url, headers=headers, data=params, timeout=20); response.raise_for_status(); data = response.json()
            except Exception as e: print(f"请求巨潮资讯网失败: {e}"); raise e
            announcements = data.get('announcements', [])
            if not announcements: break
            all_announcements.extend(announcements)
            if len(announcements) < 30: break
            page_num += 1; time.sleep(0.3)
    if not all_announcements: return pd.DataFrame()
    df = pd.DataFrame(all_announcements); df = df[['secCode', 'secName', 'announcementTitle', 'announcementTime', 'adjunctUrl']]
    df.rename(columns={'secCode': '股票代码', 'secName': '公司名称', 'announcementTitle': '公告标题', 'announcementTime': '公告日期', 'adjunctUrl': 'PDF路径'}, inplace=True)
    df['公告日期'] = pd.to_datetime(df['公告日期'], unit='ms').dt.strftime('%Y-%m-%d'); df['PDF链接'] = 'http://static.cninfo.com.cn/' + df['PDF路径']
    df.drop_duplicates(subset=['股票代码', '公告标题', '公告日期'], inplace=True); df.sort_values(by='公告日期', ascending=False, inplace=True)
    return df.drop(columns=['PDF路径'])


# --- 【关键修改】重写 scrape_akshare 函数，使其更健壮 ---
def scrape_akshare(keywords, start_date, end_date, placeholder):
    """备用数据源：使用 AkShare，并增加了对列名变化的容错处理。"""
    all_results_df = pd.DataFrame()
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        placeholder.info(f"⏳ 正在反向检索备用源: {single_date.strftime('%Y-%m-%d')} ({i+1}/{total_days})... 已找到 {len(all_results_df)} 条公告。")
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                # 调试打印：看看原始列名是什么
                print(f"Date {date_str} original columns: {daily_notices_df.columns.tolist()}")
                all_results_df = pd.concat([all_results_df, daily_notices_df], ignore_index=True)
        except Exception as e:
            print(f"AkShare: Failed to fetch data for {date_str}, error: {e}")
            continue
        time.sleep(0.3)

    if all_results_df.empty:
        return pd.DataFrame()

    keyword_pattern = '|'.join(keywords)
    # 确保'公告标题'列存在
    if '公告标题' in all_results_df.columns:
        filtered_df = all_results_df[all_results_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()
    else:
        # 如果连标题列都没有，就没办法筛选了，返回空
        return pd.DataFrame()

    if filtered_df.empty:
        return pd.DataFrame()

    # --- 健壮的列处理 ---
    # 定义一个映射关系，key是我们的目标列名，value是可能的原始列名
    column_mapping = {
        '股票代码': ['股票代码'],
        '公司名称': ['股票简称', '公司名称'],
        '公告标题': ['公告标题'],
        '公告日期': ['公告日期'],
        'PDF链接': ['公告链接']
    }
    
    final_df = pd.DataFrame()
    for target_col, source_cols in column_mapping.items():
        for source_col in source_cols:
            if source_col in filtered_df.columns:
                final_df[target_col] = filtered_df[source_col]
                break # 找到一个就跳出内层循环
    
    # 确保所有目标列都存在，如果不存在则填充N/A
    for target_col in column_mapping.keys():
        if target_col not in final_df.columns:
            final_df[target_col] = "N/A"

    # 格式化日期
    if '公告日期' in final_df.columns:
        final_df['公告日期'] = pd.to_datetime(final_df['公告日期']).dt.strftime('%Y-%m-%d')

    final_df.drop_duplicates(inplace=True)
    final_df.sort_values(by='公告日期', ascending=False, inplace=True)
    return final_df
