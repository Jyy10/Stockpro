# data_handler.py (v1.4 - 增加PDF解析超时保护)

import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
# 导入并发库，用于实现超时
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# ... (get_stock_financial_data, scrape_cninfo, scrape_akshare 函数保持不变，此处省略) ...
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

def scrape_akshare(keywords, start_date, end_date, placeholder):
    all_results = []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)
    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        placeholder.info(f"⏳ 正在反向检索备用源: {single_date.strftime('%Y-%m-%d')} ({i+1}/{total_days})... 已找到 {len(all_results)} 条公告。")
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                keyword_pattern = '|'.join(keywords)
                filtered_df = daily_notices_df[daily_notices_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()
                if not filtered_df.empty:
                    all_results.extend(filtered_df.to_dict('records'))
        except Exception as e:
            print(f"AkShare: Failed to fetch data for {date_str}, error: {e}")
            continue
        time.sleep(0.3)
    if not all_results: return pd.DataFrame()
    all_notices_df = pd.DataFrame(all_results)
    all_notices_df.rename(columns={'股票代码': '股票代码', '股票简称': '公司名称', '公告标题': '公告标题', '公告日期': '公告日期', '公告链接': 'PDF链接'}, inplace=True)
    all_notices_df['公告日期'] = pd.to_datetime(all_notices_df['公告日期']).dt.strftime('%Y-%m-%d')
    final_df = all_notices_df[['股票代码', '公司名称', '公告标题', '公告日期', 'PDF链接']]
    final_df.drop_duplicates(subset=['股票代码', '公告标题', '公告日期'], inplace=True)
    final_df.sort_values(by='公告日期', ascending=False, inplace=True)
    return final_df

# --- 【关键修改】重写 extract_details_from_pdf 函数 ---

def _do_pdf_extraction(pdf_url):
    """
    这是一个内部函数，包含了实际的、可能很慢的PDF解析逻辑。
    """
    target_company, transaction_price, shareholders = "未提取到", "未提取到", "未提取到"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(pdf_url, headers=headers, timeout=20)
    response.raise_for_status()
    with BytesIO(response.content) as pdf_file:
        reader = PdfReader(pdf_file)
        text = ""
        num_pages_to_read = min(len(reader.pages), 10) # 依然只读前10页
        for i in range(num_pages_to_read):
            page = reader.pages[i]
            if page.extract_text():
                text += page.extract_text()
    
    text = text.replace("\n", "").replace(" ", "")
    
    # 正则表达式部分保持不变
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
    """
    这是新的外部调用函数，它会管理内部函数的执行，并设置超时保护。
    """
    # 设置超时时间为 60 秒
    PDF_PARSE_TIMEOUT = 60

    # 使用线程池来执行耗时任务，并获取结果
    with ThreadPoolExecutor(max_workers=1) as executor:
        try:
            # 提交任务并等待结果，设置超时
            future = executor.submit(_do_pdf_extraction, pdf_url)
            result = future.result(timeout=PDF_PARSE_TIMEOUT)
            return result
        except TimeoutError:
            print(f"PDF解析超时: {pdf_url}")
            return "解析超时", "解析超时", "解析超时"
        except Exception as e:
            print(f"处理PDF时发生未知错误: {pdf_url}, 原因: {e}")
            return "解析出错", "解析出错", "解析出错"
