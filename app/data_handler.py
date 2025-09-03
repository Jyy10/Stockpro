# data_handler.py (v1.1 - 修正了akshare调用bug)

import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta

# 注意：这个文件里不应该有任何 st.xxxx() 的代码

def get_stock_financial_data(stock_codes):
    """获取上市公司的详细财务和行情数据"""
    # ... (此函数逻辑不变)
    data_df = pd.DataFrame()
    if not stock_codes:
        return data_df
    try:
        quote_df = ak.stock_zh_a_spot_em()
        quote_df = quote_df[quote_df['代码'].isin(stock_codes)]
        quote_df = quote_df[['代码', '总市值', '市盈率-动态']]
        quote_df.rename(columns={'代码': '股票代码'}, inplace=True)
        data_df = quote_df
    except Exception as e:
        print(f"获取行情数据时出错: {e}")
    try:
        temp_financials = []
        for code in stock_codes:
            try:
                df = ak.stock_financial_analysis_indicator(stock_code=code)
                latest_financials = df.iloc[0][['资产负债率(%)']]
                latest_financials['股票代码'] = code
                temp_financials.append(latest_financials)
            except:
                continue
        if temp_financials:
            financial_df = pd.DataFrame(temp_financials)
            if not data_df.empty:
                data_df = pd.merge(data_df, financial_df, on='股票代码', how='left')
            else:
                data_df = financial_df
    except Exception as e:
        print(f"获取财务指标时出错: {e}")
    try:
        industry_df = ak.stock_board_concept_name_em()
        temp_concepts = []
        for code in stock_codes:
            try:
                concepts = industry_df[industry_df['代码'] == code]['概念名称'].str.cat(sep=', ')
                temp_concepts.append({'股票代码': code, '行业题材': concepts})
            except:
                continue
        if temp_concepts:
            concept_df = pd.DataFrame(temp_concepts)
            if not data_df.empty:
                data_df = pd.merge(data_df, concept_df, on='股票代码', how='left')
            else:
                data_df = concept_df
    except Exception as e:
        print(f"获取行业题材时出错: {e}")
    return data_df

def extract_details_from_pdf(pdf_url):
    """(AI尽力而为) 尝试从PDF公告中提取关键信息。"""
    # ... (此函数逻辑不变)
    target_company = "未提取到"
    transaction_price = "未提取到"
    shareholders = "未提取到"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=20)
        response.raise_for_status()
        with BytesIO(response.content) as pdf_file:
            reader = PdfReader(pdf_file)
            text = ""
            num_pages_to_read = min(len(reader.pages), 10)
            for i in range(num_pages_to_read):
                page = reader.pages[i]
                if page.extract_text():
                    text += page.extract_text()
        text = text.replace("\n", "").replace(" ", "")
        match_target = re.search(r'(?:标的公司|标的资产|交易标的)为(\w+?公司)', text)
        if match_target:
            target_company = match_target.group(1)
        else:
            match_target = re.search(r'拟购买(\w+?公司)', text)
            if match_target:
                target_company = match_target.group(1)
        match_price_text = re.search(r'(?:交易作价|交易价格|交易对价)(?:暂定为|为|合计为)([\d,.\s]+(?:元|万元|亿元))', text)
        if match_price_text:
            transaction_price = match_price_text.group(1)
        match_shareholders = re.search(r'交易对方为([\w、，,（）()]+?)[，。]', text)
        if match_shareholders:
            shareholders = match_shareholders.group(1).strip('，').strip('。')
    except Exception:
        pass
    return target_company, transaction_price, shareholders


def scrape_cninfo(keywords, start_date, end_date):
    """主数据源：从巨潮资讯网抓取公告列表。出错时抛出异常。"""
    # ... (此函数逻辑不变)
    api_url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    headers = {'User-Agent': 'Mozilla/5.0'}
    str_start_date = start_date.strftime('%Y-%m-%d')
    str_end_date = end_date.strftime('%Y-%m-%d')
    all_announcements = []
    for keyword in keywords:
        page_num = 1
        while True:
            params = {
                'pageNum': page_num, 'pageSize': 30, 'column': 'sse,szse,hk',
                'tabName': 'fulltext', 'searchkey': keyword,
                'seDate': f'{str_start_date}~{str_end_date}', 'isHLtitle': 'true'
            }
            try:
                response = requests.post(api_url, headers=headers, data=params, timeout=20)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"请求巨潮资讯网失败: {e}")
                raise e
            announcements = data.get('announcements', [])
            if not announcements: break
            all_announcements.extend(announcements)
            if len(announcements) < 30: break
            page_num += 1
            time.sleep(0.3)
    if not all_announcements: return pd.DataFrame()
    df = pd.DataFrame(all_announcements)
    df = df[['secCode', 'secName', 'announcementTitle', 'announcementTime', 'adjunctUrl']]
    df.rename(columns={
        'secCode': '股票代码', 'secName': '公司名称', 'announcementTitle': '公告标题',
        'announcementTime': '公告日期', 'adjunctUrl': 'PDF路径'
    }, inplace=True)
    df['公告日期'] = pd.to_datetime(df['公告日期'], unit='ms').dt.strftime('%Y-%m-%d')
    df['PDF链接'] = 'http://static.cninfo.com.cn/' + df['PDF路径']
    df.drop_duplicates(subset=['股票代码', '公告标题', '公告日期'], inplace=True)
    df.sort_values(by='公告日期', ascending=False, inplace=True)
    return df.drop(columns=['PDF路径'])

# 【关键修正】重写 scrape_akshare 函数
def scrape_akshare(keywords, start_date, end_date):
    """备用数据源：使用 AkShare 从东方财富网抓取公告。"""
    all_results = []
    # 生成日期范围内的每一天
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]

    for single_date in date_list:
        date_str = single_date.strftime('%Y%m%d')
        print(f"AkShare: Fetching data for {date_str}")
        try:
            # AkShare 一次只获取一天的数据
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results.append(daily_notices_df)
        except Exception as e:
            print(f"AkShare: Failed to fetch data for {date_str}, error: {e}")
            continue # 如果某一天失败，继续下一天
        time.sleep(0.3) # 礼貌性延迟

    if not all_results:
        return pd.DataFrame()

    # 合并所有天的数据
    all_notices_df = pd.concat(all_results, ignore_index=True)
    
    # 筛选包含关键词的公告
    keyword_pattern = '|'.join(keywords)
    filtered_df = all_notices_df[all_notices_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()

    if filtered_df.empty:
        return pd.DataFrame()

    # 格式化DataFrame
    filtered_df.rename(columns={
        '股票代码': '股票代码',
        '股票简称': '公司名称',
        '公告标题': '公告标题',
        '公告日期': '公告日期',
        '公告链接': 'PDF链接'
    }, inplace=True)
    
    filtered_df['公告日期'] = pd.to_datetime(filtered_df['公告日期']).dt.strftime('%Y-%m-%d')
    final_df = filtered_df[['股票代码', '公司名称', '公告标题', '公告日期', 'PDF链接']]
    final_df.sort_values(by='公告日期', ascending=False, inplace=True)
    return final_df
