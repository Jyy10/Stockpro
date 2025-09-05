# data_handler.py (v3.0 - with Intelligent Parsing)
import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta

def get_company_profiles(stock_codes):
    """获取公司的基本信息（行业、主营业务），优先使用cninfo，失败则尝试East Money"""
    profiles = {}
    for code in stock_codes:
        profile_data = {'industry': '查询失败', 'main_business': '查询失败'}
        try: # 优先尝试cninfo
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profile_data = {'industry': industry, 'main_business': main_business}
        except Exception:
            try: # 备用源East Money
                profile_df = ak.stock_individual_info_em(symbol=code)
                industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
                main_business = profile_df[profile_df['item'] == '主营业务']['value'].iloc[0]
                profile_data = {'industry': industry, 'main_business': main_business}
            except Exception:
                pass
        profiles[code] = profile_data
        time.sleep(0.3)
    return profiles

def extract_details_from_pdf(pdf_url, announcer_name, announcement_title):
    """从PDF链接中提取结构化信息，并区分角色"""
    details = {
        'transaction_type': '未知',
        'acquirer': '解析失败',
        'target': '解析失败',
        'transaction_price': '解析失败',
        'summary': '未能自动生成概要'
    }
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=20)
        response.raise_for_status()

        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:10]) # 增加读取页数
        
        clean_text = re.sub(r'\s+', '', text)

        # 1. 判断交易类型和角色
        if "要约收购" in announcement_title:
            details['transaction_type'] = "要约收购"
            details['target'] = announcer_name # 公告发布方是被收购方
            acquirer_match = re.search(r'收购人(?:为)?([\w（）\(\)]+?)(?:摘要|报告书)', clean_text)
            if acquirer_match:
                details['acquirer'] = acquirer_match.group(1)
        elif "购买资产" in announcement_title or "重大资产重组" in announcement_title:
            details['transaction_type'] = "资产收购"
            details['acquirer'] = announcer_name # 公告发布方是收购方
            target_match = re.search(r'(?:标的公司|交易标的|标的资产)为([\w（）\(\)]+)', clean_text)
            if target_match:
                details['target'] = target_match.group(1)
        else:
            details['transaction_type'] = "其他资本运作"
            details['acquirer'] = announcer_name
            target_match = re.search(r'(?:标的公司|交易标的|标的资产)为([\w（）\(\)]+)', clean_text)
            if target_match:
                details['target'] = target_match.group(1)

        # 2. 提取交易价格
        price_match = re.search(r'(?:交易作价|交易对价|交易价格)(?:为|约)?([\d\.,，]+)元', clean_text)
        if price_match:
            details['transaction_price'] = f"{price_match.group(1).replace(',', '')}元"

        # 3. 生成概要
        summary_parts = [details['acquirer']]
        if details['transaction_price'] != '解析失败':
            summary_parts.append(f"拟以 {details['transaction_price']} 的价格")
        
        summary_parts.append(details['transaction_type'])
        
        if details['target'] != '解析失败' and details['target'] != details['acquirer']:
             summary_parts.append(details['target'])
        
        details['summary'] = " ".join(summary_parts) + "。"
        
        return details
        
    except Exception as e:
        print(f"  ! PDF解析时发生错误: {e}")
        return details

def scrape_akshare(keywords, start_date, end_date):
    """使用 AkShare 抓取公告"""
    # ... 此函数保持不变 ...
    all_results = []
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results.append(daily_notices_df)
        except Exception as e:
            print(f"AkShare: 在 {date_str} 获取数据失败: {e}")
        current_date += timedelta(days=1)
        time.sleep(0.5)

    if not all_results:
        return pd.DataFrame()

    full_df = pd.concat(all_results, ignore_index=True)
    keyword_pattern = '|'.join(keywords)
    filtered_df = full_df[full_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()
    
    filtered_df.rename(columns={
        '公告日期': '公告日期', '股票代码': '股票代码',
        '股票简称': '公司名称', '公告标题': '公告标题',
        '公告链接': 'PDF链接'
    }, inplace=True)
    
    if '公告日期' in filtered_df.columns:
        filtered_df['公告日期'] = pd.to_datetime(filtered_df['公告日期']).dt.date

    return filtered_df[['公告日期', '股票代码', '公司名称', '公告标题', 'PDF链接']]

