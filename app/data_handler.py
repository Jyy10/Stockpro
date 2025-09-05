# data_handler.py (v4.2 - Robust Parsing)
import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta

# --- 核心功能：数据抓取、解析与信息提取 ---

def scrape_akshare_precisely(core_keywords, modifier_keywords, start_date, end_date):
    """
    使用 akshare 并进行精准的组合关键词筛选。
    标题必须同时包含一个核心词和一个修饰词。
    """
    all_results_df_list = []
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    for single_date in date_list:
        date_str = single_date.strftime('%Y%m%d')
        try:
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
        except Exception as e:
            print(f"  - AkShare: 在 {date_str} 获取数据时发生错误: {e}")
        time.sleep(0.5)

    if not all_results_df_list:
        return pd.DataFrame()

    all_results_df = pd.concat(all_results_df_list, ignore_index=True)

    # --- 精准关键词筛选逻辑 ---
    core_pattern = '|'.join(core_keywords)
    modifier_pattern = '|'.join(modifier_keywords)
    
    contains_core = all_results_df['公告标题'].str.contains(core_pattern, regex=True, na=False)
    contains_modifier = all_results_df['公告标题'].str.contains(modifier_pattern, regex=True, na=False)
    
    filtered_df = all_results_df[contains_core & contains_modifier].copy()
    
    return filtered_df

def _do_pdf_extraction(pdf_url, timeout=30):
    """
    下载PDF并提取文本的核心逻辑，增加了超时处理。
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(pdf_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:3] if page.extract_text())
        return re.sub(r'\s+', ' ', text)
    except Exception as e:
        print(f"  ! PDF提取失败 ({pdf_url}): {e}")
        return ""

def extract_details_from_pdf(pdf_link):
    """
    从PDF文本中智能提取交易的关键信息 (v4.2 - 更稳健的解析)
    """
    text = _do_pdf_extraction(pdf_link)
    if not text:
        return ("信息提取失败", "待解析", "待解析", "待解析", "未能成功解析PDF文件，可能文件已损坏或无法访问。")

    # 初始化返回值
    transaction_type = "资产交易"
    acquirer = "待解析"
    target = "待解析"
    price = "待解析"

    # --- 1. 识别交易类型和主要角色 ---
    if any(k in text for k in ['收购报告书', '权益变动']):
        transaction_type = "公司被收购"
        acquirer_match = re.search(r'(?:收购人|信息披露义务人)(?:名称)?(?:为|：)\s*([\w（）]+)', text)
        if acquirer_match:
            acquirer = acquirer_match.group(1).strip()
        target = "公告方"
    else:
        transaction_type = "资产购买/出售"
        acquirer = "公告方"
        target_match = re.search(r'(?:标的|目标)(?:资产|公司|股权)(?:的名称为|为|：)\s*“?([^”]+)”?', text)
        if target_match:
            target = target_match.group(1).strip()
        else:
             counterparty_match = re.search(r'交易对方(?:为|：)\s*([\w（）]+)', text)
             if counterparty_match:
                 target = counterparty_match.group(1).strip()

    # --- 2. 提取交易价格 ---
    price_search = re.search(
        r'(?:交易(?:价格|作价|对价)(?:总额|合计)?|本次交易拟投入|现金对价)\s*(?:为|约为|合计|：)\s*([\d,]+\.?\d*)\s*(?:万元|亿元)', 
        text
    )
    if price_search:
        value = price_search.group(1)
        unit = "万元" if "万元" in price_search.group(0) else "亿元"
        price = f"{value} {unit}"

    # --- 3. 生成更智能的概要 ---
    if acquirer == "待解析" and target == "待解析":
        summary = "无法明确解析交易双方，建议阅读原文以获取详细信息。"
    elif transaction_type == "公司被收购":
        summary = f"本次交易为公司被收购。收购方初步判断为“{acquirer}”。"
    elif transaction_type == "资产购买/出售":
        summary = f"公告方计划进行资产交易。交易标的初步判断为“{target}”。"
    else:
        summary = "这是一次常规资产交易。"
        
    if price != "待解析":
        summary += f" 涉及的交易对价约为 {price}。"

    return transaction_type, acquirer, target, price, summary

def get_company_profiles(stock_codes):
    """
    获取公司的基本信息（行业、主营业务），增加了备用数据源。
    """
    profiles = {}
    for code in stock_codes:
        try:
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df.loc[profile_df['item'] == '行业', 'value'].iloc[0]
            main_business = profile_df.loc[profile_df['item'] == '主营业务范围', 'value'].iloc[0]
            profiles[code] = {'industry': industry, 'main_business': main_business}
            print(f"  - [cninfo] 成功获取 {code} 的档案。")
        except Exception:
            try:
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em.loc[profile_df_em['item'] == '行业', 'value'].iloc[0]
                main_business = profile_df_em.loc[profile_df_em['item'] == '主营业务', 'value'].iloc[0]
                profiles[code] = {'industry': industry, 'main_business': main_business}
                print(f"  - [East Money] 成功从备用源获取 {code} 的档案。")
            except Exception:
                profiles[code] = {'industry': '查询失败', 'main_business': '查询失败'}
        time.sleep(0.3)
    return profiles
