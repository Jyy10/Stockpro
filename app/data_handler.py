# data_handler.py (v4.0 - Intelligent Parsing)
import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from thefuzz import process as fuzz_process

# --- 核心功能：数据抓取、解析与信息提取 ---

def scrape_akshare(keywords, start_date, end_date):
    """
    使用 akshare 从巨潮资讯网抓取指定时间范围和关键词的公告。
    """
    all_results_df_list = []
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    
    print(f"准备从 {start_date} 到 {end_date} 抓取数据...")

    for single_date in reversed(date_list):
        date_str = single_date.strftime('%Y%m%d')
        print(f"⏳ 正在检索数据源: {single_date.strftime('%Y-%m-%d')}...")
        try:
            # 该函数一次性返回当日所有市场的公告
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
        except Exception as e:
            print(f"  - AkShare: 在 {date_str} 获取数据时发生错误: {e}")
        time.sleep(0.5) # 尊重数据源，避免请求过快

    if not all_results_df_list:
        print("抓取完成：在指定时间范围内未找到任何公告。")
        return pd.DataFrame()

    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"抓取完成：在应用筛选前共找到 {len(all_results_df)} 条公告。")

    # 关键词筛选
    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()
    
    if filtered_df.empty:
        print("筛选完成：未匹配到相关公告。")
        return pd.DataFrame()
        
    print(f"筛选完成：匹配到 {len(filtered_df)} 条相关的公告。")
    return filtered_df

def _do_pdf_extraction(pdf_url, timeout=30):
    """
    下载PDF并提取文本的核心逻辑，增加了超时处理。
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(pdf_url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:3]) # 只读前3页以提高效率
        return re.sub(r'\s+', ' ', text) # 将多个空白符合并为一个空格
    except Exception as e:
        print(f"  ! PDF提取失败 ({pdf_url}): {e}")
        return ""

def extract_details_from_pdf(pdf_link):
    """
    从PDF文本中智能提取交易的关键信息。
    """
    text = _do_pdf_extraction(pdf_link)
    if not text:
        return "未能解析PDF", "待解析", "待解析", "待解析", "未能从PDF中提取有效信息。"

    # 1. 识别交易类型和参与方
    transaction_type = "资产购买" # 默认为主动购买
    acquirer = "公告方"
    target = "待解析"

    # 尝试寻找被收购的线索
    passive_keywords = ['要约收购报告书', '收购报告书', '权益变动的提示性公告']
    if any(keyword in text for keyword in passive_keywords):
        transaction_type = "公司被收购"
        acquirer = "待解析"
        target = "公告方"

    # 2. 提取交易标的
    target_match = re.search(r'(?:收购|购买|标的|目标)(?:资产|公司|股权)(?:为|：|的)([\w（）]+)', text)
    if target_match:
        target_name = target_match.group(1).strip()
        if target == "待解析": target = target_name

    # 3. 提取收购方
    acquirer_match = re.search(r'(?:收购人|收购方|购买方)(?:为|：)([\w（）]+)', text)
    if acquirer_match:
        acquirer_name = acquirer_match.group(1).strip()
        if acquirer == "待解析": acquirer = acquirer_name

    # 4. 提取交易价格
    price = "待解析"
    price_match = re.search(r'交易(?:价格|作价|对价)总额(?:为|约为|合计|：)\s*([\d,]+\.?\d*)\s*(?:万元|亿元)', text)
    if price_match:
        price = price_match.group(0).replace(" ", "") # 保留单位

    # 5. 生成概要
    summary = f"交易类型为“{transaction_type}”。"
    if acquirer != "待解析" and target != "待解析":
        summary += f" 本次交易中，{acquirer} 计划收购 {target}。"
    elif target != "待解析":
        summary += f" 交易标的为 {target}。"
    if price != "待解析":
        summary += f" {price}。"

    return transaction_type, acquirer, target, price, summary

def get_company_profiles(stock_codes):
    """
    获取公司的基本信息（行业、主营业务），增加了备用数据源。
    """
    profiles = {}
    for code in stock_codes:
        try:
            # 主数据源
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df.loc[profile_df['item'] == '行业', 'value'].iloc[0]
            main_business = profile_df.loc[profile_df['item'] == '主营业务范围', 'value'].iloc[0]
            profiles[code] = {'industry': industry, 'main_business': main_business}
            print(f"  - [cninfo] 成功获取 {code} 的档案。")
        except Exception as e_cninfo:
            print(f"  ! [cninfo] 获取 {code} 档案失败: {e_cninfo}。正在尝试备用源...")
            try:
                # 备用数据源
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em.loc[profile_df_em['item'] == '行业', 'value'].iloc[0]
                main_business = profile_df_em.loc[profile_df_em['item'] == '主营业务', 'value'].iloc[0]
                profiles[code] = {'industry': industry, 'main_business': main_business}
                print(f"  - [East Money] 成功从备用源获取 {code} 的档案。")
            except Exception as e_em:
                print(f"  ! [East Money] 备用源也获取 {code} 失败: {e_em}")
                profiles[code] = {'industry': '查询失败', 'main_business': '查询失败'}
        time.sleep(0.3)
    return profiles

