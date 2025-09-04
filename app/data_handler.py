# data_handler.py (v2.9 - Final Production Version)
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
        # extractOne 返回 (匹配项, 分数)
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
    使用 AkShare 从巨潮资讯网批量获取指定时间范围内的所有公告，并根据关键词筛选。
    【最终生产版】: 移除了不再需要的 'placeholder' 参数。
    """
    all_results_df_list = []
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    total_days = len(date_list)

    print(f"AkShare: 准备从 {start_date} 到 {end_date} 检索 {total_days} 天的数据...")

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d')
        print(f"⏳ 正在检索数据源: {single_date.strftime('%Y-%m-%d')} ({i+1}/{total_days})...")
        try:
            # stock_notice_report 设计为一次性获取当天所有市场的公告
            daily_notices_df = ak.stock_notice_report(date=date_str)
            if not daily_notices_df.empty:
                all_results_df_list.append(daily_notices_df)
        except Exception as e:
            print(f"  ! AkShare: 在为 {date_str} 获取数据时发生错误: {e}")
        time.sleep(0.5) # 礼貌性延迟

    if not all_results_df_list:
        print("AkShare: 在指定时间范围内未返回任何公告数据。")
        return pd.DataFrame()

    # --- 数据整合与清洗 ---
    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    print(f"抓取完成：在应用筛选前共找到 {len(all_results_df)} 条公告。")

    # --- 关键词筛选 ---
    title_col = find_best_column_name(all_results_df.columns, ['title', '公告标题'])
    if not title_col:
        print("警告: 在AkShare返回数据中找不到可识别的'标题'列，无法进行筛选。")
        return pd.DataFrame()

    keyword_pattern = '|'.join(keywords)
    filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()
    print(f"筛选完成：匹配到 {len(filtered_df)} 条相关的公告。")

    if filtered_df.empty:
        return pd.DataFrame()

    # --- 列名标准化 ---
    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['code', '股票代码'],
        '公司名称': ['stock_name', '股票简称'],
        '公告标题': ['title', '公告标题'],
        '公告日期': ['notice_date', '公告日期'],
        'PDF链接': ['url', '链接']
    }

    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name:
            final_df[target_col] = filtered_df[best_col_name]
        else:
            final_df[target_col] = "N/A"
            print(f"警告: 找不到列 '{target_col}' 的任何匹配项。")
    
    # 格式化日期列
    if '公告日期' in final_df.columns and not pd.api.types.is_datetime64_any_dtype(final_df['公告日期']):
         final_df['公告日期'] = pd.to_datetime(final_df['公告日期']).dt.date

    final_df.drop_duplicates(subset=['公告日期', '公告标题'], inplace=True)
    final_df.sort_values(by='公告日期', ascending=False, inplace=True)
        
    return final_df

def get_company_profiles(stock_codes):
    """
    为一组股票代码获取公司档案（行业、主营业务）。
    实现了主/备用数据源切换，增强了健壮性。
    """
    profiles = {}
    valid_codes = [code for code in stock_codes if code and code != 'N/A']
    if not valid_codes:
        return profiles
        
    print(f"准备为 {len(valid_codes)} 家公司获取档案...")
    for code in valid_codes:
        profile_data = {'industry': '查询失败', 'main_business': '查询失败'}
        try:
            # 主数据源: 巨潮资讯
            print(f"  - 尝试从 cninfo 获取 {code} 的信息...")
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profile_data = {'industry': industry, 'main_business': main_business}
            print(f"  - 成功获取 {code} ({industry})")
        except Exception as e_cninfo:
            print(f"  ! [cninfo] 获取 {code} 档案失败: {e_cninfo}。正在尝试备用源...")
            try:
                # 备用数据源: 东方财富
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry_em = profile_df_em[profile_df_em['item'] == '行业']['value'].iloc[0]
                main_business_em = profile_df_em[profile_df_em['item'] == '主营业务']['value'].iloc[0]
                profile_data = {'industry': industry_em, 'main_business': main_business_em}
                print(f"  - [East Money] 成功从备用源获取 {code} ({industry_em})")
            except Exception as e_em:
                print(f"  ! [East Money] 备用源也获取 {code} 失败: {e_em}")
        
        profiles[code] = profile_data
        time.sleep(0.5)
    return profiles

def get_stock_financial_data(stock_codes):
    """为一组股票代码获取实时的财务指标数据。"""
    try:
        # 使用东方财富-股票快照接口，它能返回丰富的实时指标
        financial_df = ak.stock_snapshot_em(stock_codes)
        return financial_df
    except Exception as e:
        print(f"获取实时财务数据失败: {e}")
        return pd.DataFrame()

# PDF 解析功能 (当前后台worker不直接调用，但保留给前端或其他模块使用)
def extract_details_from_pdf(pdf_url):
    """从PDF链接中提取关键交易信息。"""
    try:
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        
        with BytesIO(response.content) as pdf_file:
            reader = PdfReader(pdf_file)
            text = "".join([page.extract_text() for page in reader.pages if page.extract_text()])
            text = re.sub(r'\s+', ' ', text) # 规范化空格

            target_company = re.search(r"交易标的\s*为\s*([^，。]+)", text)
            price = re.search(r"交易作价\s*为\s*([\d,.]+\s*元)", text)
            shareholders = re.search(r"交易对方\s*为\s*([^，。]+)", text)

            return (
                target_company.group(1).strip() if target_company else 'AI提取失败',
                price.group(1).strip() if price else 'AI提取失败',
                shareholders.group(1).strip() if shareholders else 'AI提取失败'
            )
    except Exception as e:
        print(f"PDF解析失败: {e}")
        return ('PDF解析失败', 'PDF解析失败', 'PDF解析失败')

