# app.py (v2.1 - 已修正错误并整合备用数据源逻辑)
import streamlit as st
import requests
import pandas as pd
import akshare as ak
from datetime import datetime, date, timedelta
import time
import re
from io import BytesIO
from PyPDF2 import PdfReader

# --- 1. 页面配置 ---
st.set_page_config(
    page_title="A股并购事件追踪器",
    page_icon="📈",
    layout="wide"
)

st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 巨潮资讯网 | 财务数据: AkShare")

# --- 核心功能函数 ---

@st.cache_data(ttl=3600)
def get_stock_financial_data(stock_codes):
    """获取上市公司的详细财务和行情数据"""
    # (此函数无需修改，保持原样)
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
        st.warning(f"获取行情数据时出错: {e}")

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
        st.warning(f"获取财务指标时出错: {e}")

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
        st.warning(f"获取行业题材时出错: {e}")
        
    return data_df


def extract_details_from_pdf(pdf_url):
    """(AI尽力而为) 尝试从PDF公告中提取关键信息。"""
    # (此函数无需修改，保持原样)
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

        match_price = re.search(r'(?:交易作价|交易价格|交易对价)(?:暂定为|为|合计为)([\d,.\s]+)(?:元|万元|亿元)', text)
        if match_price:
            price_value = match_price.group(1)
            price_unit = match_price.group(2) if len(match_price.groups()) > 1 and match_price.group(2) else "元"
            transaction_price = f"{price_value}{price_unit}"

        match_shareholders = re.search(r'交易对方为([\w、，,（）()]+?)[，。]', text)
        if match_shareholders:
            shareholders = match_shareholders.group(1).strip('，').strip('。')
    except Exception:
        pass
    return target_company, transaction_price, shareholders


@st.cache_data(ttl=3600)
def scrape_cninfo(keywords, start_date, end_date):
    """主数据源：根据关键词和日期范围，从巨潮资讯网抓取公告列表"""
    # (此函数无需修改，保持原样)
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
                st.error(f"请求巨潮资讯网失败: {e}，请稍后再试。")
                raise e # 抛出异常以触发备用方案
            
            announcements = data.get('announcements', [])
            if not announcements:
                break
            all_announcements.extend(announcements)
            if len(announcements) < 30:
                break
            page_num += 1
            time.sleep(0.3)
    if not all_announcements:
        return pd.DataFrame()
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

# 【关键修正1】将 scrape_akshare 函数定义移到这里
@st.cache_data(ttl=3600)
def scrape_akshare(keywords, start_date, end_date):
    """备用数据源：使用 AkShare 从东方财富网抓取公告。"""
    st.warning("⚠️ 检测到主数据源(巨潮)访问失败或无结果，已自动切换到备用数据源(东方财富)。")
    str_start_date = start_date.strftime('%Y%m%d')
    str_end_date = end_date.strftime('%Y%m%d')
    try:
        all_notices_df = ak.stock_notice_report(start_date=str_start_date, end_date=str_end_date)
        if all_notices_df.empty:
            return pd.DataFrame()
        keyword_pattern = '|'.join(keywords)
        filtered_df = all_notices_df[all_notices_df['公告标题'].str.contains(keyword_pattern, na=False)].copy()
        if filtered_df.empty:
            return pd.DataFrame()
        filtered_df.rename(columns={
            '股票代码': '股票代码', '股票简称': '公司名称', '公告标题': '公告标题',
            '公告日期': '公告日期', '公告链接': 'PDF链接'
        }, inplace=True)
        filtered_df['公告日期'] = pd.to_datetime(filtered_df['公告日期']).dt.strftime('%Y-%m-%d')
        final_df = filtered_df[['股票代码', '公司名称', '公告标题', '公告日期', 'PDF链接']]
        final_df.sort_values(by='公告日期', ascending=False, inplace=True)
        return final_df
    except Exception as e:
        st.error(f"备用数据源(AkShare)也抓取失败: {e}")
        return pd.DataFrame()

# --- 应用界面布局 ---
with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input(
        "选择公告日期范围",
        value=(default_start_date, today),
        min_value=date(2010, 1, 1),
        max_value=today,
        format="YYYY-MM-DD"
    )
    st.info("推荐使用精准关键词以避免无关内容。")
    keywords_input = st.text_area(
        "输入搜索关键词 (每行一个)",
        "重大资产重组预案\n重大资产重组草案\n发行股份购买资产预案\n发行股份购买资产草案"
    )
    keywords = [k.strip() for k in keywords_input.split('\n') if k.strip()]

if st.sidebar.button('🚀 开始抓取和分析'):
    if not keywords or len(date_range) != 2:
        st.error("请输入关键词并选择有效的日期范围。")
    else:
        start_date, end_date = date_range
        
        # 【关键修正2】在这里实现自动故障切换逻辑
        results_df = pd.DataFrame()
        try:
            with st.spinner('正在从主数据源(巨潮资讯网)抓取公告列表...'):
                results_df = scrape_cninfo(keywords, start_date, end_date)
            if results_df.empty:
                raise ValueError("主数据源未返回任何数据，尝试备用数据源。")
        except Exception as e:
            print(f"主数据源失败: {e}") # 在后台打印错误日志
            with st.spinner('主数据源异常，正在尝试从备用数据源抓取...'):
                results_df = scrape_akshare(keywords, start_date, end_date)
        
        if results_df.empty:
            st.warning("在指定条件下，主、备数据源均未找到任何相关公告。")
        else:
            st.success(f"找到 {len(results_df)} 条相关公告，开始深度解析...")
            
            all_stock_codes = results_df['股票代码'].unique().tolist()
            with st.spinner('正在批量获取上市公司财务数据...'):
                financial_data_df = get_stock_financial_data(all_stock_codes)
            
            if not financial_data_df.empty:
                results_df = pd.merge(results_df, financial_data_df, on='股票代码', how='left')

            progress_bar = st.progress(0, text="正在逐条解析PDF公告，请耐心等待...")
            extracted_data = []
            
            for index, row in results_df.iterrows():
                target_company, price, shareholders = extract_details_from_pdf(row['PDF链接'])
                row['拟并购公司名称'] = target_company
                row['交易对价'] = price
                row['涉及交易股东'] = shareholders
                extracted_data.append(row)
                progress_bar.progress((index + 1) / len(results_df), text=f"解析中: {row['公司名称']}")

            progress_bar.empty()
            final_df = pd.DataFrame(extracted_data)
            st.session_state['final_data'] = final_df

if 'final_data' in st.session_state:
    st.success("数据解析完成！")
    final_df = st.session_state['final_data']
    st.markdown("---")
    st.subheader("📊 分析结果")
    st.info("点击每条结果前的 `>` 符号可展开查看上市公司详细财务信息。")
    st.warning("AI自动提取的“拟并购公司”和“交易对价”等信息可能不准确，请务必点击公告链接进行核实。")

    for index, row in final_df.iterrows():
        summary_title = f"**{row['公司名称']} ({row['股票代码']})** | {row['公告日期']}"
        with st.expander(summary_title):
            st.markdown(f"**公告标题**: {row['公告标题']}")
            st.markdown(f"**公告链接**: [点击查看原文]({row['PDF链接']})")
            st.markdown("---")
            
            st.subheader("交易核心概要 (AI提取)")
            col1, col2, col3 = st.columns(3)
            col1.metric("拟并购公司名称", row.get('拟并购公司名称', 'N/A'))
            col2.metric("交易对价", row.get('交易对价', 'N/A'))
            col3.text_area("涉及交易股东", row.get('涉及交易股东', 'N/A'), height=100, disabled=True)
            
            st.markdown("---")

            st.subheader("上市公司快照")
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("总市值 (亿元)", f"{row.get('总市值', 0) / 1e8:.2f}" if pd.notna(row.get('总市值')) else "N/A")
            col_b.metric("市盈率 (动态)", f"{row.get('市盈率-动态'):.2f}" if pd.notna(row.get('市盈率-动态')) else "N/A")
            col_c.metric("资产负债率 (%)", f"{row.get('资产负债率(%)'):.2f}" if pd.notna(row.get('资产负债率(%)')) else "N/A")
            
            st.text_area("行业题材", row.get('行业题材', 'N/A'), height=100, disabled=True)
