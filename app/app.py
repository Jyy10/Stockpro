# app.py (v2.1 - 增加PDF解析可选开关)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import data_handler as dh

# --- 1. 页面配置 ---
st.set_page_config(
    page_title="A股并购事件追踪器",
    page_icon="📈",
    layout="wide"
)

st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 巨潮资讯网 | 财务数据: AkShare")

# --- 2. 应用界面布局 (侧边栏) ---
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

    # --- 【关键修改】在这里新增一个复选框 ---
    st.markdown("---")
    st.header("⚙️ 高级设置")
    parse_pdf_option = st.checkbox(
        "自动解析PDF内容 (可能极度耗时)", 
        value=True, 
        help="勾选后，“加载详情”会尝试下载并解析PDF原文，提取交易对方、对价等信息。此过程受网络和PDF文件质量影响极大，可能耗时数分钟或失败。取消勾选可大幅提速。"
    )


# --- 3. 初始化Session State ---
if 'announcement_list' not in st.session_state:
    st.session_state.announcement_list = pd.DataFrame()
if 'detailed_results' not in st.session_state:
    st.session_state.detailed_results = {}

# --- 4. 主程序逻辑 ---

if st.sidebar.button('🚀 获取公告列表'):
    # ... (这部分逻辑和之前完全一样) ...
    st.session_state.announcement_list = pd.DataFrame()
    st.session_state.detailed_results = {}
    if not keywords or len(date_range) != 2:
        st.error("请输入关键词并选择有效的日期范围。")
    else:
        start_date, end_date = date_range
        results_df = pd.DataFrame()
        status_placeholder = st.empty()
        try:
            with st.spinner('正在从主数据源(巨潮资讯网)抓取公告列表...'):
                results_df = dh.scrape_cninfo(keywords, start_date, end_date)
            if results_df.empty: raise ValueError("主数据源未返回任何数据")
            status_placeholder.empty()
        except Exception as e:
            with status_placeholder.container():
                st.warning("⚠️ 检测到主数据源(巨潮)访问失败或无结果，已自动切换到备用数据源(东方财富)。")
                try:
                    results_df = dh.scrape_akshare(keywords, start_date, end_date, status_placeholder)
                except Exception as e_ak:
                    st.error(f"备用数据源(AkShare)也抓取失败: {e_ak}")
        if not results_df.empty:
            st.session_state.announcement_list = results_df
            status_placeholder.success(f"成功获取 {len(results_df)} 条公告概览！")
        else:
            if 'announcement_list' not in st.session_state or st.session_state.announcement_list.empty:
                 status_placeholder.warning("在指定条件下，主、备数据源均未找到任何相关公告。")

# 阶段二：展示列表，并按需加载详情
if not st.session_state.announcement_list.empty:
    st.markdown("---")
    st.subheader("📊 公告概览列表")
    st.info("点击右侧的“加载并分析详情”按钮，可按需解析单条公告的详细信息。")
    
    df = st.session_state.announcement_list
    
    for index, row in df.iterrows():
        col1, col2 = st.columns([4, 1])
        with col1:
            st.markdown(f"**{row['公司名称']} ({row['股票代码']})**")
            st.caption(f"标题: {row['公告标题']} | 日期: {row['公告日期']}")
        with col2:
            if st.button("加载并分析详情", key=f"detail_{index}"):
                with st.spinner(f"正在分析 {row['公司名称']} ..."):
                    # 获取财务数据
                    financial_data = dh.get_stock_financial_data([row['股票代码']])
                    
                    # --- 【关键修改】根据复选框状态决定是否解析PDF ---
                    if parse_pdf_option:
                        pdf_details = dh.extract_details_from_pdf(row['PDF链接'])
                    else:
                        # 如果不解析，则返回默认值
                        pdf_details = ("已跳过解析", "已跳过解析", "已跳过解析")

                    details = {
                        'financials': financial_data.iloc[0] if not financial_data.empty else pd.Series(),
                        'pdf_extract': {
                            '拟并购公司名称': pdf_details[0],
                            '交易对价': pdf_details[1],
                            '涉及交易股东': pdf_details[2]
                        }
                    }
                    st.session_state.detailed_results[index] = details
        
        if index in st.session_state.detailed_results:
            # ... (这部分展示逻辑和之前完全一样) ...
            detail_data = st.session_state.detailed_results[index]
            financials = detail_data.get('financials', pd.Series())
            pdf_extract = detail_data.get('pdf_extract', {})
            with st.expander("✅ 查看已加载的详细分析", expanded=True):
                st.markdown(f"**公告链接**: [点击查看原文]({row['PDF链接']})")
                st.markdown("---")
                st.subheader("交易核心概要 (AI提取)")
                col_pdf1, col_pdf2, col_pdf3 = st.columns(3)
                col_pdf1.metric("拟并购公司名称", pdf_extract.get('拟并购公司名称', 'N/A'))
                col_pdf2.metric("交易对价", pdf_extract.get('交易对价', 'N/A'))
                col_pdf3.text_area("涉及交易股东", pdf_extract.get('涉及交易股东', 'N/A'), height=100, disabled=True)
                st.markdown("---")
                st.subheader("上市公司快照")
                col_fin1, col_fin2, col_fin3 = st.columns(3)
                col_fin1.metric("总市值 (亿元)", f"{financials.get('总市值', 0) / 1e8:.2f}" if pd.notna(financials.get('总市值')) else "N/A")
                col_fin2.metric("市盈率 (动态)", f"{financials.get('市盈率-动态'):.2f}" if pd.notna(financials.get('市盈率-动态')) else "N/A")
                col_fin3.metric("资产负债率 (%)", f"{financials.get('资产负债率(%)'):.2f}" if pd.notna(financials.get('资产负债率(%)')) else "N/A")
                st.text_area("行业题材", financials.get('行业题材', 'N/A'), height=100, disabled=True)
        st.markdown("---")
