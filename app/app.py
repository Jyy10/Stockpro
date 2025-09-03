# app.py (v3.0 - Worker 架构)
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import data_handler as dh
import psycopg2
import os

st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    conn_string = st.secrets["DATABASE_URI"]
    return psycopg2.connect(conn_string)

conn = init_connection()

st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词进行筛选")
    
if st.sidebar.button('🔍 查询数据库'):
    if len(date_range) == 2:
        start_date, end_date = date_range
        query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
        params = [start_date, end_date]
        if keyword_input:
            query += " AND announcement_title ILIKE %s"
            params.append(f"%{keyword_input}%")
        query += " ORDER BY announcement_date DESC, id DESC"
        df = pd.read_sql_query(query, conn, params=params)
        st.session_state.announcement_list = df
    else:
        st.error("请选择有效的日期范围。")

if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    
    for index, row in df.iterrows():
        # ... (展示逻辑和之前版本相同)
        col1, col2 = st.columns([4, 1])
        with col1:
            st.markdown(f"**{row['company_name']} ({row['stock_code']})**")
            st.caption(f"标题: {row['announcement_title']} | 日期: {row['announcement_date'].strftime('%Y-%m-%d')}")
        with col2:
            if st.button("刷新实时财务信息", key=f"detail_{index}"):
                with st.spinner("正在刷新实时数据..."):
                    financial_data = dh.get_stock_financial_data([row['stock_code']])
                    st.session_state[f"fin_{index}"] = financial_data.iloc[0] if not financial_data.empty else pd.Series()

        if f"fin_{index}" in st.session_state:
            financials = st.session_state[f"fin_{index}"]
            with st.expander("✅ 查看详情 (财务为实时)", expanded=True):
                st.markdown(f"**公告链接**: [点击查看原文]({row['pdf_link']})")
                st.markdown("---"); st.subheader("交易核心概要 (历史存档)")
                col_pdf1, col_pdf2, col_pdf3 = st.columns(3)
                col_pdf1.metric("拟并购公司名称", row.get('target_company', 'N/A'))
                col_pdf2.metric("交易对价", row.get('transaction_price', 'N/A'))
                col_pdf3.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True)
                st.markdown("---"); st.subheader("上市公司快照 (实时刷新)")
                col_fin1, col_fin2, col_fin3 = st.columns(3)
                col_fin1.metric("总市值 (亿元)", f"{financials.get('总市值', 0) / 1e8:.2f}" if pd.notna(financials.get('总市值')) else "N/A")
                col_fin2.metric("市盈率 (动态)", f"{financials.get('市盈率-动态'):.2f}" if pd.notna(financials.get('市盈率-动态')) else "N/A")
                col_fin3.metric("资产负债率 (%)", f"{financials.get('资产负债率(%)'):.2f}" if pd.notna(financials.get('资产负债率(%)')) else "N/A")
                st.text_area("行业题材", financials.get('行业题材', 'N/A'), height=100, disabled=True)
        st.markdown("---")
