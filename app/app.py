# app.py (v3.1 - 最终精简版)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os

# --- 1. 页面配置 & 数据库连接 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    try:
        conn_string = st.secrets["DATABASE_URI"]
        return psycopg2.connect(conn_string)
    except Exception as e:
        st.error(f"数据库连接失败，请检查Streamlit Secrets配置: {e}")
        return None

conn = init_connection()

# --- 2. 界面 ---
st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词进行筛选 (支持模糊搜索)")
    
if st.sidebar.button('🔍 查询数据库'):
    if conn and len(date_range) == 2:
        start_date, end_date = date_range
        try:
            query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
            params = [start_date, end_date]
            if keyword_input:
                query += " AND announcement_title ILIKE %s"
                params.append(f"%{keyword_input}%")
            query += " ORDER BY announcement_date DESC, id DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            st.session_state.announcement_list = df
            if df.empty:
                st.info("在当前条件下，数据库中未找到匹配的公告。")
        except Exception as e:
            st.error(f"查询数据库时出错: {e}")
            st.session_state.announcement_list = pd.DataFrame() # 清空结果
    elif not conn:
        st.error("数据库未连接，无法查询。")
    else:
        st.error("请选择有效的日期范围。")


# --- 3. 结果展示 ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    
    for index, row in df.iterrows():
        with st.expander(f"**{row['company_name']} ({row['stock_code']})** | {row['announcement_date'].strftime('%Y-%m-%d')}", expanded=False):
            st.markdown(f"**公告标题**: {row['announcement_title']}")
            st.markdown(f"**公告链接**: [点击查看原文]({row['pdf_link']})")
            st.markdown("---")
            st.subheader("交易核心概要 (历史存档)")
            col_pdf1, col_pdf2, col_pdf3 = st.columns(3)
            col_pdf1.metric("拟并购公司名称", row.get('target_company', 'N/A'))
            col_pdf2.metric("交易对价", row.get('transaction_price', 'N/A'))
            col_pdf3.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True)
            st.markdown("---")
            st.info("实时财务信息功能已移除，以确保应用部署和运行的稳定性。")
