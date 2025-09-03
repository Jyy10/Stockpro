# app.py (v3.7 - 增加数据库状态显示)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os

# --- 1. 页面配置 & 数据库连接 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    # ... (此函数保持不变)
    try:
        db_secrets = st.secrets.database
        conn = psycopg2.connect(
            host=db_secrets.host, port=db_secrets.port, dbname=db_secrets.dbname,
            user=db_secrets.user, password=db_secrets.password, sslmode='require'
        )
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}"); return None

conn = init_connection()

# --- 【关键修改1】新增一个函数来获取数据库状态 ---
@st.cache_data(ttl=60) # 缓存60秒，避免频繁查询
def get_db_status():
    """查询数据库中的总记录数和最新记录日期"""
    if not conn:
        return None, None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(announcement_date) FROM announcements;")
            status = cur.fetchone()
            return status[0], status[1] # 返回 (总数, 最新日期)
    except Exception:
        return None, None

# --- 2. 界面 ---
st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词筛选 (可选)", help="支持模糊搜索。留空则查询所有公告。")
    submit_button = st.button('🔍 查询数据库')

    # --- 【关键修改2】在侧边栏显示数据库状态 ---
    st.markdown("---")
    st.header("📊 数据库状态")
    total_count, latest_date = get_db_status()
    if total_count is not None:
        st.metric("已存档公告总数", f"{total_count:,}") # 使用千位分隔符
        if latest_date:
            st.metric("数据更新至", latest_date.strftime('%Y-%m-%d'))
    else:
        st.warning("无法获取数据库状态。")


# --- 3. 主程序逻辑 ---
# ... (此部分逻辑保持不变)
def run_query(start, end, keyword):
    if not conn:
        st.error("数据库未连接，无法查询。"); st.session_state.announcement_list = pd.DataFrame(); return
    try:
        query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
        params = [start, end]
        if keyword:
            query += " AND announcement_title ILIKE %s"; params.append(f"%{keyword}%")
        query += " ORDER BY announcement_date DESC, id DESC"
        df = pd.read_sql_query(query, conn, params=params)
        st.session_state.announcement_list = df
        if df.empty:
            st.info("在当前条件下，数据库中未找到匹配的公告。")
    except Exception as e:
        st.error(f"查询数据库时出错: {e}"); st.session_state.announcement_list = pd.DataFrame()

if submit_button:
    if len(date_range) == 2:
        run_query(date_range[0], date_range[1], keyword_input)
    else:
        st.error("请选择有效的日期范围。")
if 'announcement_list' not in st.session_state:
    if conn:
        st.info("首次加载，正在为您查询过去90天的所有公告...")
        run_query(default_start_date, today, "")
        st.rerun()

# --- 4. 结果展示 ---
# ... (此部分逻辑保持不变)
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    for index, row in df.iterrows():
        with st.expander(f"**{row['company_name']} ({row['stock_code']})** | {row['announcement_date'].strftime('%Y-%m-%d')}", expanded=False):
            st.markdown(f"**公告标题**: {row['announcement_title']}"); st.markdown(f"**公告链接**: [点击查看原文]({row['pdf_link']})"); st.markdown("---")
            st.subheader("交易核心概要 (历史存档)")
            col_pdf1, col_pdf2, col_pdf3 = st.columns(3)
            col_pdf1.metric("拟并购公司名称", row.get('target_company', 'N/A')); col_pdf2.metric("交易对价", row.get('transaction_price', 'N/A'))
            col_pdf3.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True); st.markdown("---")
            st.info("这是一个纯
