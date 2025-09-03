# app.py (v3.2 - 修正数据库连接逻辑并增加默认加载)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os

# --- 1. 页面配置 & 数据库连接 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    """初始化数据库连接，并处理潜在的连接错误"""
    try:
        # st.secrets是一个字典结构，需要用.get()或[]访问
        conn_string = st.secrets.get("DATABASE_URI")
        if not conn_string:
            # 兼容本地开发时使用环境变量
            conn_string = os.environ.get("DATABASE_URI")
        
        if not conn_string:
            st.error("数据库连接字符串未找到！请在Streamlit Secrets中配置 'DATABASE_URI'。")
            return None
            
        return psycopg2.connect(conn_string)
    except Exception as e:
        # 将详细错误打印到Streamlit界面，方便调试
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
    
    # 将按钮移出函数，使其能被其他部分调用
    submit_button = st.button('🔍 查询数据库')

# --- 新增：一个专门执行查询的函数 ---
def run_query(start, end, keyword):
    """根据传入的参数执行数据库查询，并更新session_state"""
    if not conn:
        st.error("数据库未连接，无法查询。")
        st.session_state.announcement_list = pd.DataFrame() # 清空结果
        return

    try:
        query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
        params = [start, end]
        if keyword:
            query += " AND announcement_title ILIKE %s"
            params.append(f"%{keyword}%")
        query += " ORDER BY announcement_date DESC, id DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        st.session_state.announcement_list = df
        if df.empty:
            st.info("在当前条件下，数据库中未找到匹配的公告。")
            
    except Exception as e:
        st.error(f"查询数据库时出错: {e}")
        st.session_state.announcement_list = pd.DataFrame() # 清空结果

# --- 3. 主程序逻辑 ---

# 如果按下了按钮，则执行查询
if submit_button:
    if len(date_range) == 2:
        run_query(date_range[0], date_range[1], keyword_input)
    else:
        st.error("请选择有效的日期范围。")

# 如果是首次加载（session_state中没有列表），则自动执行一次默认查询
if 'announcement_list' not in st.session_state:
    st.info("首次加载，正在为您查询过去90天的数据...")
    run_query(default_start_date, today, "")
    # 强制重新运行以展示结果
    st.rerun()

# --- 4. 结果展示 ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    
    # ... (展示逻辑和之前版本完全一样)
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
            st.info("这是一个纯数据浏览器，财务信息需自行查询。")
