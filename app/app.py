# app.py (v3.9 - 添加数据库统计面板)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os

# --- 页面配置 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

# --- 数据库连接 ---
@st.cache_resource(ttl=600)
def init_connection():
    try:
        # 优先使用 Streamlit Secrets
        if 'database' in st.secrets:
            db_secrets = st.secrets.database
            conn = psycopg2.connect(
                host=db_secrets.host, port=db_secrets.port, dbname=db_secrets.dbname,
                user=db_secrets.user, password=db_secrets.password, sslmode='require'
            )
        # 如果没有，则尝试使用环境变量（适用于本地或某些部署环境）
        else:
            conn = psycopg2.connect(
                host=os.environ.get("DB_HOST"), port=os.environ.get("DB_PORT"),
                dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASSWORD"), sslmode='require'
            )
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}"); return None

conn = init_connection()

# --- 页面标题 ---
st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")


# --- 【新增功能】数据库统计信息 ---
@st.cache_data(ttl=600) # 将统计结果缓存10分钟
def get_db_stats():
    """从数据库获取关键统计数据"""
    if not conn:
        return 0, None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(announcement_date) FROM announcements")
            stats = cur.fetchone()
            # 确保返回的是整数和日期对象
            total_count = int(stats[0]) if stats and stats[0] is not None else 0
            latest_date_obj = stats[1] if stats and stats[1] is not None else None
            return total_count, latest_date_obj
    except Exception as e:
        st.error(f"获取数据库统计信息时出错: {e}")
        return 0, None

total_count, latest_date = get_db_stats()

col1, col2 = st.columns(2)
with col1:
    st.metric("数据库公告总数", f"{total_count:,}" if total_count > 0 else "N/A")
with col2:
    st.metric("数据更新至", latest_date.strftime('%Y-%m-%d') if latest_date else "N/A")

st.markdown("---")
# --- 新增功能结束 ---


# --- 侧边栏筛选 ---
with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    
    # 确保日期范围有效
    start_date_val = latest_date - timedelta(days=90) if latest_date else default_start_date
    end_date_val = latest_date if latest_date else today

    date_range = st.date_input("选择公告日期范围", value=(start_date_val, end_date_val), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词筛选 (可选)", help="支持模糊搜索。留空则查询所有公告。")
    submit_button = st.button('🔍 查询数据库')

# --- 查询逻辑 ---
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
        if df.empty: st.info("在当前条件下，数据库中未找到匹配的公告。")
    except Exception as e:
        st.error(f"查询数据库时出错: {e}"); st.session_state.announcement_list = pd.DataFrame()

if submit_button:
    if len(date_range) == 2: run_query(date_range[0], date_range[1], keyword_input)
    else: st.error("请选择有效的日期范围。")
if 'announcement_list' not in st.session_state:
    if conn:
        st.info("首次加载，正在为您查询过去90天的所有公告...")
        run_query(start_date_val, end_date_val, "")
        if 'announcement_list' in st.session_state: # 检查查询是否成功执行
            st.rerun()

# --- 结果展示 ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    
    for index, row in df.iterrows():
        company_name = row.get('company_name', 'N/A')
        stock_code = row.get('stock_code', 'N/A')
        ann_date = row.get('announcement_date')
        date_str = ann_date.strftime('%Y-%m-%d') if ann_date else 'N/A'

        expander_title = f"**{company_name} ({stock_code})** | {date_str}"
        
        with st.expander(expander_title, expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("上市公司信息 (历史存档)")
                st.markdown(f"**股票代码:** {stock_code}")
                st.markdown(f"**所属行业:** {row.get('industry', 'N/A')}")
                st.text_area("主营业务:", value=row.get('main_business', 'N/A'), height=150, disabled=True, key=f"main_biz_{index}")

            with col2:
                st.subheader("交易核心概要 (AI提取)")
                st.metric("拟并购公司名称", row.get('target_company', 'N/A'))
                st.metric("交易对价", row.get('transaction_price', 'N/A'))
                st.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True, key=f"share_{index}")

            st.markdown("---")
            st.caption(f"原始公告标题: {row.get('announcement_title', 'N/A')}")
            pdf_link = row.get('pdf_link')
            if pdf_link:
                st.link_button("查看原始PDF公告", pdf_link)

elif conn: # 如果连接成功但 session_state 中没有数据
    st.info("请点击“查询数据库”按钮来加载数据。")
