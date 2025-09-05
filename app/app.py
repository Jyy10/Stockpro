# app.py (v4.2 - Final Fix)
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import sys, os

# --- 动态路径配置，确保能找到 data_handler ---
def setup_path(sidebar_ref):
    """
    动态配置模块路径，并在指定的 sidebar 引用上显示警告。
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 假设 app.py 在 stockpro/app/ 目录下, 项目根目录是 stockpro
    project_root = os.path.dirname(current_dir) 
    if project_root not in sys.path:
        sys.path.append(project_root)
    
    try:
        # 现在应该能从 app.data_handler 找到
        from app import data_handler as dh
        return dh
    except (ImportError, ModuleNotFoundError):
        sidebar_ref.warning("""
        **警告**: 无法加载后台数据模块 (data_handler)。
        实时公司快照刷新功能将不可用。
        
        **解决方案**: 请检查应用的依赖项配置 (如 requirements.txt)，确保已包含 PyPDF2 和 akshare 库。
        """)
        return None

# --- 页面配置 & 数据库连接 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    try:
        db_secrets = st.secrets.database
        conn = psycopg2.connect(
            host=db_secrets.host, port=db_secrets.port, 
            dbname=db_secrets.dbname, user=db_secrets.user, 
            password=db_secrets.password, sslmode='require'
        )
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None
conn = init_connection()

# --- 侧边栏 ---
with st.sidebar:
    # 【修复】将模块加载和警告统一放在 sidebar 的上下文中
    dh = setup_path(st.sidebar)

    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    
    date_range = st.date_input(
        "选择公告日期范围", 
        value=(default_start_date, today), 
        format="YYYY-MM-DD",
        key="date_selector_main"
    )
    keyword_input = st.text_input("输入标题/概要关键词筛选 (可选)")
    submit_button = st.button('🔍 查询数据库')

    # 数据库状态面板
    if conn:
        with st.container(border=True):
            st.subheader("📊 数据库状态")
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*), MAX(announcement_date) FROM announcements;")
                    total_records, last_update = cursor.fetchone()
                    st.metric("数据库总记录数", f"{total_records or 0} 条")
                    st.metric("数据更新至", str(last_update) if last_update else "无记录")
            except Exception as e:
                st.error(f"状态查询失败: {e}")

# --- 主页面逻辑 ---
st.title('📈 A股并购事件追踪器 (智能版)')
st.markdown("数据来源: 由后台Worker每日自动更新，并进行智能解析。")

def run_query(start, end, keyword):
    if not conn: 
        st.error("数据库未连接，无法查询。")
        st.session_state.announcement_list = pd.DataFrame()
        return
    try:
        query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
        params = [start, end]
        if keyword:
            query += " AND (announcement_title ILIKE %s OR summary ILIKE %s)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        query += " ORDER BY announcement_date DESC, id DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        st.session_state.announcement_list = df
        if df.empty: 
            st.info("在当前条件下，数据库中未找到匹配的公告。")
            
    except Exception as e:
        st.error(f"查询数据库时出错: {e}")
        st.session_state.announcement_list = pd.DataFrame()

if submit_button and len(date_range) == 2:
    with st.spinner("正在查询..."):
        run_query(date_range[0], date_range[1], keyword_input)
elif 'announcement_list' not in st.session_state:
    if conn:
        with st.spinner("首次加载，正在查询近期数据..."):
            run_query(default_start_date, today, "")

# --- 结果展示 (全新重构) ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"为您找到 {len(df)} 条相关结果！")
    
    st.subheader("公告概览")
    summary_df = df[['announcement_date', 'company_name', 'announcement_title']].rename(columns={
        'announcement_date': '公告日期',
        'company_name': '公司名称',
        'announcement_title': '公告标题'
    })
    st.dataframe(summary_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    st.subheader("公告详情")

    for index, row in df.iterrows():
        with st.container(border=True):
            st.markdown(f"##### {row.get('announcement_title')}")
            st.caption(f"公司: {row.get('company_name', 'N/A')} ({row.get('stock_code', 'N/A')}) | 日期: {row['announcement_date'].strftime('%Y-%m-%d')}")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("**智能解析概要**")
                summary = row.get('summary')
                if pd.notna(summary) and "未能" not in summary:
                    st.text_area("交易概要:", summary, height=100, disabled=True, key=f"sum_{index}", label_visibility="collapsed")
                else:
                    st.info("该公告的详细信息仍在等待后台解析...")

                st.markdown(f"**交易类型:** `{row.get('transaction_type', '待解析')}`")
                st.markdown(f"**收 购 方:** `{row.get('acquirer', '待解析')}`")
                st.markdown(f"**标 的 方:** `{row.get('target', '待解析')}`")
                st.markdown(f"**交易价格:** `{row.get('transaction_price', '待解析')}`")

            with col2:
                st.markdown("**公告方信息**")
                st.markdown(f"**股票代码:** {row.get('stock_code', 'N/A')}")
                st.markdown(f"**所属行业:** {row.get('industry', '待解析')}")
                
                if dh and st.button("刷新公司快照", key=f"refresh_{index}", use_container_width=True,
                                    disabled=(not row.get('stock_code') or row.get('stock_code') == 'N/A')):
                     with st.spinner("正在刷新..."):
                        profiles = dh.get_company_profiles([row.get('stock_code')])
                        st.session_state[f"profile_{index}"] = profiles.get(row.get('stock_code'))

                if f"profile_{index}" in st.session_state:
                    profile_data = st.session_state[f"profile_{index}"]
                    st.text_area("主营业务:", profile_data.get('main_business', 'N/A'), height=150, disabled=True, key=f"biz_{index}", label_visibility="collapsed")
                
            pdf_link = row.get('pdf_link')
            if pd.notna(pdf_link) and pdf_link != 'N/A':
                st.link_button("🔗 阅读原始公告 (PDF)", pdf_link)

