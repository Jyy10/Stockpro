# app.py (v5.0 - Master-Detail View)
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os
import sys

# --- 动态路径设置 ---
# 确保应用在任何环境下都能找到 data_handler 模块
try:
    # 假设 app.py 和 data_handler.py 在同一个父目录下
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    import data_handler as dh
    DATA_HANDLER_LOADED = True
except (ImportError, ModuleNotFoundError):
    DATA_HANDLER_LOADED = False

# --- 数据库连接 ---
@st.cache_resource(ttl=600)
def init_connection():
    try:
        db_secrets = st.secrets.database
        conn = psycopg2.connect(
            host=db_secrets.host, port=db_secrets.port, dbname=db_secrets.dbname,
            user=db_secrets.user, password=db_secrets.password, sslmode='require'
        )
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None

conn = init_connection()

# --- 页面配置 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")
st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

# --- 侧边栏 ---
with st.sidebar:
    st.header("数据库状态")
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*), MAX(announcement_date) FROM announcements;")
                total_records, last_update = cur.fetchone()
                st.metric("数据库总记录数", f"{total_records or 0} 条")
                st.metric("数据更新至", last_update.strftime('%Y-%m-%d') if last_update else "无记录")
        except Exception as e:
            st.error(f"无法获取数据库状态: {e}")
    else:
        st.warning("数据库未连接")

    if not DATA_HANDLER_LOADED:
        st.warning(
            """
            **无法加载后台数据模块 (data_handler)。** 实时公司快照刷新功能将不可用。

            **解决方案**: 请检查应用的依赖项配置 (如 requirements.txt)，确保已包含 PyPDF2 和 akshare 库。
            """
        )
    
    st.divider()
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input(
        "选择公告日期范围",
        value=(default_start_date, today),
        format="YYYY-MM-DD",
        key="date_selector_main" # 添加唯一的key
    )
    keyword_input = st.text_input("在标题中搜索关键词 (可选)", help="支持模糊搜索。")
    submit_button = st.button('🔍 查询数据库')

# --- 数据查询逻辑 ---
def run_query(start, end, keyword):
    if not conn:
        st.error("数据库未连接，无法查询。")
        return pd.DataFrame()
    try:
        query = "SELECT * FROM announcements WHERE announcement_date BETWEEN %s AND %s"
        params = [start, end]
        if keyword:
            query += " AND announcement_title ILIKE %s"
            params.append(f"%{keyword}%")
        query += " ORDER BY announcement_date DESC, id DESC"
        return pd.read_sql_query(query, conn, params=params)
    except Exception as e:
        st.error(f"查询数据库时出错: {e}")
        return pd.DataFrame()

# --- 主页面展示 ---
# 初始化Session State
if 'df_results' not in st.session_state:
    st.session_state.df_results = pd.DataFrame()
if 'selected_announcement_id' not in st.session_state:
    st.session_state.selected_announcement_id = None

# 执行查询
if submit_button:
    if len(date_range) == 2:
        with st.spinner("正在查询..."):
            st.session_state.df_results = run_query(date_range[0], date_range[1], keyword_input)
            st.session_state.selected_announcement_id = None # 每次新查询都重置选择
    else:
        st.error("请选择有效的日期范围。")

# --- 展示查询结果 ---
df = st.session_state.df_results
if not df.empty:
    st.success(f"查询到 {len(df)} 条结果！点击下方列表查看详情。")
    
    # 1. 公告概览列表
    st.subheader("公告概览")
    list_container = st.container(height=300) # 创建一个带滚动条的容器
    with list_container:
        for index, row in df.iterrows():
            # 为每一行创建一个按钮，点击后在session state中记录ID
            if st.button(f"**{row['announcement_date'].strftime('%Y-%m-%d')}** | {row['company_name']} | {row['announcement_title']}", key=f"btn_{row['id']}", use_container_width=True):
                st.session_state.selected_announcement_id = row['id']
    
    st.divider()

    # 2. 公告详情展示
    if st.session_state.selected_announcement_id is not None:
        selected_row = df[df['id'] == st.session_state.selected_announcement_id].iloc[0]
        
        st.subheader(f"公告详情: {selected_row['announcement_title']}")
        
        # 使用列来布局
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**交易概要 (AI提取)**")
            st.write(selected_row.get('summary', '暂无概要'))
            
            st.markdown(f"""
            - **交易类型**: {selected_row.get('transaction_type', 'N/A')}
            - **收购方**: {selected_row.get('acquirer', 'N/A')}
            - **标的方**: {selected_row.get('target', 'N/A')}
            - **交易对价**: {selected_row.get('transaction_price', 'N/A')}
            """)

        with col2:
            st.info("**上市公司信息 (历史存档)**")
            st.markdown(f"""
            - **公司名称**: {selected_row['company_name']} ({selected_row['stock_code']})
            - **所属行业**: {selected_row.get('industry', 'N/A')}
            """)
            st.text_area("主营业务:", value=selected_row.get('main_business', 'N/A'), height=150, disabled=True, key=f"main_biz_{selected_row['id']}")

        # PDF链接和公司快照
        st.markdown(f"**[阅读原始公告PDF]({selected_row['pdf_link']})**" if selected_row['pdf_link'] and selected_row['pdf_link'] != 'N/A' else "*无原始公告链接*")
        
        if DATA_HANDLER_LOADED:
            if st.button("刷新实时公司快照", key=f"refresh_{selected_row['id']}"):
                with st.spinner("正在获取实时数据..."):
                    # 此处可添加获取公司实时数据的逻辑
                    st.success("实时数据功能待实现。")
        
else:
    if submit_button:
        st.info("在当前条件下未找到匹配的公告。")

