# app.py (v4.1 - Graceful Degradation)
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os
import sys

# --- 【核心改进】动态调整模块搜索路径 ---
# 获取当前脚本所在的目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 将该目录添加到系统路径中，确保可以找到同级的模块
sys.path.append(current_dir)

# --- 【核心改进】安全导入可选模块 ---
DATA_HANDLER_AVAILABLE = False
DH_IMPORT_ERROR = ""
try:
    import data_handler as dh
    DATA_HANDLER_AVAILABLE = True
except ImportError as e:
    # 记录错误信息，稍后在侧边栏显示
    DH_IMPORT_ERROR = (
        f"无法加载后台数据模块 (data_handler): {e}\n\n"
        "实时财务数据刷新功能将不可用。\n\n"
        "**解决方案**: 请检查应用的依赖项配置 (如 requirements.txt)，"
        "确保已包含 `PyPDF2` 和 `akshare` 库。"
    )
    dh = None

# --- 页面配置 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

# --- 数据库连接 ---
@st.cache_resource(ttl=600)
def init_connection():
    try:
        # 优先从 Streamlit Secrets 获取
        db_secrets = st.secrets.get("database")
        if db_secrets:
            conn = psycopg2.connect(
                host=db_secrets.get("host"), port=db_secrets.get("port"),
                dbname=db_secrets.get("dbname"), user=db_secrets.get("user"),
                password=db_secrets.get("password"), sslmode='require'
            )
            return conn
        # 如果 Secrets 不存在，尝试从环境变量获取 (适用于本地调试)
        elif all(os.environ.get(k) for k in ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"]):
            conn = psycopg2.connect(
                host=os.environ.get("DB_HOST"), port=os.environ.get("DB_PORT"),
                dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASSWORD"), sslmode='require'
            )
            return conn
        else:
            st.error("数据库连接信息不完整，请检查 Streamlit Secrets 或环境变量配置。")
            return None
    except Exception as e:
        st.error(f"数据库连接失败: {e}")
        return None

conn = init_connection()

# --- 界面 ---
st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

with st.sidebar:
    st.header("🔍 筛选条件")
    
    # 如果 data_handler 导入失败，在此处显示一个明确的警告
    if not DATA_HANDLER_AVAILABLE:
        st.warning(DH_IMPORT_ERROR)

    # --- 数据库状态显示 ---
    if conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM announcements;")
                total_rows = cursor.fetchone()[0]
                
                cursor.execute("SELECT MAX(announcement_date) FROM announcements;")
                last_update_date = cursor.fetchone()[0]
                
                st.metric("数据库总记录数", f"{total_rows} 条")
                st.metric("数据更新至", f"{last_update_date.strftime('%Y-%m-%d')}" if last_update_date else "无记录")
        except (psycopg2.errors.UndefinedTable, psycopg2.ProgrammingError):
             st.warning("“announcements”表不存在。请确保后台Worker已成功运行并创建了表结构。")
        except Exception as e:
            st.error(f"无法获取数据库状态: {e}")
            conn.rollback() # 出错时回滚
    else:
        st.warning("数据库未连接")
    
    st.markdown("---") # 分割线
    
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词筛选 (可选)", help="支持模糊搜索。留空则查询所有公告。")
    submit_button = st.button('🔍 查询数据库')

# --- 查询逻辑 ---
def run_query(start, end, keyword):
    if not conn:
        st.error("数据库未连接，无法查询。")
        st.session_state.announcement_list = pd.DataFrame()
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
        st.session_state.announcement_list = pd.DataFrame()

# --- 页面加载与交互 ---
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

# --- 结果展示 ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    st.info("点击展开可查看详情，并可按需刷新该公司的实时财务数据。")
    
    for index, row in df.iterrows():
        company_name = row.get('company_name', 'N/A')
        stock_code = row.get('stock_code', 'N/A')
        
        expander_title = f"**{company_name} ({stock_code})** | {row['announcement_date'].strftime('%Y-%m-%d')}" if company_name and company_name != 'N/A' else f"**{row['announcement_title']}** | {row['announcement_date'].strftime('%Y-%m-%d')}"
        
        with st.expander(expander_title, expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("上市公司信息")
                st.markdown(f"**股票代码:** {stock_code}")
                st.markdown(f"**所属行业:** {row.get('industry', 'N/A')}")
                st.text_area("主营业务:", value=row.get('main_business', 'N/A'), height=150, disabled=True, key=f"main_biz_{index}")

            with col2:
                st.subheader("交易核心概要 (AI提取)")
                st.metric("拟并购公司名称", row.get('target_company', 'N/A'))
                st.metric("交易对价", row.get('transaction_price', 'N/A'))
                st.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True, key=f"share_{index}")

            st.markdown("---")
            st.subheader("上市公司快照 (可刷新)")

            # --- 【核心改进】根据模块是否可用，决定显示按钮还是提示信息 ---
            if DATA_HANDLER_AVAILABLE:
                if st.button("刷新实时财务数据", key=f"detail_{index}", help="仅当股票代码有效时可用", disabled=(not stock_code or stock_code == 'N/A')):
                    with st.spinner("正在刷新..."):
                        financial_data = dh.get_stock_financial_data([stock_code])
                        st.session_state[f"fin_{index}"] = financial_data.iloc[0] if not financial_data.empty else "nodata"
            else:
                st.markdown("_(功能禁用：缺少后台依赖库)_")

            if f"fin_{index}" in st.session_state:
                financials = st.session_state[f"fin_{index}"]
                if isinstance(financials, pd.Series):
                    fin_col1, fin_col2, fin_col3, fin_col4 = st.columns(4)
                    fin_col1.metric("总市值 (亿元)", f"{financials.get('总市值', 0) / 1e8:.2f}" if pd.notna(financials.get('总市值')) else "N/A")
                    fin_col2.metric("市盈率 PE (动态)", f"{financials.get('市盈率-动态'):.2f}" if pd.notna(financials.get('市盈率-动态')) else "N/A")
                    fin_col3.metric("市净率 PB", f"{financials.get('市净率'):.2f}" if pd.notna(financials.get('市净率')) else "N/A")
                    fin_col4.metric("资产负债率 (%)", f"{financials.get('资产负债率(%)'):.2f}" if pd.notna(financials.get('资产负债率(%)')) else "N/A")
                    st.text_area("行业题材", financials.get('行业题材', 'N/A'), height=100, disabled=True, key=f"concept_{index}")
                else:
                    st.warning("未能获取该公司的实时财务数据。")

