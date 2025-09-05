# app.py (v5.6 - Nested Grouping)
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os
import sys
import akshare as ak

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

# --- 数据获取与查询逻辑 ---

def get_stock_realtime_quote(stock_code):
    """
    获取单只股票的实时行情和财务指标。
    """
    if not stock_code or stock_code == 'N/A':
        return "无效的股票代码。"
    try:
        stock_spot_df = ak.stock_zh_a_spot_em()
        quote = stock_spot_df[stock_spot_df['代码'] == stock_code]
        if quote.empty:
            return f"未能找到股票代码 {stock_code} 的实时行情数据。"
        return quote.iloc[0]
    except Exception as e:
        return f"查询实时行情时出错: {e}"

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
        # 为了分组，一次性获取较多数据, 按日期降序，公司名升序
        query += f" ORDER BY announcement_date DESC, company_name ASC, id DESC LIMIT 500"
        df = pd.read_sql_query(query, conn, params=params)
        # 确保 company_name 不是 None
        df['company_name'] = df['company_name'].fillna('N/A')
        return df
    except Exception as e:
        st.error(f"查询数据库时出错: {e}")
        return pd.DataFrame()

# --- 页面配置与状态初始化 ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

if 'df_results' not in st.session_state: st.session_state.df_results = pd.DataFrame()
if 'selected_announcement_id' not in st.session_state: st.session_state.selected_announcement_id = None
if 'realtime_quote' not in st.session_state: st.session_state.realtime_quote = {}

# --- 页面标题 ---
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
        except Exception: pass
    
    st.divider()
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD", key="date_selector_main")
    keyword_input = st.text_input("在标题中搜索关键词 (可选)", help="支持模糊搜索。")
    
    if st.button('🔍 查询数据库'):
        if len(date_range) == 2:
            with st.spinner("正在查询..."):
                st.session_state.df_results = run_query(date_range[0], date_range[1], keyword_input)
                st.session_state.selected_announcement_id = None
                st.session_state.realtime_quote = {}
        else:
            st.error("请选择有效的日期范围。")

# --- 主页面展示 ---
df = st.session_state.df_results
if not df.empty:
    st.success(f"查询到 {len(df)} 条结果！")
    
    st.subheader("公告概览 (按日期 -> 公司分组)")
    list_container = st.container(height=400)
    
    # --- 【核心改进】嵌套分组展示 ---
    # 先按日期分组
    # 注意：直接对 timestamp 进行 groupby 可能因为时区问题出错，先转为 date
    df['ann_date_only'] = df['announcement_date'].dt.date
    grouped_by_date = df.sort_values('ann_date_only', ascending=False).groupby('ann_date_only', sort=False)

    with list_container:
        # 遍历每个日期组
        for ann_date, date_group in grouped_by_date:
            # 为日期创建第一级折叠
            date_expander_title = f"**{ann_date.strftime('%Y-%m-%d')}** ({len(date_group)}条公告)"
            with st.expander(date_expander_title):
                # 在日期组内，再按公司名分组
                grouped_by_company = date_group.groupby('company_name')
                for company_name, company_group in grouped_by_company:
                    # 为公司创建第二级折叠
                    company_expander_title = f"{company_name} ({len(company_group)}条公告)"
                    with st.expander(company_expander_title):
                        # 列出该公司当日的所有公告
                        for _, row in company_group.iterrows():
                            if st.button(f"{row['announcement_title']}", key=f"btn_{row['id']}", use_container_width=True):
                                st.session_state.selected_announcement_id = row['id']
                                st.session_state.realtime_quote.pop(row['id'], None)

    st.divider()

    # --- 公告详情展示 ---
    if st.session_state.selected_announcement_id is not None:
        selected_row = df[df['id'] == st.session_state.selected_announcement_id].iloc[0]
        
        st.subheader(f"公告详情: {selected_row['announcement_title']}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**交易概要 (AI提取)**")
            summary = selected_row.get('summary')
            if summary is None or summary == '未能从PDF中提取有效信息。':
                st.warning("详细信息正在后台AI解析中，请稍后刷新查看。")
            else:
                st.write(summary)

            st.markdown(f"""
            - **交易类型**: {selected_row.get('transaction_type', 'N/A')}
            - **收购方**: {selected_row.get('acquirer', 'N/A')}
            - **标的方**: {selected_row.get('target', 'N/A')}
            - **交易对价**: {selected_row.get('transaction_price', 'N/A')}
            """)

        with col2:
            st.info("**上市公司信息**")
            st.markdown(f"""
            - **公司名称**: {selected_row['company_name']} ({selected_row['stock_code']})
            - **所属行业**: {selected_row.get('industry', 'N/A')}
            """)
            st.text_area("主营业务:", value=selected_row.get('main_business', 'N/A'), height=100, disabled=True, key=f"main_biz_{selected_row['id']}")

        st.markdown(f"**[阅读原始公告PDF]({selected_row['pdf_link']})**" if selected_row['pdf_link'] and selected_row['pdf_link'] != 'N/A' else "*无原始公告链接*")
        
        if st.button("刷新实时公司快照", key=f"refresh_{selected_row['id']}"):
            with st.spinner("正在获取实时数据..."):
                quote = get_stock_realtime_quote(selected_row['stock_code'])
                st.session_state.realtime_quote[selected_row['id']] = quote
        
        quote_data = st.session_state.realtime_quote.get(selected_row['id'])
        if quote_data is not None:
            if isinstance(quote_data, pd.Series):
                st.success("**实时财务快照**")
                c1, c2, c3 = st.columns(3)
                c1.metric("总市值(亿)", f"{quote_data.get('总市值', 0) / 1e8:.2f}")
                c2.metric("市盈率(动态)", f"{quote_data.get('市盈率-动态', 0):.2f}")
                c3.metric("市净率", f"{quote_data.get('市净率', 0):.2f}")
            else:
                st.warning(quote_data)

elif 'df_results' in st.session_state and not st.session_state.df_results.empty:
    st.info("在当前条件下未找到匹配的公告。")
