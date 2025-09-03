# app.py (v3.8 - 展示行业与主营业务)

import streamlit as st
import pandas as pd
from datetime import date, timedelta
import psycopg2
import os

# --- (页面配置, 数据库连接, 界面, 查询逻辑等部分都保持不变, 此处省略) ---
st.set_page_config(page_title="A股并购事件追踪器", page_icon="📈", layout="wide")

@st.cache_resource(ttl=600)
def init_connection():
    try:
        db_secrets = st.secrets.database
        conn = psycopg2.connect(host=db_secrets.host, port=db_secrets.port, dbname=db_secrets.dbname, user=db_secrets.user, password=db_secrets.password, sslmode='require')
        return conn
    except Exception as e:
        st.error(f"数据库连接失败: {e}"); return None

conn = init_connection()

st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 由后台Worker每日自动更新")

with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input("选择公告日期范围", value=(default_start_date, today), format="YYYY-MM-DD")
    keyword_input = st.text_input("输入标题关键词筛选 (可选)", help="支持模糊搜索。留空则查询所有公告。")
    submit_button = st.button('🔍 查询数据库')

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
        run_query(default_start_date, today, "")
        st.rerun()

# --- 【关键修改】只修改结果展示部分的逻辑 ---
if 'announcement_list' in st.session_state and not st.session_state.announcement_list.empty:
    df = st.session_state.announcement_list
    st.success(f"从数据库中快速查到 {len(df)} 条结果！")
    st.info("点击展开可查看详情，并可按需刷新该公司的实时财务数据。")
    
    for index, row in df.iterrows():
        # --- 智能生成列表标题 ---
        company_name = row.get('company_name')
        stock_code = row.get('stock_code')
        
        # 如果公司名有效，则使用标准格式
        if company_name and company_name != 'N/A':
            expander_title = f"**{company_name} ({stock_code})** | {row['announcement_date'].strftime('%Y-%m-%d')}"
        # 否则，使用公告标题作为备用
        else:
            expander_title = f"**{row['announcement_title']}** | {row['announcement_date'].strftime('%Y-%m-%d')}"
        
        with st.expander(expander_title, expanded=False):
            # 左侧展示静态的历史存档信息
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("上市公司信息 (历史存档)")
                # 即使公司名缺失，也展示股票代码
                st.markdown(f"**股票代码:** {stock_code or 'N/A'}")
                st.markdown(f"**所属行业:** {row.get('industry', 'N/A')}")
                st.text_area("主营业务:", value=row.get('main_business', 'N/A'), height=150, disabled=True, key=f"main_biz_{index}")

            with col2:
                st.subheader("交易核心概要 (AI提取)")
                st.metric("拟并购公司名称", row.get('target_company', 'N/A'))
                st.metric("交易对价", row.get('transaction_price', 'N/A'))
                st.text_area("涉及交易股东", row.get('shareholders', 'N/A'), height=100, disabled=True, key=f"share_{index}")

            st.markdown("---")
            # 恢复“刷新实时财务数据”功能
            st.subheader("上市公司快照 (可刷新)")
            if st.button("刷新实时财务数据", key=f"detail_{index}", help="仅当股票代码有效时可用", disabled=(not stock_code or stock_code == 'N/A')):
                with st.spinner("正在刷新..."):
                    financial_data = dh.get_stock_financial_data([stock_code])
                    st.session_state[f"fin_{index}"] = financial_data.iloc[0] if not financial_data.empty else "nodata"

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
