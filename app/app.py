# app.py (v6.1 - Display Stock Code)
import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import psycopg2
import os
import sys
import akshare as ak
from concurrent.futures import ThreadPoolExecutor

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

# --- 高性能、多维度的数据获取逻辑 ---

def fetch_historical_data(stock_code):
    """获取历史数据以计算涨跌幅"""
    try:
        end_date = date.today().strftime('%Y%m%d')
        start_date_60 = (date.today() - timedelta(days=70)).strftime('%Y%m%d')
        hist_df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date_60, end_date=end_date, adjust="qfq")
        if len(hist_df) < 2: return None
        return hist_df.iloc[-61:]
    except Exception:
        return None

def fetch_financial_indicators(stock_code):
    """获取核心财务指标"""
    try:
        indicator_df = ak.stock_financial_analysis_indicator(symbol=stock_code)
        return indicator_df.iloc[-1]
    except Exception:
        return None

def fetch_realtime_price(stock_code):
    """获取最新股价和交易状态"""
    try:
        return ak.stock_individual_real_time_quote(symbol=stock_code)
    except Exception:
        return None

def get_stock_realtime_quote(stock_code):
    """通过并行API调用，高效获取丰富维度的公司快照数据。"""
    if not stock_code or stock_code == 'N/A':
        return "无效的股票代码。"

    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_hist = executor.submit(fetch_historical_data, stock_code)
        future_fin = executor.submit(fetch_financial_indicators, stock_code)
        future_price = executor.submit(fetch_realtime_price, stock_code)
        
        hist_df = future_hist.result()
        fin_series = future_fin.result()
        price_series = future_price.result()

    if price_series is not None:
        results['股价'] = price_series.get('price')
        results['是否停牌'] = "是" if price_series.get('open') == 0 and price_series.get('price') > 0 else "否"
    
    if hist_df is not None:
        if len(hist_df) > 30:
            results['近30天涨跌幅'] = (hist_df['收盘'].iloc[-1] / hist_df['收盘'].iloc[-31] - 1) * 100
        if len(hist_df) > 60:
            results['近60天涨跌幅'] = (hist_df['收盘'].iloc[-1] / hist_df['收盘'].iloc[-61] - 1) * 100

    if fin_series is not None:
        results['市值'] = fin_series.get('总市值')
        results['总股本'] = fin_series.get('总股本')
        results['流通股数'] = fin_series.get('流通a股')
        results['ttm净利润'] = fin_series.get('归属母公司股东的净利润-ttm')
        results['市盈率'] = fin_series.get('市盈率-ttm')
        results['净资产'] = fin_series.get('归属母公司股东的权益')
        results['市净率'] = fin_series.get('市净率')
        results['ttm收入总额'] = fin_series.get('营业总收入-ttm')
        results['市销率'] = fin_series.get('市销率-ttm')

    results['fetch_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return results if len(results) > 1 else "未能获取到任何有效的公司快照数据。"

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
        query += f" ORDER BY announcement_date DESC, company_name ASC, id DESC LIMIT 1000"
        df = pd.read_sql_query(query, conn, params=params)
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
    st.info("提示：为保证应用性能，概览最多显示最近的1000条公告。")
    
    st.subheader("公告概览 (按日期 -> 公司分组)")
    list_container = st.container(height=400)
    
    df['announcement_date'] = pd.to_datetime(df['announcement_date'], errors='coerce')
    df.dropna(subset=['announcement_date'], inplace=True)
    df['company_name'] = df['company_name'].fillna('N/A').astype(str).str.strip()
    df['ann_date_only'] = df['announcement_date'].dt.date
    grouped_by_date = df.sort_values('ann_date_only', ascending=False).groupby('ann_date_only', sort=False)

    with list_container:
        for ann_date, date_group in grouped_by_date:
            num_companies = date_group['company_name'].nunique()
            date_expander_title = f"**{ann_date.strftime('%Y-%m-%d')}** ({num_companies}家公司, {len(date_group)}条公告)"
            with st.expander(date_expander_title):
                grouped_by_company = date_group.groupby('company_name')
                for company_name, company_group in grouped_by_company:
                    company_expander_title = f"{company_name} ({len(company_group)}条公告)"
                    with st.expander(company_expander_title):
                        for _, row in company_group.iterrows():
                            if st.button(f"{row['announcement_title']}", key=f"btn_{row['id']}", use_container_width=True):
                                st.session_state.selected_announcement_id = row['id']
                                st.session_state.realtime_quote.pop(row['id'], None)

    st.divider()

    if st.session_state.selected_announcement_id is not None:
        selected_row = df[df['id'] == st.session_state.selected_announcement_id].iloc[0]
        st.subheader(f"公告详情: {selected_row['announcement_title']}")
        
        # 公告基本信息
        st.info(f"**发布公司**: {selected_row['company_name']} ({selected_row['stock_code']})")

        if st.button("刷新实时公司快照", key=f"refresh_{selected_row['id']}"):
            with st.spinner("正在获取实时数据..."):
                quote = get_stock_realtime_quote(selected_row['stock_code'])
                st.session_state.realtime_quote[selected_row['id']] = quote
        
        quote_data = st.session_state.realtime_quote.get(selected_row['id'])
        if quote_data:
            if isinstance(quote_data, dict):
                st.success("**实时财务快照**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("**交易信息**")
                    st.metric("当前股价", f"{quote_data.get('股价', 0):.2f} 元" if quote_data.get('股价') else "N/A")
                    st.metric("是否停牌", quote_data.get('是否停牌', "N/A"))
                    st.metric("近30天涨跌幅", f"{quote_data.get('近30天涨跌幅', 0):.2f}%" if quote_data.get('近30天涨跌幅') else "N/A")
                    st.metric("近60天涨跌幅", f"{quote_data.get('近60天涨跌幅', 0):.2f}%" if quote_data.get('近60天涨跌幅') else "N/A")
                with col2:
                    st.write("**市值与股本**")
                    st.metric("总市值", f"{quote_data.get('市值', 0) / 1e8:.2f} 亿元" if quote_data.get('市值') else "N/A")
                    st.metric("总股本", f"{quote_data.get('总股本', 0) / 1e8:.2f} 亿股" if quote_data.get('总股本') else "N/A")
                    st.metric("流通股", f"{quote_data.get('流通股数', 0) / 1e8:.2f} 亿股" if quote_data.get('流通股数') else "N/A")
                with col3:
                    st.write("**核心估值指标**")
                    st.metric("市盈率 (TTM)", f"{quote_data.get('市盈率', 0):.2f}" if quote_data.get('市盈率') else "N/A")
                    st.metric("市净率 (PB)", f"{quote_data.get('市净率', 0):.2f}" if quote_data.get('市净率') else "N/A")
                    st.metric("市销率 (TTM)", f"{quote_data.get('市销率', 0):.2f}" if quote_data.get('市销率') else "N/A")
                st.caption(f"数据获取时间: {quote_data.get('fetch_time', 'N/A')}")
            else:
                st.warning(quote_data)
        
        st.info(f"**交易概要 (AI提取)**")
        summary = selected_row.get('summary')
        if summary is None or summary == '未能从PDF中提取有效信息。':
            st.warning("详细信息正在后台AI解析中，请稍后刷新查看。")
        else:
            st.write(summary)

elif 'df_results' in st.session_state and not st.session_state.df_results.empty:
    st.info("在当前条件下未找到匹配的公告。")
