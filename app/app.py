# app.py (v1.1 - 增加了流式更新功能)

import streamlit as st
import pandas as pd
from datetime import date, timedelta

# 从我们的数据处理模块中导入所有需要的函数
import data_handler as dh

# --- 1. 页面配置 ---
st.set_page_config(
    page_title="A股并购事件追踪器",
    page_icon="📈",
    layout="wide"
)

st.title('📈 A股并购事件追踪器 (专业版)')
st.markdown("数据来源: 巨潮资讯网 | 财务数据: AkShare")

# --- 新增：一个专门用于渲染结果的函数 ---
def display_results(df):
    """传入一个DataFrame，将其内容以st.expander的形式展示出来"""
    st.info(f"已实时加载 {len(df)} 条结果。点击每条结果前的 `>` 符号可展开查看详细信息。")
    st.warning("AI自动提取的“拟并购公司”和“交易对价”等信息可能不准确，请务必点击公告链接进行核实。")

    for index, row in df.iterrows():
        summary_title = f"**{row['公司名称']} ({row['股票代码']})** | {row['公告日期']}"
        with st.expander(summary_title):
            st.markdown(f"**公告标题**: {row['公告标题']}")
            st.markdown(f"**公告链接**: [点击查看原文]({row['PDF链接']})")
            st.markdown("---")
            
            st.subheader("交易核心概要 (AI提取)")
            col1, col2, col3 = st.columns(3)
            col1.metric("拟并购公司名称", row.get('拟并购公司名称', 'N/A'))
            col2.metric("交易对价", row.get('交易对价', 'N/A'))
            col3.text_area("涉及交易股东", row.get('涉及交易股东', 'N/A'), height=100, disabled=True)
            
            st.markdown("---")

            st.subheader("上市公司快照")
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("总市值 (亿元)", f"{row.get('总市值', 0) / 1e8:.2f}" if pd.notna(row.get('总市值')) else "N/A")
            col_b.metric("市盈率 (动态)", f"{row.get('市盈率-动态'):.2f}" if pd.notna(row.get('市盈率-动态')) else "N/A")
            col_c.metric("资产负债率 (%)", f"{row.get('资产负债率(%)'):.2f}" if pd.notna(row.get('资产负债率(%)')) else "N/A")
            
            st.text_area("行业题材", row.get('行业题材', 'N/A'), height=100, disabled=True)


# --- 2. 应用界面布局 ---
with st.sidebar:
    st.header("🔍 筛选条件")
    today = date.today()
    default_start_date = today - timedelta(days=90)
    date_range = st.date_input(
        "选择公告日期范围",
        value=(default_start_date, today),
        min_value=date(2010, 1, 1),
        max_value=today,
        format="YYYY-MM-DD"
    )
    st.info("推荐使用精准关键词以避免无关内容。")
    keywords_input = st.text_area(
        "输入搜索关键词 (每行一个)",
        "重大资产重组预案\n重大资产重组草案\n发行股份购买资产预案\n发行股份购买资产草案"
    )
    keywords = [k.strip() for k in keywords_input.split('\n') if k.strip()]

# --- 3. 主程序逻辑 ---

# 关键改动(1): 在主页面创建一个空的占位符
results_placeholder = st.empty()

if st.sidebar.button('🚀 开始抓取和分析'):
    # 清空之前的session和占位符
    st.session_state.pop('final_data', None)
    results_placeholder.empty()

    if not keywords or len(date_range) != 2:
        st.error("请输入关键词并选择有效的日期范围。")
    else:
        start_date, end_date = date_range
        
        # 故障切换逻辑保持不变
        results_df = pd.DataFrame()
        try:
            with st.spinner('正在从主数据源(巨潮资讯网)抓取公告列表...'):
                results_df = dh.scrape_cninfo(keywords, start_date, end_date)
            if results_df.empty:
                raise ValueError("主数据源未返回任何数据")
        except Exception:
            with results_placeholder.container():
                st.warning("⚠️ 检测到主数据源(巨潮)访问失败或无结果，已自动切换到备用数据源(东方财富)。")
                st.info("正在尝试从备用数据源抓取...")
            try:
                results_df = dh.scrape_akshare(keywords, start_date, end_date)
            except Exception as e_ak:
                st.error(f"备用数据源(AkShare)也抓取失败: {e_ak}")

        if results_df.empty:
            results_placeholder.warning("在指定条件下，主、备数据源均未找到任何相关公告。")
        else:
            with results_placeholder.container():
                st.success(f"找到 {len(results_df)} 条相关公告，开始逐条深度解析...")
            
            # 批量获取财务数据 (这一步仍然需要先完成)
            with st.spinner('正在批量获取上市公司财务数据...'):
                all_stock_codes = results_df['股票代码'].unique().tolist()
                financial_data_df = dh.get_stock_financial_data(all_stock_codes)
            
            if not financial_data_df.empty:
                results_df = pd.merge(results_df, financial_data_df, on='股票代码', how='left')

            # 关键改动(2): 逐条处理并实时更新占位符
            processed_rows = []
            for index, row in results_df.iterrows():
                details = dh.extract_details_from_pdf(row['PDF链接'])
                row['拟并购公司名称'], row['交易对价'], row['涉及交易股东'] = details
                processed_rows.append(row)

                # 每处理完一条，就用最新的完整结果列表去更新占位符的内容
                with results_placeholder.container():
                    st.success(f"找到 {len(results_df)} 条公告，已实时解析 {len(processed_rows)}/{len(results_df)} 条...")
                    current_results_df = pd.DataFrame(processed_rows)
                    display_results(current_results_df) # 调用我们新增的渲染函数
            
            # 循环结束后，将最终结果存入session_state
            final_df = pd.DataFrame(processed_rows)
            st.session_state['final_data'] = final_df

# --- 4. 如果session中有数据，直接展示最终结果 ---
elif 'final_data' in st.session_state:
    final_df = st.session_state['final_data']
    with results_placeholder.container():
        st.success("数据加载完成！")
        display_results(final_df)
