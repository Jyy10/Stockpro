# frontend_data_fetcher.py (v1.0)
import pandas as pd
import akshare as ak

def get_stock_realtime_quote(stock_code):
    """
    获取单只股票的实时行情和财务指标。
    专为前端应用设计，依赖项最少。
    """
    if not stock_code or stock_code == 'N/A':
        return "无效的股票代码。"
    try:
        # 使用东方财富的实时行情接口，数据较全
        stock_spot_df = ak.stock_zh_a_spot_em()
        quote = stock_spot_df[stock_spot_df['代码'] == stock_code]
        if quote.empty:
            return f"未能找到股票代码 {stock_code} 的实时行情数据。"
        return quote.iloc[0]
    except Exception as e:
        return f"查询实时行情时出错: {e}"
