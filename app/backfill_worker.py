# backfill_worker.py (v1.3 - 增加行业和主营业务信息获取)

import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh
import time
import akshare as ak

def connect_db():
    """连接到数据库"""
    try:
        conn_string = os.environ.get('DATABASE_URI')
        conn = psycopg2.connect(conn_string)
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def get_company_profiles(stock_codes):
    """
    新增函数：批量获取多个公司的基本信息（行业、主营业务）。
    """
    profiles = {}
    print(f"准备为 {len(stock_codes)} 家公司获取基本信息...")
    for code in stock_codes:
        try:
            # 使用akshare获取个股信息
            profile_df = ak.stock_individual_info_em(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务']['value'].iloc[0]
            profiles[code] = {
                'industry': industry,
                'main_business': main_business
            }
            print(f"  - 成功获取 {code} 的信息")
        except Exception as e:
            print(f"  ! 获取 {code} 信息失败: {e}")
            profiles[code] = {
                'industry': 'N/A',
                'main_business': 'N/A'
            }
        time.sleep(0.3) # 礼貌性延迟
    return profiles

def main():
    print("历史数据回补 Worker 开始运行...")
    conn = connect_db()
    if not conn:
        return

    # 1. 确定要抓取的日期范围 (过去270天)
    # 不包含今天
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)

    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案"]
    
    print(f"准备抓取从 {start_date} 到 {end_date} 的数据，共 {total_days} 天。")
    
    cursor = conn.cursor()

    # 2. 循环每一天进行抓取
    for i, single_date in enumerate(reversed(date_list)): # 从最近的日期开始回补
        print(f"\n--- 正在处理日期: {single_date} ({i+1}/{total_days}) ---")
        df = pd.DataFrame()
        try:
            # 优先使用主数据源
            df = dh.scrape_cninfo(keywords, single_date, single_date)
        except Exception:
            # 主数据源失败，使用备用源
            print(f"主数据源抓取失败，尝试备用源...")
            class DummyPlaceholder:
                def info(self, text): pass
            df = dh.scrape_akshare(keywords, single_date, single_date, DummyPlaceholder())

        if df.empty:
            print(f"日期 {single_date}: 未找到相关公告。")
            continue

        print(f"日期 {single_date}: 找到 {len(df)} 条公告，开始处理...")

        # 批量获取当天所有涉及公司的基本信息
        unique_codes = df['股票代码'].unique().tolist()
        company_profiles = get_company_profiles(unique_codes)
        
        # 3. 逐条处理并存入数据库
        for index, row in df.iterrows():
            try:
                stock_code = row.get('股票代码')
                company_name = row.get('公司名称')
                pdf_link = row.get('PDF链接')

                if not all([stock_code, company_name, pdf_link]) or not str(pdf_link).startswith('http'):
                    print(f"  - 数据不完整或链接无效，跳过: {row.get('公告标题', 'N/A')[:20]}...")
                    continue

                cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
                if cursor.fetchone():
                    print(f"  - 公告已存在，跳过: {row['公告标题'][:20]}...")
                    continue

                pdf_details = dh.extract_details_from_pdf(pdf_link)
                profile = company_profiles.get(stock_code, {'industry': 'N/A', 'main_business': 'N/A'})
                
                insert_query = """
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, 
                                            target_company, transaction_price, shareholders, 
                                            industry, main_business)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                record = (
                    row['公告日期'], stock_code, company_name, row['公告标题'], pdf_link,
                    pdf_details[0], pdf_details[1], pdf_details[2],
                    profile['industry'], profile['main_business']
                )
                cursor.execute(insert_query, record)
                print(f"  + 成功插入: {row['公告标题'][:20]}...")

            except Exception as e:
                print(f"  ! 处理单条记录时出错: {row.get('公告标题', 'N/A')[:20]}..., 错误: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        time.sleep(1) # 每处理完一天，休息1秒

    cursor.close()
    conn.close()
    print("\n历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()
