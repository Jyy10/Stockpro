# backfill_worker.py

import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh
import time

def connect_db():
    """连接到数据库"""
    try:
        conn_string = os.environ.get('DATABASE_URI')
        conn = psycopg2.connect(conn_string)
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

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
    for i, single_date in enumerate(date_list):
        print(f"\n--- 正在处理日期: {single_date} ({i+1}/{total_days}) ---")
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

        print(f"日期 {single_date}: 找到 {len(df)} 条公告，开始处理并存入数据库...")
        
        # 3. 逐条处理并存入数据库
        for index, row in df.iterrows():
            try:
                cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (row['PDF链接'],))
                if cursor.fetchone():
                    print(f"  - 公告已存在，跳过: {row['公告标题'][:20]}...")
                    continue

                pdf_details = dh.extract_details_from_pdf(row['PDF链接'])
                record = (row['公告日期'], row['股票代码'], row['公司名称'], row['公告标题'], row['PDF链接'], pdf_details[0], pdf_details[1], pdf_details[2])
                
                insert_query = """
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(insert_query, record)
                print(f"  + 成功插入: {row['公告标题'][:20]}...")

            except Exception as e:
                print(f"  ! 处理单条记录时出错: {row['公告标题'][:20]}..., 错误: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        time.sleep(1) # 每处理完一天，休息1秒

    cursor.close()
    conn.close()
    print("\n历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()
