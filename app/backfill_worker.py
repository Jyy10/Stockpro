# backfill_worker.py (v3.3 - Daily Processing)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
import data_handler as dh # 确保 data_handler.py 在同一目录下

def connect_db():
    """连接到数据库"""
    print("--- 正在尝试连接数据库... ---")
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"), port=os.environ.get("DB_PORT"),
            dbname=os.environ.get("DB_NAME"), user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD"), sslmode='require'
        )
        print("数据库连接成功！")
        return conn
    except Exception as e:
        print(f"数据库连接失败，底层错误: {e}")
        return None

def setup_database(conn):
    """确保数据库中有 announcements 表，并包含 UNIQUE 约束"""
    print("--- 正在检查并创建数据表 (如果不存在)... ---")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS announcements (
        id SERIAL PRIMARY KEY,
        announcement_date DATE NOT NULL,
        stock_code VARCHAR(10),
        company_name VARCHAR(255),
        announcement_title TEXT NOT NULL,
        pdf_link TEXT,
        target_company TEXT,
        transaction_price TEXT,
        shareholders TEXT,
        industry TEXT,
        main_business TEXT,
        UNIQUE(announcement_date, announcement_title)
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("数据表 'announcements' 已准备就绪。")
        return True
    except Exception as e:
        print(f"创建数据表失败: {e}")
        conn.rollback()
        return False

def main():
    print("="*40)
    print(f"历史数据回补 Worker 开始运行...")
    print(f"正在使用 akshare 版本: {ak.__version__}")
    print("="*40)

    conn = connect_db()
    if not conn or not setup_database(conn):
        print("因数据库准备失败，Worker 提前终止。")
        if conn: conn.close()
        return

    # --- 准备日期范围和关键词 ---
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    keywords = [
        "重大资产", "重组", "草案", "预案", "发行股份", "购买资产",
        "吸收合并", "收购", "要约收购", "报告书"
    ]
    
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    total_days = len(date_list)
    total_successful_inserts = 0
    total_failed_inserts = 0

    print(f"\n--- 准备处理从 {start_date} 到 {end_date} 共 {total_days} 天的数据 ---")

    # --- 每日循环处理 ---
    for i, single_date in enumerate(reversed(date_list)):
        print("\n" + "="*20 + f" 正在处理日期: {single_date} ({i+1}/{total_days}) " + "="*20)

        # 步骤1: 抓取并筛选当天数据
        daily_announcements_df = dh.scrape_akshare(keywords, single_date, single_date)

        if daily_announcements_df.empty:
            print(f"  - 在 {single_date} 未找到或匹配到任何相关公告。")
            continue

        # 步骤2: 准备将当天数据入库
        print(f"  - 匹配到 {len(daily_announcements_df)} 条公告，准备入库...")
        
        successful_inserts_today = 0
        with conn.cursor() as cursor:
            for index, row in daily_announcements_df.iterrows():
                row_data = row.to_dict()
                try:
                    insert_query = """
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (announcement_date, announcement_title) DO NOTHING;
                    """
                    record_to_insert = (
                        row_data.get('公告日期'),
                        row_data.get('股票代码', 'N/A'),
                        row_data.get('公司名称', 'N/A'),
                        row_data.get('公告标题'),
                        row_data.get('PDF链接', 'N/A')
                    )
                    cursor.execute(insert_query, record_to_insert)
                    if cursor.rowcount > 0:
                        successful_inserts_today += 1
                except Exception as e:
                    print(f"    ! 插入 '{row_data.get('公告标题')}' 时发生意外错误: {e}")
                    total_failed_inserts += 1
                    conn.rollback()
        
        # 步骤3: 每日提交事务
        if successful_inserts_today > 0:
            print(f"  - 当日新插入 {successful_inserts_today} 条记录。正在提交...")
            conn.commit()
            total_successful_inserts += successful_inserts_today
        else:
            print("  - 当日无新记录入库（可能均已存在）。")
            
        time.sleep(1) # 短暂休息，避免过于频繁地请求

    conn.close()

    # --- 最终总结 ---
    print("\n" + "="*40)
    print("--- 最终入库总结 ---")
    print(f"在 {total_days} 天的处理中，共新插入: {total_successful_inserts} 条记录。")
    print(f"因插入失败而跳过: {total_failed_inserts} 条记录。")
    print("="*40)
    print("历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()

