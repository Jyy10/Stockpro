# backfill_worker.py (v3.1 - Robust Insertion)
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

    # --- 步骤1: 批量抓取数据 ---
    print("\n--- 步骤1: 批量抓取相关公告 ---")
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    keywords = [
        "重大资产", "重组", "草案", "预案", "发行股份", "购买资产", 
        "吸收合并", "收购", "要约收购", "报告书"
    ]
    
    all_announcements_df = dh.scrape_akshare(keywords, start_date, end_date)

    if all_announcements_df.empty:
        print(f"在 {start_date} 到 {end_date} 期间未找到相关公告。")
        conn.close()
        return

    # --- 步骤2: 尝试将所有匹配到的数据写入数据库 ---
    print(f"\n--- 步骤2: 准备将 {len(all_announcements_df)} 条匹配记录写入数据库 ---")
    successful_inserts = 0
    failed_inserts = 0
    
    with conn.cursor() as cursor:
        for index, row in all_announcements_df.iterrows():
            row_data = row.to_dict()
            title_preview = (row_data.get('公告标题', 'No Title') or 'No Title')[:50]
            print(f"\n[Row {index+1}/{len(all_announcements_df)}] 正在处理: {title_preview}...")
            
            try:
                ann_date = row_data.get('公告日期')
                title = row_data.get('公告标题')

                if not ann_date or not title:
                    print("  - \033[93m跳过\033[0m: 缺少公告日期或标题。")
                    continue

                # 使用 ON CONFLICT 实现高效的数据库级去重
                insert_query = """
                INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (announcement_date, announcement_title) DO NOTHING;
                """
                record_to_insert = (
                    ann_date,
                    row_data.get('股票代码', 'N/A'),
                    row_data.get('公司名称', 'N/A'),
                    title,
                    row_data.get('PDF链接', 'N/A')
                )
                
                cursor.execute(insert_query, record_to_insert)
                
                # cursor.rowcount 会告诉我们上一条命令影响了多少行
                if cursor.rowcount > 0:
                    print("  - \033[92m成功\033[0m: 新记录已插入。")
                    successful_inserts += 1
                else:
                    print("  - \033[94m跳过\033[0m: 记录已存在于数据库中。")

            except Exception as e:
                print(f"  - \033[91m失败\033[0m: 插入时发生意外错误: {e}")
                failed_inserts += 1
                conn.rollback() # 回滚当前失败的事务，准备处理下一条
    
    print("\n--- 正在提交所有更改到数据库... ---")
    conn.commit()
    conn.close()
    
    print("\n" + "="*40)
    print("--- 最终入库总结 ---")
    print(f"成功新插入: {successful_inserts} 条记录。")
    print(f"因插入失败而跳过: {failed_inserts} 条记录。")
    print(f"（因数据库中已存在而被跳过的记录未计入失败数）")
    print("="*40)
    print("历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()

