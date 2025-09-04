# backfill_worker.py (v3.0 - Production Ready)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
import data_handler as dh # 确保 data_handler.py 在同一目录下

def connect_db():
    """连接到数据库，并打印详细的调试信息"""
    print("--- 正在尝试连接数据库... ---")
    try:
        db_host = os.environ.get("DB_HOST")
        db_port = os.environ.get("DB_PORT")
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：一个或多个数据库连接环境变量未设置或为空！")
            return None

        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name,
            user=db_user, password=db_password, sslmode='require'
        )
        print("数据库连接成功！")
        return conn
    except Exception as e:
        print(f"数据库连接失败，底层错误: {e}")
        return None

def setup_database(conn):
    """确保数据库中有 announcements 表"""
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
    if not conn:
        print("因数据库连接失败，Worker 提前终止。")
        return

    if not setup_database(conn):
        print("因数据表准备失败，Worker 提前终止。")
        conn.close()
        return

    # --- 步骤1: 批量抓取时间范围内所有相关公告 ---
    print("\n--- 步骤1: 批量抓取时间范围内所有相关公告 ---")
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270) # 回补约9个月
    print(f"准备抓取从 {start_date} 到 {end_date} 的数据...")
    
    keywords = [
        "重大资产", "重组", "草案", "预案", "发行股份", "购买资产", 
        "吸收合并", "收购", "要约收购", "报告书"
    ]
    
    all_announcements_df = dh.scrape_akshare(keywords, start_date, end_date)

    if all_announcements_df.empty:
        print(f"在 {start_date} 到 {end_date} 期间未找到相关公告。")
        conn.close()
        return
        
    print("\n--- 抓取到的数据预览 (前5条) ---")
    print(all_announcements_df.head())
    print("-"*(len(str(all_announcements_df.head()))))


    # --- 步骤2 & 3: 逐条处理并存入数据库 ---
    print("\n--- 步骤2 & 3: 开始逐条处理并存入数据库 ---")
    successful_inserts = 0
    skipped_due_to_db_duplicate = 0
    skipped_due_to_session_duplicate = 0
    processed_in_session = set() # 用于本轮运行内的去重

    with conn.cursor() as cursor:
        for index, row in all_announcements_df.iterrows():
            try:
                title = row.get('公告标题', '无标题')
                code = row.get('股票代码', 'N/A')
                ann_date = row.get('公告日期')

                # 本轮去重检查
                session_key = (str(ann_date), title)
                if session_key in processed_in_session:
                    skipped_due_to_session_duplicate += 1
                    continue
                
                # 数据库去重检查
                cursor.execute(
                    "SELECT id FROM announcements WHERE announcement_date = %s AND announcement_title = %s",
                    (ann_date, title)
                )
                if cursor.fetchone():
                    skipped_due_to_db_duplicate += 1
                    continue

                # 优先入库核心信息
                insert_query = """
                INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                record_to_insert = (
                    ann_date,
                    code,
                    row.get('公司名称', '待查询'),
                    title,
                    row.get('PDF链接', '待查询'),
                    '待查询', '待查询', '待查询', '待查询', '待查询' # 详细信息字段留空
                )
                
                cursor.execute(insert_query, record_to_insert)
                successful_inserts += 1
                processed_in_session.add(session_key) # 标记为已处理

            except Exception as e:
                print(f"  ! 处理 '{title}' 时发生意外错误: {e}")
                conn.rollback()
                continue
    
    conn.commit()
    conn.close()
    
    print("\n--- 入库总结 ---")
    print(f"新插入 {successful_inserts} 条记录。")
    print(f"因数据库中已存在而跳过 {skipped_due_to_db_duplicate} 条记录。")
    print(f"因本次任务内重复而跳过 {skipped_due_to_session_duplicate} 条记录。")
    print("="*40)
    print("历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()

