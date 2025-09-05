# backfill_worker.py (v3.8 - Intelligent Enrichment)
import os
import psycopg2
import psycopg2.errors
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
import data_handler as dh

def connect_db():
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
    """确保数据库表结构正确, 并增加新字段"""
    print("--- 正在检查并升级数据表结构... ---")
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id SERIAL PRIMARY KEY, announcement_date DATE NOT NULL, stock_code VARCHAR(10),
                company_name VARCHAR(255), announcement_title TEXT NOT NULL, pdf_link TEXT,
                industry TEXT, main_business TEXT, transaction_type VARCHAR(255),
                acquirer TEXT, target TEXT, transaction_price TEXT, summary TEXT
            );""")
            
            new_columns = {
                "transaction_type": "VARCHAR(255)", "acquirer": "TEXT",
                "target": "TEXT", "summary": "TEXT"
            }
            for col, col_type in new_columns.items():
                try:
                    cursor.execute(f"ALTER TABLE announcements ADD COLUMN {col} {col_type};")
                except psycopg2.errors.DuplicateColumn:
                    pass

            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS announcements_pdf_link_key;")
            try:
                cursor.execute("""
                ALTER TABLE announcements ADD CONSTRAINT unique_announcement_date_title
                UNIQUE (announcement_date, announcement_title);""")
            except (psycopg2.errors.DuplicateObject, psycopg2.errors.DuplicateTable):
                conn.rollback()
        conn.commit()
        print("数据表 'announcements' 已准备就绪。")
        return True
    except Exception as e:
        print(f"数据库设置失败: {e}")
        conn.rollback()
        return False

def fast_forward_stage(conn):
    """阶段一：快速抓取并录入所有匹配公告的基本信息"""
    print("\n" + "="*20 + " 阶段一：快速录入基本信息 " + "="*20)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    keywords = ["重组", "购买资产", "要约收购"]
    
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    total_days = len(date_list)
    total_new_inserts = 0

    for i, single_date in enumerate(reversed(date_list)):
        print(f"\n--- 正在处理日期: {single_date} ({i+1}/{total_days}) ---")
        daily_df = dh.scrape_akshare(keywords, single_date, single_date)

        if daily_df.empty: continue

        inserts_today = 0
        with conn.cursor() as cursor:
            for _, row in daily_df.iterrows():
                try:
                    cursor.execute("""
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (announcement_date, announcement_title) DO NOTHING;
                    """, (row.get('公告日期'), row.get('股票代码', 'N/A'), row.get('公司名称', 'N/A'),
                          row.get('公告标题'), row.get('PDF链接', 'N/A')))
                    if cursor.rowcount > 0: inserts_today += 1
                except Exception:
                    conn.rollback()
        
        if inserts_today > 0:
            conn.commit()
            total_new_inserts += inserts_today
    
    print(f"\n--- 阶段一完成：共新录入 {total_new_inserts} 条公告基本信息 ---")

def enrichment_stage(conn):
    """阶段二：慢速增补，调用智能解析器并更新数据库"""
    print("\n" + "="*20 + " 阶段二：智能增补交易详情 " + "="*20)
    
    records_to_process = []
    with conn.cursor() as cursor:
        cursor.execute("""
        SELECT id, pdf_link, company_name, announcement_title, stock_code FROM announcements 
        WHERE pdf_link IS NOT NULL AND pdf_link != 'N/A' AND summary IS NULL
        ORDER BY announcement_date DESC;
        """)
        records_to_process = cursor.fetchall()

    if not records_to_process:
        print("--- 无需增补的数据，或所有数据均已增补完毕。---")
        return

    print(f"--- 发现 {len(records_to_process)} 条记录需要进行智能解析 ---")
    updated_count = 0
    
    for i, (record_id, pdf_link, announcer_name, announcement_title, stock_code) in enumerate(records_to_process):
        try:
            pdf_details = dh.extract_details_from_pdf(pdf_link, announcer_name, announcement_title)
            profiles = dh.get_company_profiles([stock_code])
            profile = profiles.get(stock_code, {})

            with conn.cursor() as cursor:
                cursor.execute("""
                UPDATE announcements SET 
                    transaction_type = %s, acquirer = %s, target = %s,
                    transaction_price = %s, summary = %s, industry = %s, main_business = %s
                WHERE id = %s;
                """, (pdf_details['transaction_type'], pdf_details['acquirer'],
                      pdf_details['target'], pdf_details['transaction_price'],
                      pdf_details['summary'], profile.get('industry'), 
                      profile.get('main_business'), record_id))
            conn.commit()
            updated_count += 1
            time.sleep(1)

        except Exception as e:
            print(f"  ! 处理记录 {record_id} 时发生错误: {e}")
            conn.rollback()

    print(f"\n--- 阶段二完成：成功增补了 {updated_count} 条记录的详细信息 ---")

def main():
    print("="*40 + "\n历史数据回补 Worker (两阶段模式) 开始运行...\n" + "="*40)
    conn = connect_db()
    if not conn or not setup_database(conn):
        if conn: conn.close()
        return

    try:
        fast_forward_stage(conn)
        enrichment_stage(conn)
    finally:
        conn.close()
        print("\n" + "="*40 + "\n历史数据回补 Worker 运行完毕。\n" + "="*40)

if __name__ == "__main__":
    main()
