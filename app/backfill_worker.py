# backfill_worker.py (v3.7 - with Enrichment Stage)
import os
import psycopg2
import psycopg2.errors
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
import data_handler as dh

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
    """确保数据库表结构正确"""
    print("--- 正在检查并修复数据表结构... ---")
    try:
        with conn.cursor() as cursor:
            # 确保表存在
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS announcements (
                id SERIAL PRIMARY KEY, announcement_date DATE NOT NULL, stock_code VARCHAR(10),
                company_name VARCHAR(255), announcement_title TEXT NOT NULL, pdf_link TEXT,
                target_company TEXT, transaction_price TEXT, shareholders TEXT,
                industry TEXT, main_business TEXT
            );""")
            # 移除旧约束
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS announcements_pdf_link_key;")
            # 添加新约束
            try:
                cursor.execute("""
                ALTER TABLE announcements ADD CONSTRAINT unique_announcement_date_title
                UNIQUE (announcement_date, announcement_title);""")
            except (psycopg2.errors.DuplicateObject, psycopg2.errors.DuplicateTable):
                conn.rollback() # 约束已存在，正常
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
    keywords = ["重组", "购买资产"]
    
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    total_days = len(date_list)
    total_new_inserts = 0

    print(f"--- 准备处理从 {start_date} 到 {end_date} 共 {total_days} 天的数据 ---")

    for i, single_date in enumerate(reversed(date_list)):
        print(f"\n--- 正在处理日期: {single_date} ({i+1}/{total_days}) ---")
        daily_df = dh.scrape_akshare(keywords, single_date, single_date)

        if daily_df.empty:
            print(f"  - 当日未找到相关公告。")
            continue

        print(f"  - 匹配到 {len(daily_df)} 条公告，准备入库...")
        inserts_today = 0
        with conn.cursor() as cursor:
            for _, row in daily_df.iterrows():
                try:
                    cursor.execute("""
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (announcement_date, announcement_title) DO NOTHING;
                    """, (row.get('公告日期'), row.get('股票代码', 'N/A'), row.get('公司名称', 'N/A'),
                          row.get('公告标题'), row.get('PDF链接', 'N/A')))
                    if cursor.rowcount > 0:
                        inserts_today += 1
                except Exception as e:
                    print(f"    ! 插入 '{row.get('公告标题')}' 时出错: {e}")
                    conn.rollback()
        
        if inserts_today > 0:
            conn.commit()
            total_new_inserts += inserts_today
            print(f"  - 当日新插入 {inserts_today} 条记录。")
    
    print(f"\n--- 阶段一完成：共新录入 {total_new_inserts} 条公告基本信息 ---")

def enrichment_stage(conn):
    """阶段二：慢速增补，读取PDF并更新数据库中的详细信息"""
    print("\n" + "="*20 + " 阶段二：慢速增补PDF详情 " + "="*20)
    
    records_to_process = []
    with conn.cursor() as cursor:
        # 查找需要被增补的记录：有PDF链接，但target_company字段为空
        cursor.execute("""
        SELECT id, pdf_link, stock_code FROM announcements 
        WHERE pdf_link IS NOT NULL AND pdf_link != 'N/A' AND target_company IS NULL
        ORDER BY announcement_date DESC;
        """)
        records_to_process = cursor.fetchall()

    if not records_to_process:
        print("--- 无需增补的数据，或所有数据均已增补完毕。---")
        return

    print(f"--- 发现 {len(records_to_process)} 条记录需要从PDF增补信息 ---")
    updated_count = 0
    
    for i, (record_id, pdf_link, stock_code) in enumerate(records_to_process):
        print(f"\n--- 正在处理第 {i+1}/{len(records_to_process)} 条记录 (ID: {record_id}) ---")
        try:
            # 1. 从PDF提取信息
            print(f"  - 正在读取PDF: {pdf_link[:50]}...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            
            # 2. 获取公司档案
            print(f"  - 正在获取 {stock_code} 的公司档案...")
            profiles = dh.get_company_profiles([stock_code])
            profile = profiles.get(stock_code, {})

            # 3. 更新数据库
            print("  - 正在更新数据库...")
            with conn.cursor() as cursor:
                cursor.execute("""
                UPDATE announcements SET 
                    target_company = %s, transaction_price = %s, shareholders = %s,
                    industry = %s, main_business = %s
                WHERE id = %s;
                """, (pdf_details.get('target_company'), pdf_details.get('transaction_price'),
                      pdf_details.get('shareholders'), profile.get('industry'),
                      profile.get('main_business'), record_id))
            conn.commit()
            updated_count += 1
            print("  - 更新成功！")
            time.sleep(1) # 尊重接口，稍作等待

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
        fast_forward_stage(conn) # 阶段一
        enrichment_stage(conn)   # 阶段二
    finally:
        conn.close()
        print("\n" + "="*40 + "\n历史数据回补 Worker 运行完毕。\n" + "="*40)

if __name__ == "__main__":
    main()

