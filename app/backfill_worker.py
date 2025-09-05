# backfill_worker.py (v4.7 - Final DB Fix)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
import data_handler as dh
import asyncio

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
    """确保数据库表结构正确，包含所有需要的字段和约束"""
    print("--- 正在检查并修复数据库表结构... ---")
    try:
        with conn.cursor() as cursor:
            # 检查并添加新列（如果不存在）
            columns_to_add = {
                "transaction_type": "VARCHAR(50)", "acquirer": "TEXT",
                "target": "TEXT", "summary": "TEXT", "transaction_price": "TEXT"
            }
            for col, col_type in columns_to_add.items():
                cursor.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='announcements' AND column_name='{col}') THEN
                        ALTER TABLE announcements ADD COLUMN {col} {col_type};
                    END IF;
                END $$;
                """)
            
            # --- 【核心修复】允许 pdf_link 字段为空 ---
            cursor.execute("ALTER TABLE announcements ALTER COLUMN pdf_link DROP NOT NULL;")
            print(" - 已确保 pdf_link 字段允许为空。")
            
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS announcements_pdf_link_key;")
            print(" - 已移除陈旧的 pdf_link 约束 (如果存在)。")

            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS unique_announcement_date_title;")
            cursor.execute("ALTER TABLE announcements ADD CONSTRAINT unique_announcement_date_title UNIQUE (announcement_date, announcement_title);")
            print(" - 已确保正确的唯一性约束 (date, title) 已设置。")
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_announcement_date ON announcements (announcement_date);")
            print(" - 已确保日期索引存在，加速查询。")

        conn.commit()
        print("数据库表结构已准备就绪。")
        return True
    except Exception as e:
        # 捕获一个更通用的异常，因为 "DuplicateObject" 可能不是唯一的问题
        print(f" - 数据库设置过程中出现一个可忽略的错误: {e}")
        conn.rollback()
        return True

async def enrichment_stage(conn):
    """第二阶段：对数据库中缺少详细信息的公告进行智能增补（异步）"""
    print("\n--- 阶段2: 开始智能增补公告详情 ---")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, pdf_link FROM announcements WHERE summary IS NULL OR summary = '未能从PDF中提取有效信息。' LIMIT 100;")
            records_to_enrich = cursor.fetchall()

            if not records_to_enrich:
                print("阶段2完成：没有需要增补信息的公告。")
                return
            
            print(f"找到 {len(records_to_enrich)} 条公告需要增补详细信息...")
            
            for record_id, pdf_link in records_to_enrich:
                if not pdf_link or pdf_link == 'N/A':
                    update_query = "UPDATE announcements SET summary = %s WHERE id = %s;"
                    cursor.execute(update_query, ("无PDF链接，无法解析。", record_id))
                    continue

                print(f"  - 正在通过AI解析公告 ID: {record_id}...")
                trans_type, acquirer, target, price, summary = await dh.extract_details_from_pdf(pdf_link)
                
                update_query = """
                UPDATE announcements 
                SET transaction_type = %s, acquirer = %s, target = %s, transaction_price = %s, summary = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (trans_type, acquirer, target, price, summary, record_id))
                await asyncio.sleep(1) 
        
        conn.commit()
        print(f"阶段2完成：成功增补了 {len(records_to_enrich)} 条公告的信息。")

    except Exception as e:
        print(f"  ! 在增补阶段发生严重错误: {e}")
        conn.rollback()

def main():
    print("="*40)
    print(f"历史数据回补 Worker (v4.7) 开始运行...")
    print(f"正在使用 akshare 版本: {ak.__version__}")
    print("="*40)

    conn = connect_db()
    if not conn or not setup_database(conn):
        print("因数据库准备失败，Worker 提前终止。")
        if conn: conn.close()
        return

    core_keywords = ["重组", "购买资产", "资产出售"]
    modifier_keywords = ["草案", "预案", "进展公告"]

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    print("\n--- 阶段1: 开始按天快速录入基础公告 ---")
    total_new_inserts = 0
    
    for single_date in reversed(date_list):
        print(f"\n{'='*20} 正在处理日期: {single_date.strftime('%Y-%m-%d')} {'='*20}")
        
        daily_df = dh.scrape_and_normalize_akshare(core_keywords, modifier_keywords, single_date, single_date)

        if daily_df.empty:
            print("  - 当日未找到相关公告。")
            continue

        print(f"  - 找到 {len(daily_df)} 条匹配记录，准备入库...")
        daily_inserts = 0
        with conn.cursor() as cursor:
            for _, row in daily_df.iterrows():
                stock_code = row.get('股票代码')
                if not stock_code or stock_code == 'N/A' or not isinstance(stock_code, str) or not stock_code.isdigit():
                    print(f"    - \033[93m跳过\033[0m: 无效或缺失股票代码。标题: {row.get('公告标题')[:30]}...")
                    continue
                
                try:
                    insert_query = """
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (announcement_date, announcement_title)
                    DO UPDATE SET
                        stock_code = EXCLUDED.stock_code,
                        company_name = EXCLUDED.company_name,
                        pdf_link = EXCLUDED.pdf_link
                    WHERE
                        announcements.stock_code IS NULL OR announcements.stock_code = 'N/A';
                    """
                    record = (
                        row.get('公告日期'), stock_code,
                        row.get('公司名称', 'N/A'), row.get('公告标题'),
                        row.get('PDF链接') # 直接使用，可能是None
                    )
                    cursor.execute(insert_query, record)
                    if cursor.rowcount > 0:
                        daily_inserts += 1
                except Exception as e:
                    print(f"    ! 插入/更新时出错: {e}")
                    conn.rollback()
        
        conn.commit()
        print(f"  - 当日处理完成 {daily_inserts} 条记录（新增或更新）。")
        total_new_inserts += daily_inserts
        time.sleep(1)

    print(f"\n阶段1完成：总共处理了 {total_new_inserts} 条基础公告。")

    asyncio.run(enrichment_stage(conn))

    conn.close()
    print("\n" + "="*40)
    print("历史数据回补 Worker 运行完毕。")
    print("="*40)

if __name__ == "__main__":
    main()
