# worker.py (v4.2 - Precise Filtering & Daily Processing)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import time
import akshare as ak
# 确保 data_handler.py 在同一目录下或在Python路径中
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
    # 此处逻辑与 backfill_worker 一致
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
            
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS announcements_pdf_link_key;")
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS unique_announcement_date_title;")
            cursor.execute("ALTER TABLE announcements ADD CONSTRAINT unique_announcement_date_title UNIQUE (announcement_date, announcement_title);")
        
        conn.commit()
        print("数据库表结构已准备就绪。")
        return True
    except psycopg2.errors.DuplicateObject:
        conn.rollback()
        return True
    except Exception as e:
        print(f"数据库设置失败: {e}")
        conn.rollback()
        return False

def enrichment_stage(conn):
    """第二阶段：智能增补"""
    # 此处逻辑与 backfill_worker 一致
    print("\n--- 阶段2: 开始智能增补公告详情 ---")
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, pdf_link FROM announcements WHERE summary IS NULL OR summary = '未能从PDF中提取有效信息。' LIMIT 50;") # 每日更新处理50条
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

                trans_type, acquirer, target, price, summary = dh.extract_details_from_pdf(pdf_link)
                
                update_query = """
                UPDATE announcements 
                SET transaction_type = %s, acquirer = %s, target = %s, transaction_price = %s, summary = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (trans_type, acquirer, target, price, summary, record_id))
                time.sleep(1)
        
        conn.commit()
    except Exception as e:
        print(f"  ! 在增补阶段发生严重错误: {e}")
        conn.rollback()

def main():
    print("="*40)
    print(f"每日更新 Worker (v4.2) 开始运行...")
    print(f"正在使用 akshare 版本: {ak.__version__}")
    print("="*40)

    conn = connect_db()
    if not conn or not setup_database(conn):
        if conn: conn.close()
        return

    # --- 精准关键词定义 ---
    core_keywords = ["重组", "购买资产", "资产出售"]
    modifier_keywords = ["草案", "预案", "进展公告"]

    # 只处理昨天和今天
    end_date = date.today()
    start_date = end_date - timedelta(days=1)
    date_list = [start_date, end_date]

    print("\n--- 阶段1: 开始按天快速录入基础公告 ---")
    total_new_inserts = 0
    
    for single_date in reversed(date_list):
        print(f"\n{'='*20} 正在处理日期: {single_date.strftime('%Y-%m-%d')} {'='*20}")
        daily_df = dh.scrape_akshare_precisely(core_keywords, modifier_keywords, single_date, single_date)

        if daily_df.empty:
            print("  - 当日未找到相关公告。")
            continue

        daily_inserts = 0
        with conn.cursor() as cursor:
            for _, row in daily_df.iterrows():
                try:
                    insert_query = """
                    INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (announcement_date, announcement_title) DO NOTHING;
                    """
                    record = (
                        row.get('公告日期'), row.get('股票代码', 'N/A'),
                        row.get('公司名称', 'N/A'), row.get('公告标题'),
                        row.get('PDF链接', 'N/A')
                    )
                    cursor.execute(insert_query, record)
                    if cursor.rowcount > 0:
                        daily_inserts += 1
                except Exception as e:
                    print(f"    ! 插入时出错: {e}")
                    conn.rollback()
        
        conn.commit()
        print(f"  - 当日新入库 {daily_inserts} 条记录。")
        total_new_inserts += daily_inserts
        time.sleep(1)

    print(f"\n阶段1完成：总共新录入了 {total_new_inserts} 条基础公告。")

    enrichment_stage(conn)

    conn.close()
    print("\n" + "="*40)
    print("每日更新 Worker 运行完毕。")
    print("="*40)

if __name__ == "__main__":
    main()
