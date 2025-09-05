# backfill_worker.py (v4.0 - Intelligent Enrichment)
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
    """确保数据库表结构正确，包含所有需要的字段和约束"""
    print("--- 正在检查并修复数据库表结构... ---")
    try:
        with conn.cursor() as cursor:
            # 检查并添加新列（如果不存在）
            columns_to_add = {
                "transaction_type": "VARCHAR(50)", "acquirer": "TEXT",
                "target": "TEXT", "summary": "TEXT"
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
            
            # 移除可能存在的、有问题的旧约束
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS announcements_pdf_link_key;")
            print(" - 已移除陈旧的 pdf_link 约束 (如果存在)。")

            # 添加或确认正确的唯一性约束
            # 首先移除旧的，以防万一名称不匹配但逻辑相同
            cursor.execute("ALTER TABLE announcements DROP CONSTRAINT IF EXISTS unique_announcement_date_title;")
            cursor.execute("ALTER TABLE announcements ADD CONSTRAINT unique_announcement_date_title UNIQUE (announcement_date, announcement_title);")
            print(" - 已确保正确的唯一性约束 (date, title) 已设置。")
        
        conn.commit()
        print("数据库表结构已准备就绪。")
        return True
    except psycopg2.errors.DuplicateObject:
        print(" - 唯一性约束已存在，无需重复添加。")
        conn.rollback() # 回滚ADD CONSTRAINT事务
        return True
    except Exception as e:
        print(f"数据库设置失败: {e}")
        conn.rollback()
        return False

def initial_ingestion_stage(conn, keywords, start_date, end_date):
    """第一阶段：快速抓取并录入基础公告信息"""
    print("\n--- 阶段1: 开始快速录入基础公告 ---")
    all_announcements_df = dh.scrape_akshare(keywords, start_date, end_date)
    
    if all_announcements_df.empty:
        print("阶段1完成：未找到新的相关公告可供录入。")
        return

    print(f"准备将 {len(all_announcements_df)} 条匹配记录写入数据库...")
    with conn.cursor() as cursor:
        for _, row in all_announcements_df.iterrows():
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
            except Exception as e:
                print(f"  ! 插入基础信息时出错: {e}")
                conn.rollback()
    conn.commit()
    print("阶段1完成：基础信息录入完毕。")

def enrichment_stage(conn):
    """第二阶段：对数据库中缺少详细信息的公告进行智能增补"""
    print("\n--- 阶段2: 开始智能增补公告详情 ---")
    try:
        with conn.cursor() as cursor:
            # 选取需要增补的记录：summary 字段为空或为特定的初始值
            cursor.execute("SELECT id, pdf_link FROM announcements WHERE summary IS NULL OR summary = '未能从PDF中提取有效信息。' LIMIT 100;") # 每次处理100条
            records_to_enrich = cursor.fetchall()

            if not records_to_enrich:
                print("阶段2完成：没有需要增补信息的公告。")
                return
            
            print(f"找到 {len(records_to_enrich)} 条公告需要增补详细信息...")
            
            for record_id, pdf_link in records_to_enrich:
                if not pdf_link or pdf_link == 'N/A':
                    # 对于没有PDF链接的，直接更新状态
                    update_query = "UPDATE announcements SET summary = %s WHERE id = %s;"
                    cursor.execute(update_query, ("无PDF链接，无法解析。", record_id))
                    continue

                print(f"  - 正在解析公告 ID: {record_id}...")
                # 调用智能解析器
                trans_type, acquirer, target, price, summary = dh.extract_details_from_pdf(pdf_link)
                
                # 更新回数据库
                update_query = """
                UPDATE announcements 
                SET transaction_type = %s, acquirer = %s, target = %s, transaction_price = %s, summary = %s
                WHERE id = %s;
                """
                cursor.execute(update_query, (trans_type, acquirer, target, price, summary, record_id))
                time.sleep(1) # 尊重PDF源，避免请求过快
        
        conn.commit()
        print(f"阶段2完成：成功增补了 {len(records_to_enrich)} 条公告的信息。")

    except Exception as e:
        print(f"  ! 在增补阶段发生严重错误: {e}")
        conn.rollback()

def main():
    print("="*40)
    print(f"历史数据回补 Worker (v4.0) 开始运行...")
    print(f"正在使用 akshare 版本: {ak.__version__}")
    print("="*40)

    conn = connect_db()
    if not conn or not setup_database(conn):
        print("因数据库准备失败，Worker 提前终止。")
        if conn: conn.close()
        return

    # 定义核心关键词
    keywords = ["重组", "购买资产"] # 精简后的关键词
    
    # 定义回补时间范围
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)

    # 执行两个核心阶段
    initial_ingestion_stage(conn, keywords, start_date, end_date)
    enrichment_stage(conn)

    conn.close()
    print("\n" + "="*40)
    print("历史数据回补 Worker 运行完毕。")
    print("="*40)

if __name__ == "__main__":
    main()

