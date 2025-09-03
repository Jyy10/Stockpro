# worker.py (修改后)

import os
import psycopg2
import pandas as pd
from datetime import date
import data_handler as dh

def connect_db():
    """连接到数据库，并打印详细的错误信息"""
    try:
        # 从环境变量中读取密钥
        db_host = os.environ.get("DB_HOST")
        db_port = os.environ.get("DB_PORT")
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        # 调试信息：检查环境变量是否被正确读取
        print("--- 正在尝试连接数据库，使用以下参数 ---")
        print(f"Host: {'已设置' if db_host else '未设置'}")
        print(f"Port: {db_port}")
        print(f"DB Name: {db_name}")
        print(f"User: {db_user}")
        print("Password: 已设置" if db_password else "未设置")
        print("-----------------------------------------")

        # 检查是否有任何一个关键参数为空
        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：一个或多个数据库连接环境变量未设置！")
            return None

        # 尝试连接
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode='require'
        )
        print("数据库连接成功！")
        return conn
    except Exception as e:
        # 【关键】打印出最详细的底层错误信息
        print(f"数据库连接失败，详细错误: {e}")
        return None

def main():
    print("每日更新 Worker 开始运行...")
    conn = connect_db()
    if not conn: return

    # 1. 确定要抓取的日期 (今天)
    today = date.today()
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案"]
    print(f"正在抓取日期: {today}")
    
    # ... (抓取和存入数据库的逻辑和 backfill_worker.py 基本一致)
    try:
        df = dh.scrape_cninfo(keywords, today, today)
    except Exception as e:
        print(f"主数据源抓取失败，尝试备用源: {e}")
        class DummyPlaceholder:
            def info(self, text): print(text)
        df = dh.scrape_akshare(keywords, today, today, DummyPlaceholder())

    if df.empty:
        print("今天没有找到相关公告。"); conn.close(); return

    print(f"找到 {len(df)} 条公告，开始处理并存入数据库...")
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (row['PDF链接'],))
            if cursor.fetchone():
                print(f"公告已存在，跳过: {row['公告标题'][:20]}...")
                continue
            pdf_details = dh.extract_details_from_pdf(row['PDF链接'])
            record = (row['公告日期'], row['股票代码'], row['公司名称'], row['公告标题'], row['PDF链接'], pdf_details[0], pdf_details[1], pdf_details[2])
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            cursor.execute(insert_query, record)
            print(f"成功插入: {row['公告标题'][:20]}...")
        except Exception as e:
            print(f"处理单条记录时出错: {row['公告标题'][:20]}..., 错误: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print("每日更新 Worker 运行完毕。")

if __name__ == "__main__":
    main()
