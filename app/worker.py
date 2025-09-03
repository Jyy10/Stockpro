# worker.py
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh

def connect_db():
    try:
        conn_string = os.environ.get('DATABASE_URI')
        conn = psycopg2.connect(conn_string)
        return conn
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return None

def main():
    print("Worker开始运行...")
    conn = connect_db()
    if not conn: return

    yesterday = date.today() - timedelta(days=1)
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案"]
    print(f"正在抓取日期: {yesterday}")
    
    try:
        df = dh.scrape_cninfo(keywords, yesterday, yesterday)
    except Exception as e:
        print(f"主数据源抓取失败，尝试备用源: {e}")
        class DummyPlaceholder:
            def info(self, text): print(text)
        df = dh.scrape_akshare(keywords, yesterday, yesterday, DummyPlaceholder())
    
    if df.empty:
        print("今天没有找到相关公告。"); conn.close(); return

    print(f"找到 {len(df)} 条公告，开始处理并存入数据库...")
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (row['PDF链接'],))
            if cursor.fetchone():
                print(f"公告已存在，跳过: {row['公告标题']}"); continue

            pdf_details = dh.extract_details_from_pdf(row['PDF链接'])
            insert_query = """
                INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            record = (row['公告日期'], row['股票代码'], row['公司名称'], row['公告标题'], row['PDF链接'], pdf_details[0], pdf_details[1], pdf_details[2])
            cursor.execute(insert_query, record)
            print(f"成功插入: {row['公告标题']}")
        except Exception as e:
            print(f"处理单条记录时出错: {row['公告标题']}, 错误: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print("Worker运行完毕。")

if __name__ == "__main__":
    main()
