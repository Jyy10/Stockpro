# backfill_worker.py (v2.2 - Final Clean Version)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh
import time
import akshare as ak

def connect_db():
    """连接到数据库"""
    try:
        db_host = os.environ.get("DB_HOST"); db_port = os.environ.get("DB_PORT"); db_name = os.environ.get("DB_NAME"); db_user = os.environ.get("DB_USER"); db_password = os.environ.get("DB_PASSWORD")
        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：数据库连接环境变量未完全设置。"); return None
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, sslmode='require')
        print("数据库连接成功！"); return conn
    except Exception as e:
        print(f"数据库连接失败，底层错误: {e}"); return None

def main():
    print(f"==============================================")
    print(f"历史数据回补 Worker 开始运行...")
    print(f"正在使用 akshare 版本: {ak.__version__}")
    print(f"==============================================")
    
    conn = connect_db()
    if not conn: return

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案", "吸收合并", "要约收购报告书", "收购报告书"]
    
    print(f"准备抓取从 {start_date} 到 {end_date} 的数据...")
    
    # 步骤一：批量抓取所有公告
    class DummyPlaceholder:
        def info(self, text): print(text)
    
    announcements_df = dh.scrape_akshare(keywords, start_date, end_date, DummyPlaceholder())

    if announcements_df is None or announcements_df.empty:
        print(f"在 {start_date} 到 {end_date} 期间未找到相关公告。 Worker 运行结束。"); conn.close(); return
    print(f"步骤1完成：共找到 {len(announcements_df)} 条相关公告。")

    # 步骤二：批量查找公司信息
    print("\n--- 步骤2: 批量查找公司信息 ---")
    unique_codes = announcements_df['股票代码'].unique().tolist()
    company_profiles = dh.get_company_profiles(unique_codes)
    print(f"步骤2完成：获取了 {len(company_profiles)} 家公司的档案。")

    # 步骤三：逐条解析并存入数据库
    print("\n--- 步骤3: 逐条解析公告并存入数据库 ---")
    cursor = conn.cursor()
    successful_inserts = 0
    
    for index, row in announcements_df.iterrows():
        announcement_title = row.get('公告标题', 'No Title')
        print(f"\n  处理 {row.get('公告日期', 'N/A')} 的公告: {announcement_title[:40]}...")
        try:
            stock_code = row.get('股票代码', 'N/A')
            pdf_link = row.get('PDF链接', '')
            if stock_code == 'N/A' or not pdf_link.startswith('http'):
                print(f"    - 核心信息不完整，跳过。"); continue

            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"    - 公告已存在，跳过。"); continue
            
            print(f"    - 正在解读PDF...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            
            profile = company_profiles.get(stock_code, {'industry': '查询失败', 'main_business': '查询失败'})
            
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record = (row['公告日期'], stock_code, row.get('公司名称', 'N/A'), announcement_title, pdf_link, pdf_details[0], pdf_details[1], pdf_details[2], profile['industry'], profile['main_business'])
            
            cursor.execute(insert_query, record)
            conn.commit()
            print(f"    => 成功存入数据库！")
            successful_inserts += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"    ! 处理该条记录时发生意外错误: {e}"); conn.rollback(); continue
    
    cursor.close(); conn.close()
    print(f"\n步骤3完成：回补期间共新插入 {successful_inserts} 条记录。")
    print("历史数据回补 Worker 运行完毕。")

# --- 确保脚本执行的入口 ---
if __name__ == "__main__":
    main()
