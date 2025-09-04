# worker.py (v2.1 - Enhanced Diagnostics)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh
import time
import akshare as ak

def connect_db():
    try:
        db_host = os.environ.get("DB_HOST"); db_port = os.environ.get("DB_PORT"); db_name = os.environ.get("DB_NAME"); db_user = os.environ.get("DB_USER"); db_password = os.environ.get("DB_PASSWORD")
        print("--- 正在尝试连接数据库 ---")
        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：一个或多个数据库连接环境变量未设置。"); return None
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, sslmode='require')
        print("数据库连接成功！"); return conn
    except Exception as e:
        print(f"数据库连接失败，底层错误: {e}"); return None

def main():
    # --- 关键诊断步骤：打印 akshare 版本、路径及函数存在性 ---
    print(f"==============================================")
    print(f"诊断信息：当前运行的 akshare 版本为: {ak.__version__}")
    print(f"诊断信息：akshare 模块加载路径为: {ak.__file__}")
    print(f"诊断信息：'stock_zh_a_notice' 函数是否存在: {'stock_zh_a_notice' in dir(ak)}")
    print(f"==============================================")

    print("每日更新 Worker 开始运行...")
    conn = connect_db()
    if not conn: return

    print("\n--- 步骤1: 查找当日公告 ---")
    today = date.today()
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案", "吸收合并", "要约收购报告书", "收购报告书"]
    print(f"正在抓取日期: {today}")
    
    announcements_df = pd.DataFrame()
    try:
        class DummyPlaceholder:
            def info(self, text): print(text)
        announcements_df = dh.scrape_akshare(keywords, today, today, DummyPlaceholder())
    except Exception as e:
        print(f"数据源抓取失败: {e}")

    if announcements_df is None or announcements_df.empty:
        print("今天没有找到相关公告。"); conn.close(); return
    print(f"步骤1完成：找到 {len(announcements_df)} 条相关公告。")

    print("\n--- 步骤2: 批量查找公司信息 ---")
    unique_codes = announcements_df['股票代码'].unique().tolist()
    company_profiles = dh.get_company_profiles(unique_codes)
    print(f"步骤2完成：获取了 {len(company_profiles)} 家公司的档案。")

    print("\n--- 步骤3: 逐条解析公告并存入数据库 ---")
    cursor = conn.cursor()
    successful_inserts = 0
    
    for index, row in announcements_df.iterrows():
        announcement_title = row.get('公告标题', '无标题')
        print(f"\n  处理公告: {announcement_title[:40]}...")
        try:
            stock_code = row.get('股票代码', 'N/A')
            pdf_link = row.get('PDF链接', '')
            if stock_code == 'N/A' or not pdf_link.startswith('http'):
                print(f"    - 核心信息不完整，跳过。")
                continue

            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"    - 公告已存在于数据库，跳过。")
                continue
            
            print(f"    - 正在解读PDF...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            print(f"    - PDF解读完成。")
            
            profile = company_profiles.get(stock_code, {'industry': '查询失败', 'main_business': '查询失败'})
            
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record = (row['公告日期'], stock_code, row.get('公司名称', 'N/A'), announcement_title, pdf_link, pdf_details[0], pdf_details[1], pdf_details[2], profile['industry'], profile['main_business'])
            
            cursor.execute(insert_query, record)
            print(f"    => 成功存入数据库！")
            successful_inserts += 1

        except Exception as e:
            print(f"    ! 处理该条记录时发生意外错误: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print(f"\n步骤3完成：成功处理并新插入 {successful_inserts} 条记录。")
    print("每日更新 Worker 运行完毕。")

if __name__ == "__main__":
    main()

