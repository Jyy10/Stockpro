# backfill_worker.py (v2.6 - Final Production Version)
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
            print("错误：一个或多个数据库连接环境变量未设置或为空！"); return None
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, sslmode='require')
        print("数据库连接成功！"); return conn
    except Exception as e:
        print(f"数据库连接失败: {e}"); return None

def main():
    """主执行函数"""
    print("=============================================")
    print("历史数据回补 Worker 开始运行...")
    try:
        print(f"正在使用 akshare 版本: {ak.__version__}")
    except Exception as e:
        print(f"无法获取 akshare 版本: {e}")
    print("=============================================")

    conn = connect_db()
    if not conn: return

    # --- 【核心改进】使用更全面的关键词列表 ---
    keywords = ["重大资产", "重组", "收购", "购买资产", "发行股份", "吸收合并", "要约收购", "报告书", "预案", "草案", "控制权变更"]
    
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    
    print(f"准备抓取从 {start_date} 到 {end_date} 的数据...")
    
    class DummyPlaceholder:
        def info(self, text): print(text)
            
    announcements_df = dh.scrape_akshare(keywords, start_date, end_date, DummyPlaceholder())

    if announcements_df is None or announcements_df.empty:
        print(f"在 {start_date} 到 {end_date} 期间未找到相关公告。Worker 运行完毕。"); conn.close(); return
    print(f"步骤1完成：在时间范围内共找到 {len(announcements_df)} 条相关公告。")

    print("\n步骤2: 开始批量查找公司信息...")
    unique_codes = announcements_df['股票代码'].unique().tolist()
    company_profiles = dh.get_company_profiles(unique_codes)
    print(f"步骤2完成：获取了 {len(company_profiles)} 家公司的档案。")

    print("\n步骤3: 开始逐条解析公告并存入数据库...")
    cursor = conn.cursor()
    successful_inserts = 0
    
    for index, row in announcements_df.iterrows():
        stock_code = row.get('股票代码', 'N/A')
        pdf_link = row.get('PDF链接', '')
        announcement_title = row.get('公告标题', '无标题')
        
        print(f"\n  处理公告: {row.get('公告日期')} - {announcement_title[:50]}...")

        if stock_code == 'N/A' or not pdf_link.startswith('http'):
            print(f"    - 核心信息不完整 (代码: {stock_code}, 链接: {pdf_link})，跳过。")
            continue

        try:
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"    - 公告已存在于数据库，跳过。")
                continue
            
            print(f"    - 正在解读PDF...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            
            profile = company_profiles.get(stock_code, {'industry': '查询失败', 'main_business': '查询失败'})
            
            insert_query = """
                INSERT INTO announcements 
                (announcement_date, stock_code, company_name, announcement_title, pdf_link, 
                target_company, transaction_price, shareholders, industry, main_business) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            record = (
                row.get('公告日期'), stock_code, row.get('公司名称', 'N/A'), announcement_title, pdf_link,
                pdf_details[0], pdf_details[1], pdf_details[2],
                profile['industry'], profile['main_business']
            )
            
            cursor.execute(insert_query, record)
            print(f"    => 成功存入数据库！")
            successful_inserts += 1

        except psycopg2.Error as db_err:
            print(f"    ! 数据库操作错误: {db_err}"); conn.rollback()
        except Exception as e:
            print(f"    ! 处理该条记录时发生意外错误: {e}"); conn.rollback()
            
    conn.commit()
    cursor.close()
    conn.close()
    print(f"\n步骤3完成：成功处理并新插入 {successful_inserts} 条记录。")
    print("历史数据回补 Worker 运行完毕。")

if __name__ == "__main__":
    main()

