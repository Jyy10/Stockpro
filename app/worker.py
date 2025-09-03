# worker.py (v1.8 - 最终健壮版)
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
        print("--- 正在尝试连接数据库，使用以下参数 ---"); print(f"Host: {'已设置' if db_host else '未设置'}"); print(f"Port: {db_port}"); print(f"DB Name: {db_name}"); print(f"User: {db_user}"); print(f"Password: {'已设置' if db_password else '未设置'}"); print("---------------------------------------------")
        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：一个或多个数据库连接环境变量未设置或为空！请检查GitHub Secrets。"); return None
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, sslmode='require')
        print("数据库连接成功！"); return conn
    except Exception as e:
        print(f"数据库连接失败，详细底层错误: {e}"); return None

def get_company_profiles(stock_codes):
    """使用最稳定的巨潮资讯接口获取公司信息。"""
    # ... (此函数保持不变)
    profiles = {}
    valid_codes = [code for code in stock_codes if code and code != 'N/A']
    if not valid_codes: return profiles
    print(f"准备为 {len(valid_codes)} 家公司获取基本信息...")
    for code in valid_codes:
        try:
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profiles[code] = {'industry': industry, 'main_business': main_business}
            print(f"  - 成功获取 {code} ({industry}) 的信息")
        except Exception as e:
            print(f"  ! 获取 {code} 信息失败: {e}"); profiles[code] = {'industry': '查询失败', 'main_business': '查询失败'}
        time.sleep(0.5)
    return profiles

def main():
    print("每日更新 Worker 开始运行 (三步原则模式)...")
    conn = connect_db()
    if not conn: return

    # --- 步骤一：查找当日公告 ---
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

    # --- 步骤二：严格查找公司信息 ---
    print("\n--- 步骤2: 批量查找公司信息 ---")
    unique_codes = announcements_df['股票代码'].unique().tolist()
    company_profiles = get_company_profiles(unique_codes)
    print(f"步骤2完成：获取了 {len(company_profiles)} 家公司的档案。")

    # --- 步骤三：逐条解读公告详情并存入数据库 ---
    print("\n--- 步骤3: 逐条解析公告并存入数据库 ---")
    cursor = conn.cursor()
    successful_inserts = 0
    
    for index, row in announcements_df.iterrows():
        announcement_title = row.get('公告标题', '无标题')
        print(f"\n  处理公告: {announcement_title[:40]}...")
        try:
            # 数据校验
            stock_code = row.get('股票代码', 'N/A')
            company_name = row.get('公司名称', 'N/A')
            pdf_link = row.get('PDF链接', '')
            if stock_code == 'N/A' or company_name == 'N/A' or not pdf_link.startswith('http'):
                print(f"    - 核心信息不完整，跳过。")
                continue

            # 数据库去重
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"    - 公告已存在于数据库，跳过。")
                continue
            
            # 解读公告详情 (最耗时的一步)
            print(f"    - 正在解读PDF...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            print(f"    - PDF解读完成。")
            
            # 整合所有信息
            profile = company_profiles.get(stock_code, {'industry': '查询失败', 'main_business': '查询失败'})
            
            # 准备入库
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record = (row['公告日期'], stock_code, company_name, announcement_title, pdf_link, pdf_details[0], pdf_details[1], pdf_details[2], profile['industry'], profile['main_business'])
            
            cursor.execute(insert_query, record)
            print(f"    => 成功存入数据库！")
            successful_inserts += 1

        except Exception as e:
            print(f"    ! 处理该条记录时发生意外错误: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print(f"\n步骤3完成：成功处理并新插入 {successful_inserts} 条记录。")
    print("每日更新 Worker 运行完毕。")
