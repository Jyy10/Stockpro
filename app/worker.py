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
    profiles = {}; print(f"准备为 {len(stock_codes)} 家公司获取基本信息...")
    for code in stock_codes:
        try:
            profile_df = ak.stock_individual_info_em(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务']['value'].iloc[0]
            profiles[code] = {'industry': industry, 'main_business': main_business}
            print(f"  - 成功获取 {code} 的信息")
        except Exception as e:
            print(f"  ! 获取 {code} 信息失败: {e}"); profiles[code] = {'industry': 'N/A', 'main_business': 'N/A'}
        time.sleep(0.3)
    return profiles

def main():
    print("每日更新 Worker 开始运行...")
    conn = connect_db()
    if not conn: return

    # ... (抓取公告的逻辑不变) ...
    today = date.today()
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案"]
    print(f"正在抓取日期: {today}")
    df = pd.DataFrame()
    try:
        df = dh.scrape_cninfo(keywords, today, today)
    except Exception as e:
        print(f"主数据源抓取失败，尝试备用源: {e}")
        class DummyPlaceholder:
            def info(self, text): print(text)
        df = dh.scrape_akshare(keywords, today, today, DummyPlaceholder())
    
    # --- 【关键修改】在这里，我们只检查PDF链接的有效性 ---
    if df is None or df.empty:
        print("今天没有找到相关公告。"); conn.close(); return

    print(f"找到 {len(df)} 条公告，开始处理...")
    unique_codes = df['股票代码'].unique().tolist()
    company_profiles = get_company_profiles(unique_codes)
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        try:
            pdf_link = row.get('PDF链接')
            # 只要求PDF链接有效即可，其他信息可以为空
            if not pdf_link or not str(pdf_link).startswith('http'):
                print(f"  - 链接无效，跳过: {row.get('公告标题', 'N/A')[:20]}...")
                continue

            # 如果公告已存在，则跳过
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"  - 公告已存在，跳过: {row.get('公告标题', 'N/A')[:20]}...")
                continue
            
            # 获取其他信息，如果缺失则使用'N/A'作为默认值
            stock_code = row.get('股票代码', 'N/A')
            company_name = row.get('公司名称', 'N/A')
            announcement_title = row.get('公告标题', '无标题') # 标题也给一个默认值
            announcement_date = row.get('公告日期')

            pdf_details = dh.extract_details_from_pdf(pdf_link)
            profile = company_profiles.get(stock_code, {'industry': 'N/A', 'main_business': 'N/A'})
            
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record = (announcement_date, stock_code, company_name, announcement_title, pdf_link, pdf_details[0], pdf_details[1], pdf_details[2], profile['industry'], profile['main_business'])
            
            cursor.execute(insert_query, record)
            print(f"  + 成功插入: {announcement_title[:20]}...")
        except Exception as e:
            print(f"  ! 处理单条记录时出错: {row.get('公告标题', 'N/A')[:20]}..., 错误: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print("每日更新 Worker 运行完毕。")

if __name__ == "__main__":
    main()
