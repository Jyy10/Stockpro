# backfill_worker.py (v1.5 - 最终修正版)

import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh # data_handler.py v1.7 (稳定版)
import time
import akshare as ak

def connect_db():
    """连接到数据库，并打印详细的调试信息"""
    try:
        db_host = os.environ.get("DB_HOST")
        db_port = os.environ.get("DB_PORT")
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        print("--- 正在尝试连接数据库，使用以下参数 ---")
        print(f"Host: {'已设置' if db_host else '未设置'}")
        print(f"Port: {db_port}")
        print(f"DB Name: {db_name}")
        print(f"User: {db_user}")
        print(f"Password: {'已设置' if db_password else '未设置'}")
        print("---------------------------------------------")

        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("错误：一个或多个数据库连接环境变量未设置或为空！请检查GitHub Secrets。")
            return None

        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name,
            user=db_user, password=db_password, sslmode='require'
        )
        print("数据库连接成功！")
        return conn
    except Exception as e:
        print(f"数据库连接失败，详细底层错误: {e}")
        return None

def get_company_profiles(stock_codes):
    """获取公司的基本信息（行业、主营业务）"""
    profiles = {}
    print(f"准备为 {len(stock_codes)} 家公司获取基本信息...")
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
    print("历史数据回补 Worker 开始运行 (三步原则模式)...")
    conn = connect_db()
    if not conn: return

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=270)
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案", "吸收合并", "要约收购报告书", "收购报告书"]
    
    print(f"准备抓取从 {start_date} 到 {end_date} 的数据...")
    
    for i, single_date in enumerate(reversed(date_list)):
        print(f"\n{'='*20} 正在处理日期: {single_date} ({i+1}/{len(date_list)}) {'='*20}")

        # --- 步骤一：查找当日公告 ---
        # (此处省略了 scrape_akshare 的调用，逻辑和上面 worker.py 一样)
        announcements_df = ... 
        if announcements_df is None or announcements_df.empty:
            print(f"日期 {single_date}: 未找到相关公告。")
            continue
        print(f"步骤1完成：找到 {len(announcements_df)} 条相关公告。")

        # --- 步骤二：严格查找公司信息 ---
        # (逻辑和上面 worker.py 一样)
        unique_codes = ...
        company_profiles = get_company_profiles(unique_codes)
        print(f"步骤2完成：获取了 {len(company_profiles)} 家公司的档案。")

        # --- 步骤三：逐条解读公告详情并存入数据库 ---
        # (逻辑和上面 worker.py 完全一样)
        print("\n--- 步骤3: 逐条解析公告并存入数据库 ---")
        # ... for 循环 ...
        
        conn.commit() # 每天提交一次
        time.sleep(1) # 每天休息一秒

    conn.close()
    print("\n历史数据回补 Worker 运行完毕。")
