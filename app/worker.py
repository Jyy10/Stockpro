# worker.py (v1.6 - 内置终极调试功能)
# backfill_worker.py 也请使用同样的代码，只需修改日期范围逻辑

import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh # data_handler.py 现在是 v1.8 版本
import time
import akshare as ak
from thefuzz import process as fuzz_process

def connect_db():
    """连接到数据库，并打印详细的调试信息"""
    try:
        db_host = os.environ.get("DB_HOST")
        db_port = os.environ.get("DB_PORT")
        db_name = os.environ.get("DB_NAME")
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")

        print("--- 正在尝试连接数据库，读取到以下环境变量 ---")
        print(f"Host: {db_host}")
        print(f"Port: {db_port}")
        print(f"DB Name: {db_name}")
        print(f"User: {db_user}")
        # 为了安全，我们只打印密码是否存在
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

# ... (get_company_profiles 和 main 函数请保持您现有的最新版本)
# ... (它们会调用上面这个新的 connect_db 函数)

def find_best_row_value(df, target_keywords, min_score=85):
    # ... (此函数保持不变)
    pass

def get_company_profiles(stock_codes):
    # ... (此函数保持不变)
    pass

def main():
    # ... (此函数保持不变)
    pass

if __name__ == "__main__":
    main()
