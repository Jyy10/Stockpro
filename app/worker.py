# worker.py (v1.9 - Resilient API Fix)
import os
import psycopg2
import pandas as pd
from datetime import date, timedelta
import data_handler as dh # Use the updated data handler
import time

def connect_db():
    try:
        db_host = os.environ.get("DB_HOST"); db_port = os.environ.get("DB_PORT"); db_name = os.environ.get("DB_NAME"); db_user = os.environ.get("DB_USER"); db_password = os.environ.get("DB_PASSWORD")
        print("--- Attempting to connect to the database with the following parameters ---"); print(f"Host: {'Set' if db_host else 'Not Set'}"); print(f"Port: {db_port}"); print(f"DB Name: {db_name}"); print(f"User: {db_user}"); print(f"Password: {'Set' if db_password else 'Not Set'}"); print("---------------------------------------------")
        if not all([db_host, db_port, db_name, db_user, db_password]):
            print("Error: One or more database connection environment variables are not set or are empty. Please check GitHub Secrets."); return None
        conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password, sslmode='require')
        print("Database connection successful!"); return conn
    except Exception as e:
        print(f"Database connection failed, underlying error: {e}"); return None

# The get_company_profiles function is now centralized in data_handler.py
# This function is no longer needed here.

def main():
    print("Daily Update Worker started (Three-Step Principle Mode)...")
    conn = connect_db()
    if not conn: return

    # --- Step 1: Find today's announcements ---
    print("\n--- Step 1: Finding today's announcements ---")
    today = date.today()
    keywords = ["重大资产重组预案", "重大资产重组草案", "发行股份购买资产预案", "发行股份购买资产草案", "吸收合并", "要约收购报告书", "收购报告书"]
    print(f"Fetching data for date: {today}")
    
    announcements_df = pd.DataFrame()
    try:
        # A simple placeholder class for logging within the scraping function
        class DummyPlaceholder:
            def info(self, text): print(text)
        # Use the updated scraping function from the data handler
        announcements_df = dh.scrape_akshare(keywords, today, today, DummyPlaceholder())
    except Exception as e:
        print(f"Data source scraping failed: {e}")

    if announcements_df is None or announcements_df.empty:
        print("No relevant announcements found for today."); conn.close(); return
    print(f"Step 1 Complete: Found {len(announcements_df)} relevant announcements.")

    # --- Step 2: Batch find company information ---
    print("\n--- Step 2: Batch finding company information ---")
    unique_codes = announcements_df['股票代码'].unique().tolist()
    # Use the new, resilient function from the data handler
    company_profiles = dh.get_company_profiles(unique_codes)
    print(f"Step 2 Complete: Retrieved profiles for {len(company_profiles)} companies.")

    # --- Step 3: Parse each announcement and insert into the database ---
    print("\n--- Step 3: Parsing announcements and inserting into DB ---")
    cursor = conn.cursor()
    successful_inserts = 0
    
    for index, row in announcements_df.iterrows():
        announcement_title = row.get('公告标题', 'No Title')
        print(f"\n  Processing announcement: {announcement_title[:40]}...")
        try:
            # Data validation
            stock_code = row.get('股票代码', 'N/A')
            company_name = row.get('公司名称', 'N/A')
            pdf_link = row.get('PDF链接', '')
            if stock_code == 'N/A' or not pdf_link.startswith('http'):
                print(f"    - Incomplete core information, skipping.")
                continue

            # Database deduplication
            cursor.execute("SELECT id FROM announcements WHERE pdf_link = %s", (pdf_link,))
            if cursor.fetchone():
                print(f"    - Announcement already exists in the database, skipping.")
                continue
            
            # Parse announcement details (most time-consuming step)
            print(f"    - Parsing PDF...")
            pdf_details = dh.extract_details_from_pdf(pdf_link)
            print(f"    - PDF parsing complete.")
            
            # Integrate all information
            profile = company_profiles.get(stock_code, {'industry': 'Query Failed', 'main_business': 'Query Failed'})
            
            # Prepare for insertion
            insert_query = """INSERT INTO announcements (announcement_date, stock_code, company_name, announcement_title, pdf_link, target_company, transaction_price, shareholders, industry, main_business) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record = (row['公告日期'], stock_code, company_name, announcement_title, pdf_link, pdf_details[0], pdf_details[1], pdf_details[2], profile['industry'], profile['main_business'])
            
            cursor.execute(insert_query, record)
            print(f"    => Successfully inserted into the database!")
            successful_inserts += 1

        except Exception as e:
            print(f"    ! An unexpected error occurred while processing this record: {e}"); conn.rollback(); continue
    
    conn.commit(); cursor.close(); conn.close()
    print(f"\nStep 3 Complete: Successfully processed and newly inserted {successful_inserts} records.")
    print("Daily Update Worker finished.")

if __name__ == "__main__":
    main()
