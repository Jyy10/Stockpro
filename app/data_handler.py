# data_handler.py (v2.1 - Diagnostic Version)

import requests
import pandas as pd
import akshare as ak
import re
from io import BytesIO
from PyPDF2 import PdfReader
import time
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from thefuzz import process as fuzz_process

def find_best_column_name(available_columns, target_keywords, min_score=85):
    """Finds the best matching column name from a list of available columns based on fuzzy string matching."""
    best_match = None
    highest_score = 0
    for keyword in target_keywords:
        match, score = fuzz_process.extractOne(keyword, available_columns)
        if score > highest_score:
            highest_score = score
            best_match = match
    if highest_score >= min_score:
        return best_match
    else:
        return None

def get_company_profiles(stock_codes):
    """
    Fetches company profiles (industry, main business) with a fallback mechanism for stability.
    Tries cninfo first, then East Money as a backup.
    """
    profiles = {}
    valid_codes = [code for code in stock_codes if code and code != 'N/A']
    if not valid_codes:
        return profiles

    print(f"Preparing to fetch profiles for {len(valid_codes)} companies...")
    for code in valid_codes:
        profile_data = {'industry': 'Query Failed', 'main_business': 'Query Failed'}
        try:
            # Primary Source: cninfo
            print(f"  - Trying to get info for {code} from cninfo...")
            profile_df = ak.stock_profile_cninfo(symbol=code)
            industry = profile_df[profile_df['item'] == '行业']['value'].iloc[0]
            main_business = profile_df[profile_df['item'] == '主营业务范围']['value'].iloc[0]
            profile_data = {'industry': industry, 'main_business': main_business}
            print(f"  - [cninfo] Successfully fetched profile for {code} ({industry})")
        except Exception as e_cninfo:
            print(f"  ! [cninfo] Failed to fetch profile for {code}: {e_cninfo}. Trying fallback...")
            try:
                # Fallback Source: East Money
                profile_df_em = ak.stock_individual_info_em(symbol=code)
                industry = profile_df_em[profile_df_em['item'] == '行业']['value'].iloc[0]
                main_business = profile_df_em[profile_df_em['item'] == '主营业务']['value'].iloc[0]
                profile_data = {'industry': industry, 'main_business': main_business}
                print(f"  - [East Money] Successfully fetched profile for {code} ({industry})")
            except Exception as e_em:
                print(f"  ! [East Money] Fallback also failed for {code}: {e_em}")
        
        profiles[code] = profile_data
        time.sleep(0.5)
    return profiles

def scrape_akshare(keywords, start_date, end_date, placeholder):
    """
    Scrapes announcements from cninfo using the updated `stock_zh_a_notice` function.
    This replaces the obsolete `stock_notice_cninfo` function.
    """
    all_results_df_list = []
    delta = end_date - start_date
    date_list = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    total_days = len(date_list)
    markets = ['sh', 'sz'] # Shanghai and Shenzhen

    for i, single_date in enumerate(reversed(date_list)):
        date_str = single_date.strftime('%Y%m%d') # This API uses YYYYMMDD format
        placeholder.info(f"⏳ Backwards searching data source: {date_str} ({i+1}/{total_days})...")
        for market in markets:
            try:
                # Use the new, correct function: stock_zh_a_notice
                daily_notices_df = ak.stock_zh_a_notice(market=market, date=date_str)
                if daily_notices_df is not None and not daily_notices_df.empty:
                    all_results_df_list.append(daily_notices_df)
                else:
                    print(f"AkShare-{market}: No data returned for {date_str}.")
            except Exception as e:
                print(f"AkShare-{market}: Failed to fetch data for {date_str}, error: {e}")
            time.sleep(0.5)

    if not all_results_df_list:
        return pd.DataFrame()
    
    all_results_df = pd.concat(all_results_df_list, ignore_index=True)
    # --- DIAGNOSTIC STEP 1 ---
    # Print the total number of announcements found BEFORE filtering.
    print(f"DIAGNOSTIC: Total announcements fetched before filtering: {len(all_results_df)}")
    if not all_results_df.empty:
        print("DIAGNOSTIC: Sample of fetched data:")
        print(all_results_df.head())


    title_col = find_best_column_name(all_results_df.columns, ['title', '公告标题', '标题'])
    if not title_col:
        print("Warning: Could not find a recognizable 'title' column. Filtering not possible.")
        return pd.DataFrame()

    # --- DIAGNOSTIC STEP 2 ---
    # Temporarily bypass the keyword filtering to test the data pipeline.
    # To restore original functionality, uncomment the following two lines and delete the "filtered_df = all_results_df.copy()" line.
    # keyword_pattern = '|'.join(keywords)
    # filtered_df = all_results_df[all_results_df[title_col].str.contains(keyword_pattern, na=False)].copy()
    
    print("DIAGNOSTIC: Bypassing keyword filtering for this test run.")
    filtered_df = all_results_df.copy()


    if filtered_df.empty:
        # This will now only be true if the API returns nothing at all.
        print("No announcements found at all for the given date range.")
        return pd.DataFrame()

    final_df = pd.DataFrame()
    column_targets = {
        '股票代码': ['code', '股票代码', '代码'],
        '公司名称': ['name', '股票简称', '公司名称', '简称'],
        '公告标题': ['title', '公告标题', '标题'],
        '公告日期': ['date', '公告日期', '日期'],
        'PDF链接': ['url', '公告链接', '链接']
    }
    
    for target_col, possible_names in column_targets.items():
        best_col_name = find_best_column_name(filtered_df.columns, possible_names)
        if best_col_name:
            final_df[target_col] = filtered_df[best_col_name]
        else:
            final_df[target_col] = "N/A"
            print(f"Warning: Could not find any match for column '{target_col}'.")

    # Ensure the 'PDF链接' column contains a full URL
    pdf_link_col = 'PDF链接'
    if pdf_link_col in final_df.columns and not final_df[pdf_link_col].empty:
        base_url = 'http://static.cninfo.com.cn/'
        final_df[pdf_link_col] = final_df[pdf_link_col].apply(
            lambda url: base_url + url if isinstance(url, str) and not url.startswith('http') else url
        )

    date_col = '公告日期'
    if date_col in final_df.columns:
        final_df[date_col] = pd.to_datetime(final_df[date_col]).dt.strftime('%Y-%m-%d')

    final_df.drop_duplicates(subset=['PDF链接'], inplace=True)
    if date_col in final_df.columns:
        final_df.sort_values(by=date_col, ascending=False, inplace=True)
        
    return final_df

def get_stock_financial_data(stock_codes):
    """Placeholder for financial data fetching, referenced in app.py."""
    all_data = []
    for code in stock_codes:
        try:
            data_df = ak.stock_quote_single_pl_em(symbol=code)
            all_data.append(data_df)
        except Exception as e:
            print(f"Could not fetch financial data for {code}: {e}")
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

def _do_pdf_extraction(pdf_url):
    """Downloads a PDF and extracts text from the first few pages."""
    try:
        response = requests.get(pdf_url, timeout=20)
        response.raise_for_status()
        with BytesIO(response.content) as f:
            reader = PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages[:5])
        return text
    except Exception as e:
        print(f"Failed to extract text from PDF {pdf_url}: {e}")
        return ""

def extract_details_from_pdf(pdf_url):
    """Extracts key transaction details from PDF text using regex."""
    text = _do_pdf_extraction(pdf_url)
    if not text:
        return "Extraction Failed", "Extraction Failed", "Extraction Failed"

    target_company = re.search(r"标的公司\s*[:：为]?\s*([^\s，。(（]+)", text)
    transaction_price = re.search(r"交易作价\s*[:：为]?\s*([\d,.]+\s*[万元亿元]+)", text)
    shareholders = re.search(r"交易对方\s*[:：为]?\s*([^\s。(（]+)", text)

    return (
        target_company.group(1).strip() if target_company else "Not Found",
        transaction_price.group(1).strip() if transaction_price else "Not Found",
        shareholders.group(1).strip() if shareholders else "Not Found",
    )

