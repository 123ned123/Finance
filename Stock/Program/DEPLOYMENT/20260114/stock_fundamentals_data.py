import pandas as pd
import yfinance as yf
import sqlite3
import datetime
import time
import os

# --- CONFIGURATION ---
DB_NAME = "market_data_fundamentals.db"
# Define the target folder globally so we can use it for both DB and LOG
TARGET_FOLDER = r"/home/miniadmin/USB_MP/Stock/Daily_DB"
LOG_FILE_NAME = "scheduler_log.txt"

TICKERS = [ "SPY" ,"NVDA", "AAPL", "MSFT", "AMZN" \
            ,"GOOGL", "META", "TSLA", "BRK-B", "JPM" \
            ,"AVGO", "INTC", "WMT", "UNH", "V", "PYPL" \
            ,"MA", "HD", "BAC", "COST", "DIS","QQQ","SQQQ" ] 

MARKET_OPEN = datetime.time(9, 30)
MARKET_CLOSE = datetime.time(16, 0)

# --- LOGGING FUNCTION ---
def log_message(message):
    """
    Prints to console AND appends to a log file with a timestamp.
    """
    # Create timestamp string
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    
    # 1. Print to console
    print(full_message)
    
    # 2. Save to file
    try:
        log_path = os.path.join(TARGET_FOLDER, LOG_FILE_NAME)
        # mode 'a' means append (add to end of file)
        with open(log_path, "a") as f:
            f.write(full_message + "\n")
    except Exception as e:
        print(f"Error writing to log file: {e}")

# --- MAPPING ---
FUNDAMENTAL_MAPPING = {
    'last_price': 'last',
    'bid': 'bid',
    'ask': 'ask',
    'bidSize': 'bid_size',
    'askSize': 'ask_size',
    'currentPrice': 'current_price',
    'targetHighPrice': 'target_high_price',
    'targetLowPrice': 'target_low_price',
    'targetMeanPrice': 'target_mean_price',
    'targetMedianPrice': 'target_median_price',
    'recommendationKey': 'recommendation_key',
    'numberOfAnalystOpinions': 'number_of_analyst_opinions',
    'lastDividendValue': 'last_dividend_value',
    'lastDividendDate': 'last_dividend_date',
    'totalCash': 'total_cash',
    'totalCashPerShare': 'total_cash_per_share',
    'ebitda': 'ebitda',
    'totalDebt': 'total_debt',
    'quickRatio': 'quick_ratio',
    'currentRatio': 'current_ratio',
    'totalRevenue': 'total_revenue',
    'debtToEquity': 'debt_to_equity',
    'revenuePerShare': 'revenue_per_share',
    'returnOnAssets': 'return_on_assets',
    'returnOnEquity': 'return_on_equity',
    'grossProfits': 'gross_profits',
    'freeCashflow': 'free_cashflow',
    'operatingCashflow': 'operating_cashflow',
    'earningsGrowth': 'earnings_growth',
    'revenueGrowth': 'revenue_growth',
    'grossMargins': 'gross_margins',
    'ebitdaMargins': 'ebitda_margins',
    'operatingMargins': 'operating_margins',
    'regularMarketChange': 'regular_market_change',
    'regularMarketChangePercent': 'regular_market_change_percent',
    'regularMarketPrice': 'regular_market_price',
    'postMarketChangePercent': 'post_market_change_percent',
    'postMarketPrice': 'post_market_price',
    'postMarketChange': 'post_market_change',
    'averageDailyVolume3Month': 'average_daily_volume_3_month',
    'open': 'open_price',
    'dayLow': 'day_low',
    'dayHigh': 'day_high',
    'regularMarketPreviousClose': 'regular_market_previous_close',
    'regularMarketOpen': 'regular_market_open',
    'regularMarketDayLow': 'regular_market_day_low',
    'regularMarketDayHigh': 'regular_market_day_high',
    'dividendRate': 'dividend_rate',
    'dividendYield': 'dividend_yield',
    'trailingPE': 'trail_PE',
    'forwardPE': 'forward_PE',
    'trailingEps': 'trail_Eps',
    'forwardEps': 'forward_Eps',
    'previous_close': 'prev_close',
    'volume': 'volume',
    'regularMarketVolume': 'reg_market_volume',
    'averageVolume': 'avg_volume',
    'averageVolume10days': 'ave_10d_volume',
    'currency': 'currency',
    'exchange': 'exchange',
    'profitMargins': 'profit_margin',
    'floatShares': 'float_shares',
    'enterpriseValue': 'company_value',
    'sharesOutstanding': 'outstanding_share',
    'sharesShort': 'share_short',
    'sharesPercentSharesOut': 'share_percent_shares_out',
    'heldPercentInsiders': 'insider_held_percent',
    'heldPercentInstitutions': 'institution_held_percent',
    'shortRatio': 'short_ratio',
    'shortPercentOfFloat': 'short_percent_of_float',
    'impliedSharesOutstanding': 'implied_shares_outstanding',
    'bookValue': 'book_value',
    'priceToBook': 'price_2_book'
}

def get_fundamental_data(ticker):
    """
    Fetches the current fundamental snapshot for a ticker and saves it to the database.
    """
    log_message(f"[{ticker}] Fetching fundamental data...")

    db_path = os.path.join(TARGET_FOLDER, DB_NAME)

    try:
        stock = yf.Ticker(ticker)
        info_data = stock.info
        
        row_data = {
            'ticker': ticker,
            'time_scraped': datetime.datetime.now()
        }

        for api_key, db_column in FUNDAMENTAL_MAPPING.items():
            row_data[db_column] = info_data.get(api_key, None)
            
            # Fallback logic
            if db_column == 'trail_Eps' and row_data[db_column] is None:
                row_data[db_column] = info_data.get('epsTrailing', None)
            if db_column == 'forward_Eps' and row_data[db_column] is None:
                row_data[db_column] = info_data.get('epsForward', None)
            if db_column == 'volume' and row_data[db_column] is None:
                row_data[db_column] = info_data.get('last_volume', None)

        df = pd.DataFrame([row_data])

        conn = sqlite3.connect(db_path)
        df.to_sql("stock_fundamentals", conn, if_exists="append", index=False)
        conn.close()

        log_message(f"[{ticker}] Successfully saved fundamentals.")

    except Exception as e:
        log_message(f"[{ticker}] Error: {e}")

def job():
    """
    This function checks if the current time is within market hours.
    If yes, it runs the scraper.
    """
    now_time = datetime.datetime.now().time()
    
    # Check if within market hours
    if MARKET_OPEN <= now_time <= MARKET_CLOSE:
        log_message("--- Starting Scheduled Run ---")
        for symbol in TICKERS:
            get_fundamental_data(symbol)
        log_message("--- Run Complete ---")
    else:
        # We don't necessarily want to log this to the file every minute, 
        # or the file will get huge. Printing to screen only for waiting status:
        print(f"Market Closed. Current time: {now_time}. Waiting...", end='\r')

# --- SCHEDULING LOGIC ---
if __name__ == "__main__":
    log_message("Scheduler Started. Waiting for market hours (Mon-Fri, 9:00-16:00)...")

    # Define the specific exit time
    EXIT_TIME = datetime.time(16, 10) # Added 10 mins buffer to finish last run

    while True:
        now = datetime.datetime.now()
        
        # --- EXIT CHECK ---
        if now.time() >= EXIT_TIME:
            log_message(f"Current time is {now.strftime('%H:%M')}. Past exit time. Exiting program.")
            break

        # Check if today is a weekday (0=Mon, 4=Fri)
        if now.weekday() < 5:
            job()
        else:
            # Optional: Log once a day that it's the weekend if you want, otherwise pass
            pass
            
        # Sleep for 10 minutes (600 seconds)
        time.sleep(600)

'''
# previous code

# # this is the scheduled version 
# # **** this is production version at the mini server to get fundamentals daily ****

# import pandas as pd
# import yfinance as yf
# import sqlite3
# import datetime
# import schedule
# import time
# import os

# # --- CONFIGURATION ---
# DB_NAME = "market_data_fundamentals.db"
# TICKERS = [ "SPY" ,"NVDA", "AAPL", "MSFT", "AMZN" \
#            ,"GOOGL", "META", "TSLA", "BRK-B", "JPM" \
#             ,"AVGO", "INTC", "WMT", "UNH", "V", "PYPL" \
#             ,"MA", "HD", "BAC", "COST", "DIS" ] 
# MARKET_OPEN = datetime.time(9, 0)
# MARKET_CLOSE = datetime.time(16, 0)

# # 1. Define the Mapping (kept exactly as you had it)
# FUNDAMENTAL_MAPPING = {
#     'last_price': 'last',
#     'bid': 'bid',
#     'ask': 'ask',
#     'bidSize': 'bid_size',
#     'askSize': 'ask_size',
#     'currentPrice': 'current_price',
#     'targetHighPrice': 'target_high_price',
#     'targetLowPrice': 'target_low_price',
#     'targetMeanPrice': 'target_mean_price',
#     'targetMedianPrice': 'target_median_price',
#     'recommendationKey': 'recommendation_key',
#     'numberOfAnalystOpinions': 'number_of_analyst_opinions',
#     'lastDividendValue': 'last_dividend_value',
#     'lastDividendDate': 'last_dividend_date',
#     'totalCash': 'total_cash',
#     'totalCashPerShare': 'total_cash_per_share',
#     'ebitda': 'ebitda',
#     'totalDebt': 'total_debt',
#     'quickRatio': 'quick_ratio',
#     'currentRatio': 'current_ratio',
#     'totalRevenue': 'total_revenue',
#     'debtToEquity': 'debt_to_equity',
#     'revenuePerShare': 'revenue_per_share',
#     'returnOnAssets': 'return_on_assets',
#     'returnOnEquity': 'return_on_equity',
#     'grossProfits': 'gross_profits',
#     'freeCashflow': 'free_cashflow',
#     'operatingCashflow': 'operating_cashflow',
#     'earningsGrowth': 'earnings_growth',
#     'revenueGrowth': 'revenue_growth',
#     'grossMargins': 'gross_margins',
#     'ebitdaMargins': 'ebitda_margins',
#     'operatingMargins': 'operating_margins',
#     'regularMarketChange': 'regular_market_change',
#     'regularMarketChangePercent': 'regular_market_change_percent',
#     'regularMarketPrice': 'regular_market_price',
#     'postMarketChangePercent': 'post_market_change_percent',
#     'postMarketPrice': 'post_market_price',
#     'postMarketChange': 'post_market_change',
#     'averageDailyVolume3Month': 'average_daily_volume_3_month',
#     'open': 'open_price',
#     'dayLow': 'day_low',
#     'dayHigh': 'day_high',
#     'regularMarketPreviousClose': 'regular_market_previous_close',
#     'regularMarketOpen': 'regular_market_open',
#     'regularMarketDayLow': 'regular_market_day_low',
#     'regularMarketDayHigh': 'regular_market_day_high',
#     'dividendRate': 'dividend_rate',
#     'dividendYield': 'dividend_yield',
#     'trailingPE': 'trail_PE',
#     'forwardPE': 'forward_PE',
#     'trailingEps': 'trail_Eps',
#     'forwardEps': 'forward_Eps',
#     'previous_close': 'prev_close',
#     'volume': 'volume',
#     'regularMarketVolume': 'reg_market_volume',
#     'averageVolume': 'avg_volume',
#     'averageVolume10days': 'ave_10d_volume',
#     'currency': 'currency',
#     'exchange': 'exchange',
#     'profitMargins': 'profit_margin',
#     'floatShares': 'float_shares',
#     'enterpriseValue': 'company_value',
#     'sharesOutstanding': 'outstanding_share',
#     'sharesShort': 'share_short',
#     'sharesPercentSharesOut': 'share_percent_shares_out',
#     'heldPercentInsiders': 'insider_held_percent',
#     'heldPercentInstitutions': 'institution_held_percent',
#     'shortRatio': 'short_ratio',
#     'shortPercentOfFloat': 'short_percent_of_float',
#     'impliedSharesOutstanding': 'implied_shares_outstanding',
#     'bookValue': 'book_value',
#     'priceToBook': 'price_2_book'
# }

# def get_fundamental_data(ticker):
#     """
#     Fetches the current fundamental snapshot for a ticker and saves it to the database.
#     """
#     print(f"[{ticker}] Fetching fundamental data...")

#     TARGET_FOLDER = r"/home/miniadmin/USB_MP/Stock/Daily_DB"

#     # 3. Create the full path safely
#     db_path = os.path.join(TARGET_FOLDER, DB_NAME)

#     try:
#         stock = yf.Ticker(ticker)
#         info_data = stock.info
        
#         row_data = {
#             'ticker': ticker,
#             'time_scraped': datetime.datetime.now()
#         }

#         for api_key, db_column in FUNDAMENTAL_MAPPING.items():
#             row_data[db_column] = info_data.get(api_key, None)
            
#             # Fallback logic
#             if db_column == 'trail_Eps' and row_data[db_column] is None:
#                 row_data[db_column] = info_data.get('epsTrailing', None)
#             if db_column == 'forward_Eps' and row_data[db_column] is None:
#                 row_data[db_column] = info_data.get('epsForward', None)
#             if db_column == 'volume' and row_data[db_column] is None:
#                 row_data[db_column] = info_data.get('last_volume', None)

#         df = pd.DataFrame([row_data])

#         conn = sqlite3.connect(db_path)
#         # Using 'append' so every 20 mins adds a new snapshot (Time Series data)
#         df.to_sql("stock_fundamentals", conn, if_exists="append", index=False)
#         conn.close()

#         print(f"[{ticker}] Successfully saved fundamentals.")

#     except Exception as e:
#         print(f"[{ticker}] Error: {e}")

# def job():
#     """
#     This function checks if the current time is within market hours.
#     If yes, it runs the scraper.
#     """
#     now = datetime.datetime.now().time()
#     # Check if within 9:00 AM to 4:00 PM
#     if MARKET_OPEN <= now <= MARKET_CLOSE:
#         print(f"\n--- Starting Scheduled Run: {datetime.datetime.now()} ---")
#         for symbol in TICKERS:
#             get_fundamental_data(symbol)
#         print("--- Run Complete ---\n")
#     else:
#         print(f"Market Closed. Current time: {now}. Waiting...", end='\r')

# # --- SCHEDULING LOGIC ---
# if __name__ == "__main__":
#     print("Scheduler Started. Waiting for market hours (Mon-Fri, 9:30-16:00)...")

#     # Define the specific exit time
#     EXIT_TIME = datetime.time(16, 00)

#     # 2. Infinite loop to keep the script running
#     while True:
#         now = datetime.datetime.now()
        
#         # --- EXIT CHECK ---
#         # If the current time is past 5:00 PM, break the loop to exit
#         if now.time() >= EXIT_TIME:
#             print(f"Current time is {now.strftime('%H:%M')}. Past 4:00 PM. Exiting program.")
#             break

#         # Check if today is a weekday (0=Mon, 4=Fri)
#         if now.weekday() < 5:
#             job()
#         else:
#             # Optional: Pass on weekends
#             pass
            
#         # Check every 60 seconds (better accuracy than 500s)
#         time.sleep(600)

# in the while true loop I would like to add a log file. saving the one output result in a log txt 
'''
