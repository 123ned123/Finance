# ******** this is the production version to get the close and open prices and save to a specific folder ********

import pandas as pd
import yfinance as yf
import sqlite3
import datetime
import os  # <--- Import this to handle paths

tickers = [ "SPY" ,"NVDA", "AAPL", "MSFT", "AMZN" \
           ,"GOOGL", "META", "TSLA", "BRK-B", "JPM" \
            ,"AVGO", "INTC", "WMT", "UNH", "V", "PYPL" \
            ,"MA", "HD", "BAC", "COST", "DIS", "TSM", \
          "ARM", "BABA"] 

def get_ticker(tickers):
    
    # --- CONFIGURATION ---
    # 1. Define the specific folder path
    # Windows Example: r"C:\Users\YourName\Documents\FinanceData"
    # Mac/Linux Example: "/Users/yourname/Documents/FinanceData"
    # The 'r' before the string handles backslashes safely in Windows
    target_folder = r"/home/miniadmin/USB_MP/Stock/Daily_DB"
    
    # 2. Define the file name
    db_filename = "market_data_daily.db"
    
    # 3. Create the full path safely
    db_path = os.path.join(target_folder, db_filename)

    # 4. Check if directory exists, if not, create it automatically
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        print(f"Created directory: {target_folder}")

    print(f"Database will be saved to: {db_path}")

    # --- MAIN LOOP ---
    start_date = "2001-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")

        try:
            data = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=False)
            
            if data.empty:
                continue

            data.reset_index(inplace=True)
            data['ticker'] = ticker

            data = data.rename(columns={
                'Date': 'date', 'Open': 'open', 'High': 'high', 
                'Low': 'low', 'Close': 'close', 'Adj Close': 'adj_close', 
                'Volume': 'volume'
            })

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            final_columns = ['date', 'ticker', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
            data = data[final_columns]

            # --- CONNECT USING THE FULL PATH ---
            conn = sqlite3.connect(db_path)  # <--- Use db_path here
            
            table_name = ticker.replace("-", "_")
            data.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()
            
            print(f"Saved {ticker} to {db_path}")
        
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

if __name__ == "__main__":
    get_ticker(tickers)