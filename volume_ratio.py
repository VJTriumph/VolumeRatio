import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import json

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DATA_FOLDER   = "data"
OUTPUT_FOLDER = "output"
STOCK_LIST    = os.path.join(DATA_FOLDER, "niftytotalmarket_list .csv")
LOOKBACK_DAYS = 15   # trading days for avg volume

# ─────────────────────────────────────────────
# SETUP FOLDERS
# ─────────────────────────────────────────────
os.makedirs(DATA_FOLDER,   exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ─────────────────────────────────────────────
# READ STOCK LIST
# ─────────────────────────────────────────────
print("=" * 60)
print("   VOLUME RATIO DASHBOARD - DATA UPDATER")
print("=" * 60)
print()
print("Reading stock list...")

if not os.path.exists(STOCK_LIST):
    print(f"ERROR: Stock list not found at {STOCK_LIST}")
    print("Please upload your stock_list.csv to the data/ folder")
    exit(1)

df_stocks = pd.read_csv(STOCK_LIST)

# Normalize column names
df_stocks.columns = df_stocks.columns.str.strip().str.lower()

# Auto-detect column names
def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return df.columns[0]

symbols_col = find_col(df_stocks, ["symbol", "ticker", "scrip", "nse symbol", "bse symbol"])
name_col    = find_col(df_stocks, ["company name", "name", "company", "stock name"])
sector_col  = find_col(df_stocks, ["sector", "industry", "segment"])

# Verify optional cols exist
name_col   = name_col   if name_col   in df_stocks.columns else None
sector_col = sector_col if sector_col in df_stocks.columns else None

stocks = df_stocks[symbols_col].dropna().str.strip().tolist()
print(f"Total stocks loaded  : {len(stocks)}")
print(f"Symbol column        : {symbols_col}")
print(f"Company name column  : {name_col or 'Not found'}")
print(f"Sector column        : {sector_col or 'Not found'}")
print()

# ─────────────────────────────────────────────
# FETCH DATA FROM YAHOO FINANCE
# ─────────────────────────────────────────────
end_date   = datetime.today()
start_date = end_date - timedelta(days=LOOKBACK_DAYS * 3)

results = []
failed  = []

print(f"Fetching data for {len(stocks)} stocks from Yahoo Finance...")
print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print("-" * 60)

for i, symbol in enumerate(stocks, 1):
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
        )

        if hist.empty or len(hist) < 2:
            print(f"  [{i:4d}/{len(stocks)}] {symbol:<20} SKIP - No/insufficient data")
            failed.append({"symbol": symbol, "reason": "no data"})
            continue

        hist = hist.tail(LOOKBACK_DAYS + 1)

        today_volume   = int(hist["Volume"].iloc[-1])
        prev_volumes   = hist["Volume"].iloc[:-1]
        avg_volume_15d = int(prev_volumes.mean()) if len(prev_volumes) > 0 else 0
        volume_ratio   = round(today_volume / avg_volume_15d, 4) if avg_volume_15d > 0 else 0

        today_close = round(float(hist["Close"].iloc[-1]), 2)
        prev_close  = round(float(hist["Close"].iloc[-2]), 2)
        pct_change  = round(((today_close - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0

        today_high  = round(float(hist["High"].iloc[-1]), 2)
        today_low   = round(float(hist["Low"].iloc[-1]), 2)
        today_open  = round(float(hist["Open"].iloc[-1]), 2)

        row          = df_stocks[df_stocks[symbols_col].str.strip() == symbol]
        company_name = str(row[name_col].values[0]).strip()   if name_col   and not row.empty else symbol
        sector       = str(row[sector_col].values[0]).strip() if sector_col and not row.empty else "N/A"

        results.append({
            "Symbol"         : symbol,
            "Company Name"   : company_name,
            "Sector"         : sector,
            "Open"           : today_open,
            "High"           : today_high,
            "Low"            : today_low,
            "LTP"            : today_close,
            "Pct Change"     : pct_change,
            "Today Volume"   : today_volume,
            "15D Avg Volume" : avg_volume_15d,
            "Volume Ratio"   : volume_ratio,
            "Days of Data"   : len(hist),
            "Date"           : hist.index[-1].strftime("%Y-%m-%d"),
        })

        if i % 25 == 0 or i == len(stocks):
            print(f"  Progress: {i}/{len(stocks)} | Results: {len(results)} | Failed: {len(failed)}")

        time.sleep(0.05)

    except Exception as e:
        print(f"  [{i:4d}/{len(stocks)}] {symbol:<20} ERROR - {str(e)[:60]}")
        failed.append({"symbol": symbol, "reason": str(e)})

print("-" * 60)

# ─────────────────────────────────────────────
# BUILD & SORT DATAFRAME
# ─────────────────────────────────────────────
if not results:
    print("No data fetched. Check your symbols (should end with .NS or .BO)")
    exit(1)

df_result = pd.DataFrame(results)
df_result = df_result.sort_values("Volume Ratio", ascending=False).reset_index(drop=True)
df_result.insert(0, "Rank", range(1, len(df_result) + 1))

# ─────────────────────────────────────────────
# SAVE CSV
# ─────────────────────────────────────────────
today_str  = datetime.today().strftime("%Y%m%d")
csv_dated  = os.path.join(OUTPUT_FOLDER, f"volume_ratio_{today_str}.csv")
csv_latest = os.path.join(OUTPUT_FOLDER, "volume_ratio_latest.csv")

df_result.to_csv(csv_dated,  index=False)
df_result.to_csv(csv_latest, index=False)

print(f"\nCSV (dated)  : {csv_dated}")
print(f"CSV (latest) : {csv_latest}")

# ─────────────────────────────────────────────
# SAVE JSON FOR DASHBOARD
# ─────────────────────────────────────────────
json_file = os.path.join(OUTPUT_FOLDER, "latest.json")
meta = {
    "last_updated"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_stocks"  : len(results),
    "failed_stocks" : len(failed),
    "csv_file"      : csv_dated,
    "data"          : df_result.to_dict(orient="records")
}
with open(json_file, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=2, ensure_ascii=False)

print(f"JSON         : {json_file}")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────
extreme = len(df_result[df_result["Volume Ratio"] > 5])
high    = len(df_result[(df_result["Volume Ratio"] > 3) & (df_result["Volume Ratio"] <= 5)])
medium  = len(df_result[(df_result["Volume Ratio"] > 1.5) & (df_result["Volume Ratio"] <= 3)])
normal  = len(df_result[df_result["Volume Ratio"] <= 1.5])

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Stocks processed     : {len(results)}")
print(f"  Failed / skipped     : {len(failed)}")
print(f"  Date                 : {df_result['Date'].iloc[0]}")
print()
print("  Volume Ratio Breakdown:")
print(f"    Extreme  (>5x)     : {extreme}")
print(f"    High     (3x-5x)   : {high}")
print(f"    Moderate (1.5x-3x) : {medium}")
print(f"    Normal   (<1.5x)   : {normal}")
print()
if failed:
    print(f"  Failed symbols (first 10):")
    for f in failed[:10]:
        print(f"    {f['symbol']}: {f['reason'][:50]}")
    if len(failed) > 10:
        print(f"    ... and {len(failed)-10} more")
    print()
print("Done! Open dashboard.html in your browser.")
print("=" * 60)
