import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import json

DATA_FOLDER    = "data"
OUTPUT_FOLDER  = "output"
STOCK_LIST     = os.path.join(DATA_FOLDER, "niftytotalmarket_list.csv")
LOOKBACK_DAYS  = 15
FETCH_INFO     = False

os.makedirs(DATA_FOLDER,   exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("=" * 60)
print("  VOLUME RATIO DASHBOARD - DATA UPDATER")
print("=" * 60)
print("Reading stock list...")

if not os.path.exists(STOCK_LIST):
    print(f"ERROR: {STOCK_LIST} not found")
    exit(1)

df_stocks = pd.read_csv(STOCK_LIST)
df_stocks.columns = df_stocks.columns.str.strip().str.lower()

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return df.columns[0]

symbols_col = find_col(df_stocks, ["symbol", "ticker", "scrip"])
name_col    = find_col(df_stocks, ["company name", "name", "company"])
sector_col  = find_col(df_stocks, ["sector", "industry", "segment"])
name_col    = name_col   if name_col   in df_stocks.columns else None
sector_col  = sector_col if sector_col in df_stocks.columns else None

# Build lookup dict: base_symbol (no .NS/.BO) -> (name, sector)
sector_map = {}
for _, row in df_stocks.iterrows():
    sym  = str(row[symbols_col]).strip()
    base = sym.replace(".NS","").replace(".BO","").upper()
    sname  = str(row[name_col]).strip()   if name_col   else sym
    ssect  = str(row[sector_col]).strip() if sector_col else "Other"
    if ssect in ("nan","N/A","","None"): ssect = "Other"
    sector_map[base] = (sname, ssect)
    sector_map[sym]  = (sname, ssect)

stocks = df_stocks[symbols_col].dropna().str.strip().tolist()

def ensure_ns(sym):
    if sym.endswith(".NS") or sym.endswith(".BO"):
        return sym
    return sym + ".NS"

stocks = [ensure_ns(s) for s in stocks]
print(f"Loaded {len(stocks)} stocks")

SECTOR_CACHE_FILE = os.path.join(DATA_FOLDER, "sector_cache.json")

def load_sector_cache():
    if os.path.exists(SECTOR_CACHE_FILE):
        try:
            with open(SECTOR_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_sector_cache(cache):
    with open(SECTOR_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

sector_cache = load_sector_cache()
print(f"Sector cache: {len(sector_cache)} entries")

YAHOO_SECTOR_MAP = {
    "Financial Services"    : "Financial Services",
    "Technology"            : "Technology",
    "Healthcare"            : "Healthcare",
    "Consumer Cyclical"     : "Consumer Discretionary",
    "Consumer Defensive"    : "Fast Moving Consumer Goods",
    "Basic Materials"       : "Chemicals",
    "Industrials"           : "Capital Goods",
    "Energy"                : "Energy",
    "Communication Services": "Telecom",
    "Real Estate"           : "Real Estate",
    "Utilities"             : "Power",
}

def get_info(symbol, ticker_obj):
    base = symbol.replace(".NS","").replace(".BO","").upper()
    if base in sector_map:
        sname, ssect = sector_map[base]
        if ssect and ssect not in ("Other","nan","N/A",""):
            return sname, ssect
        name_fallback = sname
    elif symbol in sector_map:
        sname, ssect = sector_map[symbol]
        if ssect and ssect not in ("Other","nan","N/A",""):
            return sname, ssect
        name_fallback = sname
    else:
        name_fallback = base

    if symbol in sector_cache:
        c = sector_cache[symbol]
        return c.get("name", name_fallback), c.get("sector", "Other")

    if FETCH_INFO:
        try:
            info    = ticker_obj.info
            yname   = info.get("longName") or info.get("shortName") or name_fallback
            ysector = info.get("sector") or info.get("industry") or "Other"
            ysector = YAHOO_SECTOR_MAP.get(ysector, ysector)
            sector_cache[symbol] = {"name": yname, "sector": ysector}
            return yname, ysector
        except Exception:
            pass

    return name_fallback, sector_map.get(base, (name_fallback,"Other"))[1]

end_date   = datetime.today()
start_date = end_date - timedelta(days=LOOKBACK_DAYS * 3)

results, failed = [], []
print(f"Fetching {len(stocks)} stocks...")
print("-" * 60)

for i, symbol in enumerate(stocks, 1):
    try:
        ticker = yf.Ticker(symbol)
        hist   = ticker.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
        )
        if hist.empty or len(hist) < 2:
            failed.append({"symbol": symbol, "reason": "no data"})
            continue
        hist      = hist.tail(LOOKBACK_DAYS + 1)
        today_vol = int(hist["Volume"].iloc[-1])
        avg_vol   = int(hist["Volume"].iloc[:-1].mean()) if len(hist) > 1 else 0
        vol_ratio = round(today_vol / avg_vol, 4) if avg_vol > 0 else 0
        close     = round(float(hist["Close"].iloc[-1]), 2)
        prev_close= round(float(hist["Close"].iloc[-2]), 2)
        pct_chg   = round(((close - prev_close) / prev_close) * 100, 2) if prev_close > 0 else 0

        cname, sector = get_info(symbol, ticker)

        results.append({
            "Symbol"        : symbol,
            "Company Name"  : cname,
            "Sector"        : sector,
            "Close"         : close,
            "Pct Change"    : pct_chg,
            "Today Volume"  : today_vol,
            "15D Avg Volume": avg_vol,
            "Volume Ratio"  : vol_ratio,
            "Days of Data"  : len(hist),
            "Date"          : hist.index[-1].strftime("%Y-%m-%d"),
        })
        if i % 50 == 0 or i == len(stocks):
            print(f"  {i}/{len(stocks)}  OK:{len(results)}  Failed:{len(failed)}")
        time.sleep(0.05)
    except Exception as e:
        failed.append({"symbol": symbol, "reason": str(e)})

save_sector_cache(sector_cache)
print(f"Cache saved: {len(sector_cache)} entries")

if not results:
    print("No data fetched.")
    exit(1)

df_result = pd.DataFrame(results)
df_result = df_result.sort_values("Volume Ratio", ascending=False).reset_index(drop=True)
df_result.insert(0, "Rank", range(1, len(df_result) + 1))

today_str  = datetime.today().strftime("%Y%m%d")
csv_dated  = os.path.join(OUTPUT_FOLDER, f"volume_ratio_{today_str}.csv")
csv_latest = os.path.join(OUTPUT_FOLDER, "volume_ratio_latest.csv")
json_file  = os.path.join(OUTPUT_FOLDER, "latest.json")

df_result.to_csv(csv_dated,  index=False)
df_result.to_csv(csv_latest, index=False)

sector_summary = df_result.groupby("Sector").size().sort_values(ascending=False).to_dict()

meta = {
    "last_updated"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_stocks"  : len(results),
    "failed_stocks" : len(failed),
    "sector_summary": sector_summary,
    "data"          : df_result.to_dict(orient="records"),
}
with open(json_file, "w", encoding="utf-8") as f:
    json.dump(meta, f, indent=2, ensure_ascii=False)

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Processed : {len(results)} | Failed: {len(failed)}")
print(f"  Sectors   : {df_result['Sector'].nunique()}")
for s, c in list(sector_summary.items())[:15]:
    print(f"  {s:<35} {c}")
print(f"  CSV  : {csv_latest}")
print(f"  JSON : {json_file}")
print("=" * 60)
print("Done!")
