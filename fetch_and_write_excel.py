#!/usr/bin/env python3
"""
Fetch NSE Security-wise Deliverable Positions CSV, filter to NIFTY-50,
compute Delivery% and write Nifty50_Delivery.xlsx
"""

import os
import sys
import io
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

NIFTY50_CSV = 'https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv'
REPORT_PAGE_URL = 'https://www.nseindia.com/report-detail/eq_security'
OUT_FILE = 'Nifty50_Delivery.xlsx'
REPORT_CSV_ENV = 'REPORT_CSV_URL'

def session_with_headers():
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com"
    })
    return s

def fetch_nifty50_symbols():
    r = requests.get(NIFTY50_CSV, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    sym_col = None
    for c in df.columns:
        if 'symbol' in c.lower():
            sym_col = c
            break
    if sym_col is None:
        raise RuntimeError("Couldn't find symbol column in NIFTY50 CSV")
    symbols = df[sym_col].astype(str).str.strip().str.upper().unique().tolist()
    return set(symbols)

def discover_csv_link(session):
    env_url = os.getenv(REPORT_CSV_ENV)
    if env_url:
        return env_url
    session.get("https://www.nseindia.com", timeout=30)
    r = session.get(REPORT_PAGE_URL, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    csv_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".csv" in href.lower():
            csv_links.append(href)
    full = []
    for href in csv_links:
        if href.startswith("http"):
            full.append(href)
        else:
            full.append(requests.compat.urljoin("https://www.nseindia.com", href))
    for link in full:
        low = link.lower()
        if "deliver" in low or "security" in low:
            return link
    if full:
        return full[0]
    raise RuntimeError("Could not discover CSV link on NSE report page; provide REPORT_CSV_URL env var")

def download_csv_text(session, csv_url):
    headers = {"User-Agent": USER_AGENT, "Referer": "https://www.nseindia.com"}
    r = session.get(csv_url, headers=headers, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to download CSV: HTTP {r.status_code} for {csv_url}")
    if not r.text or len(r.text) < 50:
        raise RuntimeError("Downloaded CSV appears empty or too small")
    return r.text

def parse_deliverable_csv(csv_text):
    df = pd.read_csv(io.StringIO(csv_text), dtype=str, skip_blank_lines=True)
    cols = {c.lower(): c for c in df.columns}
    sym_col = None
    traded_col = None
    deliver_col = None
    for k, v in cols.items():
        if 'symbol' in k and 'series' not in k:
            sym_col = v
        if 'traded' in k and ('qty' in k or 'quantity' in k):
            traded_col = v
        if 'deliver' in k and ('qty' in k or 'quantity' in k):
            deliver_col = v
    if not sym_col or not traded_col or not deliver_col:
        raise RuntimeError("CSV missing expected columns. Found: " + ", ".join(df.columns))
    out = pd.DataFrame()
    out['Symbol'] = df[sym_col].astype(str).str_strip().str.upper()
    out['TradedQty'] = pd.to_numeric(df[traded_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    out['DeliveryQty'] = pd.to_numeric(df[deliver_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    out['DeliveryPct'] = (out['DeliveryQty'] / out['TradedQty'].replace({0: pd.NA})) * 100
    out['DeliveryPct'] = out['DeliveryPct'].fillna(0).round(2)
    date_col = None
    for c in df.columns:
        if 'date' in c.lower():
            date_col = c
            break
    if date_col:
        out['Date'] = df[date_col].astype(str)
    else:
        out['Date'] = datetime.utcnow().strftime("%Y-%m-%d")
    return out[['Date', 'Symbol', 'TradedQty', 'DeliveryQty', 'DeliveryPct']]

def filter_nifty50(out_df, nifty_symbols):
    out_df['Symbol'] = out_df['Symbol'].astype(str).str.upper()
    return out_df[out_df['Symbol'].isin(nifty_symbols)].reset_index(drop=True)

def write_excel(df, path):
    df.to_excel(path, sheet_name='Delivery', index=False)
    print(f"Wrote {path} rows: {len(df)}")

def main():
    sess = session_with_headers()
    try:
        print("Fetching NIFTY-50 list...")
        nifty_symbols = fetch_nifty50_symbols()
        print("Discovering deliverable CSV link...")
        csv_link = discover_csv_link(sess)
        print("CSV link:", csv_link)
        csv_text = download_csv_text(sess, csv_link)
        print("Parsing CSV...")
        df = parse_deliverable_csv(csv_text)
        print("Filtering to NIFTY-50...")
        df_nifty = filter_nifty50(df, nifty_symbols)
        if df_nifty.empty:
            print("Warning: no NIFTY-50 rows matched. Check CSV format or symbol cases.")
        write_excel(df_nifty, OUT_FILE)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(2)

if __name__ == '__main__':
    main()
