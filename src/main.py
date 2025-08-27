import os, json
from typing import List
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

ROOT = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(ROOT), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ---- 技術指標 ----
def sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window=window, min_periods=window).mean()

def rsi_wilder(s: pd.Series, length: int = 14) -> pd.Series:
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def bbands(s: pd.Series, length: int = 20, k: float = 2.0):
    basis = s.rolling(length, min_periods=length).mean()
    dev = s.rolling(length, min_periods=length).std(ddof=0)
    upper = basis + k * dev
    lower = basis - k * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# ---- 資料抓取 + 指標計算 ----
def fetch_with_indicators(ticker: str, period: str, interval: str, cfg) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns=str.title)  # Open High Low Close Volume
    close = df["Close"].astype(float)

    # RSI
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    # SMAs
    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    # BBands
    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    df.insert(0, "Ticker", ticker)
    df.reset_index(inplace=True)  # 把 Date 變成欄位

    cols = ["Date","Ticker","Open","High","Low","Close","Volume",
            f"RSI_{rsi_len}"] + [f"SMA_{w}" for w in cfg.get("sma_windows", [20,50,200])] +            [f"BB_{bb_len}_Basis", f"BB_{bb_len}_Upper", f"BB_{bb_len}_Lower", f"BB_{bb_len}_Width"]

    return df.loc[:, [c for c in cols if c in df.columns]

# ---- Google Sheets ----
def gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

def write_dataframe(ws, df: pd.DataFrame):
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.clear()
    ws.update(values, value_input_option="RAW")

def main():
    cfg = load_cfg()
    tickers: List[str] = cfg["tickers"]
    period = cfg.get("period", "6mo")
    interval = cfg.get("interval", "1d")

    frames = []
    for t in tickers:
        df = fetch_with_indicators(t, period, interval, cfg)
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("No data fetched for any ticker.")

    out = pd.concat(frames, ignore_index=True)

    gc = gspread_client()
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws = sh.worksheet(cfg["worksheet"])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=cfg["worksheet"], rows="100", cols="26")

    write_dataframe(ws, out)
    print(f"Done. Rows: {len(out)} | Cols: {len(out.columns)}")

if __name__ == "__main__":
    main()
