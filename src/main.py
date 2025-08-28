import os, json, requests
from typing import List
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------- 技術指標 ----------
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

# ---------- FinMind 抓資料 ----------
def fetch_tw_stock(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise RuntimeError("找不到 FINMIND_TOKEN，請在 GitHub Secrets 設定。")

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": ticker,
        "start_date": start_date,
        "end_date": end_date
    }
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, params=params, headers=headers)
    data = r.json()
    if data.get("status") != 200:
        raise RuntimeError(f"FinMind error: {data}")
    df = pd.DataFrame(data["data"])
    if df.empty:
        return pd.DataFrame()

    df.rename(columns={
        "date": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "Trading_Volume": "Volume"
    }, inplace=True)

    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=False)
    return df

# ---------- 加上技術指標 ----------
def add_indicators(df: pd.DataFrame, ticker: str, cfg) -> pd.DataFrame:
    close = df["Close"].astype(float)

    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    df.insert(0, "Ticker", ticker)
    df.reset_index(inplace=True)

    cols = [
        "Date","Ticker","Open","High","Low","Close","Volume",
        f"RSI_{rsi_len}"
    ] + [f"SMA_{w}" for w in cfg.get("sma_windows", [20, 50, 200])] + [
        f"BB_{bb_len}_Basis", f"BB_{bb_len}_Upper", f"BB_{bb_len}_Lower", f"BB_{bb_len}_Width"
    ]
    return df.loc[:, [c for c in cols if c in df.columns]]

# ---------- Google Sheets ----------
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
    # --- START: 診斷用程式碼 ---
    print("--- 診斷資訊 ---")
    sheet_id_in_cfg = cfg.get("sheet_id", "未在 config.json 中找到 sheet_id")
    print(f"讀取到的 Sheet ID: '{sheet_id_in_cfg}'")
    print("--------------------")
    # --- END: 診斷用程式碼 ---

    # 股票清單模式
    mode = cfg.get("ticker_mode", "list")
    if mode == "auto_etf":
        etf = cfg.get("etf", "0050")
        end_date = cfg.get("end_date", pd.Timestamp.today().strftime("%Y-%m-%d"))
        tickers = get_etf_components(etf, end_date)
        print(f"{etf} 成分股 {len(tickers)} 檔：", ", ".join(tickers[:10]), "...")
    else:
        tickers = cfg.get("tickers", [])

    frames = []
    for t in tickers:
        raw = fetch_tw_stock(t, cfg.get("start_date","2025-01-01"), cfg.get("end_date","2025-12-31"))
        if raw.empty:
            print(f"跳過 {t}（沒資料）")
            continue
        frames.append(add_indicators(raw, t, cfg))

    if not frames:
        raise RuntimeError("沒有任何股票資料")

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
