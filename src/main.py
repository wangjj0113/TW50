import os
import json
import requests
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# 取得 config.json 的路徑（假設 config.json 在專案根目錄）
HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# -------- 技術指標 --------
def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def rsi_wilder(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def bbands(series: pd.Series, length: int = 20, k: float = 2.0):
    basis = series.rolling(length, min_periods=length).mean()
    dev   = series.rolling(length, min_periods=length).std(ddof=0)
    upper = basis + k * dev
    lower = basis - k * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# -------- FinMind 抓取股價 --------
def fetch_tw_stock(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """呼叫 FinMind API 取得台股歷史資料"""
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise RuntimeError("環境變數 FINMIND_TOKEN 未設定，請確認 Secrets 名稱與 workflow 設定。")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": ticker,
        "start_date": start_date,
        "end_date": end_date
    }
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    data = resp.json()
    if data.get("status") != 200:
        raise RuntimeError(f"FinMind API 回傳錯誤: {data}")
    df = pd.DataFrame(data["data"])
    if df.empty:
        return pd.DataFrame()

    # 重新命名欄位與格式化
    df.rename(columns={
        "date": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "Trading_Volume": "Volume"
    }, inplace=True)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# -------- 計算技術指標 --------
def add_indicators(df: pd.DataFrame, ticker: str, cfg: dict) -> pd.DataFrame:
    close = df["Close"].astype(float)
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"]  = basis
    df[f"BB_{bb_len}_Upper"]  = upper
    df[f"BB_{bb_len}_Lower"]  = lower
    df[f"BB_{bb_len}_Width"]  = width

    df.insert(0, "Ticker", ticker)
    df.reset_index(inplace=True)

    cols = (
        ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume", f"RSI_{rsi_len}"] +
        [f"SMA_{w}" for w in cfg.get("sma_windows", [20, 50, 200])] +
        [f"BB_{bb_len}_Basis", f"BB_{bb_len}_Upper", f"BB_{bb_len}_Lower", f"BB_{bb_len}_Width"]
    )
    return df.loc[:, [c for c in cols if c in df.columns]]

# -------- 選股篩選（長期/短線） --------
def filter_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    範例篩選條件：
      - 長期持有：收盤價 > 200 日均線 AND 20 日均線 > 50 日均線 AND RSI_14 < 70
      - 短線機會：RSI_14 < 30 OR 收盤價 < 布林下軌
    回傳不重覆的前 10 檔股票。
    """
    long_term  = df[
        (df["Close"] > df["SMA_200"]) &
        (df["SMA_20"] > df["SMA_50"]) &
        (df["RSI_14"] < 70)
    ]
    short_term = df[
        (df["RSI_14"] < 30) |
        (df["Close"] < df["BB_20_Lower"])
    ]
    combined = pd.concat([long_term, short_term]).drop_duplicates(subset=["Ticker"])
    return combined.head(10)

# -------- Google Sheets 相關 --------
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
    tickers    = cfg.get("tickers", [])
    start_date = cfg.get("start_date")
    end_date   = cfg.get("end_date")

    frames = []
    for ticker in tickers:
        raw = fetch_tw_stock(ticker, start_date, end_date)
        if not raw.empty:
            frames.append(add_indicators(raw, ticker, cfg))

    if not frames:
        raise RuntimeError("沒有任何股票資料被成功抓取，請檢查代號或日期。")

    out = pd.concat(frames, ignore_index=True)

    # 寫入完整資料到指定工作表
    gc = gspread_client()
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws_main = sh.worksheet(cfg["worksheet"])
    except gspread.WorksheetNotFound:
        ws_main = sh.add_worksheet(title=cfg["worksheet"], rows="1000", cols="30")
    write_dataframe(ws_main, out)

    # 篩選出前 10 檔候選股
    top10 = filter_candidates(out)

    # 寫入 Top10 工作表，名稱可在 config.json 中用 worksheet_top10 自定義，預設為 "Top10"
    top10_name = cfg.get("worksheet_top10", "Top10")
    try:
        ws_top10 = sh.worksheet(top10_name)
    except gspread.WorksheetNotFound:
        ws_top10 = sh.add_worksheet(title=top10_name, rows="100", cols="30")

    write_dataframe(ws_top10, top10)

    # 在 log 中列印出選出的股票代號
    print("篩選出的股票代號（不超過 10 檔）：", top10["Ticker"].tolist())

if __name__ == "__main__":
    main()
