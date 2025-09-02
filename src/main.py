import os
import json
import requests
import pandas as pd
import numpy as np
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials

# ====== 技術指標 ======
def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def sma(series, window):
    return series.rolling(window).mean()

def bbands(series, window=20, num_std=2):
    sma_ = series.rolling(window).mean()
    std_ = series.rolling(window).std()
    upper = sma_ + num_std * std_
    lower = sma_ - num_std * std_
    width = upper - lower
    return sma_, upper, lower, width

# 股票代號 → 中文名稱
NAME_MAP = {
    "2330": "台積電",
    "2317": "鴻海",
    "2881": "富邦金",
    "2882": "國泰金",
    "2454": "聯發科",
    "2308": "台達電",
    "0050": "元大台灣50",
}

# ====== FinMind API ======
def fetch_stock_price(ticker, start_date, end_date, token):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "token": token,
    }
    r = requests.get(url, params=params)
    data = r.json()
    if "data" not in data or len(data["data"]) == 0:
        return None
    df = pd.DataFrame(data["data"])
    df = df.rename(columns={
        "date": "Date",
        "stock_id": "Ticker",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    })
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# ====== 加入技術指標 ======
def add_indicators(df, cfg):
    close = df["Close"]

    # RSI
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi(close, rsi_len)

    # SMA
    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, w)

    # 布林通道
    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    # 短線訊號
    df["ShortSignal"] = np.where(
        (df[f"RSI_{rsi_len}"] < 30) | (df["Close"] < df[f"BB_{bb_len}_Lower"]),
        "Buy",
        np.where(
            (df[f"RSI_{rsi_len}"] > 70) | (df["Close"] > df[f"BB_{bb_len}_Upper"]),
            "Sell",
            "Neutral"
        )
    )

    # 長期趨勢
    df["LongTrend"] = np.where(
        (df["Close"] > df["SMA_200"]) & (df["SMA_20"] > df["SMA_50"]),
        "Uptrend",
        np.where(
            (df["Close"] < df["SMA_200"]) & (df["SMA_20"] < df["SMA_50"]),
            "Downtrend",
            "Neutral"
        )
    )

    # 股票名稱
    df["Name"] = df["Ticker"].map(NAME_MAP).fillna(df["Ticker"])

    return df

# ====== Google Sheets ======
def get_gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

def write_dataframe(ws, df):
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.clear()
    ws.update("A2", values)
    # 更新時間戳
    now = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    ws.update("A1", f"Last Update (Asia/Taipei): {now}")

# ====== 主程式 ======
def main():
    # 載入 config
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    tickers = cfg["tickers"]
    start_date = cfg.get("start_date", "2025-01-01")
    end_date = cfg.get("end_date", "2025-12-31")
    sheet_id = cfg["sheet_id"]

    token = os.environ.get("FINMIND_TOKEN", "")

    all_data = []
    for t in tickers:
        df = fetch_stock_price(t, start_date, end_date, token)
        if df is None:
            continue
        df = add_indicators(df, cfg)
        all_data.append(df)

    if not all_data:
        raise RuntimeError("No data fetched")

    result = pd.concat(all_data, ignore_index=True)

    # 建立 Sheets 連線
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)

    # 主表 TW50
    ws_main = sh.worksheet(cfg.get("worksheet", "TW50"))
    write_dataframe(ws_main, result)

    # Top10
    ws_top10 = sh.worksheet(cfg.get("worksheet_top10", "Top10"))
    result_latest = result.sort_values(["Date", f"RSI_{cfg['rsi_length']}"]).drop_duplicates("Ticker", keep="last")
    top10 = result_latest.head(10)
    write_dataframe(ws_top10, top10)

if __name__ == "__main__":
    main()
