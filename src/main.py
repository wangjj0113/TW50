import os
import json
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timezone, timedelta

# 連線到 Google Sheet
def connect_google_sheet():
    creds = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    client = gspread.service_account_from_dict(creds)
    sheet_id = os.environ["SHEET_ID"]
    return client.open_by_key(sheet_id)

# 計算技術指標
def add_indicators(df, sma_windows=[20, 50, 200], rsi_len=14, bb_len=20):
    df["SMA_20"] = df["Close"].rolling(20).mean()
    df["SMA_50"] = df["Close"].rolling(50).mean()
    df["SMA_200"] = df["Close"].rolling(200).mean()

    # RSI
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(rsi_len).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(rsi_len).mean()
    rs = gain / loss
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # 布林通道
    df["BB_Mid"] = df["Close"].rolling(bb_len).mean()
    df["BB_Std"] = df["Close"].rolling(bb_len).std()
    df["BB_Upper"] = df["BB_Mid"] + 2 * df["BB_Std"]
    df["BB_Lower"] = df["BB_Mid"] - 2 * df["BB_Std"]

    # 簡單買賣訊號
    df["ShortTrend"] = df["SMA_20"] > df["SMA_50"]
    df["LongTrend"] = df["SMA_50"] > df["SMA_200"]

    return df

# 更新 Google Sheet
def write_sheet(ws, df):
    ws.clear()
    set_with_dataframe(ws, df)

def write_timestamp(ws):
    now = datetime.now(timezone(timedelta(hours=8)))  # 台北時間
    ws.update("A1", f"Last Update (Asia/Taipei): {now.strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    mode = os.environ.get("MODE", "dev")
    sheet = connect_google_sheet()

    # 讀取 config.json
    with open("config.json", "r") as f:
        cfg = json.load(f)

    if mode not in cfg:
        raise RuntimeError(f"❌ config.json 缺少 {mode} 設定")

    for name, sheet_name in cfg[mode].items():
        tickers = cfg["tickers"].get(name, [])
        if not tickers:
            print(f"⚠️ {name} 沒有設定 tickers，跳過")
            continue

        print(f"[INFO] 下載 {name}: {tickers}")
        data = yf.download(tickers, period="6mo", interval="1d", group_by="ticker", auto_adjust=True)

        frames = []
        for t in tickers:
            df = data[t].copy()
            df["Ticker"] = t
            df = add_indicators(df)
            frames.append(df)

        full = pd.concat(frames)
        ws = sheet.worksheet(sheet_name)
        write_sheet(ws, full.reset_index())
        write_timestamp(ws)

if __name__ == "__main__":
    main()
