import os
import json
import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# === 指標計算 ===
def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def add_indicators(df):
    df = df.copy()
    df["SMA20"] = df["Close"].rolling(20).mean()
    df["SMA50"] = df["Close"].rolling(50).mean()
    df["SMA200"] = df["Close"].rolling(200).mean()

    mid = df["Close"].rolling(20).mean()
    std = df["Close"].rolling(20).std(ddof=0)
    df["BB_Mid"] = mid
    df["BB_Upper"] = mid + 2 * std
    df["BB_Lower"] = mid - 2 * std

    # ✅ 確保 RSI 是一維
    close_series = pd.Series(df["Close"].values, index=df.index)
    df["RSI14"] = rsi(close_series, 14)
    return df

def generate_signal(row):
    signals = []

    # RSI
    if row["RSI14"] < 30:
        signals.append("超賣，可能反彈（偏多）")
    elif row["RSI14"] > 70:
        signals.append("超買，可能回檔（偏空）")

    # 均線
    if row["Close"] > row["SMA20"] and row["SMA20"] > row["SMA50"]:
        signals.append("短中期均線多頭排列（偏多）")
    elif row["Close"] < row["SMA20"] and row["SMA20"] < row["SMA50"]:
        signals.append("短中期均線空頭排列（偏空）")

    # 布林通道
    if row["Close"] > row["BB_Upper"]:
        signals.append("價格突破布林上軌，可能過熱")
    elif row["Close"] < row["BB_Lower"]:
        signals.append("價格跌破布林下軌，可能超跌")

    return "；".join(signals) if signals else "觀望"

# === Google Sheets 連線 ===
def get_gspread_client():
    service_account_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    return client

def upload_to_sheets(sheet_id, df, tab_name):
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="1000", cols="30")

    ws.clear()
    ws.update([df.columns.values.tolist()] + df.values.tolist())

# === 主程式 ===
def main():
    SHEET_ID = os.environ["SHEET_ID"]

    with open("config.json", "r") as f:
        config = json.load(f)

    tickers = config["tickers"]

    all_data = []
    for ticker in tickers:
        print(f"[INFO] 下載 {ticker} 數據中...")
        df = yf.download(ticker, period="1y", interval="1d")
        if df.empty:
            print(f"[WARN] {ticker} 找不到數據，已跳過")
            continue

        df = df.reset_index()
        df = df.rename(columns={
            "Date": "Date", "Open": "Open", "High": "High",
            "Low": "Low", "Close": "Close", "Volume": "Volume"
        })

        df = add_indicators(df)
        df["操作建議"] = df.apply(generate_signal, axis=1)

        latest = df.iloc[-1].to_dict()
        latest["Ticker"] = ticker
        all_data.append(latest)

    if not all_data:
        print("[ERROR] 沒有任何數據")
        return

    result = pd.DataFrame(all_data)
    result["更新時間"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    upload_to_sheets(SHEET_ID, result, "Top5_hot20")
    print("[INFO] 資料已更新到 Google Sheets")

if __name__ == "__main__":
    main()
