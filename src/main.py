import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime

# Google Sheet 認證
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GCP_SERVICE_ACCOUNT_JSON"]

creds = Credentials.from_service_account_info(eval(SERVICE_ACCOUNT_JSON), scopes=["https://www.googleapis.com/auth/spreadsheets"])
client = gspread.authorize(creds)
sheet = client.open_by_key(SHEET_ID)

# === 新增操作建議產生器 ===
def generate_signal(row):
    rsi = row["RSI14"]
    close = row["Close"]
    sma20 = row["SMA20"]
    bb_upper = row["BB_Upper"]
    bb_lower = row["BB_Lower"]

    if pd.isna(rsi) or pd.isna(sma20) or pd.isna(bb_upper) or pd.isna(bb_lower):
        return "資料不足，觀望"

    # RSI 判斷
    if rsi > 70:
        return "RSI過熱 → 觀望或停利"
    elif rsi < 30:
        return "RSI低檔 → 可考慮分批佈局"

    # 布林通道判斷
    if close >= bb_upper:
        return "接近壓力區 → 建議賣出或觀望"
    elif close <= bb_lower:
        return "接近支撐區 → 建議買入"

    # SMA20 判斷
    if close > sma20:
        return "多頭格局 → 拉回可考慮買入"
    elif close < sma20:
        return "空頭格局 → 站回均線再觀望"

    return "訊號不明 → 觀望"

# === 主程式 ===
def main():
    tickers = ["2330.TW", "2317.TW", "2303.TW", "2308.TW", "2881.TW"]  # 測試用
    dfs = []

    for ticker in tickers:
        data = yf.download(ticker, period="6mo")
        if data.empty:
            print(f"⚠️ {ticker} 沒有資料，跳過")
            continue

        df = data.tail(1).copy()
        df["Ticker"] = ticker

        # 計算 RSI, SMA, BB
        df["SMA20"] = data["Close"].rolling(window=20).mean().iloc[-1]
        df["SMA50"] = data["Close"].rolling(window=50).mean().iloc[-1]
        df["RSI14"] = compute_rsi(data["Close"], 14)
        df["BB_Mid"] = df["SMA20"]
        df["BB_Upper"] = df["SMA20"] + 2 * data["Close"].rolling(20).std().iloc[-1]
        df["BB_Lower"] = df["SMA20"] - 2 * data["Close"].rolling(20).std().iloc[-1]

        # === 新增操作建議 ===
        df["操作建議"] = df.apply(generate_signal, axis=1)

        dfs.append(df)

    if dfs:
        result = pd.concat(dfs)
        ws = sheet.worksheet("Top5_hot20")
        ws.update([result.reset_index().columns.values.tolist()] + result.reset_index().values.tolist())
        print("✅ 已更新 Google Sheet，包含操作建議")

# RSI 計算函式
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

if __name__ == "__main__":
    main()
