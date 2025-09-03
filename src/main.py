import yfinance as yf
import pandas as pd
import gspread
import json
from datetime import datetime
from google.oauth2.service_account import Credentials

# 載入設定檔
with open("config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

SHEET_ID = cfg["sheet_id"]
WORKSHEET = cfg["worksheet"]

# Google Sheets 權限
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(json.loads(cfg["google_credentials_json"]), scopes=SCOPES)
gc = gspread.authorize(creds)

# 技術指標計算
def add_indicators(df):
    df["RSI_14"] = df["Close"].pct_change().rolling(14).apply(
        lambda x: 100 - (100 / (1 + (x[x > 0].mean() / (-x[x < 0].mean() if x[x < 0].mean() != 0 else 1))))
    )
    df["SMA_20"] = df["Close"].rolling(20).mean()
    df["SMA_50"] = df["Close"].rolling(50).mean()
    df["SMA_200"] = df["Close"].rolling(200).mean()
    df["BB_20_Basis"] = df["Close"].rolling(20).mean()
    df["BB_20_Upper"] = df["BB_20_Basis"] + 2 * df["Close"].rolling(20).std()
    df["BB_20_Lower"] = df["BB_20_Basis"] - 2 * df["Close"].rolling(20).std()
    df["BB_20_Width"] = (df["BB_20_Upper"] - df["BB_20_Lower"]) / df["BB_20_Basis"]
    return df

# 訊號判斷
def add_signals(df):
    df["LongTrend"] = df.apply(lambda x: "Bullish" if x["SMA_20"] > x["SMA_200"] else "Neutral", axis=1)
    df["ShortTrend"] = df.apply(lambda x: "Buy" if x["RSI_14"] < 30 else ("Sell" if x["RSI_14"] > 70 else "Neutral"), axis=1)
    df["EntryZone"] = df.apply(lambda x: True if x["Close"] < x["BB_20_Lower"] else False, axis=1)
    df["ExitZone"] = df.apply(lambda x: True if x["Close"] > x["BB_20_Upper"] else False, axis=1)
    df["ShortSignal"] = df.apply(lambda x: "Buy" if x["EntryZone"] else ("Sell" if x["ExitZone"] else "Hold"), axis=1)
    return df

# 主程式
def main():
    ws = gc.open_by_key(SHEET_ID).worksheet(WORKSHEET)

    all_data = []

    for ticker in cfg["tickers"]:
        print(f"下載 {ticker}...")
        data = yf.download(ticker, period="6mo", interval="1d")
        if data.empty:
            continue
        data = data.reset_index()
        data["Ticker"] = ticker
        data = add_indicators(data)
        data = add_signals(data)
        all_data.append(data)

    if not all_data:
        print("沒有抓到任何資料")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    final_df["Last Update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ws.clear()
    ws.update([final_df.columns.values.tolist()] + final_df.values.tolist())
    print("✅ 已更新到 Google Sheets")

if __name__ == "__main__":
    main()
