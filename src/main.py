import yfinance as yf
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os

# 取得 Google Sheets client
def get_gspread_client():
    creds_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
    creds = Credentials.from_service_account_info(eval(creds_json), scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds)

# 計算技術指標
def add_indicators(df):
    df['RSI14'] = df['Close'].rolling(window=14).apply(lambda x: (
        100 - (100 / (1 + (x.pct_change().add(1).cumprod().iloc[-1] - 1) /
        abs(x.pct_change().add(1).cumprod().iloc[-1] - 1)))
    ) if len(x) == 14 else None, raw=False)

    df['SMA20'] = df['Close'].rolling(window=20).mean()
    df['SMA50'] = df['Close'].rolling(window=50).mean()
    df['SMA200'] = df['Close'].rolling(window=200).mean()

    df['BB_Mid'] = df['Close'].rolling(window=20).mean()
    df['BB_Std'] = df['Close'].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Mid'] + 2 * df['BB_Std']
    df['BB_Lower'] = df['BB_Mid'] - 2 * df['BB_Std']
    return df

# 建議訊號：只取最新一日數據
def make_advice(df):
    try:
        rsi = df['RSI14'].iloc[-1]
        close = df['Close'].iloc[-1]
        upper = df['BB_Upper'].iloc[-1]
        lower = df['BB_Lower'].iloc[-1]

        if pd.isna(rsi) or pd.isna(close) or pd.isna(upper) or pd.isna(lower):
            return "資料不足"

        if rsi < 30 and close > lower:
            return "建議買進"
        elif rsi > 70 and close < upper:
            return "建議賣出"
        else:
            return "觀望"
    except Exception as e:
        return f"錯誤: {e}"

# 主程式
def main():
    SHEET_ID = os.environ.get("SHEET_ID")
    tickers = ["2330.TW", "2317.TW", "2303.TW", "2881.TW", "2882.TW"]

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)
    worksheet = sh.sheet1

    rows = []
    for ticker in tickers:
        print(f"[INFO] 抓取 {ticker} 資料中...")
        df = yf.download(ticker, period="1y", interval="1d")
        if df.empty:
            rows.append([ticker, "查無資料"])
            continue

        df = add_indicators(df)
        advice = make_advice(df)

        rows.append([
            ticker,
            df.index[-1].strftime("%Y-%m-%d"),
            df['Open'].iloc[-1],
            df['High'].iloc[-1],
            df['Low'].iloc[-1],
            df['Close'].iloc[-1],
            df['Volume'].iloc[-1],
            df['RSI14'].iloc[-1],
            df['SMA20'].iloc[-1],
            df['SMA50'].iloc[-1],
            df['SMA200'].iloc[-1],
            df['BB_Upper'].iloc[-1],
            df['BB_Lower'].iloc[-1],
            advice
        ])

    # 清空並更新 Google Sheet
    worksheet.clear()
    worksheet.update(
        [["代號", "日期", "Open", "High", "Low", "Close", "Volume",
          "RSI14", "SMA20", "SMA50", "SMA200", "BB_Upper", "BB_Lower", "建議"]] + rows
    )
    print("[INFO] 已更新至 Google Sheet")

if __name__ == "__main__":
    main()
