import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, timezone
import gspread
from gspread_dataframe import set_with_dataframe


# ========= 公用：連接 Google Sheet =========
def connect_google_sheet():
    creds = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    client = gspread.service_account_from_dict(creds)
    sheet_id = os.getenv("SHEET_ID")
    return client.open_by_key(sheet_id)


# ========= 公用：寫入更新時間 =========
def write_timestamp(ws, cell="A1", label="Last Update (Asia/Taipei)"):
    """
    在指定工作表 ws 的 cell（預設 A1）寫入「Last Update (Asia/Taipei): YYYY-MM-DD HH:MM:SS」。
    """
    taipei_now = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
    ws.update(cell, [[f"{label}: {taipei_now}"]])


# ========= 技術指標計算 =========
def add_indicators(df, sma_windows=[20, 50, 200], rsi_len=14, bb_len=20):
    """計算 SMA、RSI、布林通道"""
    df = df.copy()
    for w in sma_windows:
        df[f"SMA_{w}"] = df["Close"].rolling(window=w).mean()

    # RSI
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=rsi_len).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_len).mean()
    rs = gain / loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # 布林通道
    df["BB_Mid"] = df["Close"].rolling(window=bb_len).mean()
    df["BB_Std"] = df["Close"].rolling(window=bb_len).std()
    df["BB_Upper"] = df["BB_Mid"] + 2 * df["BB_Std"]
    df["BB_Lower"] = df["BB_Mid"] - 2 * df["BB_Std"]

    return df


# ========= 交易訊號 =========
def add_signals(df):
    df["ShortTrend"] = df.apply(lambda x: "Buy" if x["Close"] > x["SMA_20"] else "Sell", axis=1)
    df["LongTrend"] = df.apply(lambda x: "Buy" if x["Close"] > x["SMA_200"] else "Sell", axis=1)
    return df


# ========= 主程式 =========
def main():
    mode = os.getenv("MODE", "dev")
    print(f"[INFO] MODE={mode}")

    # 讀取設定檔
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    tw50_name = cfg[mode]["TW50"]
    top10_name = cfg[mode]["Top10"]

    print(f"[INFO] config 表單名稱: TW50={tw50_name}, Top10={top10_name}")

    # 連接 Google Sheet
    gs = connect_google_sheet()
    ws_tw50 = gs.worksheet(tw50_name)
    ws_top10 = gs.worksheet(top10_name)

    # 股票代碼
    tickers = cfg["tickers"]
    print(f"[INFO] tickers: {tickers}")

    # 抓取資料
    data = []
    for t in tickers:
        print(f"[DL] {t} ...")
        df = yf.download(t, period="6mo", interval="1d", progress=False)
        if df.empty:
            print(f"[WARN] {t} 沒有資料")
            continue
        df = add_indicators(df)
        df = add_signals(df)
        df["Ticker"] = t
        df["Date"] = df.index.strftime("%Y-%m-%d")
        data.append(df)

    if not data:
        raise RuntimeError("沒有成功下載任何股票資料")

    all_df = pd.concat(data)

    # ========== 寫入 TW50 Sheet ==========
    set_with_dataframe(ws_tw50, all_df.reset_index(drop=True))
    write_timestamp(ws_tw50)

    # ========== 寫入 Top10 Sheet ==========
    last_df = all_df.groupby("Ticker").tail(1)
    set_with_dataframe(ws_top10, last_df.reset_index(drop=True))
    write_timestamp(ws_top10)

    print("[INFO] ✅ 完成寫入 Google Sheets")


if __name__ == "__main__":
    main()

