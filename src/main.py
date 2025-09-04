# -*- coding: utf-8 -*-
"""
TW50-TOP5 系統 main.py
版本：v2025.09.04-1
功能：
1. 從 yfinance 抓取 TW50 成分股日資料
2. 計算 SMA20/50/200、RSI14、布林通道
3. 生成 5 個分頁：TW50、Top10、Top20、金融股、非金融股
4. Top5 顯示「進場/出場建議區間」
5. 中文標題：Ticker + 名稱
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime, timedelta
import pytz
import json

# ===================== 基本設定 =====================
# 從 GitHub Secrets 讀取 Sheet ID
SHEET_ID = os.environ["SHEET_ID"]

# TW50 成分股（簡化示例，可擴充）
TICKERS = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "2454.TW": "聯發科",
    "2881.TW": "富邦金",
    "2882.TW": "國泰金",
    "2303.TW": "聯電",
    "2308.TW": "台達電",
    "1301.TW": "台塑",
    "2412.TW": "中華電",
    "3711.TW": "日月光投控",
}

# 金融股代號（判斷金融 vs 非金融）
FIN_TICKERS = ["2881.TW", "2882.TW"]

# ===================== 函式 =====================
def add_indicators(df):
    df["SMA20"] = df["Close"].rolling(20).mean()
    df["SMA50"] = df["Close"].rolling(50).mean()
    df["SMA200"] = df["Close"].rolling(200).mean()
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    df["RSI14"] = 100 - (100 / (1 + rs))
    df["BB_Mid"] = df["Close"].rolling(20).mean()
    df["BB_Upper"] = df["BB_Mid"] + 2 * df["Close"].rolling(20).std()
    df["BB_Lower"] = df["BB_Mid"] - 2 * df["Close"].rolling(20).std()
    return df

def get_gspread_client():
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds_dict = json.loads(creds_json)
    return gspread.service_account_from_dict(creds_dict)

def fetch_data():
    all_data = []
    for ticker, name in TICKERS.items():
        df = yf.download(ticker, period="6mo", interval="1d")
        if df.empty:
            print(f"⚠️ 無資料: {ticker}")
            continue
        df = add_indicators(df)
        df = df.tail(1)  # 只取最後一天
        df.insert(0, "Name", name)
        df.insert(0, "Ticker", ticker)
        all_data.append(df)
    return pd.concat(all_data)

def generate_top5(df):
    ranked = df.sort_values("Volume", ascending=False).head(5).copy()
    ranked["Buy_Zone"] = (ranked["BB_Lower"] + ranked["SMA20"]) / 2
    ranked["Sell_Zone"] = (ranked["BB_Upper"] + ranked["SMA20"]) / 2
    return ranked

def update_sheet():
    client = get_gspread_client()
    sheet = client.open_by_key(SHEET_ID)

    tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    df_all = fetch_data()

    # 全量
    ws = sheet.worksheet("TW50") if "TW50" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("TW50", 1000, 20)
    ws.clear()
    ws.update("A1", f"Last Update (Asia/Taipei): {now}")
    set_with_dataframe(ws, df_all.reset_index())

    # Top10
    ws = sheet.worksheet("Top10") if "Top10" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("Top10", 1000, 20)
    ws.clear()
    top10 = df_all.sort_values("RSI14", ascending=False).head(10)
    set_with_dataframe(ws, top10.reset_index())

    # Top20
    ws = sheet.worksheet("Top20") if "Top20" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("Top20", 1000, 20)
    ws.clear()
    top20 = df_all.sort_values("Volume", ascending=False).head(20)
    set_with_dataframe(ws, top20.reset_index())

    # 金融
    ws = sheet.worksheet("Fin") if "Fin" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("Fin", 1000, 20)
    ws.clear()
    fin = df_all[df_all["Ticker"].isin(FIN_TICKERS)]
    set_with_dataframe(ws, fin.reset_index())

    # 非金融
    ws = sheet.worksheet("NonFin") if "NonFin" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("NonFin", 1000, 20)
    ws.clear()
    nonfin = df_all[~df_all["Ticker"].isin(FIN_TICKERS)]
    set_with_dataframe(ws, nonfin.reset_index())

    # Top5 with 建議區間
    ws = sheet.worksheet("Top5") if "Top5" in [w.title for w in sheet.worksheets()] else sheet.add_worksheet("Top5", 1000, 20)
    ws.clear()
    top5 = generate_top5(df_all)
    set_with_dataframe(ws, top5.reset_index())

    print("✅ TW50 TOP5 更新完成")

if __name__ == "__main__":
    update_sheet()
