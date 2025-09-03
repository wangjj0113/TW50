# -*- coding: utf-8 -*-
"""
TW50/Top10 自動化主程式
功能：
- 從 yfinance 下載台股 0050 成分股
- 計算 SMA、RSI、布林通道
- 輸出至 Google Sheets (TW50, Top10)
- Top10 自動排序，並計算建議進出場區間
"""

import os
import json
import datetime as dt
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone, timedelta

# ================= 工具函式 =================

def with_tw_suffix(tickers):
    out = []
    for t in tickers:
        if not t.endswith(".TW"):
            out.append(t + ".TW")
        else:
            out.append(t)
    return out

def load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg["mode"] = cfg.get("mode", "prod")
    cfg["tickers"] = with_tw_suffix(cfg.get("tickers", []))
    return cfg

def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(period, min_periods=period).mean()
    ma_down = down.rolling(period, min_periods=period).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(0)

def add_indicators(df):
    df = df.sort_values(["Ticker","Date"]).reset_index(drop=True)
    g = df.groupby("Ticker", group_keys=False)
    df["SMA_20"]  = g["Close"].transform(lambda s: s.rolling(20, min_periods=1).mean())
    df["SMA_50"]  = g["Close"].transform(lambda s: s.rolling(50, min_periods=1).mean())
    df["SMA_200"] = g["Close"].transform(lambda s: s.rolling(200, min_periods=1).mean())
    df["RSI_14"]  = g["Close"].transform(lambda s: rsi(s, 14))
    bb_avg = g["Close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bb_std = g["Close"].transform(lambda s: s.rolling(20, min_periods=20).std())
    df["BB_20_Basis"] = bb_avg
    df["BB_20_Upper"] = bb_avg + 2 * bb_std
    df["BB_20_Lower"] = bb_avg - 2 * bb_std
    df["BB_20_Width"] = (df["BB_20_Upper"] - df["BB_20_Lower"]) / df["BB_20_Basis"]
    return df

def download_prices(tickers, start="2024-01-01", end=None):
    end = end or dt.date.today().isoformat()
    frames = []
    for t in tickers:
        d1 = yf.download(t, start=start, end=end, auto_adjust=False, progress=False)
        if d1.empty:
            continue
        d1 = d1.reset_index()[["Date","Open","High","Low","Close","Volume"]]
        d1["Ticker"] = t
        frames.append(d1)
    if not frames:
        return pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Volume"])
    return pd.concat(frames, ignore_index=True)

# ================= Google Sheets =================

def connect_google_sheet():
    svc_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not svc_json:
        raise RuntimeError("缺少 GOOGLE_SERVICE_ACCOUNT_JSON secret")
    creds = json.loads(svc_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    gc = gspread.service_account_from_dict(creds, scopes=scopes)
    return gc

def safe_write_dataframe(ws, df, note_ts=True):
    if df is None or df.empty:
        print(f"[WARN] DataFrame 空，跳過 {ws.title}")
        return
    # 數值統一四捨五入
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].round(3)
    values = []
    if note_ts:
        tz = timezone(timedelta(hours=8))
        now_str = datetime.now(tz).strftime("Last Update (Asia/Taipei): %Y-%m-%d %H:%M:%S")
        values.append([now_str])
    values.append(list(df.columns))
    values.extend(df.astype(str).values.tolist())
    ws.clear()
    ws.update("A1", values)

# ================= Top10 選股 =================

def build_top10(df, top_n=10):
    if df is None or df.empty:
        return pd.DataFrame()
    # 取各股票最新一筆
    df["_dt_"] = pd.to_datetime(df["Date"], errors="coerce")
    last = df.sort_values("_dt_").groupby("Ticker", as_index=False).tail(1)
    # 排序規則：先 RSI 高 → 再成交量大
    last = last.sort_values(["RSI_14","Volume"], ascending=[False,False]).head(top_n).copy()

    # 建議進場區間：BB_20_Lower ~ SMA_20
    last["建議進場區間"] = last.apply(
        lambda r: f"{r['BB_20_Lower']:.2f} ~ {r['SMA_20']:.2f}" if pd.notna(r["BB_20_Lower"]) and pd.notna(r["SMA_20"]) else "",
        axis=1
    )
    # 建議出場區間：SMA_20 ~ BB_20_Upper
    last["建議出場區間"] = last.apply(
        lambda r: f"{r['SMA_20']:.2f} ~ {r['BB_20_Upper']:.2f}" if pd.notna(r["BB_20_Upper"]) and pd.notna(r["SMA_20"]) else "",
        axis=1
    )

    keep_cols = [
        "Date","Ticker","Close","RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Lower","BB_20_Upper","BB_20_Basis","BB_20_Width",
        "建議進場區間","建議出場區間"
    ]
    for c in keep_cols:
        if c not in last.columns:
            last[c] = ""
    return last[keep_cols].reset_index(drop=True)

# ================= 主流程 =================

def main():
    cfg = load_cfg()
    mode = cfg["mode"]
    print(f"[INFO] MODE={mode}")

    prices = download_prices(cfg["tickers"], start=cfg.get("start_date","2024-01-01"))
    if prices.empty:
        raise RuntimeError("下載不到任何價格")

    df = add_indicators(prices)

    # 連線 Google Sheet
    gc = connect_google_sheet()
    sh = gc.open_by_key(cfg["sheet_id"])
    tw50_ws = sh.worksheet(cfg["sheets"][mode]["tw50"])
    top10_ws = sh.worksheet(cfg["sheets"][mode]["top10"])

    # 輸出 TW50
    safe_write_dataframe(tw50_ws, df)

    # 輸出 Top10
    top10 = build_top10(df, top_n=10)
    safe_write_dataframe(top10_ws, top10)

    print("[OK] 更新完成")

if __name__ == "__main__":
    main()
