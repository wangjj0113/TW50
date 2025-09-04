# -*- coding: utf-8 -*-
"""
TW50 分組 + Hot20 + Top5_hot20(含Signal) 主程式
輸出分頁：
  1) TW50_fin       金融股完整表
  2) TW50_nonfin    非金融完整表
  3) Top10_nonfin   非金融 Top10（RSI↓, Volume↓）
  4) Hot20_nonfin   非金融「最新一筆成交量前20」快照
  5) Top5_hot20     Hot20內再篩前5（含 Signal=Buy/Sell/Neutral）
"""

import os
import json
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe


# ========= 金融股名單 =========
FIN_TICKERS = {
    "2880.TW","2881.TW","2882.TW","2883.TW","2884.TW","2885.TW",
    "2886.TW","2887.TW","2888.TW","2889.TW","2890.TW","2891.TW",
    "2892.TW","2897.TW","2898.TW","2899.TW","5871.TW","5876.TW"
}


# ========= 工具 =========
def load_config(cfg_path: str = "config.json"):
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)

def taipei_now_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df = df.xs(df.columns.levels[1][0], axis=1, level=1)
    df = df.rename(columns=str.title)
    df.index.name = "Date"
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["SMA20"] = out["Close"].rolling(20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(200, min_periods=1).mean()
    delta = out["Close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    roll_up = pd.Series(gain).rolling(14, min_periods=1).mean()
    roll_down = pd.Series(loss).rolling(14, min_periods=1).mean()
    rs = roll_up / (roll_down + 1e-9)
    out["RSI14"] = 100.0 - (100.0 / (1.0 + rs))
    out["BB_Mid"] = out["Close"].rolling(20, min_periods=1).mean()
    out["BB_Std"] = out["Close"].rolling(20, min_periods=1).std(ddof=0)
    out["BB_Upper"] = out["BB_Mid"] + 2 * out["BB_Std"]
    out["BB_Lower"] = out["BB_Mid"] - 2 * out["BB_Std"]
    return out

def classify_signal(row) -> str:
    if row["RSI14"] < 40 and row["Close"] <= row["BB_Lower"]:
        return "Buy"
    elif row["RSI14"] > 60 and row["Close"] >= row["BB_Upper"]:
        return "Sell"
    else:
        return "Neutral"


# ========= GSpread =========
def get_gspread_client():
    json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not json_str:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    creds = json.loads(json_str)
    gc = gspread.service_account_from_dict(creds)
    return gc

def safe_replace_worksheet(sh, title: str, df: pd.DataFrame, stamp: str):
    if title in [ws.title for ws in sh.worksheets()]:
        sh.del_worksheet(sh.worksheet(title))
    ws = sh.add_worksheet(title, rows=str(len(df)+5), cols=str(len(df.columns)+5))
    set_with_dataframe(ws, df.reset_index(), include_index=False, include_column_header=True)
    ws.update("A1", f"Last Update (Asia/Taipei): {stamp}")


# ========= 主程式 =========
def main():
    cfg = load_config()
    tickers = cfg.get("tickers", [])
    sheet_id = cfg.get("sheet_id")
    period = cfg.get("period", "12mo")
    interval = cfg.get("interval", "1d")

    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    records = {}
    for t in tickers:
        try:
            df = fetch_history(t, period, interval)
            df = add_indicators(df)
            records[t] = df
        except Exception as e:
            print("Error:", t, e)

    stamp = taipei_now_str()
    all_df = []
    for t, df in records.items():
        df["Ticker"] = t
        all_df.append(df)
    df_all = pd.concat(all_df)
    df_all.reset_index(inplace=True)

    # 分金融/非金融
    df_fin = df_all[df_all["Ticker"].isin(FIN_TICKERS)].copy()
    df_nonfin = df_all[~df_all["Ticker"].isin(FIN_TICKERS)].copy()

    # Top10 (非金融)
    last_nonfin = df_nonfin.groupby("Ticker").tail(1)
    df_top10 = last_nonfin.sort_values(["RSI14","Volume"], ascending=[False,False]).head(10)

    # Hot20 (非金融成交量前20)
    df_hot20 = last_nonfin.sort_values("Volume", ascending=False).head(20)

    # Top5 from Hot20 + Signal
    df_top5 = df_hot20.sort_values(["RSI14","Volume"], ascending=[False,False]).head(5).copy()
    df_top5["Signal"] = df_top5.apply(classify_signal, axis=1)

    # 輸出
    safe_replace_worksheet(sh, "TW50_fin", df_fin, stamp)
    safe_replace_worksheet(sh, "TW50_nonfin", df_nonfin, stamp)
    safe_replace_worksheet(sh, "Top10_nonfin", df_top10, stamp)
    safe_replace_worksheet(sh, "Hot20_nonfin", df_hot20, stamp)
    safe_replace_worksheet(sh, "Top5_hot20", df_top5, stamp)


if __name__ == "__main__":
    main()
