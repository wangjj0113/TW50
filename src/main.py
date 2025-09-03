# -*- coding: utf-8 -*-
"""
TW50 / Top10 自動化主程式（安全寫入版 + 股票名稱顯示）
- 功能：SMA20/50/200、RSI14、Bollinger、Top10 排序（RSI↑/Volume↑）與建議進出場區間
- A1 顯示台北時區更新時間
- 新增：Name 欄位，顯示股票中文名稱（Ticker + Name）
"""

import os
import json
from datetime import datetime
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe


# ========= 股票清單與名稱對照 =========

TICKER_NAME_MAP = {
    "2330.TW": "台積電",
    "2317.TW": "鴻海",
    "6505.TW": "台塑化",
    "2454.TW": "聯發科",
    "2412.TW": "中華電",
    "2881.TW": "富邦金",
    "2882.TW": "國泰金",
    "2308.TW": "台達電",
    "2002.TW": "中鋼",
    "2303.TW": "聯電",
    "1303.TW": "南亞",
    "1326.TW": "台化",
    "2886.TW": "兆豐金",
    "2884.TW": "玉山金",
    "2885.TW": "元大金",
    "2891.TW": "中信金",
    "2880.TW": "華南金",
    "2883.TW": "開發金",
    "2887.TW": "台新金",
    "2888.TW": "新光金",
    "2892.TW": "第一金",
    "2890.TW": "永豐金",
    "5871.TW": "中租-KY",
    "1216.TW": "統一",
    "1101.TW": "台泥",
    "1102.TW": "亞泥",
    "9904.TW": "寶成",
    "2889.TW": "國票金",
    "2897.TW": "王道銀行",
    "3008.TW": "大立光",
    "3045.TW": "台灣大",
    "4904.TW": "遠傳",
    "3711.TW": "日月光投控",
    "2899.TW": "永豐金控",
    "5876.TW": "上海商銀",
    "9910.TW": "豐泰",
    "2603.TW": "長榮",
    "2609.TW": "陽明",
    "2615.TW": "萬海",
    "2633.TW": "台灣高鐵",
    "2898.TW": "安泰銀",
    "1402.TW": "遠東新",
    "1590.TW": "亞德客-KY",
    "2379.TW": "瑞昱",
    "2382.TW": "廣達",
    "2395.TW": "研華",
    "2408.TW": "南亞科",
    "3006.TW": "晶豪科",
    "3481.TW": "群創"
}


# ========= 基本設定 =========

def load_config(cfg_path: str = "config.json") -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("mode", "prod")
    cfg.setdefault("period", "12mo")
    cfg.setdefault("interval", "1d")
    if "sheets" not in cfg:
        cfg["sheets"] = {"prod": cfg.get("prod", {}), "dev": cfg.get("dev", {})}
    return cfg


def taipei_now_str() -> str:
    tz = pd.Timestamp.now(tz="Asia/Taipei")
    return tz.strftime("%Y-%m-%d %H:%M")


# ========= 指標計算 =========

def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    df = df.rename(columns=str.title)
    df.index.name = "Date"
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["SMA20"] = out["Close"].rolling(window=20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(window=50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(window=200, min_periods=1).mean()
    delta = out["Close"].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(window=14, min_periods=14).mean()
    avg_loss = loss.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    out["RSI14"] = 100 - (100 / (1 + rs))
    mid = out["Close"].rolling(window=20, min_periods=1).mean()
    std = out["Close"].rolling(window=20, min_periods=1).std(ddof=0)
    out["BB_Mid"] = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std
    return out


def build_tw50_table(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    frames = []
    for tk in tickers:
        raw = fetch_history(tk, period, interval)
        if raw.empty:
            continue
        ind = add_indicators(raw)
        ind.insert(0, "Ticker", tk)
        ind.insert(1, "Name", TICKER_NAME_MAP.get(tk, ""))  # 股票名稱
        frames.append(ind.reset_index())
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    return df_all.sort_values(["Date", "Ticker"]).reset_index(drop=True)


def build_top10(df_tw50: pd.DataFrame) -> pd.DataFrame:
    if df_tw50.empty:
        return pd.DataFrame()
    last_by_ticker = (
        df_tw50.sort_values(["Ticker", "Date"])
               .groupby("Ticker", as_index=False)
               .tail(1)
               .reset_index(drop=True)
    )
    last_by_ticker["Entry_Low"] = last_by_ticker["BB_Lower"]
    last_by_ticker["Entry_High"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_Low"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_High"] = last_by_ticker["BB_Upper"]
    ranked = last_by_ticker.sort_values(["RSI14", "Volume"], ascending=[False, False])
    top10 = ranked.head(10).copy()
    return top10.reset_index(drop=True)


# ========= Google Sheets 安全寫入 =========

def get_gspread_client():
    try:
        return gspread.service_account()
    except Exception:
        json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if json_str:
            import json as _json
            return gspread.service_account_from_dict(_json.loads(json_str))
        raise


def safe_replace_worksheet(sh, target_title: str, df: pd.DataFrame, note_time: str):
    temp_title = f"{target_title}__tmp"
    try:
        ws_tmp = sh.worksheet(temp_title)
        sh.del_worksheet(ws_tmp)
    except gspread.WorksheetNotFound:
        pass
    ws_tmp = sh.add_worksheet(title=temp_title, rows=100, cols=26)
    ws_tmp.update_acell("A1", f"Last update (Asia/Taipei): {note_time}")
    ws_tmp.update_acell("A2", "")
    if not df.empty:
        set_with_dataframe(ws_tmp, df, row=3, include_index=False, include_column_header=True, resize=True)
    else:
        ws_tmp.update_acell("A3", "No Data")
    try:
        ws_old = sh.worksheet(target_title)
        sh.del_worksheet(ws_old)
    except gspread.WorksheetNotFound:
        pass
    ws_tmp.update_title(target_title)


# ========= 主流程 =========

def main():
    cfg = load_config()
    tickers = cfg.get("tickers", [])
    sheet_id = cfg.get("sheet_id")
    mode = cfg.get("mode", "prod")
    sheets = cfg["sheets"][mode]
    tw50_title = sheets.get("tw50", "TW50")
    top10_title = sheets.get("top10", "Top10")
    df_tw50 = build_tw50_table(tickers, period=cfg["period"], interval=cfg["interval"])
    df_top10 = build_top10(df_tw50)
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()
    safe_replace_worksheet(sh, tw50_title, df_tw50, stamp)
    safe_replace_worksheet(sh, top10_title, df_top10, stamp)
    print("[INFO] All done.")


if __name__ == "__main__":
    main()
