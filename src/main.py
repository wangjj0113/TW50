# -*- coding: utf-8 -*-
"""
TW50 / Top10 自動化主程式（安全寫入 + 中文名稱 + Top10欄位排序 + 防呆MultiIndex）
- 指標：SMA20/50/200、RSI14、Bollinger(20)
- Top10：依 RSI14↓、Volume↓ 排序；優先顯示 Ticker/Name/Close/RSI14/Volume 與建議進出場
- A1：台北時區時間戳
- 防呆：就算誤把多檔代號寫成一個字串，亦能攤平或抽出第一檔避免 'Volume not unique'
"""

import os
import json
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe


# ========= 股票名稱對照 =========

TICKER_NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海",   "6505.TW": "台塑化", "2454.TW": "聯發科",
    "2412.TW": "中華電", "2881.TW": "富邦金", "2882.TW": "國泰金", "2308.TW": "台達電",
    "2002.TW": "中鋼",   "2303.TW": "聯電",   "1303.TW": "南亞",   "1326.TW": "台化",
    "2886.TW": "兆豐金", "2884.TW": "玉山金", "2885.TW": "元大金", "2891.TW": "中信金",
    "2880.TW": "華南金", "2883.TW": "開發金", "2887.TW": "台新金", "2888.TW": "新光金",
    "2892.TW": "第一金", "2890.TW": "永豐金", "5871.TW": "中租-KY","1216.TW": "統一",
    "1101.TW": "台泥",   "1102.TW": "亞泥",   "9904.TW": "寶成",   "2889.TW": "國票金",
    "2897.TW": "王道銀行","3008.TW": "大立光","3045.TW": "台灣大","4904.TW": "遠傳",
    "3711.TW": "日月光投控", "2899.TW": "永豐金控", "5876.TW": "上海商銀", "9910.TW": "豐泰",
    "2603.TW": "長榮", "2609.TW": "陽明", "2615.TW": "萬海", "2633.TW": "台灣高鐵",
    "2898.TW": "安泰銀", "1402.TW": "遠東新", "1590.TW": "亞德客-KY", "2379.TW": "瑞昱",
    "2382.TW": "廣達", "2395.TW": "研華", "2408.TW": "南亞科", "3006.TW": "晶豪科",
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
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")


# ========= 指標計算 =========

def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    盡量以「單一代號」抓；若誤傳多檔（導致 MultiIndex），自動抽出第一檔避免 Volume 重複造成錯誤。
    """
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)

    # 若回來是 MultiIndex 欄位（通常是一次抓多檔），處理一下
    if isinstance(df.columns, pd.MultiIndex):
        # 第二層通常是代號
        level1_vals = list(df.columns.levels[1])
        # 若只有一個代號 → 直接抽那一層
        if len(level1_vals) == 1:
            df = df.xs(level1_vals[0], axis=1, level=1)
        else:
            # 多個代號被塞進來（大概率是 tickers 誤傳為單一字串）
            # 抽第一個代號，避免 'Volume' label not unique
            pick = level1_vals[0]
            df = df.xs(pick, axis=1, level=1)

    # 統一欄位命名
    df = df.rename(columns=str.title)  # open/close → Open/Close
    df.index.name = "Date"
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # SMA
    out["SMA20"] = out["Close"].rolling(window=20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(window=50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(window=200, min_periods=1).mean()

    # RSI14
    delta = out["Close"].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(window=14, min_periods=14).mean()
    avg_loss = loss.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    out["RSI14"] = 100 - (100 / (1 + rs))
    out["RSI14"] = out["RSI14"].fillna(method="bfill")

    # Bollinger(20)
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
        ind.insert(1, "Name", TICKER_NAME_MAP.get(tk, ""))
        frames.append(ind.reset_index())

    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, axis=0, ignore_index=True)

    # 統一欄位順序（便於閱讀）
    pref = [
        "Date", "Ticker", "Name", "Close", "RSI14", "Volume",
        "SMA20", "SMA50", "SMA200",
        "Open", "High", "Low",
        "BB_Lower", "BB_Mid", "BB_Upper"
    ]
    for c in pref:
        if c not in df_all.columns:
            df_all[c] = np.nan
    df_all = df_all[pref].sort_values(["Date", "Ticker"]).reset_index(drop=True)
    return df_all


def build_top10(df_tw50: pd.DataFrame) -> pd.DataFrame:
    if df_tw50.empty:
        return pd.DataFrame()

    # 每檔最後一筆
    last_by_ticker = (
        df_tw50.sort_values(["Ticker", "Date"])
               .groupby("Ticker", as_index=False)
               .tail(1)
               .reset_index(drop=True)
    )

    # 建議進/出場（基於布林）
    last_by_ticker["Entry_Low"] = last_by_ticker["BB_Lower"]
    last_by_ticker["Entry_High"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_Low"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_High"] = last_by_ticker["BB_Upper"]

    ranked = last_by_ticker.sort_values(["RSI14", "Volume"], ascending=[False, False]).copy()
    top10 = ranked.head(10).copy()

    # Top10 欄位順序（優先顯示重點）
    top_cols = [
        "Date", "Ticker", "Name", "Close", "RSI14", "Volume",
        "Entry_Low", "Entry_High", "Exit_Low", "Exit_High",
        "SMA20", "SMA50", "SMA200",
        "BB_Lower", "BB_Mid", "BB_Upper"
    ]
    for c in top_cols:
        if c not in top10.columns:
            top10[c] = np.nan
    return top10[top_cols].reset_index(drop=True)


# ========= Google Sheets 安全寫入 =========

def get_gspread_client():
    try:
        return gspread.service_account()
    except Exception:
        # 也支援從環境變數字串載入 service account JSON
        json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if json_str:
            import json as _json
            return gspread.service_account_from_dict(_json.loads(json_str))
        raise


def safe_replace_worksheet(sh, target_title: str, df: pd.DataFrame, note_time: str):
    temp_title = f"{target_title}__tmp"
    # 清理舊 tmp
    try:
        ws_tmp_old = sh.worksheet(temp_title)
        sh.del_worksheet(ws_tmp_old)
    except gspread.WorksheetNotFound:
        pass

    # 新建 tmp
    ws_tmp = sh.add_worksheet(title=temp_title, rows=100, cols=26)
    ws_tmp.update_acell("A1", f"Last update (Asia/Taipei): {note_time}")
    ws_tmp.update_acell("A2", "")

    # 內容從第 3 列開始
    if not df.empty:
        set_with_dataframe(ws_tmp, df, row=3, include_index=False, include_column_header=True, resize=True)
    else:
        ws_tmp.update_acell("A3", "No Data")

    # 刪舊 → 改名
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
    if not tickers:
        raise RuntimeError("config.json 缺少 tickers 清單。")

    sheet_id = cfg.get("sheet_id")
    if not sheet_id or "留空" in str(sheet_id):
        raise RuntimeError("config.json 的 sheet_id 尚未填入姐的 Google Sheet ID。")

    mode = cfg.get("mode", "prod")
    sheets = cfg["sheets"][mode]
    tw50_title = sheets.get("tw50", "TW50")
    top10_title = sheets.get("top10", "Top10")

    period = cfg.get("period", "12mo")
    interval = cfg.get("interval", "1d")

    df_tw50 = build_tw50_table(tickers, period, interval)
    df_top10 = build_top10(df_tw50)

    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()

    safe_replace_worksheet(sh, tw50_title, df_tw50, stamp)
    safe_replace_worksheet(sh, top10_title, df_top10, stamp)

    print("[INFO] All done.")


if __name__ == "__main__":
    main()
