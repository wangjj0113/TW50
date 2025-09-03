# -*- coding: utf-8 -*-
"""
TW50 / Top10 自動化主程式（安全寫入版）
- 修正：多股票時技術指標逐檔計算，避免 "Cannot set a DataFrame with multiple columns to the single column ..." 錯誤
- 功能：SMA20/50/200、RSI14、Bollinger、Top10 排序（RSI↑/Volume↑）與建議進出場區間
- A1 顯示台北時區更新時間
"""

import os
import json
from datetime import datetime
from typing import Dict, Tuple, List

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe


# ========= 基本設定 =========

def load_config(cfg_path: str = "config.json") -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # 預設值（兼容舊版 config）
    cfg.setdefault("mode", "prod")
    cfg.setdefault("period", "12mo")
    cfg.setdefault("interval", "1d")
    # sheets 結構允許兩種：
    # 1) {"prod":{"tw50":"TW50","top10":"Top10"},"dev":{"tw50":"TW50_dev","top10":"Top10_dev"}}
    # 2) {"sheets":{"prod":{...},"dev":{...}}}
    if "sheets" not in cfg:
        # 舊版：可能直接在最外層有 prod/dev
        prod = cfg.get("prod", {"tw50": "TW50", "top10": "Top10"})
        dev = cfg.get("dev", {"tw50": "TW50_dev", "top10": "Top10_dev"})
        cfg["sheets"] = {"prod": prod, "dev": dev}
    # 預設表名
    cfg["sheets"]["prod"].setdefault("tw50", "TW50")
    cfg["sheets"]["prod"].setdefault("top10", "Top10")
    cfg["sheets"]["dev"].setdefault("tw50", "TW50_dev")
    cfg["sheets"]["dev"].setdefault("top10", "Top10_dev")
    return cfg


def taipei_now_str() -> str:
    # 避免依賴 pytz，使用 pandas 時區
    tz = pd.Timestamp.now(tz="Asia/Taipei")
    return tz.strftime("%Y-%m-%d %H:%M")


# ========= 資料抓取與指標 =========

def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    # 標準化欄位名
    df = df.rename(columns=str.title)  # open/close -> Open/Close
    df.index.name = "Date"
    return df


def add_indicators_for_one(df: pd.DataFrame) -> pd.DataFrame:
    """對單一股票的 OHLCV 計算 SMA、RSI、Bollinger，回傳與原 df 同索引的 DataFrame"""
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
    out["RSI14"] = out["RSI14"].fillna(method="bfill")  # 前段補齊

    # Bollinger (20)
    mid = out["Close"].rolling(window=20, min_periods=1).mean()
    std = out["Close"].rolling(window=20, min_periods=1).std(ddof=0)
    out["BB_Mid"] = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std

    return out


def build_tw50_table(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    """
    產出「長表」：每列為 (Date, Ticker, Open, High, Low, Close, Volume, SMA20, SMA50, SMA200, BB_*, RSI14)
    方便寫入 Google Sheets 與後續分析。
    """
    frames = []
    for tk in tickers:
        raw = fetch_history(tk, period, interval)
        if raw.empty:
            continue
        ind = add_indicators_for_one(raw)
        ind.insert(0, "Ticker", tk)
        frames.append(ind.reset_index())

    if not frames:
        return pd.DataFrame()

    cols_order = [
        "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
        "SMA20", "SMA50", "SMA200", "BB_Mid", "BB_Upper", "BB_Lower", "RSI14"
    ]
    df_all = pd.concat(frames, axis=0, ignore_index=True)
    # 欄位齊一
    for c in cols_order:
        if c not in df_all.columns:
            df_all[c] = np.nan
    df_all = df_all[cols_order]
    # 排序（日期 -> 股票）
    df_all = df_all.sort_values(["Date", "Ticker"]).reset_index(drop=True)
    return df_all


def build_top10(df_tw50: pd.DataFrame) -> pd.DataFrame:
    """
    從 TW50 全量表抽取每檔股票的「最近一筆」數據，依 RSI14↑、Volume↑ 排序，取前 10 檔。
    並提供建議進出場區間（基於布林帶）。
    """
    if df_tw50.empty:
        return pd.DataFrame()

    # 每檔股票最後一日資料
    last_by_ticker = (
        df_tw50.sort_values(["Ticker", "Date"])
               .groupby("Ticker", as_index=False)
               .tail(1)
               .reset_index(drop=True)
    )

    # 建議區間（可按需調整）
    # 進場：BB_Lower ~ BB_Mid；出場：BB_Mid ~ BB_Upper
    last_by_ticker["Entry_Low"] = last_by_ticker["BB_Lower"]
    last_by_ticker["Entry_High"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_Low"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_High"] = last_by_ticker["BB_Upper"]

    # 排序邏輯：
    # - RSI14 由高到低（動能較強）與 Volume 由高到低（成交動能）
    ranked = last_by_ticker.sort_values(["RSI14", "Volume"], ascending=[False, False])
    top10 = ranked.head(10).copy()

    sel_cols = [
        "Date", "Ticker", "Close", "Volume",
        "RSI14", "SMA20", "SMA50", "SMA200",
        "BB_Lower", "BB_Mid", "BB_Upper",
        "Entry_Low", "Entry_High", "Exit_Low", "Exit_High"
    ]
    # 兼容欄位缺失
    for c in sel_cols:
        if c not in top10.columns:
            top10[c] = np.nan
    return top10[sel_cols].reset_index(drop=True)


# ========= Google Sheets 安全寫入 =========

def get_gspread_client():
    """
    1) 若環境變數 GOOGLE_APPLICATION_CREDENTIALS 指向 service account 憑證檔，gspread 會自動讀取
    2) 或者專案根目錄有 service_account.json 也可（請自行命名與放置）
    """
    try:
        return gspread.service_account()
    except Exception:
        # 嘗試用環境變數內容（若以文字 Secret 提供）
        json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if json_str:
            import json as _json
            return gspread.service_account_from_dict(_json.loads(json_str))
        raise


def safe_replace_worksheet(sh, target_title: str, df: pd.DataFrame, note_time: str):
    """
    安全覆寫流程：
    - 新建 temp 工作表，寫入內容
    - 將 A1 設為「更新時間（台北）」；資料從第 3 列開始（保留 A1/A2）
    - 刪除舊表；temp 改名為 target
    - 若舊表不存在則直接改名
    """
    temp_title = f"{target_title}__tmp"

    # 若 temp 已存在先刪
    try:
        ws_tmp = sh.worksheet(temp_title)
        sh.del_worksheet(ws_tmp)
    except gspread.WorksheetNotFound:
        pass

    # 建 temp，至少 3 列 3 欄避免 set_with_dataframe 異常
    ws_tmp = sh.add_worksheet(title=temp_title, rows=100, cols=26)

    # A1 時間戳
    ws_tmp.update_acell("A1", f"Last update (Asia/Taipei): {note_time}")
    ws_tmp.update_acell("A2", "")  # 空一行

    # 寫入資料從第 3 列開始
    if not df.empty:
        set_with_dataframe(ws_tmp, df, row=3, include_index=False, include_column_header=True, resize=True)
    else:
        ws_tmp.update_acell("A3", "No Data")

    # 刪除舊表，將 temp 改名為 target
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

    print(f"[INFO] MODE={mode} | period={period} interval={interval}")
    print(f"[INFO] Targets: TW50='{tw50_title}', Top10='{top10_title}'")
    print(f"[INFO] Tickers: {len(tickers)} -> {tickers[:6]}{'...' if len(tickers)>6 else ''}")

    # 1) 產出 TW50 全量表
    df_tw50 = build_tw50_table(tickers, period=period, interval=interval)
    print(f"[INFO] TW50 rows={len(df_tw50)}")

    # 2) 產出 Top10
    df_top10 = build_top10(df_tw50)
    print(f"[INFO] Top10 rows={len(df_top10)}")

    # 3) 寫入 Google Sheet（安全覆寫）
    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()

    safe_replace_worksheet(sh, tw50_title, df_tw50, stamp)
    safe_replace_worksheet(sh, top10_title, df_top10, stamp)

    print("[INFO] All done.")


if __name__ == "__main__":
    main()
