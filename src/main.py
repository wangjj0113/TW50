# -*- coding: utf-8 -*-
"""
TW50 分組主程式
- 分成金融股 / 非金融股
- 輸出：
  1) TW50_nonfin (非金融完整表)
  2) Top10_nonfin (非金融 Top10)
  3) TW50_fin (金融完整表)
"""

import os
import json
from typing import Dict, List, Tuple

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

# ========= 股票名稱對照（可補充） =========
TICKER_NAME_MAP = {
    "2330.TW": "台積電","2317.TW": "鴻海","6505.TW": "台塑化","2454.TW": "聯發科",
    "2412.TW": "中華電","2881.TW": "富邦金","2882.TW": "國泰金","2308.TW": "台達電",
    "2002.TW": "中鋼","2303.TW": "聯電","1303.TW": "南亞","1326.TW": "台化",
    "2886.TW": "兆豐金","2884.TW": "玉山金","2885.TW": "元大金","2891.TW": "中信金",
    "2880.TW": "華南金","2883.TW": "開發金","2887.TW": "台新金","2888.TW": "新光金",
    "2892.TW": "第一金","2890.TW": "永豐金","5871.TW": "中租-KY","1216.TW": "統一",
    "1101.TW": "台泥","1102.TW": "亞泥","9904.TW": "寶成","2889.TW": "國票金",
    "2897.TW": "王道銀行","3008.TW": "大立光","3045.TW": "台灣大","4904.TW": "遠傳",
    "3711.TW": "日月光投控","2899.TW": "永豐金控","5876.TW": "上海商銀","9910.TW": "豐泰",
    "2603.TW": "長榮","2609.TW": "陽明","2615.TW": "萬海","2633.TW": "台灣高鐵",
    "2898.TW": "安泰銀","1402.TW": "遠東新","1590.TW": "亞德客-KY","2379.TW": "瑞昱",
    "2382.TW": "廣達","2395.TW": "研華","2408.TW": "南亞科","3006.TW": "晶豪科","3481.TW": "群創"
}


# ========= 設定 =========
def load_config(cfg_path: str = "config.json") -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("period", "12mo")
    cfg.setdefault("interval", "1d")
    return cfg


def taipei_now_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")


# ========= 抓價 + 指標 =========
def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        level1_vals = list(df.columns.levels[1])
        df = df.xs(level1_vals[0], axis=1, level=1)
    df = df.rename(columns=str.title)
    df.index.name = "Date"
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["SMA20"] = out["Close"].rolling(20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(200, min_periods=1).mean()
    delta = out["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    out["RSI14"] = 100 - (100 / (1 + rs))
    out["RSI14"] = out["RSI14"].fillna(method="bfill")
    mid = out["Close"].rolling(20, min_periods=1).mean()
    std = out["Close"].rolling(20, min_periods=1).std(ddof=0)
    out["BB_Mid"] = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std
    return out


def build_table(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
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
    df_all = pd.concat(frames, ignore_index=True)
    pref = [
        "Date","Ticker","Name","Close","RSI14","Volume",
        "SMA20","SMA50","SMA200","Open","High","Low",
        "BB_Lower","BB_Mid","BB_Upper"
    ]
    for c in pref:
        if c not in df_all.columns:
            df_all[c] = np.nan
    return df_all[pref].sort_values(["Date","Ticker"]).reset_index(drop=True)


def build_top10(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    last = df.sort_values(["Ticker","Date"]).groupby("Ticker", as_index=False).tail(1)
    last["Entry_Low"]  = last["BB_Lower"]
    last["Entry_High"] = last["BB_Mid"]
    last["Exit_Low"]   = last["BB_Mid"]
    last["Exit_High"]  = last["BB_Upper"]
    ranked = last.sort_values(["RSI14","Volume"], ascending=[False,False])
    top10 = ranked.head(10).copy()
    cols = ["Date","Ticker","Name","Close","RSI14","Volume",
            "Entry_Low","Entry_High","Exit_Low","Exit_High",
            "SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"]
    for c in cols:
        if c not in top10.columns: top10[c] = np.nan
    return top10[cols].reset_index(drop=True)


# ========= Google Sheets =========
def get_gspread_client():
    json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not json_str or not json_str.strip():
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    import json as _json
    return gspread.service_account_from_dict(_json.loads(json_str))


def safe_replace_worksheet(sh, title: str, df: pd.DataFrame, note_time: str):
    temp = f"{title}__tmp"
    try:
        sh.del_worksheet(sh.worksheet(temp))
    except gspread.WorksheetNotFound:
        pass
    ws = sh.add_worksheet(title=temp, rows=100, cols=26)
    ws.update_acell("A1", f"Last update (Asia/Taipei): {note_time}")
    ws.update_acell("A2", "")
    if not df.empty:
        set_with_dataframe(ws, df, row=3, include_index=False, include_column_header=True, resize=True)
    else:
        ws.update_acell("A3", "No Data")
    try:
        sh.del_worksheet(sh.worksheet(title))
    except gspread.WorksheetNotFound:
        pass
    ws.update_title(title)


# ========= 主流程 =========
def main():
    cfg = load_config()
    tickers = cfg.get("tickers", [])
    if not tickers:
        raise RuntimeError("config.json 缺少 tickers")

    sheet_id = cfg.get("sheet_id")
    if not sheet_id:
        raise RuntimeError("config.json 缺少 sheet_id")

    period, interval = cfg["period"], cfg["interval"]

    # 分組
    fin = [t for t in tickers if t in FIN_TICKERS]
    nonfin = [t for t in tickers if t not in FIN_TICKERS]

    print(f"[INFO] 金融股 {len(fin)} 檔, 非金融股 {len(nonfin)} 檔")

    df_fin = build_table(fin, period, interval)
    df_nonfin = build_table(nonfin, period, interval)
    df_top10_nonfin = build_top10(df_nonfin)

    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()

    safe_replace_worksheet(sh, "TW50_fin", df_fin, stamp)
    safe_replace_worksheet(sh, "TW50_nonfin", df_nonfin, stamp)
    safe_replace_worksheet(sh, "Top10_nonfin", df_top10_nonfin, stamp)

    print("[INFO] All done.")


if __name__ == "__main__":
    main()
