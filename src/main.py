# -*- coding: utf-8 -*-
"""
TW50 / Top10 自動化主程式（安全寫入 + 中文名稱 + Top10欄位排序 + 防呆MultiIndex + 驗證閘門）
- 指標：SMA20/50/200、RSI14、Bollinger(20)
- Top10：依 RSI14↓、Volume↓ 排序；顯示 Ticker/Name/Close/RSI14/Volume 與建議進出場
- A1：台北時區時間戳
- 防呆：處理 yfinance 多檔誤用造成的 MultiIndex
- 驗證：寫入前健檢；通過才寫入 prod，否則寫入 dev 並輸出 QA 報告
"""

import os
import json
from typing import Dict, List, Tuple

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
    cfg["sheets"].setdefault("prod", {"tw50": "TW50", "top10": "Top10"})
    cfg["sheets"].setdefault("dev", {"tw50": "TW50_dev", "top10": "Top10_dev"})
    # 驗證預設
    cfg.setdefault("validation", {})
    v = cfg["validation"]
    v.setdefault("max_days_lag", 7)
    v.setdefault("allow_missing_ratio", 0.1)
    v.setdefault("strict", True)
    v.setdefault("write_qa_sheet", True)
    v.setdefault("qa_sheet_title", "QA_Report")
    return cfg

def taipei_now_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

# ========= 資料抓取與指標 =========
def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """盡量以單一代號抓；若誤傳多檔（導致 MultiIndex），抽第一檔避免 'Volume not unique'。"""
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        level1_vals = list(df.columns.levels[1])
        pick = level1_vals[0]
        df = df.xs(pick, axis=1, level=1)
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
    out["RSI14"] = out["RSI14"].fillna(method="bfill")
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
    last_by_ticker = (
        df_tw50.sort_values(["Ticker", "Date"])
               .groupby("Ticker", as_index=False)
               .tail(1)
               .reset_index(drop=True)
    )
    last_by_ticker["Entry_Low"]  = last_by_ticker["BB_Lower"]
    last_by_ticker["Entry_High"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_Low"]   = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_High"]  = last_by_ticker["BB_Upper"]
    ranked = last_by_ticker.sort_values(["RSI14", "Volume"], ascending=[False, False]).copy()
    top10 = ranked.head(10).copy()
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

# ========= 驗證（Validation Gate） =========
def validate_data(df_tw50: pd.DataFrame, tickers: List[str], rules: Dict) -> Tuple[bool, pd.DataFrame]:
    qa_rows = []
    now = pd.Timestamp.now(tz="Asia/Taipei").normalize()
    max_days_lag = int(rules.get("max_days_lag", 7))
    allow_missing_ratio = float(rules.get("allow_missing_ratio", 0.1))

    total = len(tickers)
    present = df_tw50["Ticker"].nunique() if not df_tw50.empty else 0
    missing = sorted(list(set(tickers) - set(df_tw50["Ticker"].unique()))) if present else tickers

    lag_violations, rsi_violations, vol_violations, nan_heavy = [], 0, 0, 0
    if not df_tw50.empty:
        last = (df_tw50.sort_values(["Ticker","Date"]).groupby("Ticker").tail(1))
        last["Date"] = pd.to_datetime(last["Date"])
        last["lag_days"] = (now.tz_localize(None) - last["Date"]).dt.days
        lag_violations = last.loc[last["lag_days"] > max_days_lag, ["Ticker","Name","Date","lag_days"]].to_dict("records")
        rsi_bad = last[(last["RSI14"] < 0) | (last["RSI14"] > 100) | (last["RSI14"].isna())]
        rsi_violations = len(rsi_bad)
        vol_bad = last[(last["Volume"] < 0) | (last["Volume"].isna())]
        vol_violations = len(vol_bad)
        required_cols = ["Close","RSI14","BB_Lower","BB_Mid","BB_Upper"]
        na_rate = df_tw50[required_cols].isna().mean().mean()
        if na_rate > 0.2:
            nan_heavy = 1

    qa_rows += [
        {"Check": "tickers_total", "Value": total, "Detail": ""},
        {"Check": "tickers_present", "Value": present, "Detail": ""},
        {"Check": "tickers_missing", "Value": len(missing), "Detail": ", ".join(missing[:20]) + ("..." if len(missing)>20 else "")},
        {"Check": "lag_violations", "Value": len(lag_violations), "Detail": str(lag_violations[:5]) + ("..." if len(lag_violations)>5 else "")},
        {"Check": "rsi_violations", "Value": rsi_violations, "Detail": ""},
        {"Check": "volume_violations", "Value": vol_violations, "Detail": ""},
        {"Check": "nan_heavy", "Value": nan_heavy, "Detail": "NA rate > 20% on key cols" if nan_heavy else ""}
    ]

    missing_ratio_ok = (len(missing) / max(total,1)) <= allow_missing_ratio
    pass_flag = (len(lag_violations) == 0) and (rsi_violations == 0) and (vol_violations == 0) and missing_ratio_ok and (nan_heavy == 0)
    qa_df = pd.DataFrame(qa_rows)
    return pass_flag, qa_df

# ========= Google Sheets 安全寫入 =========
def get_gspread_client():
    # ✅ 先讀 GitHub Secrets (環境變數)；沒有再回退本地檔案
    json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if json_str:
        import json as _json
        return gspread.service_account_from_dict(_json.loads(json_str))
    return gspread.service_account()

def safe_replace_worksheet(sh, target_title: str, df: pd.DataFrame, note_time: str):
    temp_title = f"{target_title}__tmp"
    try:
        ws_tmp_old = sh.worksheet(temp_title)
        sh.del_worksheet(ws_tmp_old)
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

def write_qa_sheet(sh, qa_df: pd.DataFrame, title: str, note_time: str):
    if qa_df is None or qa_df.empty: return
    safe_replace_worksheet(sh, title, qa_df, note_time)

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
    sheets = cfg["sheets"]
    prod_names = sheets["prod"]
    dev_names  = sheets["dev"]

    period = cfg.get("period", "12mo")
    interval = cfg.get("interval", "1d")

    print(f"[INFO] MODE={mode} | period={period} interval={interval}")
    print(f"[INFO] Tickers ({len(tickers)}): {tickers[:8]}{'...' if len(tickers)>8 else ''}")

    # 1) 產出 TW50 / Top10
    df_tw50 = build_tw50_table(tickers, period, interval)
    df_top10 = build_top10(df_tw50)

    # 2) 驗證
    pass_flag, qa_df = validate_data(df_tw50, tickers, cfg["validation"])
    print("\n[QA] Summary\n", qa_df.to_string(index=False), "\n")
    if pass_flag:
        print("[QA] ✅ 驗證通過：可寫入 prod")
    else:
        print("[QA] ❌ 驗證未通過：將寫入 dev，避免污染 prod")

    # 3) 寫入 Google Sheets（安全覆寫）
    client = get_gspread_client()
    try:
        sa_email = getattr(client.auth, "service_account_email", "unknown")
        print(f"[INFO] Using Service Account: {sa_email}")
    except Exception:
        pass

    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()

    target = prod_names if pass_flag or not cfg["validation"]["strict"] else dev_names
    tw50_title = target.get("tw50", "TW50")
    top10_title = target.get("top10", "Top10")

    safe_replace_worksheet(sh, tw50_title, df_tw50, stamp)
    safe_replace_worksheet(sh, top10_title, df_top10, stamp)

    if cfg["validation"].get("write_qa_sheet", True):
        qa_title = cfg["validation"].get("qa_sheet_title", "QA_Report")
        write_qa_sheet(sh, qa_df, qa_title, stamp)

    print("[INFO] All done.")

if __name__ == "__main__":
    main()
