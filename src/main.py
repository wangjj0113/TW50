# -*- coding: utf-8 -*-
"""
TW50 / Top10 主程式（Secrets優先 + 中文名稱 + 指標 + Top10 + 驗證閘門 + 安全覆寫）
- 指標：SMA20/50/200、RSI14、Bollinger(20)
- Top10：依 RSI14↓、Volume↓ 排序，附建議進/出場(布林帶)
- 安全：gspread 先讀 GCP_SERVICE_ACCOUNT_JSON（GitHub Secrets），缺才回退本地檔
- 驗證：資料健檢不過→寫入 dev 並產生 QA 報告；通過→寫入 prod
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
    v.setdefault("strict", True)                  # 不通過就不寫 prod
    v.setdefault("write_qa_sheet", True)
    v.setdefault("qa_sheet_title", "QA_Report")
    return cfg


def taipei_now_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")


# ========= 抓價 + 指標 =========
def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """單一代號抓；若誤變多檔（MultiIndex），抽第一檔避免欄名衝突。"""
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        level1_vals = list(df.columns.levels[1])
        df = df.xs(level1_vals[0], axis=1, level=1)
    df = df.rename(columns=str.title)
    df.index.name = "Date"
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # SMA
    out["SMA20"] = out["Close"].rolling(20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(200, min_periods=1).mean()
    # RSI14
    delta = out["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean()
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    out["RSI14"] = 100 - (100 / (1 + rs))
    out["RSI14"] = out["RSI14"].fillna(method="bfill")
    # Bollinger(20)
    mid = out["Close"].rolling(20, min_periods=1).mean()
    std = out["Close"].rolling(20, min_periods=1).std(ddof=0)
    out["BB_Mid"] = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std
    return out


def build_tw50_table(tickers: List[str], period: str, interval: str) -> pd.DataFrame:
    frames = []
    for tk in tickers:
        raw = fetch_history(tk, period, interval)
        if raw.empty:  # 抓不到就跳過
            continue
        ind = add_indicators(raw)
        ind.insert(0, "Ticker", tk)
        ind.insert(1, "Name", TICKER_NAME_MAP.get(tk, ""))
        frames.append(ind.reset_index())

    if not frames:
        return pd.DataFrame()

    df_all = pd.concat(frames, ignore_index=True)
    # 欄位順序
    pref = [
        "Date", "Ticker", "Name", "Close", "RSI14", "Volume",
        "SMA20", "SMA50", "SMA200",
        "Open", "High", "Low",
        "BB_Lower", "BB_Mid", "BB_Upper"
    ]
    for c in pref:
        if c not in df_all.columns:
            df_all[c] = np.nan
    return df_all[pref].sort_values(["Date", "Ticker"]).reset_index(drop=True)


def build_top10(df_tw50: pd.DataFrame) -> pd.DataFrame:
    if df_tw50.empty:
        return pd.DataFrame()

    last_by_ticker = (
        df_tw50.sort_values(["Ticker", "Date"])
               .groupby("Ticker", as_index=False)
               .tail(1)
               .reset_index(drop=True)
    )
    # 建議區間（布林帶）
    last_by_ticker["Entry_Low"]  = last_by_ticker["BB_Lower"]
    last_by_ticker["Entry_High"] = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_Low"]   = last_by_ticker["BB_Mid"]
    last_by_ticker["Exit_High"]  = last_by_ticker["BB_Upper"]

    ranked = last_by_ticker.sort_values(["RSI14", "Volume"], ascending=[False, False])
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
    qa = []
    now = pd.Timestamp.now(tz="Asia/Taipei").normalize()
    max_days_lag = int(rules.get("max_days_lag", 7))
    allow_missing_ratio = float(rules.get("allow_missing_ratio", 0.1))

    total = len(tickers)
    present = df_tw50["Ticker"].nunique() if not df_tw50.empty else 0
    missing = sorted(list(set(tickers) - set(df_tw50["Ticker"].unique()))) if present else tickers

    lag_viol, rsi_bad_n, vol_bad_n, nan_heavy = [], 0, 0, 0
    if not df_tw50.empty:
        last = df_tw50.sort_values(["Ticker","Date"]).groupby("Ticker").tail(1)
        last["Date"] = pd.to_datetime(last["Date"])
        last["lag_days"] = (now.tz_localize(None) - last["Date"]).dt.days
        lag_viol = last.loc[last["lag_days"] > max_days_lag, ["Ticker","Name","Date","lag_days"]].to_dict("records")
        rsi_bad_n = len(last[(last["RSI14"].lt(0)) | (last["RSI14"].gt(100)) | (last["RSI14"].isna())])
        vol_bad_n = len(last[(last["Volume"].lt(0)) | (last["Volume"].isna())])
        na_rate = df_tw50[["Close","RSI14","BB_Lower","BB_Mid","BB_Upper"]].isna().mean().mean()
        nan_heavy = 1 if na_rate > 0.2 else 0

    qa += [
        {"Check":"tickers_total","Value":total,"Detail":""},
        {"Check":"tickers_present","Value":present,"Detail":""},
        {"Check":"tickers_missing","Value":len(missing),"Detail":", ".join(missing[:20]) + ("..." if len(missing)>20 else "")},
        {"Check":"lag_violations","Value":len(lag_viol),"Detail":str(lag_viol[:5]) + ("..." if len(lag_viol)>5 else "")},
        {"Check":"rsi_violations","Value":rsi_bad_n,"Detail":""},
        {"Check":"volume_violations","Value":vol_bad_n,"Detail":""},
        {"Check":"nan_heavy","Value":nan_heavy,"Detail":"NA rate > 20% on key cols" if nan_heavy else ""},
    ]

    missing_ratio_ok = (len(missing) / max(total,1)) <= allow_missing_ratio
    passed = (len(lag_viol)==0) and (rsi_bad_n==0) and (vol_bad_n==0) and missing_ratio_ok and (nan_heavy==0)
    return passed, pd.DataFrame(qa)


# ========= Google Sheets 安全寫入 =========
def get_gspread_client():
    """優先用 GitHub Secrets (GCP_SERVICE_ACCOUNT_JSON)，沒有才回退本地檔。"""
    json_str = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if json_str:
        import json as _json
        return gspread.service_account_from_dict(_json.loads(json_str))
    return gspread.service_account()  # 本地 service_account.json（本機調試時可用）


def safe_replace_worksheet(sh, target_title: str, df: pd.DataFrame, note_time: str):
    temp_title = f"{target_title}__tmp"
    # 清理舊 tmp
    try:
        sh.del_worksheet(sh.worksheet(temp_title))
    except gspread.WorksheetNotFound:
        pass
    # 新建 tmp、寫 A1
    ws_tmp = sh.add_worksheet(title=temp_title, rows=100, cols=26)
    ws_tmp.update_acell("A1", f"Last update (Asia/Taipei): {note_time}")
    ws_tmp.update_acell("A2", "")
    # 資料從第 3 列開始
    if not df.empty:
        set_with_dataframe(ws_tmp, df, row=3, include_index=False, include_column_header=True, resize=True)
    else:
        ws_tmp.update_acell("A3", "No Data")
    # 刪舊 → 改名
    try:
        sh.del_worksheet(sh.worksheet(target_title))
    except gspread.WorksheetNotFound:
        pass
    ws_tmp.update_title(target_title)


def write_qa_sheet(sh, qa_df: pd.DataFrame, title: str, note_time: str):
    if qa_df is not None and not qa_df.empty:
        safe_replace_worksheet(sh, title, qa_df, note_time)


# ========= 主流程 =========
def main():
    cfg = load_config()
    tickers = cfg.get("tickers", [])
    if not tickers:
        raise RuntimeError("config.json 缺少 tickers 清單。")

    sheet_id = cfg.get("sheet_id")
    if not sheet_id:
        raise RuntimeError("config.json 的 sheet_id 尚未填入姐的 Google Sheet ID。")

    period, interval = cfg.get("period","12mo"), cfg.get("interval","1d")
    prod_names, dev_names = cfg["sheets"]["prod"], cfg["sheets"]["dev"]

    print(f"[INFO] period={period} interval={interval}")
    print(f"[INFO] Tickers ({len(tickers)}): {tickers[:8]}{'...' if len(tickers)>8 else ''}")

    # 1) 建表
    df_tw50 = build_tw50_table(tickers, period, interval)
    df_top10 = build_top10(df_tw50)

    # 2) 驗證
    passed, qa_df = validate_data(df_tw50, tickers, cfg["validation"])
    print("\n[QA] Summary\n", qa_df.to_string(index=False), "\n")
    print("[QA] ✅ 驗證通過：寫入 prod" if passed else "[QA] ❌ 驗證未過：寫入 dev")

    # 3) 寫入 Google Sheet
    client = get_gspread_client()
    try:
        print(f"[INFO] Using Service Account: {getattr(client.auth,'service_account_email','unknown')}")
    except Exception:
        pass
    sh = client.open_by_key(sheet_id)
    stamp = taipei_now_str()

    target = prod_names if passed or not cfg["validation"]["strict"] else dev_names
    safe_replace_worksheet(sh, target.get("tw50","TW50"), df_tw50, stamp)
    safe_replace_worksheet(sh, target.get("top10","Top10"), df_top10, stamp)

    if cfg["validation"].get("write_qa_sheet", True):
        write_qa_sheet(sh, qa_df, cfg["validation"].get("qa_sheet_title","QA_Report"), stamp)

    # 4) Log 中印 Top10 摘要（方便快速驗收）
    try:
        cols = ["Ticker","Name","Close","RSI14","Volume","Entry_Low","Entry_High","Exit_Low","Exit_High"]
        pv = df_top10[cols].copy()
        for c in ["Close","RSI14","Entry_Low","Entry_High","Exit_Low","Exit_High"]:
            pv[c] = pd.to_numeric(pv[c], errors="coerce").round(2)
        print("\n[SUMMARY] Top10")
        print(pv.to_string(index=False), "\n")
    except Exception as e:
        print(f"[WARN] 無法印出 Top10 摘要：{e}")

    print("[INFO] All done.")


if __name__ == "__main__":
    main()
