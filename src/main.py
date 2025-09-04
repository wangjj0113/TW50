# -*- coding: utf-8 -*-
"""
TW50-TOP5 main.py
版本：v2025.09.04-Top5-zh-4
對應 Secrets（和 workflow 相同名稱）：
  - SHEET_ID
  - GCP_SERVICE_ACCOUNT_JSON
功能：
  - 產出 5 個分頁：TW50_fin / TW50_nonfin / Top10_nonfin / Hot20_nonfin / Top5_hot20
  - Top5_hot20：公司名稱 + 中文欄位 + 訊號(買進/賣出/觀望) + 建議進/出場區間
  - 每頁 A1 寫「資料截至 (Asia/Taipei)」
  - 自動跳過抓不到資料的代號，於 log 顯示「已跳過清單」
"""

import os, json, time
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe

# ===== 代號 ↔ 公司名稱（可擴充）=====
TICKER_NAME_MAP = {
    "2330.TW":"台積電","2317.TW":"鴻海","2454.TW":"聯發科","2303.TW":"聯電","2308.TW":"台達電",
    "2379.TW":"瑞昱","2382.TW":"廣達","2395.TW":"研華","2408.TW":"南亞科","2412.TW":"中華電",
    "3006.TW":"晶豪科","3008.TW":"大立光","3711.TW":"日月光投控","2603.TW":"長榮","2609.TW":"陽明",
    "2615.TW":"萬海","1216.TW":"統一","1402.TW":"遠東新","1301.TW":"台塑","1326.TW":"台化",
    "1101.TW":"台泥","1102.TW":"亞泥","2002.TW":"中鋼","4904.TW":"遠傳","3481.TW":"群創",
    # 金融
    "2880.TW":"華南金","2881.TW":"富邦金","2882.TW":"國泰金","2883.TW":"開發金","2884.TW":"玉山金",
    "2885.TW":"元大金","2886.TW":"兆豐金","2887.TW":"台新金","2888.TW":"新光金","2889.TW":"國票金",  # 修正
    "2890.TW":"永豐金","2891.TW":"中信金","2892.TW":"第一金","2897.TW":"王道銀行","2898.TW":"安泰銀",
    "5871.TW":"中租-KY","5876.TW":"上海商銀"
}

# 金融股集合（28xx + 其他金融）
FIN_TICKERS = {t for t in TICKER_NAME_MAP if t.startswith("28")}
FIN_TICKERS.update({"5871.TW","5876.TW"})

# ===== 小工具 =====
def taipei_now_str():
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def get_gspread_client():
    js = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not js:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    return gspread.service_account_from_dict(json.loads(js))

def get_sheet():
    sheet_id = os.environ.get("SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("缺少 SHEET_ID Secret")
    print("[INFO] SHEET_ID:", sheet_id)
    return get_gspread_client().open_by_key(sheet_id)

def get_or_create(sh, title, rows=2000, cols=30):
    for ws in sh.worksheets():
        if ws.title == title: return ws
    return sh.add_worksheet(title=title, rows=rows, cols=cols)

def upsert_df(ws, df, stamp_text):
    ws.clear()
    ws.update("A1", f"資料截至 (Asia/Taipei): {stamp_text}")
    if df is None or df.empty:
        ws.update("A3", "No Data")
        return
    set_with_dataframe(ws, df, row=2, include_index=False, include_column_header=True)

# ===== 指標計算 =====
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # 均線
    df["SMA20"]  = df["Close"].rolling(20, min_periods=20).mean()
    df["SMA50"]  = df["Close"].rolling(50, min_periods=50).mean()
    df["SMA200"] = df["Close"].rolling(200, min_periods=200).mean()
    # RSI14
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))
    # 布林
    mid = df["Close"].rolling(20, min_periods=20).mean()
    std = df["Close"].rolling(20, min_periods=20).std()
    df["BB_Mid"]   = mid
    df["BB_Upper"] = mid + 2 * std
    df["BB_Lower"] = mid - 2 * std
    return df

def fetch_last_row(ticker: str, period="12mo", interval="1d") -> pd.DataFrame | None:
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
        if df.empty:
            print(f"[WARN] {ticker} 無資料，跳過")
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df[["Open","High","Low","Close","Volume"]]
        df = add_indicators(df)
        return df.tail(1).copy()
    except Exception as e:
        print(f"[WARN] 下載失敗 {ticker}: {e}")
        return None

# ===== 主流程 =====
def main():
    print("== TW50 vTop5 zh ==")
    sh = get_sheet()
    stamp = taipei_now_str()

    # 讀取清單（優先 config.json 的 tickers，否則用 TICKER_NAME_MAP keys）
    tickers = []
    cfg_path = "config.json"
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                tickers = cfg.get("tickers") or cfg.get("TW50") or []
        except Exception as e:
            print("[WARN] 讀取 config.json 失敗，改用內建清單", e)
    if not tickers:
        tickers = list(TICKER_NAME_MAP.keys())

    rows, failed = [], []
    for t in tickers:
        row = fetch_last_row(t, period="12mo", interval="1d")
        if row is None or row.empty:
            failed.append(t)
            continue
        row.insert(0, "公司名稱", TICKER_NAME_MAP.get(t, ""))
        row.insert(0, "股票代號", t)
        rows.append(row.reset_index().rename(columns={"index":"Date"}))

    if not rows:
        raise RuntimeError("本次沒有任何代號成功抓到資料")

    df_all = pd.concat(rows, ignore_index=True)

    # 分金融 / 非金融
    is_fin = df_all["股票代號"].isin(FIN_TICKERS) | df_all["股票代號"].str.startswith("28")
    df_fin    = df_all[is_fin].copy()
    df_nonfin = df_all[~is_fin].copy()

    # 整理全量欄位順序（全量表）
    base_cols = ["股票代號","公司名稱","Date","Open","High","Low","Close","Volume",
                 "RSI14","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"]
    base_cols = [c for c in base_cols if c in df_all.columns]
    df_fin_all    = df_fin[base_cols].copy()
    df_nonfin_all = df_nonfin[base_cols].copy()

    # Top10（非金融，依 RSI↓、Volume↓）
    top10 = df_nonfin.sort_values(["RSI14","Volume"], ascending=[False, False]).head(10).copy()

    # Hot20（非金融，依成交量）
    hot20 = df_nonfin.sort_values("Volume", ascending=False).head(20).copy()

    # Top5 from Hot20（加訊號＋進出場區間；中文欄位）
    top5 = hot20.sort_values(["RSI14","Volume"], ascending=[False, False]).head(5).copy()

    # 訊號（保守版：RSI + 布林）
    def signal(row):
        if pd.notna(row["RSI14"]) and pd.notna(row["BB_Lower"]) and row["RSI14"] < 40 and row["Close"] <= row["BB_Lower"]:
            return "買進"
        if pd.notna(row["RSI14"]) and pd.notna(row["BB_Upper"]) and row["RSI14"] > 60 and row["Close"] >= row["BB_Upper"]:
            return "賣出"
        return "觀望"
    top5["訊號"] = top5.apply(signal, axis=1)

    # 進/出場區間（布林下~中 / 中~上）
    top5["建議進場下界"] = top5["BB_Lower"]
    top5["建議進場上界"] = top5["BB_Mid"]
    top5["建議出場下界"] = top5["BB_Mid"]
    top5["建議出場上界"] = top5["BB_Upper"]

    # Top5 欄位中文順序
    top5_cols = ["股票代號","公司名稱","Date","Close","RSI14","訊號",
                 "建議進場下界","建議進場上界","建議出場下界","建議出場上界",
                 "Open","High","Low","Volume","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"]
    top5_cols = [c for c in top5_cols if c in top5.columns]
    top5_out = top5[top5_cols].rename(columns={"Close":"收盤價"}).copy()

    # === 寫入各分頁 ===
    for title, data in [
        ("TW50_fin",    df_fin_all),
        ("TW50_nonfin", df_nonfin_all),
        ("Top10_nonfin",top10),
        ("Hot20_nonfin",hot20),
        ("Top5_hot20",  top5_out),
    ]:
        ws = get_or_create(sh, title)
        upsert_df(ws, data, stamp)
        time.sleep(0.3)

    # 額外：把本次「已跳過」清單印出（看 Actions log）
    if failed:
        print("[WARN] 這些代號找不到資料 → 已跳過：", ", ".join(failed))
    else:
        print("[INFO] 本次所有代號皆成功")

    print("✅ 全部分頁更新完成")

if __name__ == "__main__":
    main()
