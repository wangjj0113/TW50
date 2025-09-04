# -*- coding: utf-8 -*-
"""
TW50 TOP5 – 每日更新腳本（含操作建議與進出場區間）
--------------------------------------------------
功能摘要
1) 從 yfinance 下載 TW50 成分股日資料（自動跳過無資料的代號）
2) 指標：SMA20/50/200、RSI14、布林通道(20,2)
3) 依「是否金融股」分成兩張表：TW50_fin、TW50_nonfin
4) 依成交量排序做：
   - Top10_nonfin（非金前 10）
   - Hot20_nonfin（非金前 20）
   - Top5_hot20（從 Hot20 篩出 5 檔，含『操作建議』與『建議進/出場區間』）
5) A1 顯示台北時區更新時間
環境變數（GitHub Actions secrets）
- SHEET_ID
- GCP_SERVICE_ACCOUNT_JSON
- FMP/FINNHUB_TOKEN (可留白；程式會照常跑)
"""

import os
import math
import json
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials


# ----------------------------
# 基本設定
# ----------------------------
SHEET_ID = os.environ["SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GCP_SERVICE_ACCOUNT_JSON"]
TZ = ZoneInfo("Asia/Taipei")

# TW50 清單（可改成從 config.json 載入；這裡內建一份常見代號，漏掉不影響，抓不到會自動跳過）
TW50_TICKERS = [
    # 電/半導體/電子
    "2330.TW","2317.TW","2454.TW","2308.TW","2303.TW","2382.TW","2379.TW","8046.TW","3034.TW",
    "6669.TW","3711.TW","3037.TW","2207.TW","2327.TW","1301.TW","1303.TW","1326.TW","1101.TW",
    "1102.TW","2002.TW","1216.TW","2891.TW","2892.TW","2881.TW","2882.TW","2883.TW","2884.TW",
    "2885.TW","2886.TW","2887.TW","2888.TW","2890.TW","2897.TW","2899.TW","2880.TW","2889.TW",
    "2603.TW","2615.TW","2609.TW","2610.TW","2301.TW","2357.TW","2383.TW","2313.TW","5871.TW",
    "9910.TW","9904.TW","6505.TW","5876.TW","2882.TW"  # 重複也無妨，會去重
]
TW50_TICKERS = sorted(list({t for t in TW50_TICKERS}))

# 金融股判斷（台灣市場「28xx」多為金融族群）
def is_financial(ticker: str) -> bool:
    try:
        code = ticker.split(".")[0]
        return code.startswith("28")
    except Exception:
        return False


# ----------------------------
# 指標計算
# ----------------------------
def compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    對完整歷史 df 加指標；回傳含最新一列（今日/最近交易日）的資料列
    """
    out = df.copy()
    out["SMA20"]  = out["Close"].rolling(20, min_periods=20).mean()
    out["SMA50"]  = out["Close"].rolling(50, min_periods=50).mean()
    out["SMA200"] = out["Close"].rolling(200, min_periods=200).mean()

    out["RSI14"] = compute_rsi(out["Close"], 14)

    std20 = out["Close"].rolling(20, min_periods=20).std()
    out["BB_Mid"]   = out["SMA20"]
    out["BB_Upper"] = out["SMA20"] + 2 * std20
    out["BB_Lower"] = out["SMA20"] - 2 * std20

    # 只取最後一列輸出（搭配完整歷史計算出的最新指標）
    last = out.tail(1).copy()
    return last


# ----------------------------
# 操作建議（人話）
# ----------------------------
def generate_signal(row: pd.Series) -> str:
    rsi     = row.get("RSI14", np.nan)
    close   = row.get("Close", np.nan)
    sma20   = row.get("SMA20", np.nan)
    bb_u    = row.get("BB_Upper", np.nan)
    bb_l    = row.get("BB_Lower", np.nan)

    # 指標不足：觀望
    if any(pd.isna([rsi, close, sma20, bb_u, bb_l])):
        return "資料不足｜觀望"

    hints = []

    # 1) RSI 區間
    if rsi >= 70:
        hints.append("RSI過熱")
    elif rsi <= 30:
        hints.append("RSI低檔")

    # 2) 價位 vs 布林
    if close >= bb_u:
        hints.append("觸壓力區")
        action = "賣出或觀望"
    elif close <= bb_l:
        hints.append("觸支撐區")
        action = "買入或分批佈局"
    else:
        # 3) 價位 vs SMA20
        if close >= sma20:
            hints.append("站上SMA20(多)")
            action = "拉回佈局"
        else:
            hints.append("跌破SMA20(空)")
            action = "站回再說"

    prefix = "、".join(hints) if hints else "訊號中性"
    return f"{prefix}｜建議：{action}"


def compute_entry_exit(row: pd.Series) -> tuple[str, str]:
    """
    產生建議進/出場「區間」文字（人話版本）
    - 進場：介於 BB_Lower ~ SMA20 視為較安全的左側/回檔買點
    - 出場：介於 SMA20 ~ BB_Upper 視為短線壓力區（偏向停利/降風險）
    """
    close = row.get("Close", np.nan)
    sma20 = row.get("SMA20", np.nan)
    bb_u  = row.get("BB_Upper", np.nan)
    bb_l  = row.get("BB_Lower", np.nan)

    if any(pd.isna([close, sma20, bb_u, bb_l])):
        return ("—", "—")

    buy_low  = min(bb_l, sma20)
    buy_high = max(bb_l, sma20)
    sell_low = min(sma20, bb_u)
    sell_high= max(sma20, bb_u)

    buy_txt  = f"{buy_low:.2f} ~ {buy_high:.2f}"
    sell_txt = f"{sell_low:.2f} ~ {sell_high:.2f}"
    return (buy_txt, sell_txt)


# ----------------------------
# 下載 & 組表
# ----------------------------
def safe_yf_download(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """
    包裝 yfinance 下載，找不到資料會回空表，並印 WARN。
    """
    try:
        df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
        if df is None or df.empty:
            print(f"[WARN] {ticker} 無資料（可能暫無報價/被下市？）→ 跳過")
            return pd.DataFrame()
        # yfinance 會用 MultiIndex 欄位，確保是一般欄
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df[["Open","High","Low","Close","Volume"]].copy()
    except Exception as e:
        print(f"[WARN] {ticker} 下載失敗 → {e}")
        return pd.DataFrame()


def collect_latest_frame(tickers: list[str]) -> pd.DataFrame:
    rows = []
    for t in tickers:
        df_hist = safe_yf_download(t)
        if df_hist.empty:
            continue
        last = add_indicators(df_hist)
        last["Ticker"] = t

        # 取公司名稱（若取不到就留空）
        try:
            info = yf.Ticker(t).get_info()  # 部分版本可能較慢/可能缺值
            cname = info.get("shortName") or info.get("longName")
        except Exception:
            cname = None
        last["公司名稱"] = cname

        # 建議 + 區間
        last["操作建議"] = last.apply(generate_signal, axis=1)
        buy, sell = compute_entry_exit(last.iloc[0])
        last["建議買入區間"] = buy
        last["建議賣出區間"] = sell

        rows.append(last)

        # 禮貌性降速，避免對方 API 節流（雲端可視需要調整/拿掉）
        time.sleep(0.2)

    if not rows:
        return pd.DataFrame()

    out = pd.concat(rows, ignore_index=False)
    out.index.name = "Date"
    out.reset_index(inplace=True)
    # 欄位排序（可依喜好調整）
    cols = [
        "Date","Ticker","公司名稱","Open","High","Low","Close","Volume",
        "RSI14","SMA20","SMA50","SMA200","BB_Mid","BB_Upper","BB_Lower",
        "操作建議","建議買入區間","建議賣出區間"
    ]
    exist_cols = [c for c in cols if c in out.columns]
    out = out[exist_cols]
    return out


# ----------------------------
# Google Sheet I/O
# ----------------------------
def gs_client():
    creds = Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def safe_replace_worksheet(gc, sheet_id: str, title: str, df: pd.DataFrame, stamp_text: str):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=100, cols=30)

    # A1 放時間
    ws.update("A1", [[stamp_text]])

    if df is None or df.empty:
        ws.update("A3", [["（本次無可用資料）"]])
        return

    values = [df.columns.tolist()] + df.values.tolist()
    ws.update(f"A3", values)  # 從第3列開始，保留 A1 時間、A2 空行


# ----------------------------
# 主流程
# ----------------------------
def main():
    print("[INFO] 開始 TW50 TOP5 更新")

    # 下載資料 & 指標
    df_all = collect_latest_frame(TW50_TICKERS)
    if df_all.empty:
        raise RuntimeError("本次所有代號皆無可用資料，請稍後再試")

    # 分流：金融 / 非金融
    df_fin    = df_all[df_all["Ticker"].str.startswith("28")].copy()
    df_nonfin = df_all[~df_all["Ticker"].str.startswith("28")].copy()

    # 排名：非金成交量 Top10 / Top20
    df_nonfin_sorted = df_nonfin.sort_values("Volume", ascending=False, kind="mergesort")
    top10  = df_nonfin_sorted.head(10).copy()
    hot20  = df_nonfin_sorted.head(20).copy()

    # 從 Hot20 篩 Top5（先以 Volume 排，再以 RSI14 由低到高挑 5 檔做「逢低布局」）
    hot20_rank = hot20.sort_values(["RSI14","Volume"], ascending=[True, False], kind="mergesort")
    top5 = hot20_rank.head(5).copy()

    # 更新時間（台北）
    stamp = f"Last Update (Asia/Taipei): {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}"
    print(stamp)

    # 寫入 Google Sheet
    gc = gs_client()
    safe_replace_worksheet(gc, SHEET_ID, "TW50_fin",    df_fin,    stamp)
    safe_replace_worksheet(gc, SHEET_ID, "TW50_nonfin", df_nonfin, stamp)
    safe_replace_worksheet(gc, SHEET_ID, "Top10_nonfin", top10,    stamp)
    safe_replace_worksheet(gc, SHEET_ID, "Hot20_nonfin", hot20,    stamp)
    safe_replace_worksheet(gc, SHEET_ID, "Top5_hot20",   top5,     stamp)

    print("[OK] TW50 TOP5 更新完成")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERROR] 例外：", e)
        traceback.print_exc()
        raise
