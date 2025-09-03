# -*- coding: utf-8 -*-
"""
TW50 / Top10 -> Google Sheets (DEV/TEST)
- GitHub Actions: set env MODE=dev (test) or MODE=prod (正式)
- config.json (test 用) 例:
{
  "sheet_id": "...",
  "worksheet": "TW50_test",
  "worksheet_top10": "Top10_test",
  "rsi_length": 14,
  "sma_windows": [20, 50, 200],
  "bb_length": 20,
  "start_date": "2025-01-01",
  "end_date": "2025-12-31"
  // "tickers": [...]  # 可省略，省略時使用 fallback 清單
}
"""

import os
import io
import json
import time
import math
import pytz
import logging
import datetime as dt
from typing import List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe

# ---------- 基礎設定 ----------
TZ = pytz.timezone("Asia/Taipei")
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)

# ---------- 公用 ----------
def tw_now() -> str:
    return dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def load_cfg() -> dict:
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    mode = os.getenv("MODE", "dev")
    cfg["mode"] = "prod" if mode == "prod" else "dev"
    return cfg

def with_tw_suffix(ts: List[str]) -> List[str]:
    # 補 .TW 後綴
    return [t if t.endswith(".TW") else f"{t}.TW" for t in ts]

def fallback_tickers() -> List[str]:
    # 如果 config 沒給 tickers，就用一組最小保險清單（可自行改回 0050 成分）
    return ["2330", "2317", "2882", "2881", "2454"]

# ---------- 抓價 ----------
def fetch_prices(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    codes = with_tw_suffix(tickers)
    frames = []
    for code, t in zip(codes, tickers):
        logging.info(f"[DL] {t}.TW")
        df = yf.download(code, start=start, end=end, interval="1d", progress=False)
        if df is None or df.empty:
            continue
        df = df.reset_index().rename(columns={
            "Date": "Date", "Open": "Open", "High":"High", "Low":"Low",
            "Close":"Close", "Volume":"Volume"
        })
        df["Ticker"] = t
        frames.append(df)
        time.sleep(0.2)  # 禮貌性間隔，避免風控
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # 保險：欄位次序
    cols = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
    out = out[cols]
    return out

# ---------- 指標 ----------
def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.rolling(length).mean()
    avg_loss = loss.rolling(length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out

def bollinger_basis(series: pd.Series, n: int = 20, k: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    ma = series.rolling(n).mean()
    sd = series.rolling(n).std()
    upper = ma + k * sd
    lower = ma - k * sd
    width = (upper - lower)
    return ma, upper, lower, width

def per_stock(stock_df: pd.DataFrame, sma_windows: List[int], rsi_len: int, bb_len: int) -> pd.DataFrame:
    df = stock_df.sort_values("Date").copy()

    # 移動平均：逐欄建立，保證單一 Series 賦值
    for w in sma_windows:
        df[f"SMA_{w}"] = df["Close"].rolling(w).mean()

    # RSI
    df["RSI_14"] = rsi(df["Close"], rsi_len)

    # 布林
    bb_mid, bb_up, bb_lo, bb_w = bollinger_basis(df["Close"], bb_len, 2.0)
    df["BB_20_Basis"] = bb_mid
    df["BB_20_Upper"] = bb_up
    df["BB_20_Lower"] = bb_lo
    df["BB_20_Width"] = bb_w

    # 短/長趨勢
    def trend_short(row):
        if pd.isna(row["SMA_20"]) or pd.isna(row["SMA_50"]):
            return "Neutral"
        if row["SMA_20"] > row["SMA_50"]:
            return "Up"
        if row["SMA_20"] < row["SMA_50"]:
            return "Down"
        return "Neutral"

    def trend_long(row):
        if pd.isna(row["SMA_50"]) or pd.isna(row["SMA_200"]):
            return "Neutral"
        if row["SMA_50"] > row["SMA_200"]:
            return "Up"
        if row["SMA_50"] < row["SMA_200"]:
            return "Down"
        return "Neutral"

    df["ShortTrend"] = df.apply(trend_short, axis=1)
    df["LongTrend"] = df.apply(trend_long, axis=1)

    # 進出場區間（布林）
    df["EntryZone"] = (df["Close"] <= df["BB_20_Lower"]).fillna(False)
    df["ExitZone"]  = (df["Close"] >= df["BB_20_Upper"]).fillna(False)

    # 中文建議（簡化版邏輯）
    def short_signal(row):
        c, rsi_v = row["Close"], row["RSI_14"]
        if pd.isna(c) or pd.isna(rsi_v):
            return "Hold"
        if rsi_v < 30 or row["EntryZone"]:
            return "Buy"
        if rsi_v > 70 or row["ExitZone"]:
            return "Sell"
        return "Hold"

    df["ShortSignal"] = df.apply(short_signal, axis=1)

    return df

def add_indicators(base: pd.DataFrame, rsi_len: int, sma_windows: List[int], bb_len: int) -> pd.DataFrame:
    # 以每檔個別計算，再 concat 回來（避免多欄塞單欄問題）
    out = (
        base
        .groupby("Ticker", group_keys=False)
        .apply(lambda x: per_stock(x, sma_windows, rsi_len, bb_len))
        .reset_index(drop=True)
    )
    return out

# ---------- Top10 ----------
def build_top10(df: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
    if df.empty:
        return df
    # 各檔最新一列
    idx = df.groupby("Ticker")["Date"].idxmax()
    latest = df.loc[idx].copy()
    # 條件：短線=Buy，依 RSI 由低→高
    mask = latest["ShortSignal"].eq("Buy")
    top = latest.loc[mask].sort_values(["RSI_14", "Ticker"], ascending=[True, True]).head(top_k)
    # Top10 欄位精簡（可依你表格需求調整）
    keep = [
        "Date","Ticker","Close",
        "RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Lower","BB_20_Upper",
        "ShortSignal","LongTrend"
    ]
    existing = [c for c in keep if c in top.columns]
    return top[existing].reset_index(drop=True)

# ---------- Sheets ----------
def get_gspread_client():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("環境變數 GOOGLE_CREDENTIALS_JSON 缺失")
    creds = json.loads(raw)
    gc = gspread.service_account_from_dict(creds)
    return gc

def write_sheet(ws, df: pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

def write_timestamp(ws):
    ts = f"Last Update (Asia/Taipei): {tw_now()}"
    ws.update("A1", ts)

# ---------- 主流程 ----------
def pick_sheet_names(cfg: dict) -> Tuple[str, str]:
    """
    以 config.json 的鍵值為準：
      - worksheet: dev/測試用 TW50 分頁
      - worksheet_top10: dev/測試用 Top10 分頁
    """
    tw50_name = cfg.get("worksheet", "TW50_test")
    top10_name = cfg.get("worksheet_top10", "Top10_test")
    # 保護：dev 模式不允許寫到正式名單
    env = cfg["mode"]
    if env == "dev" and (tw50_name in ("TW50", "Top10") or top10_name in ("TW50", "Top10")):
        raise RuntimeError("DEV 模式不允許寫入正式分頁，請確認 config.json 的 worksheet 名稱")
    return tw50_name, top10_name

def main():
    cfg = load_cfg()
    mode = cfg["mode"]
    logging.info(f"MODE={mode}")

    # 1) 讀 Sheet 與目標分頁名
    sheet_id = cfg.get("sheet_id", "").strip()
    if not sheet_id:
        raise RuntimeError("config.json 缺少 sheet_id")
    tw50_sheet_name, top10_sheet_name = pick_sheet_names(cfg)
    logging.info(f"[INFO] 寫入目標：TW50={tw50_sheet_name}，Top10={top10_sheet_name}")

    # 2) 構成股池
    tickers = cfg.get("tickers")
    if not tickers or not isinstance(tickers, list) or len(tickers) == 0:
        logging.info("[INFO] config 未提供 tickers，使用 fallback 清單")
        tickers = fallback_tickers()
    logging.info(f"[INFO] 標的數：{len(tickers)} -> {tickers[:5]}{' ...' if len(tickers)>5 else ''}")

    # 3) 抓價
    start = cfg.get("start_date", "2025-01-01")
    end   = cfg.get("end_date",   dt.date.today().strftime("%Y-%m-%d"))
    base = fetch_prices(tickers, start, end)
    if base.empty:
        raise RuntimeError("抓不到任何價格資料，請檢查網路/代碼/期間")

    # 4) 指標
    rsi_len = int(cfg.get("rsi_length", 14))
    sma_windows = cfg.get("sma_windows", [20, 50, 200])
    bb_len = int(cfg.get("bb_length", 20))
    if not isinstance(sma_windows, list) or not all(isinstance(x, int) for x in sma_windows):
        sma_windows = [20, 50, 200]

    full = add_indicators(base, rsi_len, sma_windows, bb_len)

    # 5) Top10
    top10 = build_top10(full, top_k=10)

    # 6) 寫入 Google Sheets
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    # TW50
    ws_tw50 = sh.worksheet(tw50_sheet_name)
    write_sheet(ws_tw50, full)
    write_timestamp(ws_tw50)

    # Top10
    ws_top10 = sh.worksheet(top10_sheet_name)
    write_sheet(ws_top10, top10)
    write_timestamp(ws_top10)

    logging.info("[DONE] 寫入完成")

if __name__ == "__main__":
    main()
