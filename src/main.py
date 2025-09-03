# -*- coding: utf-8 -*-
"""
TW50 / Top10 -> Google Sheets (DEV/TEST, 防重複欄位修正版)
"""

import os
import json
import time
import logging
import datetime as dt
from typing import List, Tuple

import pytz
import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe

# ---------- 基礎設定 ----------
TZ = pytz.timezone("Asia/Taipei")
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

def tw_now() -> str:
    return dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

# ---------- 設定 ----------
def load_cfg() -> dict:
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    mode = os.getenv("MODE", "dev")
    cfg["mode"] = "prod" if mode == "prod" else "dev"
    return cfg

def with_tw_suffix(ts: List[str]) -> List[str]:
    return [t if t.endswith(".TW") else f"{t}.TW" for t in ts]

def fallback_tickers() -> List[str]:
    # 沒給 tickers 就用最小保險清單
    return ["2330", "2317", "2882", "2881", "2454"]

# ---------- 清理工具 ----------
def _ensure_single_close(df: pd.DataFrame) -> pd.DataFrame:
    """確保只有一欄 Close，若有重複欄名或多欄同名，收斂為單欄。"""
    # 先去掉重複欄名（保留第一個）
    df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    # 若 Close 不存在就嘗試從 'Adj Close' 補
    if "Close" not in df.columns and "Adj Close" in df.columns:
        df["Close"] = df["Adj Close"]

    # 若還是沒有 Close，就建一欄空的，避免後續炸掉
    if "Close" not in df.columns:
        df["Close"] = np.nan

    # 若 Close 是 DataFrame（極少見），壓成單欄
    close_obj = df["Close"]
    if isinstance(close_obj, pd.DataFrame):
        # 取第一欄
        df["Close"] = pd.to_numeric(close_obj.iloc[:, 0], errors="coerce")

    # 統一數值型態
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

# ---------- 抓價 ----------
def fetch_prices(tickers: List[str], start: str, end: str) -> pd.DataFrame:
    codes = with_tw_suffix(tickers)
    frames = []
    for code, t in zip(codes, tickers):
        logging.info(f"[DL] {t}.TW")
        df = yf.download(code, start=start, end=end, interval="1d", progress=False)
        if df is None or df.empty:
            continue

        # 將 MultiIndex 欄轉一般欄
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([str(x) for x in col if str(x) != ""])
                          for col in df.columns]

        df = df.reset_index()
        # 統一欄名（有些版本是小寫）
        rename_map = {
            "Date": "Date", "Open": "Open", "High": "High",
            "Low": "Low", "Close": "Close", "Adj Close": "Adj Close",
            "Volume": "Volume"
        }
        df = df.rename(columns=rename_map)

        # 清理重複欄並確保 Close 是單欄
        df = _ensure_single_close(df)

        # 只留需要欄（若不存在就會自動補在 _ensure_single_close）
        keep = ["Date", "Open", "High", "Low", "Close", "Volume"]
        for k in keep:
            if k not in df.columns:
                df[k] = np.nan
        df = df[keep]

        df["Ticker"] = t
        frames.append(df)
        time.sleep(0.2)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    # 欄位順序保險
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
    return 100.0 - (100.0 / (1.0 + rs))

def bollinger_basis(series: pd.Series, n: int = 20, k: float = 2.0):
    ma = series.rolling(n).mean()
    sd = series.rolling(n).std()
    upper = ma + k * sd
    lower = ma - k * sd
    width = (upper - lower)
    return ma, upper, lower, width

def per_stock(stock_df: pd.DataFrame,
              sma_windows: List[int],
              rsi_len: int,
              bb_len: int) -> pd.DataFrame:
    df = stock_df.sort_values("Date").copy()

    # 再保險：確保只有一欄 Close、且為數值
    df = _ensure_single_close(df)

    # SMA 逐欄建立（右側永遠是 Series）
    for w in sma_windows:
        df[f"SMA_{w}"] = df["Close"].rolling(int(w)).mean()

    # RSI
    df["RSI_14"] = rsi(df["Close"], int(rsi_len))

    # 布林
    bb_mid, bb_up, bb_lo, bb_w = bollinger_basis(df["Close"], int(bb_len), 2.0)
    df["BB_20_Basis"] = bb_mid
    df["BB_20_Upper"] = bb_up
    df["BB_20_Lower"] = bb_lo
    df["BB_20_Width"] = bb_w

    # 趨勢
    def trend_short(row):
        a, b = row.get("SMA_20"), row.get("SMA_50")
        if pd.isna(a) or pd.isna(b): return "Neutral"
        if a > b: return "Up"
        if a < b: return "Down"
        return "Neutral"

    def trend_long(row):
        a, b = row.get("SMA_50"), row.get("SMA_200")
        if pd.isna(a) or pd.isna(b): return "Neutral"
        if a > b: return "Up"
        if a < b: return "Down"
        return "Neutral"

    df["ShortTrend"] = df.apply(trend_short, axis=1)
    df["LongTrend"]  = df.apply(trend_long, axis=1)

    # 進出場區間 + 短線建議
    df["EntryZone"] = (df["Close"] <= df["BB_20_Lower"]).fillna(False)
    df["ExitZone"]  = (df["Close"] >= df["BB_20_Upper"]).fillna(False)

    def short_signal(row):
        if pd.isna(row["RSI_14"]): return "Hold"
        if row["RSI_14"] < 30 or row["EntryZone"]: return "Buy"
        if row["RSI_14"] > 70 or row["ExitZone"]: return "Sell"
        return "Hold"

    df["ShortSignal"] = df.apply(short_signal, axis=1)
    return df

def add_indicators(base: pd.DataFrame, rsi_len: int, sma_windows: List[int], bb_len: int) -> pd.DataFrame:
    return (
        base.groupby("Ticker", group_keys=False)
            .apply(lambda x: per_stock(x, sma_windows, rsi_len, bb_len))
            .reset_index(drop=True)
    )

# ---------- Top10 ----------
def build_top10(df: pd.DataFrame, top_k: int = 10) -> pd.DataFrame:
    if df.empty: return df
    idx = df.groupby("Ticker")["Date"].idxmax()
    latest = df.loc[idx].copy()
    mask = latest["ShortSignal"].eq("Buy")
    top = latest.loc[mask].sort_values(["RSI_14", "Ticker"], ascending=[True, True]).head(top_k)
    keep = ["Date","Ticker","Close","RSI_14","SMA_20","SMA_50","SMA_200",
            "BB_20_Lower","BB_20_Upper","ShortSignal","LongTrend"]
    keep = [c for c in keep if c in top.columns]
    return top[keep].reset_index(drop=True)

# ---------- Sheets ----------
def get_gspread_client():
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("缺少 GOOGLE_CREDENTIALS_JSON")
    creds = json.loads(raw)
    return gspread.service_account_from_dict(creds)

def write_sheet(ws, df: pd.DataFrame):
    ws.clear()
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

def write_timestamp(ws):
    ws.update("A1", f"Last Update (Asia/Taipei): {tw_now()}")

# ---------- 主流程 ----------
def pick_sheet_names(cfg: dict) -> Tuple[str, str]:
    tw50_name = cfg.get("worksheet", "TW50_test")
    top10_name = cfg.get("worksheet_top10", "Top10_test")
    if cfg["mode"] == "dev" and (tw50_name in ("TW50","Top10") or top10_name in ("TW50","Top10")):
        raise RuntimeError("DEV 模式不允許寫入正式分頁，請檢查 config.json")
    return tw50_name, top10_name

def main():
    cfg = load_cfg()
    logging.info(f"MODE={cfg['mode']}")

    sheet_id = cfg.get("sheet_id", "").strip()
    if not sheet_id:
        raise RuntimeError("config.json 缺少 sheet_id")

    tw50_sheet_name, top10_sheet_name = pick_sheet_names(cfg)
    logging.info(f"Target sheets: TW50={tw50_sheet_name}, Top10={top10_sheet_name}")

    tickers = cfg.get("tickers")
    if not tickers: 
        logging.info("[INFO] 使用 fallback tickers")
        tickers = fallback_tickers()

    start = cfg.get("start_date", "2025-01-01")
    end   = cfg.get("end_date",   dt.date.today().strftime("%Y-%m-%d"))

    base = fetch_prices(tickers, start, end)
    if base.empty:
        raise RuntimeError("抓不到價格資料")

    rsi_len = int(cfg.get("rsi_length", 14))
    sma_windows = cfg.get("sma_windows", [20, 50, 200])
    bb_len = int(cfg.get("bb_length", 20))
    if not isinstance(sma_windows, list): sma_windows = [20, 50, 200]

    full = add_indicators(base, rsi_len, sma_windows, bb_len)
    top10 = build_top10(full, top_k=10)

    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    ws_tw50 = sh.worksheet(tw50_sheet_name)
    write_sheet(ws_tw50, full);  write_timestamp(ws_tw50)

    ws_top10 = sh.worksheet(top10_sheet_name)
    write_sheet(ws_top10, top10); write_timestamp(ws_top10)

    logging.info("[DONE] 寫入完成")

if __name__ == "__main__":
    main()
