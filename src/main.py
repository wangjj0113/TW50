# src/main.py
# -*- coding: utf-8 -*-

"""
TA to Google Sheets (no pandas-ta dependency)

- 讀取 config.json（以 MODE 選 dev/prod）
- 用 yfinance 擷取收盤價/成交量（修正單檔回傳 DataFrame 的行為）
- 計算 SMA_20 / SMA_50 / SMA_200、RSI_14、布林通道（20）
- 依 config 寫入 TW50 / Top10（或 *_test）兩張工作表
- A1 寫入更新時間（Asia/Taipei）
"""

from __future__ import annotations
import os, json, math, datetime as dt
from typing import List, Dict

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# -----------------------
# 基本設定/工具
# -----------------------

TAIWAN_TZ = dt.timezone(dt.timedelta(hours=8))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def log(msg: str):
    print(f"[INFO] {msg}", flush=True)


def fatal(msg: str):
    print(f"[FATAL] {msg}", flush=True)
    raise SystemExit(1)


def load_cfg() -> Dict:
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


def with_tw_suffix(tickers: List[str]) -> List[str]:
    out = []
    for t in tickers:
        t = str(t).strip()
        out.append(t if t.endswith(".TW") else f"{t}.TW")
    return out


def mode_env() -> str:
    # 來自 GitHub Actions inputs / 或環境變數
    return os.getenv("MODE", "dev").strip().lower()


def pick_sheet_names(cfg: Dict) -> Dict[str, str]:
    """
    回傳 {'tw50': <sheet_name>, 'top10': <sheet_name>, 'sheet_id': <key>}
    依據 config.json 的 sheets -> env 選擇
    """
    env = mode_env()
    if env not in cfg["sheets"]:
        fatal(f"config.json 缺少環境 '{env}' 的 sheets 設定")

    # 名稱
    names = cfg["sheets"][env]
    if not isinstance(names, dict):
        fatal("config.json sheets[env] 應為物件")

    # Sheet ID：可放在 config 或 secrets；先以 config 為主，沒有則讀環境變數 SHEET_ID
    sheet_id = names.get("sheet_id") or os.getenv("SHEET_ID")
    if not sheet_id:
        fatal("找不到 Google 試算表的 sheet_id（請在 config.json 的對應環境或 Secrets.SHEET_ID 提供）")

    for k in ("tw50", "top10"):
        if k not in names or not names[k]:
            fatal(f"config.json sheets[{env}] 缺少 '{k}' 的工作表名稱")

    return {"tw50": names["tw50"], "top10": names["top10"], "sheet_id": sheet_id}


def connect_google_sheet(sheet_id: str) -> gspread.Spreadsheet:
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        fatal("GOOGLE_SERVICE_ACCOUNT_JSON secret 未設定")

    try:
        info = json.loads(creds_json)
    except Exception:
        fatal("GOOGLE_SERVICE_ACCOUNT_JSON 不是合法 JSON（請直接貼 service account 的整份 JSON 內容）")

    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(sheet_id)


# -----------------------
# 資料抓取/指標
# -----------------------

def fetch_one(ticker: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
    """
    下載單一股票日線資料。
    yfinance 對單檔直接回傳 DataFrame（無需 to_frame）。
    """
    df = yf.download(
        ticker,
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=True,
        threads=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    keep = ["Open", "High", "Low", "Close", "Volume"]
    for k in keep:
        if k not in df.columns:
            df[k] = np.nan

    df = df.reset_index()
    # yfinance 會給 'Date' 欄
    df["Ticker"] = ticker.replace(".TW", "")
    return df[["Date"] + keep + ["Ticker"]]


def fetch_prices(tickers: List[str]) -> pd.DataFrame:
    frames = []
    for t in tickers:
        log(f"[DL] {t}")
        df = fetch_one(t)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out.sort_values(["Ticker", "Date"], inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)

    roll_up = pd.Series(up, index=series.index).rolling(period).mean()
    roll_down = pd.Series(down, index=series.index).rolling(period).mean()

    rs = roll_up / (roll_down + 1e-12)
    return 100.0 - (100.0 / (1.0 + rs))


def add_indicators(base: pd.DataFrame) -> pd.DataFrame:
    if base.empty:
        return base

    def per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        g["SMA_20"] = g["Close"].rolling(20, min_periods=1).mean()
        g["SMA_50"] = g["Close"].rolling(50, min_periods=1).mean()
        g["SMA_200"] = g["Close"].rolling(200, min_periods=1).mean()

        g["RSI_14"] = rsi(g["Close"], 14)

        # BBands(20, 2)
        mid = g["Close"].rolling(20, min_periods=1).mean()
        std = g["Close"].rolling(20, min_periods=1).std(ddof=0)
        g["BB_20_Lower"] = mid - 2 * std
        g["BB_20_Upper"] = mid + 2 * std
        g["BB_20_Basis"] = mid
        g["BB_20_Width"] = (g["BB_20_Upper"] - g["BB_20_Lower"]) / (mid + 1e-12)

        # 簡單訊號（可依需求再調整）
        # 短線：收盤突破/跌破中軌
        cond_buy = (g["Close"] > g["BB_20_Basis"]) & (g["Close"].shift(1) <= g["BB_20_Basis"].shift(1))
        cond_sell = (g["Close"] < g["BB_20_Basis"]) & (g["Close"].shift(1) >= g["BB_20_Basis"].shift(1))
        g["ShortSignal"] = np.where(cond_buy, "Buy", np.where(cond_sell, "Sell", ""))

        # 長趨勢：SMA_50 相對 SMA_200
        g["LongTrend"] = np.where(g["SMA_50"] > g["SMA_200"], "Up", np.where(g["SMA_50"] < g["SMA_200"], "Down", "Neutral"))

        return g

    out = base.groupby("Ticker", group_keys=False).apply(per_stock)
    out.reset_index(drop=True, inplace=True)
    return out


def top10_selector(df: pd.DataFrame) -> pd.DataFrame:
    """取各股最近一筆，照成交量由大到小取 10 檔。"""
    if df.empty:
        return df

    last = (
        df.sort_values(["Ticker", "Date"])
          .groupby("Ticker", as_index=False)
          .tail(1)
    )
    last = last.sort_values("Volume", ascending=False)
    return last.head(10)


# -----------------------
# 寫 Sheet
# -----------------------

def write_timestamp(ws):
    ts = dt.datetime.now(TAIWAN_TZ).strftime("Last Update (Asia/Taipei): %Y-%m-%d %H:%M:%S")
    # A1 放字串，避免傳入 list 觸發 400
    ws.update_acell("A1", ts)


def write_table(ws, df: pd.DataFrame):
    # 從 A2 寫表格；避免 A1 被覆蓋時間字樣
    if df is None:
        df = pd.DataFrame()
    # 將 numpy 資料型別轉成原生，避免 gspread 400
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_float_dtype(df[c]) or pd.api.types.is_integer_dtype(df[c]):
            df[c] = df[c].astype(object)
    set_with_dataframe(ws, df, row=2, include_index=False, include_column_header=True, resize=True)


def ensure_worksheet(spread: gspread.Spreadsheet, name: str) -> gspread.Worksheet:
    try:
        return spread.worksheet(name)
    except gspread.WorksheetNotFound:
        return spread.add_worksheet(title=name, rows=2000, cols=50)


# -----------------------
# Main
# -----------------------

def main():
    cfg = load_cfg()
    env = mode_env()
    log(f"MODE={env}")

    # 取得頁籤與 sheet_id
    names = pick_sheet_names(cfg)
    page_tw50 = names["tw50"]
    page_top10 = names["top10"]
    sheet_id = names["sheet_id"]
    log(f"配置 環境標籤: TW50={page_tw50}，Top10={page_top10}")

    # 股票清單（允許在 config 寫不含 .TW）
    raw = cfg.get("tickers", [])
    if not raw:
        fatal("config.json 的 tickers 為空")
    tickers = with_tw_suffix([str(t) for t in raw])
    log(f"tickers: {tickers}")

    # 擷取資料 + 指標
    base = fetch_prices(tickers)
    if base.empty:
        fatal("無法取得任何行情資料")
    full = add_indicators(base)

    # 準備兩個表
    latest_cols = [
        "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
        "RSI_14", "SMA_20", "SMA_50", "SMA_200",
        "BB_20_Lower", "BB_20_Upper", "BB_20_Basis", "BB_20_Width",
        "ShortSignal", "LongTrend",
    ]

    # TW50：保留全部最近 N 天（這裡保留近 200 交易日，視需求調整）
    tw50_df = (
        full.sort_values(["Ticker", "Date"])
            .groupby("Ticker", as_index=False)
            .tail(200)
            .loc[:, latest_cols]
    )

    # Top10：取最近一筆、按成交量排序前 10
    top10_df = top10_selector(full).loc[:, latest_cols]

    # 連線 Google Sheet
    ss = connect_google_sheet(sheet_id)

    # TW50
    ws_tw = ensure_worksheet(ss, page_tw50)
    write_timestamp(ws_tw)
    write_table(ws_tw, tw50_df)

    # Top10
    ws_t10 = ensure_worksheet(ss, page_top10)
    write_timestamp(ws_t10)
    write_table(ws_t10, top10_df)

    log("全部完成 ✅")


if __name__ == "__main__":
    main()
