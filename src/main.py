# -*- coding: utf-8 -*-
import os, json, time
from datetime import datetime, timezone, timedelta
from typing import List

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials


# ---------------- 基本工具 ----------------
def tw_now_str():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def load_cfg():
    # config.json 可選；主要由環境變數決定
    cfg = {
        "mode": os.getenv("MODE", "prod"),
        "sheet_id": os.getenv("SHEET_ID", "")
    }
    # 若有本地 config.json，補上設置
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                local = json.load(f)
                cfg.update({k: v for k, v in local.items() if v})
        except Exception:
            pass

    # 正式分頁名稱固定（防誤寫入 test）
    cfg.setdefault("TW50_sheet_name", "TW50")
    # 0050 成分可自行替換；先放常見幾檔以恢復運作
    cfg.setdefault("tickers", ["2330", "2317", "2454", "2882", "2881"])
    return cfg


def connect_sheet(sheet_id: str):
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not raw:
        raise RuntimeError("缺少 GOOGLE_SERVICE_ACCOUNT_JSON（請到 Settings > Secrets 設定）")
    try:
        sa_info = json.loads(raw)
    except Exception as e:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 不是合法 JSON") from e

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)


# ---------------- 資料抓取 ----------------
def with_tw_suffix(codes: List[str]) -> List[str]:
    out = []
    for c in codes:
        s = str(c).strip().upper()
        if not s:
            continue
        out.append(s if s.endswith(".TW") else f"{s}.TW")
    return out


def fetch_one(ticker: str, start: str = None, end: str = None) -> pd.DataFrame:
    """單檔下載，避免 yfinance 多檔結構差異造成錯誤。"""
    df = yf.download([ticker], start=start, end=end, interval="1d", progress=False, auto_adjust=True)
    if df is None or df.empty:
        return pd.DataFrame()
    # 單檔下載時 columns 可能是單層；用統一欄名
    if isinstance(df.columns, pd.MultiIndex):
        df = df["Close"].to_frame(name="Close").join(
            df["Open"].to_frame(name="Open")
        ).join(df["High"].to_frame(name="High")).join(
            df["Low"].to_frame(name="Low")
        ).join(df["Volume"].to_frame(name="Volume"))
    df = df.reset_index().rename(columns=str.title)
    keep = ["Date", "Open", "High", "Low", "Close", "Volume"]
    for k in keep:
        if k not in df.columns:
            df[k] = np.nan
    return df[keep]


def fetch_prices(tickers: List[str]) -> pd.DataFrame:
    frames = []
    for t in with_tw_suffix(tickers):
        base = fetch_one(t)
        if base.empty:
            continue
        base["Ticker"] = t.replace(".TW", "")
        frames.append(base)
        time.sleep(0.15)  # 禮貌性間隔
    if not frames:
        return pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Volume"])
    out = pd.concat(frames, ignore_index=True)
    # 排序 & 型別
    out["Date"] = pd.to_datetime(out["Date"]).dt.date
    out = out.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    return out


# ---------------- 指標 & 中文欄位 ----------------
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()

    def per_stock(g: pd.DataFrame) -> pd.DataFrame:
        g = g.sort_values("Date").copy()
        close = pd.to_numeric(g["Close"], errors="coerce")
        # SMA
        g["SMA_20"]  = close.rolling(20, min_periods=1).mean()
        g["SMA_50"]  = close.rolling(50, min_periods=1).mean()
        g["SMA_200"] = close.rolling(200, min_periods=1).mean()
        # RSI(14)（簡化穩定版）
        delta = close.diff()
        gain = delta.clip(lower=0.0).rolling(14, min_periods=14).mean()
        loss = (-delta).clip(lower=0.0).rolling(14, min_periods=14).mean().replace(0, np.nan)
        rs = gain / loss
        g["RSI_14"] = 100 - (100 / (1 + rs))
        # 布林(20,2)
        ma20 = close.rolling(20, min_periods=1).mean()
        sd20 = close.rolling(20, min_periods=1).std(ddof=0).fillna(0)
        g["BB_20_Basis"] = ma20
        g["BB_20_Upper"] = ma20 + 2 * sd20
        g["BB_20_Lower"] = ma20 - 2 * sd20
        g["BB_20_Width"] = (g["BB_20_Upper"] - g["BB_20_Lower"]).abs()
        # 中文欄位（與你原本習慣一致）
        g["短線趨勢"] = np.where(g["SMA_20"] > g["SMA_50"], "上升",
                         np.where(g["SMA_20"] < g["SMA_50"], "下降", "中立"))
        g["長線趨勢"] = np.where(g["SMA_50"] > g["SMA_200"], "上升",
                         np.where(g["SMA_50"] < g["SMA_200"], "下降", "中立"))
        g["進場區間"] = g["Close"] <= g["BB_20_Lower"]
        g["出場區間"] = g["Close"] >= g["BB_20_Upper"]
        g["短線建議"] = np.where((g["RSI_14"] < 30) | g["進場區間"], "買入",
                          np.where((g["RSI_14"] > 70) | g["出場區間"], "賣出", "觀望"))
        g["長線建議"] = np.where((g["SMA_50"] > g["SMA_200"]) & (g["SMA_20"] > g["SMA_50"]), "持有", "觀望")
        return g

    out = df.groupby("Ticker", group_keys=False).apply(per_stock).reset_index(drop=True)
    return out


# ---------------- Sheets I/O（不使用 gspread-dataframe） ----------------
TW50_HEADERS = [
    "Date","Ticker","Open","High","Low","Close","Volume",
    "RSI_14","SMA_20","SMA_50","SMA_200",
    "BB_20_Basis","BB_20_Upper","BB_20_Lower","BB_20_Width",
    "短線趨勢","長線趨勢","進場區間","出場區間","短線建議","長線建議"
]

def ws_write_full(ws, df: pd.DataFrame):
    # 清空 -> A1 時間戳 -> 從 A2 開始寫內容
    ws.clear()
    ws.update("A1", [[f"Last Update (Asia/Taipei): {tw_now_str()}"]])

    # 若空資料，至少寫表頭
    if df.empty:
        ws.update("A2", [TW50_HEADERS])
        return

    df2 = df.copy()
    # 欄位順序
    exist = [c for c in TW50_HEADERS if c in df2.columns]
    df2 = df2[exist]

    # 轉成 values（保留 True/False）
    values = [exist] + df2.astype(object).where(pd.notnull(df2), "").values.tolist()
    ws.update("A2", values, value_input_option="RAW")


# ---------------- Main ----------------
def main():
    cfg = load_cfg()
    if cfg["mode"] != "prod":
        # 主程式恢復：強制保護，只允許寫正式分頁於 prod
        print(f"[WARN] MODE={cfg['mode']} 目前是恢復主程式，請在 workflow 設定 MODE=prod")
    sheet_id = cfg.get("sheet_id") or os.getenv("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("缺少 sheet_id（Secrets:SHEET_ID 或 config.json）")

    sh = connect_sheet(sheet_id)
    ws_tw50 = sh.worksheet(cfg["TW50_sheet_name"])  # 正式分頁 TW50

    # 下載 + 計算
    prices = fetch_prices(cfg["tickers"])
    if prices.empty:
        raise RuntimeError("抓不到任何報價，請檢查 tickers 或網路")
    enriched = add_indicators(prices)

    # 寫入 TW50
    ws_write_full(ws_tw50, enriched)
    print("[OK] TW50 已更新完畢（正式分頁）")


if __name__ == "__main__":
    main()
