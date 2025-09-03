# src/main.py
# -*- coding: utf-8 -*-

import os, json, datetime as dt
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# ---------- 基本工具 ----------

def now_taipei_str():
    tz = dt.timezone(dt.timedelta(hours=8))
    return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    mode = os.getenv("MODE", cfg.get("mode", "dev")).strip().lower()
    if mode not in ("dev", "prod"):
        mode = "dev"
    cfg["__mode__"] = mode
    return cfg

def pick_sheet_names(cfg):
    mode = cfg["__mode__"]
    pages = cfg["sheets"][mode]
    tw50_name = pages["tw50"]
    top10_name = pages["top10"]

    # 安全防呆：dev 只能寫 *_test；prod 不能寫 *_test
    if mode == "dev" and (not tw50_name.endswith("_test") or not top10_name.endswith("_test")):
        raise RuntimeError("DEV 模式的分頁名稱必須以 _test 結尾，請檢查 config.json")
    if mode == "prod" and (tw50_name.endswith("_test") or top10_name.endswith("_test")):
        raise RuntimeError("PROD 模式不得寫入 _test 分頁，請檢查 config.json")
    return tw50_name, top10_name

def with_tw_suffix(codes):
    out = []
    for t in codes:
        t = str(t).strip().upper()
        if not t:
            continue
        out.append(t if t.endswith(".TW") else f"{t}.TW")
    return out

# ---------- 技術指標 ----------

def sma(series, win):
    return series.rolling(win, min_periods=win).mean()

def rsi_wilder(close, period=14):
    # 經典 Wilder RSI（向量化，groupby 安全）
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    # 初始均值
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()

    # Wilder 平滑
    avg_gain = avg_gain.copy()
    avg_loss = avg_loss.copy()
    for i in range(period+1, len(close)):
        if pd.isna(avg_gain.iat[i-1]) or pd.isna(avg_loss.iat[i-1]):
            continue
        avg_gain.iat[i] = (avg_gain.iat[i-1]*(period-1) + gain.iat[i]) / period
        avg_loss.iat[i] = (avg_loss.iat[i-1]*(period-1) + loss.iat[i]) / period

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

def bollinger(close, win=20, num_std=2):
    ma = close.rolling(win, min_periods=win).mean()
    sd = close.rolling(win, min_periods=win).std(ddof=0)
    upper = ma + num_std * sd
    lower = ma - num_std * sd
    width = (upper - lower) / ma
    return ma, upper, lower, width

# ---------- 資料抓取 ----------

def fetch_prices(tickers, start, end):
    """
    逐檔下載，避免把整組 list 丟給 yfinance 造成型態錯誤。
    回傳欄位：Date, Ticker, Open, High, Low, Close, Volume
    """
    rows = []
    for t in tickers:
        t = str(t).strip()
        if not t:
            continue
        print(f"[DL] {t} ...")
        try:
            df = yf.download([t], start=start, end=end, progress=False, auto_adjust=False)
            if df.empty:
                continue
            df = df.rename(columns=str.title)
            code_no_suffix = t.replace(".TW", "")
            df["Ticker"] = code_no_suffix
            df.reset_index(inplace=True)
            df["Date"] = pd.to_datetime(df["Date"]).dt.date
            rows.append(df[["Date","Ticker","Open","High","Low","Close","Volume"]])
        except Exception as e:
            print(f"[WARN] {t} 下載失敗：{e}")
    if not rows:
        return pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Volume"])
    out = pd.concat(rows, ignore_index=True)
    out.sort_values(["Ticker","Date"], inplace=True)
    return out

# ---------- 指標計算與中文欄位 ----------

def add_indicators(df, sma_windows=(20,50,200), rsi_len=14, bb_len=20):
    if df.empty:
        return df.assign(
            RSI_14=[], SMA_20=[], SMA_50=[], SMA_200=[],
            BB_20_Basis=[], BB_20_Upper=[], BB_20_Lower=[], BB_20_Width=[],
            ShortTrend=[], LongTrend=[], EntryZone=[], ExitZone=[], ShortSignal=[]
        )

    df = df.copy()
    # 逐股票計算
    def per_stock(g):
        g = g.sort_values("Date").copy()
        close = g["Close"]

        g["SMA_20"]  = sma(close, 20)
        g["SMA_50"]  = sma(close, 50)
        g["SMA_200"] = sma(close, 200)

        g["RSI_14"] = rsi_wilder(close, period=14)

        bb_mid, bb_up, bb_lo, bb_w = bollinger(close, win=20, num_std=2)
        g["BB_20_Basis"] = bb_mid
        g["BB_20_Upper"] = bb_up
        g["BB_20_Lower"] = bb_lo
        g["BB_20_Width"] = bb_w

        # 中文欄位（簡化版、一致且穩定）
        g["ShortTrend"] = np.where(g["SMA_20"] > g["SMA_50"], "Up", np.where(g["SMA_20"] < g["SMA_50"], "Down", "Neutral"))
        g["LongTrend"]  = np.where(g["SMA_50"] > g["SMA_200"], "Up", np.where(g["SMA_50"] < g["SMA_200"], "Down", "Neutral"))

        g["EntryZone"] = (g["Close"] <= g["BB_20_Lower"])
        g["ExitZone"]  = (g["Close"] >= g["BB_20_Upper"])
        g["ShortSignal"] = np.where(g["EntryZone"], "Buy", np.where(g["ExitZone"], "Sell", "Hold"))

        return g

    out = df.groupby("Ticker", group_keys=False).apply(per_stock)
    return out

# ---------- Google Sheets IO ----------

def open_gsheet(sheet_id: str):
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("找不到 GOOGLE_CREDENTIALS_JSON 環境變數（Secrets）")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(sheet_id)

def write_dataframe(ws, df: pd.DataFrame, note: str):
    # 清空 + 寫入
    ws.clear()
    # 時戳在 A1
    ws.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"]])
    if df.empty:
        return
    # 欄名 + 內容
    header = [ 
        "Date","Ticker","Open","High","Low","Close","Volume",
        "RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Basis","BB_20_Upper","BB_20_Lower","BB_20_Width",
        "ShortTrend","LongTrend","EntryZone","ExitZone","ShortSignal"
    ]
    df2 = df.loc[:, header].copy()
    values = [header] + df2.astype(object).where(pd.notna(df2), "").values.tolist()
    ws.update("A2", values)  # 從 A2 開始放資料
    # 註解（可在名稱列上方再寫一行，避免遮到 header）
    ws.update("A2", [header])  # 保險再寫一次標頭
    # 額外說明（可選）
    # ws.update("V1", [[note]])

# ---------- Top10 邏輯 ----------

def build_top10(df):
    if df.empty:
        return pd.DataFrame(columns=["Date","Ticker","Close","RSI_14","ShortSignal"])
    last = df.sort_values(["Ticker","Date"]).groupby("Ticker", as_index=False).tail(1)
    pick = last.loc[last["ShortSignal"] == "Buy"].copy()
    pick = pick.sort_values(["RSI_14","Ticker"], ascending=[True, True]).head(10)
    # 精簡欄位（給操作頁）
    out = pick[["Date","Ticker","Close","RSI_14","ShortSignal"]].reset_index(drop=True)
    return out

# ---------- 主流程 ----------

def main():
    cfg = load_cfg()
    mode = cfg["__mode__"]
    tw50_name, top10_name = pick_sheet_names(cfg)
    sheet_id = cfg["sheet_id"]
    start = cfg.get("start_date")
    end   = cfg.get("end_date")
    codes = cfg.get("tickers", [])
    print(f"[INFO] MODE={mode}")
    print(f"[INFO] 分頁對應：TW50={tw50_name}, Top10={top10_name}")
    if not codes:
        raise RuntimeError("config.json 缺少 tickers")

    tickers = with_tw_suffix(codes)
    print(f"[INFO] config 共有 tickers：自動從 TW50 分頁最終覆寫為準 ——")
    print(f"[INFO] Tickers: {codes}")

    # 1) 抓價
    px = fetch_prices(tickers, start, end)
    # 2) 指標
    full = add_indicators(px)
    # 3) 打開試算表
    sh = open_gsheet(sheet_id)
    ws_tw50 = sh.worksheet(tw50_name)
    ws_top10 = sh.worksheet(top10_name)

    # 4) 寫 TW50
    write_dataframe(ws_tw50, full, note=f"TW50 base data ({mode})")

    # 5) 產 Top10 並寫入
    top10 = build_top10(full)
    ws_top10.clear()
    ws_top10.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"]])
    if not top10.empty:
        header = ["Date","Ticker","Close","RSI_14","ShortSignal"]
        values = [header] + top10.astype(object).where(pd.notna(top10), "").values.tolist()
        ws_top10.update("A2", values)
        # ws_top10.update("F1", [["條件：ShortSignal=Buy → RSI 由低到高 → 取前10"]])

    print("[DONE] Sheets updated.")

if __name__ == "__main__":
    main()
