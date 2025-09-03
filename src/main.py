# -*- coding: utf-8 -*-
"""
TW50 → Google Sheet（無 pandas-ta）
修正點：
- 指標一律用 groupby().transform(...)，右側回傳 Series，避免
  "Cannot set a DataFrame with multiple columns to the single column ..."。
- 台股代號自動補 .TW
- A1 寫入時間戳，資料從 A2 起用 gspread_dataframe.set_with_dataframe 輸出
"""

import os
import json
import math
import datetime as dt
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe

# ---------- 小工具 ----------

def with_tw_suffix(tickers):
    out = []
    for t in tickers:
        t = str(t).strip()
        if not t:
            continue
        if not t.endswith(".TW"):
            t += ".TW"
        out.append(t)
    return out

def load_cfg():
    # 優先讀 repo 的 config.json
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # sheet_id 允許用 Secrets 覆蓋
    sheet_id_env = os.getenv("SHEET_ID")
    if sheet_id_env:
        cfg["sheet_id"] = sheet_id_env

    # mode 允許用環境變數覆蓋（dev/prod）
    mode_env = os.getenv("MODE", "").strip().lower()
    if mode_env in ("dev", "prod"):
        cfg["mode"] = mode_env
    else:
        cfg["mode"] = cfg.get("mode", "prod")

    # 期間
    cfg["start_date"] = cfg.get("start_date", "2024-01-01")
    cfg["end_date"] = cfg.get("end_date", dt.date.today().isoformat())

    # 台股代號補 .TW
    cfg["tickers"] = with_tw_suffix(cfg.get("tickers", []))
    return cfg

def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    ma_up = up.rolling(period, min_periods=period).mean()
    ma_down = down.rolling(period, min_periods=period).mean()

    rs = ma_up / ma_down.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(0)

def add_indicators(df):
    # df 需包含：Ticker, Date, Open, High, Low, Close, Volume
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)
    g = df.groupby("Ticker", group_keys=False)

    # 均線：用 transform → Series（與 index 對齊）
    df["SMA_20"]  = g["Close"].transform(lambda s: s.rolling(20,  min_periods=1).mean())
    df["SMA_50"]  = g["Close"].transform(lambda s: s.rolling(50,  min_periods=1).mean())
    df["SMA_200"] = g["Close"].transform(lambda s: s.rolling(200, min_periods=1).mean())

    # RSI 14
    df["RSI_14"] = g["Close"].transform(lambda s: rsi(s, 14))

    # 布林 20（同樣用 transform 取得 Series）
    bb_avg = g["Close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    bb_std = g["Close"].transform(lambda s: s.rolling(20, min_periods=20).std())
    df["BB_20_Basis"] = bb_avg
    df["BB_20_Upper"] = bb_avg + 2 * bb_std
    df["BB_20_Lower"] = bb_avg - 2 * bb_std
    df["BB_20_Width"] = (df["BB_20_Upper"] - df["BB_20_Lower"]) / df["BB_20_Basis"]

    # 簡單訊號（可之後再調整邏輯）
    df["ShortSignal"] = np.where(df["Close"] > df["BB_20_Upper"], "Buy",
                          np.where(df["Close"] < df["BB_20_Lower"], "Sell", "Hold"))
    df["LongTrend"] = np.where(df["SMA_50"] > df["SMA_200"], "Up",
                        np.where(df["SMA_50"] < df["SMA_200"], "Down", "Neutral"))

    # 版面友善欄位順序
    cols = [
        "Date","Ticker","Open","High","Low","Close","Volume",
        "RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Basis","BB_20_Upper","BB_20_Lower","BB_20_Width",
        "ShortSignal","LongTrend"
    ]
    existing = [c for c in cols if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    return df[existing + rest]

def download_prices(tickers, start, end):
    # yfinance 下載 multi-index → 攤平成長表
    data = yf.download(tickers, start=start, end=end, auto_adjust=False, group_by="ticker", threads=True)
    frames = []
    for t in tickers:
        if t not in data.columns.get_level_values(0):
            # 個別 ticker 下載不到也略過
            try:
                d1 = yf.download(t, start=start, end=end, auto_adjust=False)
                if d1.empty:
                    continue
                d1 = d1.reset_index()[["Date","Open","High","Low","Close","Volume"]]
            except Exception:
                continue
        else:
            d1 = data[t][["Open","High","Low","Close","Volume"]].reset_index()

        d1["Ticker"] = t
        frames.append(d1)

    if not frames:
        return pd.DataFrame(columns=["Date","Ticker","Open","High","Low","Close","Volume"])
    out = pd.concat(frames, ignore_index=True)
    # 確保日期是 date / datetime
    if not np.issubdtype(out["Date"].dtype, np.datetime64):
        out["Date"] = pd.to_datetime(out["Date"])
    return out

# ---------- Google Sheets ----------

def connect_google_sheet():
    # 服務金鑰：建議放在 GitHub Secret GOOGLE_SERVICE_ACCOUNT_JSON
    svc_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not svc_json:
        raise RuntimeError("找不到環境變數 GOOGLE_SERVICE_ACCOUNT_JSON（請放 service account JSON 內容）")
    try:
        creds = json.loads(svc_json)
    except Exception as e:
        raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON 不是合法 JSON：{e}")
    gc = gspread.service_account_from_dict(creds)
    return gc

def write_timestamp(ws, tz_name="Asia/Taipei"):
    # A1 寫時間，資料從 A2 開始
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))) if tz_name == "Asia/Taipei" else dt.datetime.now()
    ws.update("A1", f"Last Update ({tz_name}): {now.strftime('%Y-%m-%d %H:%M:%S')}")

def write_dataframe(ws, df):
    # 從 A2 輸出（避免覆蓋 A1 的時間戳）
    if df is None or df.empty:
        # 清一下舊資料，但保留 A1
        ws.batch_clear(["A2:Z"])
        return
    set_with_dataframe(ws, df, row=2, col=1, include_index=False, include_column_header=True, resize=True)

# ---------- 主流程 ----------

def pick_sheet_names(cfg):
    env = cfg.get("mode", "prod")
    sheets = cfg.get("sheets", {})
    if env not in sheets:
        raise RuntimeError(f"MODE={env} 沒有對應的 sheets 設定，請確認 config.json")
    tw50_name = sheets[env].get("tw50")
    top10_name = sheets[env].get("top10")
    if not tw50_name or not top10_name:
        raise RuntimeError("config.json 的 sheets 設定缺少 tw50 或 top10 名稱")
    return tw50_name, top10_name

def main():
    print(f"[INFO] MODE={os.getenv('MODE', 'prod')}")
    cfg = load_cfg()
    print(f"[INFO] 讀到設定：MODE={cfg['mode']}，TW50={cfg['sheets'][cfg['mode']]['tw50']}，Top10={cfg['sheets'][cfg['mode']]['top10']}")
    print(f"[INFO] 使用 tickers：{cfg['tickers'][:5]} ...（共 {len(cfg['tickers'])} 檔）")

    # 下載資料
    prices = download_prices(cfg["tickers"], cfg["start_date"], cfg["end_date"])
    if prices.empty:
        raise RuntimeError("下載不到任何價格資料，請檢查 tickers / 期間或網路")

    # 計算技術指標（這裡已修成 transform → Series）
    base = add_indicators(prices)

    # 連線 Sheet
    gc = connect_google_sheet()
    sh = gc.open_by_key(cfg["sheet_id"])
    tw50_ws_name, top10_ws_name = pick_sheet_names(cfg)

    # TW50 分頁：全部輸出
    tw50_ws = sh.worksheet(tw50_ws_name)
    write_timestamp(tw50_ws, tz_name="Asia/Taipei")
    # 欄位順序友善化
    out_cols = [
        "Date","Ticker","Open","High","Low","Close","Volume",
        "RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Basis","BB_20_Upper","BB_20_Lower","BB_20_Width",
        "ShortSignal","LongTrend"
    ]
    out_cols = [c for c in out_cols if c in base.columns]
    write_dataframe(tw50_ws, base[out_cols])

    # Top10 分頁：挑 10 檔（示例：RSI_14 最高前 10）
    tmp = base.dropna(subset=["RSI_14"])
    top10 = (
        tmp.sort_values(["Date","RSI_14"], ascending=[True, False])
           .groupby("Date", as_index=False)
           .head(10)
           .sort_values(["Date","RSI_14"], ascending=[True, False])
    )
    top10_ws = sh.worksheet(top10_ws_name)
    write_timestamp(top10_ws, tz_name="Asia/Taipei")
    write_dataframe(top10_ws, top10[out_cols])

    print("[OK] 輸出完成")

if __name__ == "__main__":
    main()
