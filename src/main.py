# -*- coding: utf-8 -*-
"""
TA to Google Sheets (test)
環境：GitHub Actions Ubuntu + Python + gspread + yfinance
重點：
- 分頁名稱完全由 config.json 指定（支援空格，例如 "Top 10 test"）
- 不用 apply，全部向量化計算指標與訊號
- Top10：短線=Buy → 依 RSI(由低到高) 取前10
- A1 寫入台北時間時間戳
"""

import os, json, io, sys, time, math, datetime as dt
from datetime import timezone, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread.exceptions import WorksheetNotFound

# ========== 共用 ==========

def tw_now_str():
    tw = timezone(timedelta(hours=8))
    return dt.datetime.now(tw).strftime("%Y-%m-%d %H:%M:%S")

def _load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    env = os.getenv("MODE", cfg.get("mode", "dev"))
    if env not in ("dev", "prod"):
        env = "dev"
    cfg["_env"] = env
    return cfg

def _open_sheet(cfg):
    svc_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not svc_json:
        raise RuntimeError("缺少 GOOGLE_CREDENTIALS_JSON（到 Actions → Secrets 設定你的 Service Account JSON）")
    sa_info = json.loads(svc_json)
    gc = gspread.service_account_from_dict(sa_info)
    return gc.open_by_key(cfg["sheet_id"])

def _get_ws(sh, name):
    try:
        return sh.worksheet(name)
    except WorksheetNotFound as e:
        exist = [ws.title for ws in sh.worksheets()]
        raise WorksheetNotFound(
            f"找不到分頁：{name}\n目前存在：{exist}\n→ 請把 Google Sheet 分頁改成「{name}」，或在 config.json 改成現有名稱。"
        ) from e

def pick_ws_pair(cfg):
    sh = _open_sheet(cfg)
    env = cfg["_env"]
    tw50_name  = cfg["sheets"][env]["tw50"]
    top10_name = cfg["sheets"][env]["top10"]
    print(f"[INFO] MODE={env}")
    print(f"[INFO] 目標分頁：TW50='{tw50_name}', Top10='{top10_name}'")
    tw50_ws  = _get_ws(sh, tw50_name)
    top10_ws = _get_ws(sh, top10_name)
    return tw50_ws, top10_ws

def with_tw_suffix(ts):
    return [t if str(t).endswith(".TW") else f"{t}.TW" for t in ts]

# ========== 下載 & 指標 ==========

def fetch_prices(tickers, start, end):
    """
    回傳欄位：Date, Ticker, Open, High, Low, Close, Volume
    """
    rows = []
    for t in tickers:
        print(f"[DL] {t} ...")
        try:
            df = yf.download(t, start=start, end=end, progress=False, auto_adjust=False)
            if df.empty:
                continue
            df = df.rename(columns=str.title)  # Open High Low Close Volume
            df["Ticker"] = t.replace(".TW", "")
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

def add_indicators(df, rsi_len=14, sma_windows=(20,50,200), bb_len=20):
    """
    以 Ticker 分組計算各種技術指標（向量化）
    """
    if df.empty:
        return df

    df = df.copy()
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    def _group(g):
        g = g.sort_values("Date")
        # SMA
        for n in sma_windows:
            g[f"SMA_{n}"] = g["Close"].rolling(n, min_periods=1).mean()

        # RSI(14)
        delta = g["Close"].diff()
        up = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        roll_up = up.rolling(rsi_len, min_periods=1).mean()
        roll_dn = down.rolling(rsi_len, min_periods=1).mean()
        rs = roll_up / roll_dn.replace(0, np.nan)
        g["RSI_14"] = 100 - (100 / (1 + rs))
        g["RSI_14"] = g["RSI_14"].fillna(0)

        # Bollinger(20,2)
        ma = g["Close"].rolling(bb_len, min_periods=1).mean()
        sd = g["Close"].rolling(bb_len, min_periods=1).std().fillna(0)
        g["BB_20_Basis"] = ma
        g["BB_20_Upper"] = ma + 2*sd
        g["BB_20_Lower"] = ma - 2*sd
        g["BB_20_Width"] = (g["BB_20_Upper"] - g["BB_20_Lower"]).abs()

        return g

    df = df.groupby("Ticker", group_keys=False).apply(_group)
    return df

def add_signals(df):
    if df.empty:
        return df

    close = df["Close"].astype(float)
    rsi   = df["RSI_14"].astype(float)
    sma20 = df["SMA_20"].astype(float)
    sma50 = df["SMA_50"].astype(float)
    sma200= df["SMA_200"].astype(float)
    bb_lo = df["BB_20_Lower"].astype(float)
    bb_hi = df["BB_20_Upper"].astype(float)

    df["ShortTrend"] = np.where(sma20 > sma50, "Up", np.where(sma20 < sma50, "Down", "Neutral"))
    df["LongTrend"]  = np.where(sma50 > sma200,"Up", np.where(sma50 < sma200,"Down", "Neutral"))

    df["EntryZone"]  = close <= bb_lo
    df["ExitZone"]   = close >= bb_hi
    df["多空壓力"]    = close >= bb_hi
    df["多空支撐"]    = close <= bb_lo

    df["ShortSignal"] = np.where(
        (rsi < 30) | (close <= bb_lo), "Buy",
        np.where((rsi > 70) | (close >= bb_hi) | (close < sma20), "Sell", "Hold")
    )
    df["LongSignal"] = np.where(
        (sma50 > sma200) & (sma20 > sma50), "Hold",
        np.where((sma50 < sma200) & (sma20 < sma50), "Neutral", "Neutral")
    )

    df["建議理由_短線"] = np.where(
        df["ShortSignal"]=="Buy",  "RSI<30 或 觸及下軌",
        np.where(df["ShortSignal"]=="Sell","RSI>70/觸上軌/跌破SMA20","區間整理")
    )
    df["建議理由_長線"] = np.where(
        df["LongSignal"]=="Hold",  "均線多頭排列",
        np.where(df["LongSignal"]=="Neutral","均線空/盤整","觀望")
    )

    return df

# ========== Google Sheets I/O ==========

TW50_HEADERS = [
    "Date","Ticker","Open","High","Low","Close","Volume",
    "RSI_14","SMA_20","SMA_50","SMA_200",
    "BB_20_Basis","BB_20_Upper","BB_20_Lower","BB_20_Width",
    "ShortTrend","LongTrend","EntryZone","ExitZone","ShortSignal","LongSignal",
    "建議理由_短線","建議理由_長線"
]

TOP10_HEADERS = [
    "Date","Ticker","Close","RSI_14","SMA_20","SMA_50","SMA_200",
    "ShortSignal","建議理由_短線"
]

def sheet_write_whole(ws, values):
    """整表覆蓋（保留 A1 時間戳，內容從第2列開始寫）"""
    # 清掉 A2 以後
    rows = len(values)
    cols = len(values[0]) if rows else 1
    # 先把標題 + 內容寫上去
    ws.clear()
    # A1 時間戳
    ws.update("A1", [[f"Last Update (Asia/Taipei): {tw_now_str()}"]])
    # 標題從 A2 寫
    start_cell = "A2"
    ws.update(start_cell, values, value_input_option="RAW")

def df_to_values(df, headers):
    if df.empty:
        return [headers]  # 至少寫入表頭
    out = df.copy()
    out = out[headers].copy()
    # 布林轉成 TRUE/FALSE 便於過濾
    for c in out.columns:
        if out[c].dtype == bool:
            out[c] = out[c].astype(bool)
    values = [headers] + out.astype(object).where(pd.notnull(out), "").values.tolist()
    return values

# ========== Top10 ==========

def make_top10(df):
    """短線=Buy → 依 RSI 由低到高 → 取前10"""
    if df.empty:
        return pd.DataFrame(columns=TOP10_HEADERS)
    last = (
        df.sort_values("Date")
          .groupby("Ticker", as_index=False)
          .tail(1)  # 每檔取最後一天
    )
    pick = last[last["ShortSignal"]=="Buy"].copy()
    pick = pick.sort_values("RSI_14", ascending=True).head(10)
    return pick[TOP10_HEADERS].copy()

# ========== 取得 tickers（config 不寫也行） ==========

def load_tickers_from_sheet(ws):
    """嘗試從既有 TW50 表抓 Ticker 欄；抓不到就回預設"""
    try:
        data = ws.get_all_records()
        if not data:
            raise ValueError("empty")
        series = pd.DataFrame(data).get("Ticker")
        tickers = [str(t) for t in series.dropna().unique().tolist() if str(t).strip()]
        tickers = [t for t in tickers if t.isalnum()]  # 粗略過濾
        if tickers:
            return tickers
    except Exception:
        pass
    # 預設少量避免打太久
    return ["2330","2317","2882","2881","2454"]

# ========== Main ==========

def main():
    cfg = _load_cfg()
    tw50_ws, top10_ws = pick_ws_pair(cfg)

    # 取得 tickers（config 沒寫就從表抓；都沒有就用預設）
    tickers = cfg.get("tickers")
    if not tickers:
        print("[INFO] config 未提供 tickers，嘗試從 TW50 分頁讀取現有清單 …")
        tickers = load_tickers_from_sheet(tw50_ws)
    print(f"[INFO] Tickers: {tickers}")

    start = cfg.get("start_date", "2025-01-01")
    end   = cfg.get("end_date",   dt.date.today().isoformat())

    prices = fetch_prices(with_tw_suffix(tickers), start, end)
    if prices.empty:
        raise RuntimeError("沒有抓到任何價格資料，請確認 tickers / 日期區間。")

    base = add_indicators(prices,
                          rsi_len=cfg.get("rsi_length",14),
                          sma_windows=tuple(cfg.get("sma_windows",[20,50,200])),
                          bb_len=cfg.get("bb_length",20))
    base = add_signals(base)

    # 寫入 TW50 分頁
    tw50_values = df_to_values(base, TW50_HEADERS)
    sheet_write_whole(tw50_ws, tw50_values)
    print("[OK] TW50 更新完成")

    # Top10
    top10_df = make_top10(base)
    top10_values = df_to_values(top10_df, TOP10_HEADERS)
    sheet_write_whole(top10_ws, top10_values)
    print("[OK] Top10 更新完成")

if __name__ == "__main__":
    try:
        main()
        print("[DONE]", tw_now_str())
    except Exception as e:
        print("[FATAL]", e)
        sys.exit(1)
