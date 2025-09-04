# -*- coding: utf-8 -*-
"""
TW50 分組 + Hot20 + Top5_hot20(含Signal) 主程式（抗 404 強化版）
輸出：
  1) TW50_fin
  2) TW50_nonfin
  3) Top10_nonfin
  4) Hot20_nonfin
  5) Top5_hot20
"""

import os, json, time
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe

# 金融股清單
FIN_TICKERS = {
    "2880.TW","2881.TW","2882.TW","2883.TW","2884.TW","2885.TW",
    "2886.TW","2887.TW","2888.TW","2889.TW","2890.TW","2891.TW",
    "2892.TW","2897.TW","2898.TW","2899.TW","5871.TW","5876.TW"
}

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def taipei_now_str():
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def fetch_history(ticker, period, interval):
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(df.columns.levels[1][0], axis=1, level=1)
        df = df.rename(columns=str.title)
        df.index.name = "Date"
        return df
    except Exception as e:
        print(f"[WARN] fetch failed: {ticker} -> {e}")
        return pd.DataFrame()

def add_indicators(df):
    out = df.copy()
    out["SMA20"] = out["Close"].rolling(20, min_periods=1).mean()
    out["SMA50"] = out["Close"].rolling(50, min_periods=1).mean()
    out["SMA200"] = out["Close"].rolling(200, min_periods=1).mean()
    delta = out["Close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    roll_up = pd.Series(gain, index=out.index).rolling(14, min_periods=1).mean()
    roll_down = pd.Series(loss, index=out.index).rolling(14, min_periods=1).mean()
    rs = roll_up / (roll_down + 1e-9)
    out["RSI14"] = 100.0 - (100.0 / (1.0 + rs))
    mid = out["Close"].rolling(20, min_periods=1).mean()
    std = out["Close"].rolling(20, min_periods=1).std(ddof=0)
    out["BB_Mid"] = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std
    return out

def classify_signal(row):
    if row["RSI14"] < 40 and row["Close"] <= row["BB_Lower"]:
        return "Buy"
    if row["RSI14"] > 60 and row["Close"] >= row["BB_Upper"]:
        return "Sell"
    return "Neutral"

def get_gspread_client():
    js = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not js:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    return gspread.service_account_from_dict(json.loads(js))

# --- 關鍵修正：兩階段寫入 + 重試，避免 404 ---
def safe_replace_worksheet(sh, title, df, stamp, retries=3, wait=1.2):
    tmp_title = f"{title}__tmp"

    # 先刪除舊的 tmp
    try:
        sh.del_worksheet(sh.worksheet(tmp_title))
    except gspread.WorksheetNotFound:
        pass

    # 先建立 tmp
    rows = max(100, (len(df) + 10) if df is not None else 100)
    cols = max(26, (len(df.columns) + 10) if df is not None else 26)
    sh.add_worksheet(title=tmp_title, rows=rows, cols=cols)

    # 重新抓 tmp 物件（確保拿到正確 worksheet id）
    ws = sh.worksheet(tmp_title)

    # 更新 A1（加重試，避免短暫 404）
    for i in range(retries):
        try:
            ws.update_acell("A1", f"Last Update (Asia/Taipei): {stamp}")
            break
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(wait)

    # 寫入表格
    if df is not None and not df.empty:
        for i in range(retries):
            try:
                set_with_dataframe(ws, df.reset_index(drop=True), row=3, include_index=False, include_column_header=True)
                break
            except Exception as e:
                if i == retries - 1:
                    raise
                time.sleep(wait)
    else:
        ws.update_acell("A3", "No Data")

    # 刪除舊正式分頁
    try:
        sh.del_worksheet(sh.worksheet(title))
    except gspread.WorksheetNotFound:
        pass

    # 將 tmp 改名為正式名稱（也加重試）
    for i in range(retries):
        try:
            ws.update_title(title)
            break
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(wait)

def main():
    print("== TW50 v5: fin/nonfin + Hot20 + Top5 (Signal) ==")
    cfg = load_config()
    tickers = cfg.get("tickers", [])
    sheet_id = cfg.get("sheet_id")
    period = cfg.get("period", "12mo")
    interval = cfg.get("interval", "1d")

    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    # 抓資料
    frames = []
    for t in tickers:
        df = fetch_history(t, period, interval)
        if df.empty:
            print(f"[WARN] skip empty: {t}")
            continue
        ind = add_indicators(df)
        ind["Ticker"] = t
        frames.append(ind.reset_index())
    if not frames:
        raise RuntimeError("全部 ticker 都抓不到資料，請檢查網路/清單/權限")

    df_all = pd.concat(frames, ignore_index=True)

    # 分金融/非金融
    df_fin = df_all[df_all["Ticker"].isin(FIN_TICKERS)].copy()
    df_nonfin = df_all[~df_all["Ticker"].isin(FIN_TICKERS)].copy()

    # 取最新一筆做排名/快照
    last_nonfin = (
        df_nonfin.sort_values(["Ticker","Date"])
                 .groupby("Ticker", as_index=False)
                 .tail(1)
    )

    # Top10（非金融，RSI↓、Volume↓）
    df_top10 = last_nonfin.sort_values(["RSI14","Volume"], ascending=[False,False]).head(10).copy()

    # Hot20（非金融 Volume 前20）
    df_hot20 = last_nonfin.sort_values("Volume", ascending=False).head(20).copy()

    # Top5 from Hot20 + Signal
    df_top5 = df_hot20.sort_values(["RSI14","Volume"], ascending=[False,False]).head(5).copy()
    df_top5["Signal"] = df_top5.apply(classify_signal, axis=1)

    # 寫入
    stamp = taipei_now_str()
    safe_replace_worksheet(sh, "TW50_fin", df_fin, stamp)
    safe_replace_worksheet(sh, "TW50_nonfin", df_nonfin, stamp)
    safe_replace_worksheet(sh, "Top10_nonfin", df_top10, stamp)
    safe_replace_worksheet(sh, "Hot20_nonfin", df_hot20, stamp)
    safe_replace_worksheet(sh, "Top5_hot20", df_top5, stamp)
    print("[DONE] All sheets updated.")

if __name__ == "__main__":
    main()
