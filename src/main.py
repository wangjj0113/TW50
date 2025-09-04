# -*- coding: utf-8 -*-
"""
TW50 / TOP5 自動化主程式 (fix: Date -> str before writing)

重點修正：
- 在寫入 Google Sheet 之前，將 DataFrame 的 'Date' 欄位轉為字串
  避免「TypeError: Object of type Timestamp is not JSON serializable」

環境變數（GitHub Actions Secrets）：
- SHEET_ID                      目標 Google 試算表 ID
- GOOGLE_SERVICE_ACCOUNT_JSON   服務帳戶 JSON 內容（整段貼入）
- FINNHUB_TOKEN（可選）        若有，回補成交量等；沒有則用 yfinance

分頁（若不存在會自動建立）：
- "TW50_fin"
- "TW50_nonfin"
- "Top10_nonfin"
- "Hot20_nonfin"
- "Top5_hot20"

備註：
- 若有個別代號抓不到資料，會在 log 顯示並「跳過」；整體不中斷
- 寫入前統一把所有日期轉成字串，避免 JSON 序列化錯誤
"""

import os
import json
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials

# ========= 實用小工具 =========

TZ_TAIPEI = timezone(timedelta(hours=8))


def now_taipei_str():
    return datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(alpha=1/period, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-9)
    return 100 - (100 / (1 + rs))


def bbands(close: pd.Series, window: int = 20, num_sd: float = 2.0):
    mid = close.rolling(window, min_periods=window).mean()
    std = close.rolling(window, min_periods=window).std()
    upper = mid + num_sd * std
    lower = mid - num_sd * std
    return mid, upper, lower


# ========= Google Sheets 連線 =========

def get_gspread_client():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("缺少 GOOGLE_SERVICE_ACCOUNT_JSON Secret")
    info = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc


def open_or_create_ws(ss, title: str, rows=1000, cols=30):
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=rows, cols=cols)
    return ws


def write_df(ws, df: pd.DataFrame):
    """安全覆寫整張表：放標題列 + 資料；A1 放更新時間"""
    # ---- 關鍵修正：所有日期欄位統一轉字串 ----
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype.__class__.__name__ == "Timestamp":
            df[col] = df[col].astype(str)
    if "Date" in df.columns:
        df["Date"] = df["Date"].astype(str)
    # --------------------------------------

    values = [df.columns.tolist()] + df.astype(object).fillna("").values.tolist()

    ws.clear()
    # 更新時間放在 A1
    ws.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"]])
    # 內容從第 3 列開始（留一空行觀感較清楚）
    ws.update(f"A3", values)


# ========= 抓行情 + 指標 =========

def fetch_one(ticker: str, period: str = "1y"):
    """回傳該代號近一年日線，含指標；若抓不到回傳 None"""
    try:
        df = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
        if df is None or df.empty:
            print(f"[WARN] yfinance 無資料：{ticker}（跳過）")
            return None
        df = df.rename_axis("Date").reset_index()  # 這裡 Date 是 Timestamp
        # 技術指標
        df["RSI14"] = rsi(df["Close"], 14)
        df["SMA20"] = df["Close"].rolling(20, min_periods=20).mean()
        df["SMA50"] = df["Close"].rolling(50, min_periods=50).mean()
        df["SMA200"] = df["Close"].rolling(200, min_periods=200).mean()
        mid, up, low = bbands(df["Close"], 20, 2.0)
        df["BB_Mid"] = mid
        df["BB_Upper"] = up
        df["BB_Lower"] = low
        # 填上代號
        df.insert(1, "Ticker", ticker)
        return df
    except Exception as e:
        print(f"[ERROR] 抓取失敗 {ticker}: {e}（跳過）")
        return None


def build_universe(tickers):
    dfs = []
    for t in tickers:
        d = fetch_one(t)
        if d is not None:
            dfs.append(d)
        time.sleep(0.2)  # 禮貌性節流
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    return out


def add_simple_names(df: pd.DataFrame):
    """補公司名稱（簡單做法：取 yfinance 的 shortName；取不到就留空）"""
    if df.empty:
        return df
    unique = df["Ticker"].drop_duplicates().tolist()
    names = {}
    for t in unique:
        try:
            info = yf.Ticker(t).info
            names[t] = info.get("shortName", "")
        except Exception:
            names[t] = ""
        time.sleep(0.1)
    df.insert(2, "公司名稱", df["Ticker"].map(names))
    return df


# ========= 產出各分頁 =========

FIN_SHEET = "TW50_fin"
NONFIN_SHEET = "TW50_nonfin"
TOP10_NONFIN = "Top10_nonfin"
HOT20_NONFIN = "Hot20_nonfin"
TOP5_HOT20 = "Top5_hot20"

# 你也可以把清單放在 config.json；這裡先內建防呆（列一些常見成分作示範）
FALLBACK_FIN = [
    "2880.TW", "2881.TW", "2882.TW", "2883.TW", "2884.TW", "2885.TW",
    "2886.TW", "2887.TW", "2888.TW", "2890.TW", "2891.TW", "2892.TW",
]
FALLBACK_NONFIN = [
    "2330.TW", "2317.TW", "2454.TW", "2303.TW", "2412.TW",
    "6505.TW", "2308.TW", "3711.TW", "1216.TW", "2889.TW"  # 允許存在，抓不到會自動跳過
]


def load_universe_from_config():
    cfg_path = os.path.join(os.path.dirname(__file__), "..", "config.json")
    fin = FALLBACK_FIN
    nonfin = FALLBACK_NONFIN
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            fin = cfg.get("tickers_fin", fin)
            nonfin = cfg.get("tickers_nonfin", nonfin)
        except Exception as e:
            print(f"[WARN] 讀取 config.json 失敗，改用內建清單：{e}")
    return fin, nonfin


def make_rank_tables(df_nonfin: pd.DataFrame):
    """產 Top10 / Hot20 / Top5_hot20（簡化版規則）"""
    if df_nonfin.empty:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    latest = df_nonfin.sort_values("Date").groupby("Ticker").tail(1)

    # Top10：以 RSI 由低到高（超賣靠前），同分看 Volume 大到小
    top10 = latest.sort_values(["RSI14", "Volume"], ascending=[True, False]).head(10).copy()

    # Hot20：以近一日 Volume 由大到小
    hot20 = latest.sort_values("Volume", ascending=False).head(20).copy()

    # Top5_hot20：從 Hot20 內再挑 RSI 介於 45~60（偏中性上行）最接近 50 的前 5 檔
    hot20["dist_to_50"] = (hot20["RSI14"] - 50).abs()
    top5_hot = hot20.sort_values(["dist_to_50", "Volume"], ascending=[True, False]).head(5).copy()
    top5_hot.drop(columns=["dist_to_50"], inplace=True)

    # 產建議欄位（簡易版：以布林帶與均線）
    def advice(row):
        close = row["Close"]
        bb_low, bb_mid, bb_up = row["BB_Lower"], row["BB_Mid"], row["BB_Upper"]
        sma20, sma50 = row["SMA20"], row["SMA50"]
        txt = []
        if pd.notna(bb_low) and close <= bb_low:
            txt.append("逼近下軌—留意反彈")
        if pd.notna(bb_up) and close >= bb_up:
            txt.append("逼近上軌—留意回檔")
        if pd.notna(sma20) and pd.notna(sma50):
            if sma20 > sma50:
                txt.append("短多結構(20>50)")
            elif sma20 < sma50:
                txt.append("短空結構(20<50)")
        return "；".join(txt) if txt else "觀望"

    def range_buy(row):
        if pd.notna(row["BB_Lower"]) and pd.notna(row["BB_Mid"]):
            return f"{row['BB_Lower']:.2f} ~ {row['BB_Mid']:.2f}"
        return ""

    def range_sell(row):
        if pd.notna(row["BB_Mid"]) and pd.notna(row["BB_Upper"]):
            return f"{row['BB_Mid']:.2f} ~ {row['BB_Upper']:.2f}"
        return ""

    for table in (top10, hot20, top5_hot):
        table["操作建議"] = table.apply(advice, axis=1)
        table["建議進場區間"] = table.apply(range_buy, axis=1)
        table["建議出場區間"] = table.apply(range_sell, axis=1)

    return top10, hot20, top5_hot


# ========= 主流程 =========

def main():
    print("[INFO] TW50 自動更新開始")

    SHEET_ID = os.environ.get("SHEET_ID")
    if not SHEET_ID:
        raise RuntimeError("缺少 SHEET_ID Secret")

    gc = get_gspread_client()
    ss = gc.open_by_key(SHEET_ID)

    fin_list, nonfin_list = load_universe_from_config()

    # 金融 & 非金
    print("[INFO] 抓取金融股…")
    df_fin = add_simple_names(build_universe(fin_list))
    print("[INFO] 抓取非金融…")
    df_nonfin = add_simple_names(build_universe(nonfin_list))

    # 建表（有就用、沒有就建）
    ws_fin = open_or_create_ws(ss, FIN_SHEET)
    ws_nonfin = open_or_create_ws(ss, NONFIN_SHEET)
    ws_top10 = open_or_create_ws(ss, TOP10_NONFIN)
    ws_hot20 = open_or_create_ws(ss, HOT20_NONFIN)
    ws_top5 = open_or_create_ws(ss, TOP5_HOT20)

    # 寫入（統一 Date->str 已在 write_df 內處理）
    if not df_fin.empty:
        # 只保留常用欄位順序
        keep = ["Date", "Ticker", "公司名稱", "Open", "High", "Low", "Close", "Volume",
                "RSI14", "SMA20", "SMA50", "SMA200", "BB_Mid", "BB_Upper", "BB_Lower"]
        df_fin = df_fin[[c for c in keep if c in df_fin.columns]]
        write_df(ws_fin, df_fin)
    else:
        ws_fin.clear()
        ws_fin.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])

    if not df_nonfin.empty:
        keep = ["Date", "Ticker", "公司名稱", "Open", "High", "Low", "Close", "Volume",
                "RSI14", "SMA20", "SMA50", "SMA200", "BB_Mid", "BB_Upper", "BB_Lower"]
        df_nonfin = df_nonfin[[c for c in keep if c in df_nonfin.columns]]
        write_df(ws_nonfin, df_nonfin)

        # 產 Top 表
        top10, hot20, top5 = make_rank_tables(df_nonfin)
        if not top10.empty:
            write_df(ws_top10, top10)
        else:
            ws_top10.clear(); ws_top10.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])
        if not hot20.empty:
            write_df(ws_hot20, hot20)
        else:
            ws_hot20.clear(); ws_hot20.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])
        if not top5.empty:
            write_df(ws_top5, top5)
        else:
            ws_top5.clear(); ws_top5.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])
    else:
        ws_nonfin.clear()
        ws_nonfin.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])
        for w in (ws_top10, ws_hot20, ws_top5):
            w.clear()
            w.update("A1", [[f"Last Update (Asia/Taipei): {now_taipei_str()}"], ["（本次無資料）"]])

    print("[INFO] 全部完成 ✅")


if __name__ == "__main__":
    main()
