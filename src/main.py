# -*- coding: utf-8 -*-
"""
TW50 TOP5 自動更新腳本（google-auth 版）
------------------------------------------------
• 讀取 repo 根目錄的 config.json 取得標的清單（若沒有就用內建 TW50 清單）
• 透過 yfinance 抓取近 400 交易日資料，計算：
  - RSI14, SMA20/50/200, 布林通道(20, 2)
• 產生操作建議（中文）與建議買/賣區間、停損/停利價位
• 分頁輸出：
  - TW50_fin：金融股
  - TW50_nonfin：非金融股
  - Top10_nonfin：非金融成交量前 10
  - Hot20_nonfin：非金融「熱度」前 20（量比 + 乖離 + 波動 綜合）
  - Top5_hot20：從 Hot20 中挑出前 5，附中文操作建議與區間
• 失敗的代號會被「略過」並記錄在 Logs 分頁，不會中斷整體流程
• 需的 Secrets（GitHub Actions → Settings → Secrets and variables → Actions）：
  - SHEET_ID                  目標 Google 試算表 ID
  - GCP_SERVICE_ACCOUNT_JSON  服務帳戶 JSON 內容（整包貼上）
  - FINMIND_TOKEN             （可留空，保留欄位不使用）

※ 已全面改用 google.oauth2.service_account.Credentials，不依賴 oauth2client
"""

import os
import io
import json
import math
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials


# ------------- 讀取環境變數 / Google 認證 -----------------

SHEET_ID = os.getenv("SHEET_ID", "").strip()
SERVICE_ACCOUNT_INFO = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()

if not SHEET_ID:
    raise RuntimeError("找不到 SHEET_ID Secret")

if not SERVICE_ACCOUNT_INFO:
    raise RuntimeError("找不到 GCP_SERVICE_ACCOUNT_JSON Secret")


def get_gspread_client():
    creds_json = json.loads(SERVICE_ACCOUNT_INFO)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


# ------------- 共用工具 -----------------

def ts():
    """產生台北時區字串（僅用於寫到表內）"""
    return datetime.utcnow().astimezone().strftime("%Y-%m-%d %H:%M:%S")


def to_py(v):
    """把 numpy / Timestamp 轉成可序列化（寫入 gspread 的 list[list]）"""
    if pd.isna(v):
        return ""
    if isinstance(v, (np.generic,)):
        return v.item()
    if isinstance(v, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    if isinstance(v, (float,)):
        # 統一保留 6 位小數，避免太長
        return float(round(v, 6))
    return v


def safe_open_ws(sh, title):
    """取分頁；不存在就建立"""
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=1000, cols=26)


def replace_worksheet_df(sh, title, df):
    """
    用 DataFrame 覆蓋整張分頁（含表頭）；自動把不可序列化的類型轉掉
    """
    ws = safe_open_ws(sh, title)
    # 轉 list of lists
    values = [list(df.columns)]
    for _, row in df.iterrows():
        values.append([to_py(x) for x in row.values.tolist()])

    # 清空 & 寫入
    ws.clear()
    if values:
        # Google Sheets API 一次寫入
        ws.update("A1", values, value_input_option="RAW")
    # 左上角註記時間
    ws.update_acell("A1", f"資料擷取（Asia/Taipei）: {ts()}")


# ------------- 指標計算 -----------------

def rsi(series, period=14):
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).ewm(alpha=1/period, adjust=False).mean()
    roll_down = pd.Series(down, index=series.index).ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    rsi_val = 100.0 - (100.0 / (1.0 + rs))
    return rsi_val


def add_indicators(df):
    df = df.copy()
    df["SMA20"] = df["Close"].rolling(20).mean()
    df["SMA50"] = df["Close"].rolling(50).mean()
    df["SMA200"] = df["Close"].rolling(200).mean()

    mid = df["Close"].rolling(20).mean()
    std = df["Close"].rolling(20).std(ddof=0)
    df["BB_Mid"] = mid
    df["BB_Upper"] = mid + 2 * std
    df["BB_Lower"] = mid - 2 * std

    df["RSI14"] = rsi(df["Close"], 14)
    return df


# ------------- 操作建議（依單一最新列） -----------------

def decision_block(row):
    """
    給定一檔股票「最新一列」資料（已含指標），輸出一組建議文字 + 數值區間
    回傳 dict
    """
    close = float(row.get("Close", np.nan))
    sma20 = float(row.get("SMA20", np.nan))
    sma50 = float(row.get("SMA50", np.nan))
    sma200 = float(row.get("SMA200", np.nan))
    bb_mid = float(row.get("BB_Mid", np.nan))
    bb_up = float(row.get("BB_Upper", np.nan))
    bb_lo = float(row.get("BB_Lower", np.nan))
    rsi14 = float(row.get("RSI14", np.nan))
    vol = float(row.get("Volume", np.nan))

    # 多空趨勢（粗略）
    trend = "盤整"
    if not math.isnan(sma200) and not math.isnan(close):
        if close > sma200 * 1.01:
            trend = "偏多"
        elif close < sma200 * 0.99:
            trend = "偏空"

    # 建議區間（皆可能為 NaN，要判斷）
    def safe(v):  # 轉成漂亮的字串
        return "" if math.isnan(v) else round(v, 3)

    buy_low = buy_high = sell_low = sell_high = stop_loss = tp1 = tp2 = np.nan
    advice = "觀望"

    if all(not math.isnan(x) for x in [bb_mid, sma20]):
        ref = min(bb_mid, sma20)
        buy_low = ref * 0.99
        buy_high = ref * 1.01

    if not math.isnan(bb_up) and not math.isnan(sma20):
        sell_low = max(bb_up, sma20 * 1.03)
        sell_high = max(bb_up * 1.02, sma20 * 1.06)

    if not math.isnan(bb_lo):
        stop_loss = bb_lo

    if not math.isnan(close):
        tp1 = close * 1.05
        tp2 = close * 1.08

    # 文案邏輯（簡潔）
    if trend == "偏多" and rsi14 >= 50 and not math.isnan(buy_low):
        advice = "偏多：回測月線/中軌附近（±1%）可分批佈局；跌破下軌停損。"
    elif trend == "偏空" and rsi14 <= 50:
        advice = "偏空：以觀望為主，站回月線再評估；短線反彈至上軌/前高區偏賣。"
    else:
        advice = "盤整：區間操作；靠近中軌偏多、上軌偏賣，嚴設停損停利。"

    return {
        "趨勢": trend,
        "RSI14": rsi14,
        "建議": advice,
        "買進區間低": safe(buy_low),
        "買進區間高": safe(buy_high),
        "賣出區間低": safe(sell_low),
        "賣出區間高": safe(sell_high),
        "停損價": safe(stop_loss),
        "停利一": safe(tp1),
        "停利二": safe(tp2),
        "成交量": vol,
    }


# ------------- 下載 + 組表 -----------------

def fetch_one(ticker):
    """
    回傳（df, short_name）
    df 欄位：Date, Open, High, Low, Close, Volume + 指標
    找不到資料時回傳 (None, "")
    """
    try:
        data = yf.download(ticker, period="400d", interval="1d", auto_adjust=True, progress=False)
    except Exception as e:
        print(f"[WARN] yfinance 下載失敗 {ticker}: {e}")
        return None, ""
    if data is None or data.empty:
        print(f"[INFO] yfinance 無資料，已跳過：{ticker}")
        return None, ""

    data = data.rename_axis("Date").reset_index()
    data = data[["Date", "Open", "High", "Low", "Close", "Volume"]]
    data = add_indicators(data)

    # 嘗試抓公司名稱（可能拿不到，拿不到就留空）
    short_name = ""
    try:
        info = yf.Ticker(ticker).info
        short_name = info.get("shortName") or ""
    except Exception:
        short_name = ""

    return data, short_name


def load_tickers():
    """
    從 repo 根目錄的 config.json 讀取：
    {
      "all": ["2330.TW", ...],
      "fin": ["2882.TW", ...]
    }
    若沒有，就用內建簡易清單。
    """
    defaults_all = [
        "2330.TW","2317.TW","2454.TW","6505.TW","2308.TW","2303.TW","2891.TW","2881.TW","2882.TW",
        "2884.TW","2885.TW","2886.TW","2887.TW","2888.TW","2889.TW","2890.TW","2892.TW","2382.TW",
        "2408.TW","1303.TW","1301.TW","1326.TW","1216.TW","1101.TW","1102.TW","2412.TW","2301.TW",
        "2603.TW","2610.TW","2609.TW","3008.TW","3711.TW","2615.TW","6547.TW","1590.TW","2002.TW",
        "2883.TW","1402.TW","9910.TW","9904.TW","8046.TW","2379.TW","2357.TW","4938.TW","3034.TW",
        "3037.TW","3045.TW","3702.TW","8150.TW"
    ]
    defaults_fin = [t for t in defaults_all if t.startswith(("288", "289"))]

    cfg = {}
    try:
        with io.open("config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        pass

    all_list = cfg.get("all", defaults_all)
    fin_list = cfg.get("fin", defaults_fin)

    nonfin_list = [t for t in all_list if t not in set(fin_list)]
    return all_list, fin_list, nonfin_list


def build_table_for_ticker(ticker):
    df, name = fetch_one(ticker)
    if df is None:
        return None

    last = df.dropna(subset=["Close"]).iloc[-1]
    dec = decision_block(last)

    out = pd.DataFrame([{
        "Date": last["Date"],
        "Ticker": ticker,
        "公司名稱": name,
        "Open": last["Open"],
        "High": last["High"],
        "Low": last["Low"],
        "Close": last["Close"],
        "Volume": last["Volume"],
        "RSI14": dec["RSI14"],
        "SMA20": last["SMA20"],
        "SMA50": last["SMA50"],
        "SMA200": last["SMA200"],
        "BB_Mid": last["BB_Mid"],
        "BB_Upper": last["BB_Upper"],
        "BB_Lower": last["BB_Lower"],
        "多空趨勢": dec["趨勢"],
        "操作建議": dec["建議"],
        "建議買進下界": dec["買進區間低"],
        "建議買進上界": dec["買進區間高"],
        "建議賣出下界": dec["賣出區間低"],
        "建議賣出上界": dec["賣出區間高"],
        "停損價": dec["停損價"],
        "停利一": dec["停利一"],
        "停利二": dec["停利二"],
    }])
    return out


def aggregate_tables(tickers):
    """
    逐檔蒐集資料，合併成一張總表；並回傳失敗清單
    """
    rows = []
    failed = []
    for tk in tickers:
        tbl = build_table_for_ticker(tk)
        if tbl is None:
            failed.append(tk)
        else:
            rows.append(tbl)

    if rows:
        all_df = pd.concat(rows, ignore_index=True)
    else:
        all_df = pd.DataFrame(columns=[
            "Date","Ticker","公司名稱","Open","High","Low","Close","Volume",
            "RSI14","SMA20","SMA50","SMA200","BB_Mid","BB_Upper","BB_Lower",
            "多空趨勢","操作建議","建議買進下界","建議買進上界","建議賣出下界","建議賣出上界","停損價","停利一","停利二"
        ])
    return all_df, failed


def rank_hotness(df):
    """
    熱度分數（僅非金融用）：量大、乖離、波動 3 項標準化後加總
    """
    d = df.copy()
    for col in ["Volume", "SMA20", "BB_Upper", "BB_Lower", "Close"]:
        if col not in d.columns:
            d[col] = np.nan

    # 乖離：|Close - SMA20| / SMA20
    d["距離中軌%"] = (d["Close"] - d["BB_Mid"]).abs() / d["BB_Mid"] * 100
    # 波動：上軌-下軌 占比
    d["波動%"] = (d["BB_Upper"] - d["BB_Lower"]) / d["BB_Mid"] * 100

    def z(x):
        x = x.replace([np.inf, -np.inf], np.nan)
        mu, sd = x.mean(skipna=True), x.std(skipna=True)
        return (x - mu) / (sd if sd and not np.isnan(sd) and sd != 0 else 1.0)

    d["z_vol"] = z(d["Volume"].astype(float))
    d["z_dist"] = z(d["距離中軌%"].astype(float))
    d["z_vola"] = z(d["波動%"].astype(float))
    d["熱度分數"] = d[["z_vol", "z_dist", "z_vola"]].sum(axis=1)

    d = d.sort_values(["熱度分數", "Volume"], ascending=[False, False])
    return d


# ------------- 主流程 -----------------

def main():
    print("[INFO] 啟動 TW50 TOP5 更新")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    all_list, fin_list, nonfin_list = load_tickers()
    print(f"[INFO] 標的數量 all={len(all_list)}, fin={len(fin_list)}, nonfin={len(nonfin_list)}")

    # 1) 先產出全表（只取最新一列）
    full_df, failed = aggregate_tables(all_list)

    # 排序方便閱讀：按 Ticker
    if not full_df.empty:
        full_df = full_df.sort_values("Ticker").reset_index(drop=True)

    # 金融 / 非金融 分拆
    fin_df = full_df[full_df["Ticker"].isin(fin_list)].reset_index(drop=True)
    nonfin_df = full_df[full_df["Ticker"].isin(nonfin_list)].reset_index(drop=True)

    # 2) Top10 非金 by Volume
    top10_nonfin = nonfin_df.sort_values("Volume", ascending=False).head(10).reset_index(drop=True)

    # 3) Hot20 非金（量/乖離/波動 綜合）
    hot20_nonfin = rank_hotness(nonfin_df).head(20).reset_index(drop=True)

    # 4) Top5_hot20（附操作建議與區間）
    top5_hot20 = hot20_nonfin.head(5).reset_index(drop=True)

    # 寫出五個分頁
    replace_worksheet_df(sh, "TW50_fin", fin_df)
    replace_worksheet_df(sh, "TW50_nonfin", nonfin_df)
    replace_worksheet_df(sh, "Top10_nonfin", top10_nonfin)
    replace_worksheet_df(sh, "Hot20_nonfin", hot20_nonfin)
    replace_worksheet_df(sh, "Top5_hot20", top5_hot20)

    # Logs 分頁（略過的代號）
    logs = pd.DataFrame({
        "略過代號（無資料）": failed,
        "時間": [ts()] * len(failed)
    })
    replace_worksheet_df(sh, "Logs", logs)

    print("[INFO] 完成 ✅")


if __name__ == "__main__":
    main()
