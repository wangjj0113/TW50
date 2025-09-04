# -*- coding: utf-8 -*-
"""
TW50 TOP5 — 主程式（穩定版）
修正要點：
1) 所有技術指標計算前，欄位一律 squeeze 成一維 Series，避免 (n,1) 造成
   ValueError: Data must be 1-dimensional
2) 寫回 Google Sheet 前，將 Timestamp 轉字串，避免 not JSON serializable
3) yfinance 缺料或 404 會自動跳過該股票並紀錄
環境變數：
- SHEET_ID: 目標 Google 試算表 ID
- GCP_SERVICE_ACCOUNT_JSON: 服務帳號 JSON（整段）
"""

import os, json, sys, time
import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2 import service_account

# --------- 工具與安全轉換 ---------

def squeeze_series(x) -> pd.Series:
    """把可能是 (n,1) 的資料，轉成一維 Series。"""
    if isinstance(x, pd.Series):
        return x.squeeze()
    if isinstance(x, pd.DataFrame):
        if x.shape[1] == 1:
            return x.iloc[:, 0].squeeze()
        # 多欄時優先找 Close
        if "Close" in x.columns:
            return x["Close"].squeeze()
        return x.iloc[:, 0].squeeze()
    # ndarray 或 list
    arr = np.asarray(x).squeeze()
    return pd.Series(arr)

def to_str_timestamp(idx):
    """把 DatetimeIndex / Timestamp 轉成文字（YYYY-MM-DD）。"""
    if isinstance(idx, pd.DatetimeIndex):
        return idx.strftime("%Y-%m-%d").tolist()
    return [str(v) for v in idx]

def log(msg):
    print(msg, flush=True)

# --------- Google Sheet 連線 ---------

def get_gspread_client():
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    info = json.loads(sa_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc

def open_or_create_worksheet(sh, title, rows=1000, cols=40):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def clear_and_write(ws, df: pd.DataFrame):
    # 全部轉字串/基本型別，避免 JSON 序列化問題
    safe = df.copy()
    for c in safe.columns:
        if np.issubdtype(safe[c].dtype, np.datetime64):
            safe[c] = safe[c].dt.strftime("%Y-%m-%d")
        else:
            safe[c] = safe[c].astype(object)
    values = [safe.columns.tolist()] + safe.values.tolist()
    ws.clear()
    ws.update(values)

# --------- 指標與建議 ---------

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    來源 df：yfinance 下載結果（含 Open/High/Low/Close/Volume），index 為日期
    回傳：加上 RSI14、SMA20/50/200、BB 中上下軌
    """
    out = df.copy()

    close = squeeze_series(out["Close"])
    high  = squeeze_series(out["High"])
    low   = squeeze_series(out["Low"])

    # 移動平均
    out["SMA20"]  = close.rolling(window=20, min_periods=1).mean()
    out["SMA50"]  = close.rolling(window=50, min_periods=1).mean()
    out["SMA200"] = close.rolling(window=200, min_periods=1).mean()

    # RSI14（簡易實作：漲跌分離的 EMA）
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    roll_up = gain.ewm(alpha=1/14, adjust=False).mean()
    roll_dn = loss.ewm(alpha=1/14, adjust=False).mean()
    rs = roll_up / (roll_dn.replace(0, np.nan))
    out["RSI14"] = 100 - (100 / (1 + rs))

    # 布林帶（20）
    mid = out["SMA20"]
    std = close.rolling(window=20, min_periods=1).std(ddof=0)
    out["BB_Mid"]   = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std

    return out

def make_advice(row) -> str:
    """
    產出中文「操作建議」：多空結構 + 是否接近支撐/壓力
    """
    c  = row["Close"]
    sma20, sma50, sma200 = row["SMA20"], row["SMA50"], row["SMA200"]
    bb_u, bb_m, bb_l = row["BB_Upper"], row["BB_Mid"], row["BB_Lower"]

    # 多空架構
    if sma20 > sma50 > sma200:
        trend = "多頭結構"
    elif sma20 < sma50 < sma200:
        trend = "空頭結構"
    else:
        trend = "盤整/轉折中"

    # 距離支撐/壓力（%）
    near_supp = (c / bb_l - 1) * 100 if bb_l and bb_l != 0 else np.nan
    near_res  = (bb_u / c - 1) * 100 if c and c != 0 else np.nan

    tip = []
    if not np.isnan(near_supp) and near_supp <= 3:
        tip.append("接近下緣支撐，偏觀察逢回佈局")
    if not np.isnan(near_res) and near_res <= 3:
        tip.append("接近上緣壓力，偏逢高減碼")

    # 簡化口訣
    if trend == "多頭結構" and not tip:
        action = "順勢偏多；回到月線/布林中軌附近再考慮加碼"
    elif trend == "空頭結構" and not tip:
        action = "偏保守；反彈至月線附近再評估"
    else:
        action = "以區間思維操作；支撐不破可小試，壓力附近減碼"

    if tip:
        return f"{trend}｜{'；'.join(tip)}｜{action}"
    return f"{trend}｜{action}"

# --------- 核心流程 ---------

def fetch_one(ticker: str, period="1y", interval="1d") -> pd.DataFrame:
    """
    下載單一代號資料；有資料則回 DataFrame，沒有資料回 None
    """
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)
        if df is None or df.empty:
            log(f"[跳過] {ticker}: 無資料")
            return None
        # 正常化欄位
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index.name = "Date"
        return df
    except Exception as e:
        log(f"[跳過] {ticker}: 下載失敗 -> {e}")
        return None

def assemble_rows(ticker: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    依下載 + 指標，產出可寫入 Sheet 的表格（全部歷史，每列一天）
    """
    df2 = compute_indicators(df)
    df2 = df2.reset_index()
    df2["Date"] = pd.to_datetime(df2["Date"]).dt.strftime("%Y-%m-%d")
    # 取得名稱（失敗就空白）
    try:
        info = yf.Ticker(ticker).fast_info
        name = getattr(yf.Ticker(ticker), "info", {}).get("shortName", "")
    except Exception:
        name = ""

    df2.insert(1, "Ticker", ticker)
    df2.insert(2, "Name", name)

    # 當天建議（只做最後一筆，也可全欄都算）
    advice = df2.apply(make_advice, axis=1)
    df2["操作建議"] = advice

    cols = [
        "Date", "Ticker", "Name",
        "Open", "High", "Low", "Close", "Volume",
        "RSI14", "SMA20", "SMA50", "SMA200",
        "BB_Mid", "BB_Upper", "BB_Lower",
        "操作建議",
    ]
    return df2[cols]

def load_tickers_from_sheet(gc, sheet_id, possible_sheets=("TW50_nonfin", "TW50_fin", "工作表1")) -> list:
    """
    嘗試從既有試算表的某個工作表讀出代號（欄名包含 'Ticker' 或第一欄）
    若讀不到，回傳預設少量測試清單。
    """
    try:
        sh = gc.open_by_key(sheet_id)
        for name in possible_sheets:
            try:
                ws = sh.worksheet(name)
            except gspread.WorksheetNotFound:
                continue
            data = ws.get_all_records()
            if not data:
                continue
            df = pd.DataFrame(data)
            if "Ticker" in df.columns:
                tickers = [str(t).strip() for t in df["Ticker"] if str(t).strip()]
                if tickers:
                    return tickers
            # 沒有欄名就抓第 1 欄
            if df.shape[1] > 0:
                col0 = df.iloc[:, 0].astype(str).str.strip().tolist()
                if col0:
                    return col0
    except Exception:
        pass

    # 後備：給幾檔常見代號（不會太多，避免超時）
    return ["2330.TW", "2303.TW", "2317.TW", "2412.TW", "2308.TW"]

def main():
    SHEET_ID = os.environ.get("SHEET_ID", "").strip()
    if not SHEET_ID:
        raise RuntimeError("缺少 SHEET_ID")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    # 來源清單：盡量從既有工作表抓，抓不到就用備用清單
    tickers = load_tickers_from_sheet(gc, SHEET_ID)
    log(f"[INFO] 本次處理標的數：{len(tickers)}")

    all_rows = []
    for t in tickers:
        log(f"[INFO] 抓取 {t} …")
        df = fetch_one(t)
        if df is None:
            continue
        rows = assemble_rows(t, df)
        all_rows.append(rows)

        # 友善節流（避免 API 暴衝）
        time.sleep(0.3)

    if not all_rows:
        raise RuntimeError("沒有任何可寫入的資料（所有標的都被跳過）")

    out = pd.concat(all_rows, ignore_index=True)

    # 寫入到一個穩定的頁籤（你要寫別的名字也可以改這裡）
    target_sheet = "TW50_nonfin"
    ws = open_or_create_worksheet(sh, target_sheet, rows=max(1000, len(out) + 10), cols=len(out.columns) + 2)
    clear_and_write(ws, out)

    log(f"[OK] 已寫入 {target_sheet}（{len(out)} 列）")

if __name__ == "__main__":
    main()
