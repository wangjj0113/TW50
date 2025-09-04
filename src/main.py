# -*- coding: utf-8 -*-
"""
TW50 TOP5 — 日更腳本（Google Sheets）
------------------------------------------------------------
1) 下載台股（yfinance .TW）日線資料
2) 計算技術指標：SMA20/50/200、RSI14、布林帶(20,2)
3) 產出中文【操作建議】、【建議買入/賣出區間】
4) 自動略過抓不到資料的代號（會列在「備註」）
5) 寫回 Google 試算表（無分頁則自動建立）
   - TW50_fin      ：金融股
   - TW50_nonfin   ：非金股（含 TOP5 熱度欄位）
   - Top5_hot20    ：依熱度排序挑前5檔（僅示意）
------------------------------------------------------------
環境變數（GitHub Actions -> Repository secrets）
- SHEET_ID                ：Google Sheet 試算表ID
- GCP_SERVICE_ACCOUNT_JSON：服務帳號的 JSON（整段貼上）
可選：
- FINMUB_TOKEN            ：若你之後要接別的資料源可用，現階段不需要
"""

import os
import json
import math
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials


# ---------- 基本設定 ----------
# 你要維護的清單（示範：TW50 常見幾檔；實務上放完整清單即可）
FIN_TICKERS = [
    "2880.TW", "2881.TW", "2882.TW", "2883.TW", "2884.TW", "2885.TW",
    "2886.TW", "2887.TW", "2888.TW", "2889.TW", "2890.TW", "2891.TW",
]
NONFIN_TICKERS = [
    "2330.TW", "2317.TW", "2454.TW", "2303.TW", "2412.TW", "2382.TW",
    "2308.TW", "3481.TW", "3711.TW", "1326.TW", "2301.TW", "6505.TW",
    "2882.TW", "2891.TW"  # 混入一兩檔金融也沒關係，程式不會掛
]

# 若你有完整「代號->公司名稱」對照，可放在這裡（沒對照就空字串）
NAME_MAP = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2303.TW": "聯電",
    "2412.TW": "中華電", "2382.TW": "廣達", "2308.TW": "台達電", "3481.TW": "群創",
    "3711.TW": "日月光投控", "1326.TW": "台化", "2301.TW": "光寶科", "6505.TW": "台塑",
    "2880.TW": "華南金", "2881.TW": "富邦金", "2882.TW": "國泰金", "2883.TW": "開發金",
    "2884.TW": "玉山金", "2885.TW": "元大金", "2886.TW": "兆豐金", "2887.TW": "台新金",
    "2888.TW": "新光金", "2889.TW": "國票金", "2890.TW": "永豐金", "2891.TW": "中信金",
}

# ---------- Google Sheets 連線 ----------
def get_gspread_client() -> gspread.Client:
    raw = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    try:
        info = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"GCP_SERVICE_ACCOUNT_JSON 不是有效 JSON: {e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def open_sheet() -> gspread.Spreadsheet:
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("缺少 SHEET_ID Secret")
    gc = get_gspread_client()
    return gc.open_by_key(sheet_id)


def get_or_create_worksheet(ss: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=5, cols=5)


def upsert_df(ss: gspread.Spreadsheet, ws_title: str, df: pd.DataFrame):
    """將 df 寫入指定分頁（覆蓋）。處理 Timestamp、NaN → 可寫入 JSON。"""
    ws = get_or_create_worksheet(ss, ws_title)

    # 先把所有日期型別轉字串，NaN 轉空字串
    safe = df.copy()

    # 保險：把 index 轉成欄位（若你不想要可拿掉）
    safe.reset_index(drop=True, inplace=True)

    # 將所有 datetime64/Timestamp 轉文字
    for col in safe.columns:
        if np.issubdtype(safe[col].dtype, np.datetime64):
            safe[col] = safe[col].astype(str)

    # 其他非純 Python 標量統一轉成可序列化
    def _to_py(x):
        if pd.isna(x):
            return ""
        if isinstance(x, (np.integer, )):
            return int(x)
        if isinstance(x, (np.floating, )):
            # 保留小數，四捨五入到 6 位（避免太長）
            return float(round(x, 6))
        if isinstance(x, (np.bool_, )):
            return bool(x)
        # Timestamp / datetime 已前面轉掉；其餘轉字串
        return x

    safe = safe.applymap(_to_py)

    values = [list(safe.columns)] + safe.values.tolist()
    ws.clear()
    ws.update("A1", values, value_input_option="RAW")


# ---------- 技術指標 ----------
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    安全版技術指標：
    - 強制把 Close 壓成 1 維 Series（避免變成 (n,1) DataFrame）
    - 所有中間計算都 .astype(float).squeeze()，確保是可寫入的 1 維
    """
    out = df.copy()

    # —— 取 Close，無論是 Series 或 (n,1) DataFrame 都壓成一維 Series ——
    close_col = out["Close"]
    if isinstance(close_col, pd.DataFrame):
        close = close_col.iloc[:, 0].squeeze()
    else:
        close = pd.Series(close_col).squeeze()

    close = pd.to_numeric(close, errors="coerce")  # 保險：轉數值

    # —— 移動均線 ——
    sma20  = close.rolling(20).mean()
    sma50  = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()

    out["SMA20"]  = sma20.astype(float)
    out["SMA50"]  = sma50.astype(float)
    out["SMA200"] = sma200.astype(float)

    # —— RSI14 ——
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss
    out["RSI14"] = (100 - (100 / (1 + rs))).astype(float)

    # —— 布林帶（用 SMA20 作為中軌）——
    mid = out["SMA20"].astype(float).squeeze()
    std = close.rolling(20).std().astype(float).squeeze()

    bb_upper = (mid + 2 * std).astype(float)
    bb_lower = (mid - 2 * std).astype(float)

    # 寫回單欄位（保證是一維）
    out["BB_Mid"]   = mid
    out["BB_Upper"] = bb_upper
    out["BB_Lower"] = bb_lower

    return out


def make_advice(last_row: pd.Series) -> Tuple[str, str, str]:
    """
    回傳 (操作建議, 建議買入區間, 建議賣出區間)
    """
    c = last_row["Close"]
    sma20 = last_row["SMA20"]
    sma50 = last_row["SMA50"]
    sma200 = last_row["SMA200"]
    r = last_row["RSI14"]
    u = last_row["BB_Upper"]
    l = last_row["BB_Lower"]
    mid = last_row["BB_Mid"]

    trend = "多頭" if (sma20 > sma50 > sma200) else ("空頭" if (sma20 < sma50 < sma200) else "盤整")
    rsi_zone = "超買" if r >= 70 else ("超賣" if r <= 30 else "中性")

    # 操作建議（偏保守）
    if trend == "多頭":
        if (c <= mid) and (rsi_zone != "超買"):
            advice = "偏多觀察→回到中軌附近分批佈局；若跌破下軌需嚴設停損"
        elif c >= u or rsi_zone == "超買":
            advice = "多頭高檔→採分批減碼/不追高，回測5%~8%再看"
        else:
            advice = "多頭延續→續抱為主，等待量縮回檔再加碼"
    elif trend == "空頭":
        if (c < mid) and (rsi_zone != "超賣"):
            advice = "偏空觀察→反彈至中軌/上軌附近逢高減碼"
        elif rsi_zone == "超賣":
            advice = "空頭超賣→僅短線反彈思維，嚴格停損"
        else:
            advice = "空頭延續→保守，等待底部訊號"
    else:
        advice = "盤整→區間高出低進，先以短打為主"

    # 區間：用布林帶 & RSI 給一個參考
    buy_lo  = round(float(max(l, mid * 0.97)), 3) if not math.isnan(l) and not math.isnan(mid) else ""
    buy_hi  = round(float(mid), 3) if not math.isnan(mid) else ""
    sell_lo = round(float(mid), 3) if not math.isnan(mid) else ""
    sell_hi = round(float(min(u, mid * 1.03)), 3) if not math.isnan(u) and not math.isnan(mid) else ""

    buy_rng  = f"{buy_lo} ~ {buy_hi}"  if buy_lo != "" and buy_hi != "" else ""
    sell_rng = f"{sell_lo} ~ {sell_hi}" if sell_lo != "" and sell_hi != "" else ""

    return advice, buy_rng, sell_rng


# ---------- 下載＆彙整 ----------
def fetch_one(ticker: str, period: str = "1y") -> Tuple[pd.DataFrame, str]:
    """回傳(含指標的DF, 備註)。抓不到就備註說明並回空DF。"""
    try:
        df = yf.download(ticker, period=period, interval="1d", auto_adjust=True, progress=False)
    except Exception as e:
        return pd.DataFrame(), f"{ticker} 下載錯誤: {e}"

    if df is None or df.empty:
        return pd.DataFrame(), f"{ticker} 無資料，已跳過"

    df = df.reset_index()  # 把 Date 變成欄位
    df = indicators(df)

    # 取最後一筆做文字建議
    last = df.iloc[-1]
    advice, buy_rng, sell_rng = make_advice(last)

    # 組欄位（你要顯示在表上的）
    out = pd.DataFrame([{
        "資料時戳 (Asia/Taipei)": pd.Timestamp(datetime.now()).strftime("%Y-%m-%d %H:%M:%S"),
        "代號": ticker,
        "公司名稱": NAME_MAP.get(ticker, ""),
        "Date": last["Date"],  # 後面會統一轉字串
        "Open":  last["Open"],
        "High":  last["High"],
        "Low":   last["Low"],
        "Close": last["Close"],
        "Volume": int(last.get("Volume", 0)) if not pd.isna(last.get("Volume", np.nan)) else "",
        "RSI14":  last["RSI14"],
        "SMA20":  last["SMA20"],
        "SMA50":  last["SMA50"],
        "SMA200": last["SMA200"],
        "BB_Mid":   last["BB_Mid"],
        "BB_Upper": last["BB_Upper"],
        "BB_Lower": last["BB_Lower"],
        "操作建議": advice,
        "建議買入區間": buy_rng,
        "建議賣出區間": sell_rng,
        "備註": "",  # 下載成功就空白
    }])

    return out, ""


def assemble_rows(tickers: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    all_rows = []
    notes = []
    for t in tickers:
        df1, note = fetch_one(t)
        if not df1.empty:
            all_rows.append(df1)
        if note:
            notes.append(note)
    if all_rows:
        return pd.concat(all_rows, ignore_index=True), notes
    else:
        # 確保回傳空DF時也有欄位，不然寫表會報錯
        cols = ["資料時戳 (Asia/Taipei)","代號","公司名稱","Date","Open","High","Low",
                "Close","Volume","RSI14","SMA20","SMA50","SMA200",
                "BB_Mid","BB_Upper","BB_Lower","操作建議","建議買入區間","建議賣出區間","備註"]
        return pd.DataFrame(columns=cols), notes


# ---------- 主流程 ----------
def main():
    print(f"[INFO] TW50 TOP5 更新")

    ss = open_sheet()

    # 金融 / 非金 各自彙整
    df_fin, notes_fin = assemble_rows(FIN_TICKERS)
    df_non, notes_non = assemble_rows(NONFIN_TICKERS)

    # 寫入前：把所有日期欄位轉字串，避免 JSON serialization 問題
    for df in (df_fin, df_non):
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 依照你的分頁規劃寫回
    upsert_df(ss, "TW50_fin", df_fin)
    upsert_df(ss, "TW50_nonfin", df_non)

    # 產生 Top5_hot20（示意：用「距離中軌%」當熱度指標，越接近下軌排序更前）
    # 你可以換成你喜歡的打分方式
    if not df_non.empty:
        tmp = df_non.copy()
        with np.errstate(divide="ignore", invalid="ignore"):
            tmp["距離中軌%"] = (tmp["Close"] - tmp["BB_Mid"]) / tmp["BB_Mid"] * 100
        tmp = tmp.sort_values("距離中軌%", ascending=True).head(5).reset_index(drop=True)
        upsert_df(ss, "Top5_hot20", tmp)

    # 把「抓不到資料」的代號記到一張備註表（可選）
    all_notes = notes_fin + notes_non
    note_df = pd.DataFrame({"備註": all_notes}) if all_notes else pd.DataFrame({"備註": ["（本次全部成功）"]})
    upsert_df(ss, "交接本", note_df)

    print("[OK] 完成！")


if __name__ == "__main__":
    main()
