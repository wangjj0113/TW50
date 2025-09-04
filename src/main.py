# main.py － TW50 TOP5（含操作建議/區間/信心分數）
# 需要的 GitHub Secrets：
# - SHEET_ID：目標 Google 試算表 ID
# - GCP_SERVICE_ACCOUNT_JSON：GCP 服務帳戶 JSON（整段內容）
# - FINMIND_TOKEN：可留空（保留未來擴充）

import os, json, math
import pandas as pd
import numpy as np
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone

# ========= 讀取環境變數 =========
SHEET_ID = os.environ.get("SHEET_ID")
SERVICE_ACCOUNT_INFO = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
FINMIND_TOKEN = os.environ.get("FINMIND_TOKEN")  # 目前未用，保留

# ========= Google Sheet =========
def get_gspread_client():
    if not SERVICE_ACCOUNT_INFO:
        raise RuntimeError("找不到 GCP_SERVICE_ACCOUNT_JSON Secret")
    creds_json = json.loads(SERVICE_ACCOUNT_INFO)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    return gspread.authorize(creds)

def open_ws(client, sheet_id, title):
    sh = client.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=40)
    return ws

# ========= 市場資料 & 指標 =========
def dl(ticker, period="6mo"):
    try:
        df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
        if df is None or df.empty:
            print(f"[WARN] {ticker} 無歷史資料，跳過")
            return pd.DataFrame()
        df = df.rename_axis("Date").reset_index()
        # 只留必要欄位
        df = df[["Date","Open","High","Low","Close","Volume"]].copy()
        return df
    except Exception as e:
        print(f"[ERROR] 下載 {ticker} 失敗：{e}")
        return pd.DataFrame()

def ta_calc(df):
    # 安全處理不足長度
    if df.empty:
        return df

    close = df["Close"]
    diff = close.diff()

    # RSI14
    gain = diff.clip(lower=0).rolling(14).mean()
    loss = (-diff.clip(upper=0)).rolling(14).mean()
    rs = gain / (loss.replace(0, np.nan))
    df["RSI14"] = 100 - (100 / (1 + rs))
    # SMA
    df["SMA20"] = close.rolling(20).mean()
    df["SMA60"] = close.rolling(60).mean()
    df["SMA200"] = close.rolling(200).mean()
    # BB(20,2)
    mid = close.rolling(20).mean()
    std = close.rolling(20).std()
    df["BB_Mid"]   = mid
    df["BB_Upper"] = mid + 2*std
    df["BB_Lower"] = mid - 2*std
    return df

# ========= 訊號/建議 =========
def classify_trend(row):
    # 以均線排列判斷趨勢（簡化版）
    if pd.notna(row["SMA200"]) and row["Close"] > row["SMA200"] and row["SMA20"] >= row["SMA60"]:
        return "多頭"
    if pd.notna(row["SMA200"]) and row["Close"] < row["SMA200"] and row["SMA20"] <= row["SMA60"]:
        return "空頭"
    return "盤整"

def score_signal(row):
    """簡易打分：-3~+3，越高越偏多"""
    s = 0
    # 均線關係
    if pd.notna(row["SMA20"]) and pd.notna(row["SMA60"]):
        s += 1 if row["SMA20"] > row["SMA60"] else -1
    if pd.notna(row["SMA200"]):
        s += 1 if row["Close"] > row["SMA200"] else -1
    # RSI 區間
    rsi = row.get("RSI14", np.nan)
    if not np.isnan(rsi):
        if 45 <= rsi <= 70:
            s += 1
        elif rsi < 35 or rsi > 75:
            s -= 1
    # 布林位置（接近下軌偏多、上軌偏空）
    if all(pd.notna([row["BB_Lower"], row["BB_Upper"], row["Close"]])):
        width = row["BB_Upper"] - row["BB_Lower"]
        if width > 0:
            pct = (row["Close"] - row["BB_Lower"]) / width  # 0~1
            if pct < 0.25:
                s += 1
            elif pct > 0.75:
                s -= 1
    return s

def advice_and_ranges(row):
    trend = classify_trend(row)
    score = score_signal(row)

    close = row["Close"]
    sma20 = row["SMA20"]
    mid   = row["BB_Mid"]
    up    = row["BB_Upper"]
    lo    = row["BB_Lower"]

    # 預設
    action = "觀望"
    entry_l, entry_h, tp, sl = "", "", "", ""
    note = []

    # 進出場邏輯（簡化且保守）
    if trend == "多頭" and score >= 2:
        action = "逢低買進"
        if pd.notna(lo) and pd.notna(sma20):
            entry_l = round(float(lo), 3)
            entry_h = round(float(sma20), 3)
        tp = round(float(up), 3) if pd.notna(up) else ""
        # 止損設下軌或近期低點 1% 下
        if pd.notna(lo):
            sl = round(float(lo * 0.99), 3)
    elif trend == "空頭" and score <= -2:
        action = "逢高減碼/做空"
        if pd.notna(sma20) and pd.notna(up):
            entry_l = round(float(sma20), 3)
            entry_h = round(float(up), 3)
        tp = round(float(mid), 3) if pd.notna(mid) else ""
        if pd.notna(up):
            sl = round(float(up * 1.01), 3)
    else:
        action = "觀望"
        # 提示靠近均線再說
        if pd.notna(sma20):
            note.append(f"等待接近SMA20（約 {round(float(sma20),3)}）")

    # 信心分數換成 0~100（易懂）
    conf = int(round((score + 3) / 6 * 100))
    conf = max(0, min(100, conf))

    # 說明補充
    note.append(f"趨勢：{trend}；打分：{score:+d}")
    comment = "；".join(note)

    return action, entry_l, entry_h, tp, sl, trend, conf, comment

# ========= 寫入表格 =========
def append_sheet(ws, df):
    if df.empty:
        return
    # 轉成原生型別，避免 Timestamp / numpy 類型造成 JSON 序列化錯
    out = df.copy()
    # 格式化日期
    if "Date" in out.columns:
        out["Date"] = out["Date"].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, (pd.Timestamp, datetime)) else x)
    out = out.where(pd.notna(out), "")  # NaN -> ""
    records = [out.columns.tolist()] + out.astype(object).values.tolist()
    ws.update(records)

def run():
    print("[INFO] TW50 TOP5 更新開始")

    # 這裡先放示範用 5 檔（實務可由 config/篩選結果來）
    tickers = ["2330.TW", "2303.TW", "2308.TW", "2301.TW", "2412.TW"]

    client = get_gspread_client()
    ws = open_ws(client, SHEET_ID, "Top5_hot20")

    rows = []
    skipped = []

    for t in tickers:
        df = dl(t, period="6mo")
        if df.empty:
            skipped.append(t)
            continue
        df = ta_calc(df)
        last = df.iloc[-1].copy()

        # 產出建議
        action, eL, eH, tp, sl, trend, conf, comment = advice_and_ranges(last)

        rows.append({
            "更新時間(Asia/Taipei)": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
            "Ticker": t,
            "Open": round(float(last["Open"]), 3),
            "High": round(float(last["High"]), 3),
            "Low": round(float(last["Low"]), 3),
            "Close": round(float(last["Close"]), 3),
            "Volume": int(last["Volume"]) if not pd.isna(last["Volume"]) else "",
            "RSI14": round(float(last["RSI14"]), 3) if not pd.isna(last["RSI14"]) else "",
            "SMA20": round(float(last["SMA20"]), 3) if not pd.isna(last["SMA20"]) else "",
            "SMA60": round(float(last["SMA60"]), 3) if not pd.isna(last["SMA60"]) else "",
            "SMA200": round(float(last["SMA200"]), 3) if not pd.isna(last["SMA200"]) else "",
            "BB_Mid": round(float(last["BB_Mid"]), 3) if not pd.isna(last["BB_Mid"]) else "",
            "BB_Upper": round(float(last["BB_Upper"]), 3) if not pd.isna(last["BB_Upper"]) else "",
            "BB_Lower": round(float(last["BB_Lower"]), 3) if not pd.isna(last["BB_Lower"]) else "",

            "趨勢判讀": trend,
            "操作建議": action,
            "建議進場下界": eL,
            "建議進場上界": eH,
            "建議停利": tp,
            "建議停損": sl,
            "信心分數(0-100)": conf,
            "備註": comment
        })

    out_df = pd.DataFrame(rows, columns=[
        "更新時間(Asia/Taipei)", "Ticker",
        "Open","High","Low","Close","Volume",
        "RSI14","SMA20","SMA60","SMA200","BB_Mid","BB_Upper","BB_Lower",
        "趨勢判讀","操作建議","建議進場下界","建議進場上界","建議停利","建議停損",
        "信心分數(0-100)","備註"
    ])
    append_sheet(ws, out_df)

    if skipped:
        print(f"[INFO] 已跳過無資料：{', '.join(skipped)}")

    print("[INFO] 完成 Top5_hot20 寫入")

if __name__ == "__main__":
    run()
