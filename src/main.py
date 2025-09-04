# -*- coding: utf-8 -*-
"""
TW50 自動化（V3 大改版，穩定版）
------------------------------------------------
✓ 統一用 google-auth + gspread（不依賴 oauth2client）
✓ 所有技術指標都先把資料壓成一維 Series（避免 (n,1)）
✓ 寫表前將 Timestamp / numpy 全部轉 Python 原生型別（避免 JSON 錯誤）
✓ 只用「純量」做判斷（不對整個 Series 做 if，避免 ambiguous）
✓ 找不到資料/API 失敗：跳過並寫入 Logs，不中斷整批
✓ 分頁不存在會自動建立，且會覆蓋寫入（全量表頭+資料）
✓ 可用 config.json 自訂標的（沒有就用內建 TW50 清單）
------------------------------------------------
需要的 Secrets：
- SHEET_ID
- GCP_SERVICE_ACCOUNT_JSON
（FINMIND_TOKEN 保留未來擴充，不必填）
"""

import os, io, json, math, time
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd
import yfinance as yf

import gspread
from google.oauth2.service_account import Credentials

# ========= 參數 =========
TZ = timezone(timedelta(hours=8))  # Asia/Taipei
# 工作表名稱
TAB_FIN       = "TW50_fin"
TAB_NONFIN    = "TW50_nonfin"
TAB_TOP10     = "Top10_nonfin"
TAB_HOT20     = "Hot20_nonfin"
TAB_TOP5H20   = "Top5_hot20"
TAB_LOGS      = "Logs"

# 內建 TW50 簡表（可被 config.json 覆蓋）
DEFAULT_ALL = [
    "2330.TW","2317.TW","2454.TW","6505.TW","2308.TW","2303.TW","2891.TW","2881.TW","2882.TW",
    "2884.TW","2885.TW","2886.TW","2887.TW","2888.TW","2889.TW","2890.TW","2892.TW","2382.TW",
    "2408.TW","1303.TW","1301.TW","1326.TW","1216.TW","1101.TW","1102.TW","2412.TW","2301.TW",
    "2603.TW","2610.TW","2609.TW","3008.TW","3711.TW","2615.TW","6547.TW","1590.TW","2002.TW",
    "2883.TW","1402.TW","9910.TW","9904.TW","8046.TW","2379.TW","2357.TW","4938.TW","3034.TW",
    "3037.TW","3045.TW","3702.TW","8150.TW"
]
DEFAULT_FIN = [t for t in DEFAULT_ALL if t.startswith(("288", "289"))]

# ========= 通用工具 =========
def now_str():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def squeeze_1d(x) -> pd.Series:
    """把可能是 (n,1) 的資料強制壓成一維 Series"""
    if isinstance(x, pd.Series):
        return x.squeeze()
    if isinstance(x, pd.DataFrame):
        s = x.iloc[:, 0] if x.shape[1] > 0 else pd.Series([], dtype="float64")
        return s.squeeze()
    return pd.Series(np.asarray(x).squeeze())

def to_native(v):
    """轉成 Google Sheets 可吃的型別"""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if isinstance(v, (pd.Timestamp, np.datetime64, datetime)):
        return pd.to_datetime(v).strftime("%Y-%m-%d")
    if isinstance(v, np.generic):
        return v.item()
    return v

def df_to_values(df: pd.DataFrame) -> List[List]:
    if df is None or df.empty:
        return []
    out = df.copy()
    # 轉日期欄位
    for c in out.columns:
        if np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = pd.to_datetime(out[c]).dt.strftime("%Y-%m-%d")
    out = out.applymap(to_native)
    return [out.columns.tolist()] + out.values.tolist()

# ========= Google Sheets =========
def gs_client() -> gspread.Client:
    raw = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON")
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    sid = os.environ.get("SHEET_ID", "").strip()
    if not sid:
        raise RuntimeError("缺少 SHEET_ID")
    return gs_client().open_by_key(sid)

def ensure_ws(sh: gspread.Spreadsheet, title: str, rows=1000, cols=40):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

def write_df(sh, title: str, df: pd.DataFrame, stamp=True):
    ws = ensure_ws(sh, title, rows=max(1000, len(df) + 10), cols=max(40, len(df.columns) + 2))
    ws.clear()
    values = df_to_values(df)
    if stamp:
        ws.update("A1", [[f"Last Update (Asia/Taipei): {now_str()}"]], value_input_option="RAW")
        start = "A3"
    else:
        start = "A1"
    if values:
        ws.update(start, values, value_input_option="RAW")

# ========= 設定（config.json 可選） =========
def load_config():
    cfg = {}
    try:
        with io.open("config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        pass
    all_list = cfg.get("all", DEFAULT_ALL)
    fin_list = cfg.get("fin", DEFAULT_FIN)
    nonfin_list = [t for t in all_list if t not in set(fin_list)]
    return all_list, fin_list, nonfin_list

# ========= 指標 =========
def rsi_ewm(close: pd.Series, period=14) -> pd.Series:
    c = squeeze_1d(close).astype(float)
    delta = c.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    up = gain.ewm(alpha=1/period, adjust=False).mean()
    dn = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = up / (dn.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = squeeze_1d(out["Close"]).astype(float)
    out["SMA20"]  = close.rolling(20, min_periods=1).mean()
    out["SMA50"]  = close.rolling(50, min_periods=1).mean()
    out["SMA200"] = close.rolling(200, min_periods=1).mean()
    out["RSI14"]  = rsi_ewm(close, 14)
    mid = out["SMA20"]
    std = close.rolling(20, min_periods=1).std(ddof=0)
    out["BB_Mid"]   = mid
    out["BB_Upper"] = mid + 2 * std
    out["BB_Lower"] = mid - 2 * std
    return out

# ========= 建議（純量判斷版） =========
def decide(row: pd.Series) -> Dict[str, Any]:
    def S(key, default=np.nan):
        try:
            v = row.get(key, default)
            return float(v) if v is not None and v != "" else default
        except Exception:
            return default

    c = S("Close"); sma20 = S("SMA20"); sma50 = S("SMA50"); sma200 = S("SMA200")
    rsi = S("RSI14"); u = S("BB_Upper"); l = S("BB_Lower"); m = S("BB_Mid")

    if any(map(lambda x: (x is np.nan) or (x != x), [c, sma20, sma50, sma200, rsi, u, l, m])):
        return {"多空": "未知", "建議": "資料不足", "進場": "", "出場": "", "信心": 0}

    # 結構
    if sma20 > sma50 > sma200 and c > sma20:
        trend = "多頭"
    elif sma20 < sma50 < sma200 and c < sma20:
        trend = "空頭"
    else:
        trend = "盤整"

    # 建議（保守）
    if trend == "多頭":
        advice = "偏多→回到中軌/20MA 附近可分批；跌破下軌停損"
        entry  = f"靠近中軌≈{m:.2f}（±1%）"
        exit_  = f"跌破下軌≈{l:.2f} 或日收跌破20MA≈{sma20:.2f}"
    elif trend == "空頭":
        advice = "偏空→反彈至中軌附近逢高減碼；站回20MA觀望"
        entry  = f"反彈至中軌≈{m:.2f}"
        exit_  = f"突破上軌≈{u:.2f} 或站回20MA≈{sma20:.2f}"
    else:
        advice = "盤整→區間思維；下緣偏多、上緣偏賣"
        entry  = f"靠近下緣≈{l:.2f}"
        exit_  = f"靠近上緣≈{u:.2f}"

    # 信心（簡易 0~100）
    score = 50
    score += 10 if trend == "多頭" else (-10 if trend == "空頭" else 0)
    score += min(15, max(0, (abs(rsi - 50) / 50) * 15))
    score = int(max(0, min(100, round(score))))

    return {"多空": trend, "建議": advice, "進場": entry, "出場": exit_, "信心": score}

# ========= 下載與彙整 =========
def fetch(ticker: str) -> Tuple[pd.DataFrame, str, str]:
    """回： (只取最後一列指標表, 公司名, 失敗訊息)"""
    try:
        hist = yf.download(ticker, period="400d", interval="1d", auto_adjust=True, progress=False)
    except Exception as e:
        return pd.DataFrame(), "", f"{ticker} 下載錯誤: {e}"

    if hist is None or hist.empty:
        return pd.DataFrame(), "", f"{ticker} 無資料"

    hist = hist.rename_axis("Date").reset_index()
    hist = hist[["Date","Open","High","Low","Close","Volume"]]
    hist = add_indicators(hist)
    last = hist.iloc[-1].copy()

    # 名稱（取不到就空白）
    name = ""
    try:
        info = yf.Ticker(ticker).info
        name = info.get("shortName") or ""
    except Exception:
        name = ""

    dec = decide(last)

    row = {
        "資料時戳(Asia/Taipei)": now_str(),
        "Date": last["Date"],
        "Ticker": ticker,
        "公司名稱": name,
        "Open": last["Open"], "High": last["High"], "Low": last["Low"], "Close": last["Close"],
        "Volume": last.get("Volume", ""),
        "RSI14": last["RSI14"], "SMA20": last["SMA20"], "SMA50": last["SMA50"], "SMA200": last["SMA200"],
        "BB_Mid": last["BB_Mid"], "BB_Upper": last["BB_Upper"], "BB_Lower": last["BB_Lower"],
        "多空趨勢": dec["多空"], "操作建議": dec["建議"], "建議進場": dec["進場"], "建議出場": dec["出場"], "信心分數": dec["信心"]
    }
    return pd.DataFrame([row]), name, ""

def aggregate(tickers: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    rows = []; errs = []
    for t in tickers:
        df1, _, err = fetch(t)
        if not df1.empty:
            rows.append(df1)
        if err:
            errs.append(err)
        time.sleep(0.25)  # 禮貌節流
    if rows:
        out = pd.concat(rows, ignore_index=True)
        # 排序：Ticker
        out = out.sort_values(["Ticker"]).reset_index(drop=True)
    else:
        out = pd.DataFrame(columns=[
            "資料時戳(Asia/Taipei)","Date","Ticker","公司名稱","Open","High","Low","Close","Volume",
            "RSI14","SMA20","SMA50","SMA200","BB_Mid","BB_Upper","BB_Lower",
            "多空趨勢","操作建議","建議進場","建議出場","信心分數"
        ])
    return out, errs

def top10_by_volume(df_nonfin: pd.DataFrame) -> pd.DataFrame:
    if df_nonfin.empty: return df_nonfin
    return df_nonfin.sort_values("Volume", ascending=False).head(10).reset_index(drop=True)

def hot20_score(df_nonfin: pd.DataFrame) -> pd.DataFrame:
    if df_nonfin.empty: return df_nonfin
    d = df_nonfin.copy()
    # 距離中軌%、波動%、量標準化
    d["距離中軌%"] = ((d["Close"] - d["BB_Mid"]) / d["BB_Mid"]).abs() * 100
    d["波動%"] = ((d["BB_Upper"] - d["BB_Lower"]) / d["BB_Mid"]).abs() * 100
    def z(x):
        x = pd.to_numeric(x, errors="coerce")
        mu, sd = x.mean(skipna=True), x.std(skipna=True)
        if sd and not math.isnan(sd) and sd != 0:
            return (x - mu) / sd
        return x * 0
    d["z_vol"]  = z(d["Volume"])
    d["z_dist"] = z(d["距離中軌%"])
    d["z_vola"] = z(d["波動%"])
    d["熱度分數"] = d[["z_vol","z_dist","z_vola"]].sum(axis=1)
    d = d.sort_values(["熱度分數","Volume"], ascending=[False, False]).reset_index(drop=True)
    return d.head(20)

# ========= 主流程 =========
def main():
    print("[INFO] 啟動 TW50 V3")

    sh = open_sheet()
    all_list, fin_list, nonfin_list = load_config()

    # 全表（取最後一列）
    fin_df, fin_errs     = aggregate(fin_list)
    nonfin_df, nonf_errs = aggregate(nonfin_list)

    # 衍生表
    top10 = top10_by_volume(nonfin_df)
    hot20 = hot20_score(nonfin_df)
    top5  = hot20.head(5).reset_index(drop=True) if not hot20.empty else hot20

    # 寫入
    write_df(sh, TAB_FIN,    fin_df)
    write_df(sh, TAB_NONFIN, nonfin_df)
    write_df(sh, TAB_TOP10,  top10)
    write_df(sh, TAB_HOT20,  hot20)
    write_df(sh, TAB_TOP5H20, top5)

    # Logs
    logs = fin_errs + nonf_errs
    logs_df = pd.DataFrame({"Time(Asia/Taipei)": [now_str()]*len(logs), "Message": logs}) if logs else pd.DataFrame({"Time(Asia/Taipei)": [now_str()], "Message": ["本次全部成功"]})
    write_df(sh, TAB_LOGS, logs_df, stamp=False)

    print("[OK] 完成 ✅")

if __name__ == "__main__":
    main()
