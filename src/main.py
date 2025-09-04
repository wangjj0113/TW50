# -*- coding: utf-8 -*-
"""
TW50 TOP5 — yfinance + TWSE 備援（強化寫入防呆 + 勝率版訊號 + 交接本）
版本：v2025-09-04-roadmap-volume

Secrets（GitHub Actions）：
  - SHEET_ID
  - GCP_SERVICE_ACCOUNT_JSON

輸出分頁：
  - TW50_fin / TW50_nonfin / Top10_nonfin / Hot20_nonfin / Top5_hot20 / 交接本

Top5_hot20 欄位：
  股票代號、公司名稱、Date、收盤價、RSI14、布林%b、訊號（買/賣/觀望）、
  建議進場下界/上界、建議出場下界/上界、距離進場%、距離出場%、Volume、Vol20、
  SMA20/50/200、BB_*
"""

import os, json, time
import numpy as np
import pandas as pd
import requests
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe

# ====== 代號 ↔ 公司名稱（可擴充）======
TICKER_NAME_MAP = {
    "2330.TW":"台積電","2317.TW":"鴻海","2454.TW":"聯發科","2303.TW":"聯電","2308.TW":"台達電",
    "2379.TW":"瑞昱","2382.TW":"廣達","2395.TW":"研華","2408.TW":"南亞科","2412.TW":"中華電",
    "3006.TW":"晶豪科","3008.TW":"大立光","3711.TW":"日月光投控","2603.TW":"長榮","2609.TW":"陽明",
    "2615.TW":"萬海","1216.TW":"統一","1402.TW":"遠東新","1301.TW":"台塑","1326.TW":"台化",
    "1101.TW":"台泥","1102.TW":"亞泥","2002.TW":"中鋼","4904.TW":"遠傳","3481.TW":"群創",
    # 金融
    "2880.TW":"華南金","2881.TW":"富邦金","2882.TW":"國泰金","2883.TW":"開發金","2884.TW":"玉山金",
    "2885.TW":"元大金","2886.TW":"兆豐金","2887.TW":"台新金","2888.TW":"新光金","2889.TW":"國票金",
    "2890.TW":"永豐金","2891.TW":"中信金","2892.TW":"第一金","2897.TW":"王道銀行","2898.TW":"安泰銀",
    "5871.TW":"中租-KY","5876.TW":"上海商銀"
}
FIN_TICKERS = {t for t in TICKER_NAME_MAP if t.startswith("28")}
FIN_TICKERS.update({"5871.TW","5876.TW"})

# ====== 小工具 ======
def taipei_now_str():
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def get_gspread_client():
    js = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not js:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    return gspread.service_account_from_dict(json.loads(js))

def get_sheet():
    sid = os.environ.get("SHEET_ID", "")
    if not sid:
        raise RuntimeError("缺少 SHEET_ID Secret")
    print("[INFO] SHEET_ID:", sid)
    return get_gspread_client().open_by_key(sid)

def get_or_create(sh, title, rows=2000, cols=30):
    for ws in sh.worksheets():
        if ws.title == title: return ws
    return sh.add_worksheet(title=title, rows=rows, cols=cols)

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """轉成 Google Sheet 友善格式：日期→字串、Inf→NaN、NaN→None、欄名字串化"""
    out = df.copy()
    for c in out.columns:
        if np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = out[c].astype(str)
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    out = out.where(pd.notnull(out), None)
    out.columns = [str(c) for c in out.columns]
    return out

def upsert_df(ws, df, stamp_text):
    ws.clear()
    ws.update("A1", [[f"資料截至 (Asia/Taipei): {stamp_text}"]])   # A1 一律 2D list
    if df is None or df.empty:
        ws.update("A3", [["No Data"]])
        return
    clean = sanitize_df(df)
    set_with_dataframe(ws, clean, row=2, include_index=False, include_column_header=True)

# ====== 指標 ======
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_index().copy()
    # 均線
    df["SMA20"]  = df["Close"].rolling(20, min_periods=20).mean()
    df["SMA50"]  = df["Close"].rolling(50, min_periods=50).mean()
    df["SMA200"] = df["Close"].rolling(200, min_periods=200).mean()
    # 成交量20MA
    df["Vol20"] = df["Volume"].rolling(20, min_periods=20).mean()
    # RSI14
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))
    # 布林
    mid = df["Close"].rolling(20, min_periods=20).mean()
    std = df["Close"].rolling(20, min_periods=20).std()
    df["BB_Mid"]   = mid
    df["BB_Upper"] = mid + 2 * std
    df["BB_Lower"] = mid - 2 * std
    return df

# ====== yfinance 主來源 ======
def fetch_yf_history(ticker: str, period="12mo", interval="1d") -> pd.DataFrame | None:
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df[["Open","High","Low","Close","Volume"]].copy()

# ====== TWSE 備援（月檔整併）======
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def _twse_month_df(stock_no: str, yyyymmdd: str) -> pd.DataFrame:
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {"response":"json","date":yyyymmdd,"stockNo":stock_no}
    r = requests.get(url, params=params, headers=HEADERS, timeout=12)
    r.raise_for_status()
    js = r.json()
    if js.get("stat") != "OK" or "data" not in js:
        return pd.DataFrame()
    cols = js["fields"]  # ['日期','成交股數','成交金額','開盤價','最高價','最低價','收盤價','漲跌價差','成交筆數']
    df = pd.DataFrame(js["data"], columns=cols)

    def _num(x):
        try: return float(str(x).replace(",","").replace("--",""))
        except: return np.nan

    df = df.rename(columns={
        "日期":"Date","開盤價":"Open","最高價":"High","最低價":"Low","收盤價":"Close","成交股數":"Volume"
    })
    df["Date"] = pd.to_datetime(df["Date"].str.replace("/","-"), format="%Y-%m-%d")
    for c in ["Open","High","Low","Close","Volume"]:
        df[c] = df[c].apply(_num)
    df = df[["Date","Open","High","Low","Close","Volume"]].dropna(subset=["Close"])
    return df.set_index("Date").sort_index()

def fetch_twse_history(ticker: str, months: int = 12) -> pd.DataFrame | None:
    stock_no = ticker.split(".")[0]
    today = pd.Timestamp.now(tz="Asia/Taipei")
    pieces = []
    for m in range(months):
        dt = today - pd.DateOffset(months=m)
        yyyymmdd = f"{dt.year}{dt.month:02d}01"
        try:
            dfm = _twse_month_df(stock_no, yyyymmdd)
            if not dfm.empty:
                pieces.append(dfm)
        except Exception:
            pass
        time.sleep(0.35)  # 節流
    if not pieces:
        return None
    df = pd.concat(pieces).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df[["Open","High","Low","Close","Volume"]]

def fetch_history_with_fallback(ticker: str) -> pd.DataFrame | None:
    df = fetch_yf_history(ticker)
    if df is not None and not df.empty:
        return df
    print(f"[INFO] yfinance 無資料 → 改用 TWSE：{ticker}")
    return fetch_twse_history(ticker, months=12)

# ====== 交接本分頁 ======
def update_roadmap(sh, stamp):
    ws = get_or_create(sh, "交接本", rows=200, cols=8)
    rows = []
    rows.append([f"交接本（自動更新）｜最後更新：{stamp}"])
    rows.append([])
    rows.append(["已完成 ✅","說明"])
    rows += [
        ["GitHub Actions 自動化","每日自動抓 TW50 寫入 Google Sheet"],
        ["技術指標","SMA20/50/200、RSI14、布林通道、Vol20"],
        ["分頁","TW50_fin / TW50_nonfin / Top10_nonfin / Hot20_nonfin / Top5_hot20"],
        ["防呆機制","yfinance→TWSE 備援；抓不到自動跳過；寫入前資料消毒"],
    ]
    rows.append([])
    rows.append(["進行中 🛠","說明"])
    rows += [
        ["勝率提升","訊號需同時滿足：成交量≥20日均量 ＋（RSI≤40 或 布林%b≤0.10）/（RSI≥60 或 布林%b≥0.90）"],
        ["公司名稱補齊","代號↔中文名稱保底不空白"],
        ["說明分頁","各分頁定義＋金融股解讀（偏存股、技術面僅參考）"],
    ]
    rows.append([])
    rows.append(["未來 🚀","說明"])
    rows += [
        ["籌碼面","外資／投信／自營商買賣超搭配技術面"],
        ["基本面","EPS、殖利率過濾弱勢標的"],
        ["自動通知","LINE / Email 每日 Top5 訊號"],
        ["即時化","盤中更新（需券商 API / 付費數據）"],
        ["至尊版","技術＋籌碼＋基本面 → 多空分數、Dashboard"],
    ]
    ws.clear()
    ws.update("A1", rows)

# ====== 主流程 ======
def main():
    print("== TW50 TOP5（yfinance + TWSE fallback + 勝率版訊號 + 交接本）==")
    sh = get_sheet()
    stamp = taipei_now_str()

    # 清單：先讀 config.json 的 "tickers"/"TW50"，否則用內建 map keys
    tickers = []
    if os.path.exists("config.json"):
        try:
            with open("config.json","r",encoding="utf-8") as f:
                cfg = json.load(f)
                tickers = cfg.get("tickers") or cfg.get("TW50") or []
        except Exception as e:
            print("[WARN] 讀取 config.json 失敗，改用內建清單", e)
    if not tickers:
        tickers = list(TICKER_NAME_MAP.keys())

    rows, failed = [], []
    for t in tickers:
        hist = fetch_history_with_fallback(t)
        if hist is None or hist.empty:
            print(f"[WARN] {t} 查無日線資料，已跳過")
            failed.append(t)
            continue
        df = add_indicators(hist)
        last = df.tail(1).copy()
        last.insert(0, "公司名稱", TICKER_NAME_MAP.get(t, ""))
        last.insert(0, "股票代號", t)
        last = last.reset_index().rename(columns={"index":"Date"})
        rows.append(last)

    if not rows:
        raise RuntimeError("本次沒有任何代號成功抓到資料")

    df_all = pd.concat(rows, ignore_index=True)

    # 金融 / 非金融
    is_fin = df_all["股票代號"].isin(FIN_TICKERS) | df_all["股票代號"].str.startswith("28")
    df_fin    = df_all[is_fin].copy()
    df_nonfin = df_all[~is_fin].copy()

    # 全量欄位
    base_cols = ["股票代號","公司名稱","Date","Open","High","Low","Close","Volume","Vol20",
                 "RSI14","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"]
    base_cols = [c for c in base_cols if c in df_all.columns]
    df_fin_all    = df_fin[base_cols].copy()
    df_nonfin_all = df_nonfin[base_cols].copy()

    # Top10（非金）：RSI、Volume 由高到低
    top10 = df_nonfin.sort_values(["RSI14","Volume"], ascending=[False, False]).head(10).copy()

    # Hot20（非金）：成交量最高 20
    hot20 = df_nonfin.sort_values("Volume", ascending=False).head(20).copy()

    # Top5 from Hot20（加入更高勝率訊號）
    top5 = hot20.sort_values(["RSI14","Volume"], ascending=[False, False]).head(5).copy()

    # 布林%b（0=貼近下軌、1=貼近上軌）
    bb_range = (top5["BB_Upper"] - top5["BB_Lower"]).replace(0, np.nan)
    top5["BB_percent"] = (top5["Close"] - top5["BB_Lower"]) / bb_range

    # —— 勝率提升：成交量過濾（Volume 必須 ≥ Vol20）——
    def signal_with_volume(r):
        vol_ok = pd.notna(r.get("Vol20")) and pd.notna(r.get("Volume")) and (r["Volume"] >= r["Vol20"])
        if vol_ok:
            if (pd.notna(r["BB_percent"]) and r["BB_percent"] <= 0.10) or (pd.notna(r["RSI14"]) and r["RSI14"] <= 40):
                return "買進"
            if (pd.notna(r["BB_percent"]) and r["BB_percent"] >= 0.90) or (pd.notna(r["RSI14"]) and r["RSI14"] >= 60):
                return "賣出"
        return "觀望"
    top5["訊號"] = top5.apply(signal_with_volume, axis=1)

    # 進/出場區間（布林下~中 / 中~上）
    top5["建議進場下界"] = top5["BB_Lower"]
    top5["建議進場上界"] = top5["BB_Mid"]
    top5["建議出場下界"] = top5["BB_Mid"]
    top5["建議出場上界"] = top5["BB_Upper"]

    # 與進/出場「距離%」
    top5["距離進場%"] = np.where(
        top5["Close"] <= top5["BB_Mid"],
        (top5["Close"] - top5["BB_Lower"]) / top5["Close"] * 100,
        0.0
    )
    top5["距離出場%"] = np.where(
        top5["Close"] >= top5["BB_Mid"],
        (top5["BB_Upper"] - top5["Close"]) / top5["Close"] * 100,
        0.0
    )

    # Top5 欄位輸出（中文）
    top5_cols = [
        "股票代號","公司名稱","Date","Close","RSI14","BB_percent","訊號",
        "建議進場下界","建議進場上界","建議出場下界","建議出場上界",
        "距離進場%","距離出場%","Volume","Vol20",
        "Open","High","Low","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"
    ]
    top5_out = top5[[c for c in top5_cols if c in top5.columns]].rename(
        columns={"Close":"收盤價","BB_percent":"布林%b"}
    )

    # 寫入各分頁（全面防呆）
    for title, data in [
        ("TW50_fin",    df_fin_all),
        ("TW50_nonfin", df_nonfin_all),
        ("Top10_nonfin",top10),
        ("Hot20_nonfin",hot20),
        ("Top5_hot20",  top5_out),
    ]:
        ws = get_or_create(sh, title)
        upsert_df(ws, data, stamp)
        time.sleep(0.25)

    # 交接本
    update_roadmap(sh, stamp)

    if failed:
        print("[WARN] 這些代號找不到資料 → 已跳過：", ", ".join(failed))
    else:
        print("[INFO] 本次所有代號皆成功")

    print("✅ 全部分頁更新完成")

if __name__ == "__main__":
    main()
