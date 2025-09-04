# -*- coding: utf-8 -*-
"""
TW50 TOP5 — 勝率強化版（完整檔）
yfinance + TWSE備援、資料消毒、防呆寫入、Top清單與進出場建議、ATR風控

版本：v2025-09-04-winrate-pro

分頁輸出：
  - TW50_fin（金融）
  - TW50_nonfin（非金融）
  - Top10_nonfin（非金 Top10：RSI、Volume 排序）
  - Hot20_nonfin（非金 成交量前20）
  - Top5_hot20（Hot20裡再挑Top5，附進出場與停損/停利）
  - 交接本（簡要說明與 Roadmap）

GitHub Actions 需要的 Secrets：
  - SHEET_ID
  - GCP_SERVICE_ACCOUNT_JSON
"""

import os, json, time
import numpy as np
import pandas as pd
import requests
import yfinance as yf

import gspread
from gspread_dataframe import set_with_dataframe

# ========== 代號↔名稱（可以自己慢慢補；缺的就留空白不影響計算） ==========
TICKER_NAME_MAP = {
    # 非金融（部份示意）
    "2330.TW":"台積電","2317.TW":"鴻海","2454.TW":"聯發科","2303.TW":"聯電","2308.TW":"台達電",
    "2382.TW":"廣達","2379.TW":"瑞昱","2395.TW":"研華","2412.TW":"中華電","1216.TW":"統一",
    "1301.TW":"台塑","1326.TW":"台化","1402.TW":"遠東新","1101.TW":"台泥","1102.TW":"亞泥",
    "2002.TW":"中鋼","3008.TW":"大立光","3711.TW":"日月光投控","2603.TW":"長榮","2609.TW":"陽明",
    "2615.TW":"萬海","3481.TW":"群創","3006.TW":"晶豪科","2408.TW":"南亞科",
    # 金融（28xx + 常見金融股）
    "2880.TW":"華南金","2881.TW":"富邦金","2882.TW":"國泰金","2883.TW":"開發金","2884.TW":"玉山金",
    "2885.TW":"元大金","2886.TW":"兆豐金","2887.TW":"台新金","2888.TW":"新光金","2889.TW":"國票金",
    "2890.TW":"永豐金","2891.TW":"中信金","2892.TW":"第一金","2897.TW":"王道銀行","2898.TW":"安泰銀",
    "5871.TW":"中租-KY","5876.TW":"上海商銀",
}

FIN_TICKERS = {t for t in TICKER_NAME_MAP if t.startswith("28")}
FIN_TICKERS.update({"5871.TW", "5876.TW"})  # 金融但不是 28xx 的

# ========== 小工具 ==========
def taipei_now_str():
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def get_gspread_client():
    js = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not js:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    try:
        data = json.loads(js)
    except Exception as e:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON 不是合法 JSON") from e
    return gspread.service_account_from_dict(data)

def get_sheet():
    sid = os.environ.get("SHEET_ID", "")
    if not sid:
        raise RuntimeError("缺少 SHEET_ID Secret")
    print("[INFO] Target SHEET_ID:", sid)
    return get_gspread_client().open_by_key(sid)

def get_or_create(sh, title, rows=2000, cols=30):
    for ws in sh.worksheets():
        if ws.title == title:
            return ws
    return sh.add_worksheet(title=title, rows=rows, cols=cols)

def sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    """避免 gspread 傳輸 Protobuf listValue 錯誤：datetime→str、NaN→None、inf→NaN。"""
    out = df.copy()
    # datetime 轉字串
    for c in out.columns:
        if np.issubdtype(out[c].dtype, np.datetime64):
            out[c] = out[c].astype(str)
    # 數值特例處理
    out.replace([np.inf, -np.inf], np.nan, inplace=True)
    out = out.where(pd.notnull(out), None)
    # 欄名保證是字串
    out.columns = [str(c) for c in out.columns]
    # index 不上傳
    return out

def upsert_df(ws, df, stamp_text):
    ws.clear()
    ws.update("A1", [[f"資料截至 (Asia/Taipei): {stamp_text}"]])
    if df is None or df.empty:
        ws.update("A3", [["No Data"]])
        return
    clean = sanitize_df(df)
    set_with_dataframe(ws, clean, row=2, include_index=False, include_column_header=True)

# ========== 指標計算 ==========
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_index().copy()
    # 均線
    df["SMA20"]  = df["Close"].rolling(20, min_periods=20).mean()
    df["SMA50"]  = df["Close"].rolling(50, min_periods=50).mean()
    df["SMA200"] = df["Close"].rolling(200, min_periods=200).mean()
    # 成交量 20MA
    df["Vol20"] = df["Volume"].rolling(20, min_periods=20).mean()
    # RSI14（Wilder）
    delta = df["Close"].diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))
    # 布林通道
    mid = df["Close"].rolling(20, min_periods=20).mean()
    std = df["Close"].rolling(20, min_periods=20).std()
    df["BB_Mid"]   = mid
    df["BB_Upper"] = mid + 2 * std
    df["BB_Lower"] = mid - 2 * std
    # ATR14（風控）
    prev_close = df["Close"].shift(1)
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - prev_close).abs()
    tr3 = (df["Low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["ATR14"] = tr.rolling(14, min_periods=14).mean()
    return df

# ========== 下載資料：yfinance 主、TWSE 備援 ==========
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def fetch_yf_history(ticker: str, period="12mo", interval="1d") -> pd.DataFrame | None:
    try:
        raw = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    except Exception:
        return None
    if raw is None or raw.empty:
        return None
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]
    df = raw[["Open","High","Low","Close","Volume"]].copy()
    return df

def _twse_month_df(stock_no: str, yyyymmdd: str) -> pd.DataFrame:
    url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
    params = {"response":"json","date":yyyymmdd,"stockNo":stock_no}
    r = requests.get(url, params=params, headers=HEADERS, timeout=12)
    r.raise_for_status()
    js = r.json()
    if js.get("stat") != "OK" or "data" not in js:
        return pd.DataFrame()
    cols = js["fields"]
    df = pd.DataFrame(js["data"], columns=cols)

    def _num(x):
        try:
            return float(str(x).replace(",","").replace("--",""))
        except:
            return np.nan

    df = df.rename(columns={
        "日期":"Date","開盤價":"Open","最高價":"High","最低價":"Low","收盤價":"Close","成交股數":"Volume"
    })
    df["Date"] = pd.to_datetime(df["Date"].str.replace("/","-"), format="%Y-%m-%d", errors="coerce")
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
            part = _twse_month_df(stock_no, yyyymmdd)
            if not part.empty:
                pieces.append(part)
        except Exception:
            pass
        time.sleep(0.35)  # 禮貌性間隔，避免被擋
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

# ========== 交接本（說明分頁） ==========
def update_roadmap(sh, stamp):
    ws = get_or_create(sh, "交接本", rows=300, cols=8)
    rows = []
    rows.append([f"交接本（自動更新）｜最後更新：{stamp}"])
    rows.append([])
    rows.append(["已完成 ✅","說明"])
    rows += [
        ["每日自動化","GitHub Actions → TW50 → Google Sheet"],
        ["技術指標","SMA20/50/200、RSI14、布林帶、Vol20、ATR14"],
        ["分頁","TW50_fin / TW50_nonfin / Top10_nonfin / Hot20_nonfin / Top5_hot20"],
        ["防呆","yfinance→TWSE 備援；抓不到自動跳過；寫入前消毒"],
    ]
    rows.append([])
    rows.append(["進行中 🛠","說明"])
    rows += [
        ["勝率提升","成交量≥Vol20 + 趨勢同向(多頭/空頭) + 嚴謹門檻(布林%b/RSI)"],
        ["名稱補齊","代號↔中文名稱保底不空白（缺的日後補齊）"],
    ]
    rows.append([])
    rows.append(["未來 🚀","說明"])
    rows += [
        ["籌碼面","外資/投信/自營商買賣超"],
        ["基本面","EPS、殖利率"],
        ["通知","LINE/Email 推播 Top5 訊號"],
        ["盤中","需券商API/付費即時流"],
        ["Dashboard","技術＋籌碼＋基本面 → 多空分數"],
    ]
    ws.clear()
    ws.update("A1", rows)

# ========== 主流程 ==========
def main():
    print("== TW50 TOP5（winrate-pro）==")
    sh = get_sheet()
    stamp = taipei_now_str()

    # 讀 config.json（可放 tickers 清單；沒有就用內建 MAP 的 key）
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
        last = df.tail(1).copy()  # 取最新一列
        # 補公司名稱（沒有就空字串，不影響）
        cname = TICKER_NAME_MAP.get(t, "")
        last.insert(0, "公司名稱", cname)
        last.insert(0, "股票代號", t)
        last = last.reset_index().rename(columns={"index":"Date"})
        rows.append(last)

    if not rows:
        raise RuntimeError("本次沒有任何代號成功抓到資料")

    df_all = pd.concat(rows, ignore_index=True)

    # 金融 / 非金融 分流
    is_fin = df_all["股票代號"].isin(FIN_TICKERS) | df_all["股票代號"].str.startswith("28")
    df_fin    = df_all[is_fin].copy()
    df_nonfin = df_all[~is_fin].copy()

    # 全量欄位
    base_cols = ["股票代號","公司名稱","Date","Open","High","Low","Close","Volume","Vol20",
                 "RSI14","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper","ATR14"]
    base_cols = [c for c in base_cols if c in df_all.columns]
    df_fin_all    = df_fin[base_cols].copy()
    df_nonfin_all = df_nonfin[base_cols].copy()

    # Top10（非金）：RSI、Volume 高到低
    top10 = df_nonfin.sort_values(["RSI14","Volume"], ascending=[False, False]).head(10).copy()

    # Hot20（非金）：成交量最高 20
    hot20 = df_nonfin.sort_values("Volume", ascending=False).head(20).copy()

    # Top5：Hot20 中再依 RSI、Volume 挑前5
    top5 = hot20.sort_values(["RSI14","Volume"], ascending=[False, False]).head(5).copy()

    # 布林%b
    bb_range = (top5["BB_Upper"] - top5["BB_Lower"]).replace(0, np.nan)
    top5["BB_percent"] = (top5["Close"] - top5["BB_Lower"]) / bb_range

    # 趨勢過濾（同向才打訊號）
    top5["多頭"] = (top5["Close"] > top5["SMA50"]) & (top5["SMA50"] > top5["SMA200"])
    top5["空頭"] = (top5["Close"] < top5["SMA50"]) & (top5["SMA50"] < top5["SMA200"])

    # 成交量過濾（放量才有效）
    top5["VolOK"] = top5["Volume"] >= top5["Vol20"]

    # 嚴謹門檻（更保守）
    def signal_pro(r):
        if r["VolOK"]:
            # 多頭只做多、空頭只做空
            if r["多頭"] and (
                (pd.notna(r["BB_percent"]) and r["BB_percent"] <= 0.12) or
                (pd.notna(r["RSI14"]) and r["RSI14"] <= 38)
            ):
                return "買進"
            if r["空頭"] and (
                (pd.notna(r["BB_percent"]) and r["BB_percent"] >= 0.88) or
                (pd.notna(r["RSI14"]) and r["RSI14"] >= 62)
            ):
                return "賣出"
        return "觀望"

    top5["訊號"] = top5.apply(signal_pro, axis=1)

    # 建議進出場（布林帶區間）
    top5["建議進場下界"] = top5["BB_Lower"]
    top5["建議進場上界"] = top5["BB_Mid"]
    top5["建議出場下界"] = top5["BB_Mid"]
    top5["建議出場上界"] = top5["BB_Upper"]

    # 參考距離（百分比）
    top5["距離進場%"] = np.where(
        pd.notna(top5["BB_Mid"]) & (top5["Close"] <= top5["BB_Mid"]),
        (top5["Close"] - top5["BB_Lower"]) / top5["Close"] * 100, 0.0
    )
    top5["距離出場%"] = np.where(
        pd.notna(top5["BB_Mid"]) & (top5["Close"] >= top5["BB_Mid"]),
        (top5["BB_Upper"] - top5["Close"]) / top5["Close"] * 100, 0.0
    )

    # 風控（ATR）
    # 停損 = 1×ATR；停利 = 2.5×ATR（可調）
    top5["建議停損%"] = (top5["ATR14"] / top5["Close"]) * 100
    top5["建議停利%"] = (top5["ATR14"] * 2.5 / top5["Close"]) * 100

    # Top5輸出欄位
    top5_cols = [
        "股票代號","公司名稱","Date","Close","RSI14","BB_percent","多頭","空頭","VolOK","訊號",
        "建議進場下界","建議進場上界","建議出場下界","建議出場上界",
        "距離進場%","距離出場%","建議停損%","建議停利%"
    ]
    top5_out = top5[top5_cols].copy()

    # ====== 寫入 Google Sheet ======
    upsert_df(get_or_create(sh,"TW50_fin"),     df_fin_all,    stamp)
    upsert_df(get_or_create(sh,"TW50_nonfin"),  df_nonfin_all, stamp)
    upsert_df(get_or_create(sh,"Top10_nonfin"), top10,         stamp)
    upsert_df(get_or_create(sh,"Hot20_nonfin"), hot20,         stamp)
    upsert_df(get_or_create(sh,"Top5_hot20"),   top5_out,      stamp)

    # 交接本
    update_roadmap(sh, stamp)

    print(f"[INFO] Update 完成：成功 {len(df_all)} 檔；跳過 {len(failed)} 檔 -> {failed}")


if __name__ == "__main__":
    main()
