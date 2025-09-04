# -*- coding: utf-8 -*-
"""
TW50-TOP5 main.py
版本：v2025.09.04-Top5-zh-2
重點：
- 讀取環境變數：SHEET_ID、GCP_SERVICE_ACCOUNT_JSON
- 產出分頁：TW50_fin / TW50_nonfin / Top10_nonfin / Hot20_nonfin / Top5_hot20
- Top5_hot20：中文欄位 + 公司名稱 + 訊號(買進/賣出/觀望) + 建議進/出場區間
- A1 顯示「資料截至 (Asia/Taipei)」
- 僅更新上述資料分頁；不會碰到你手動維護的「說明」分頁
"""

import os, json, time
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe

# -----------------------
# 基本設定與輔助
# -----------------------
def taipei_now_str():
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")

def get_gspread_client():
    js = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
    if not js:
        raise RuntimeError("缺少 GCP_SERVICE_ACCOUNT_JSON Secret")
    creds = json.loads(js)
    print("[INFO] Using service account:", creds.get("client_email", ""))
    return gspread.service_account_from_dict(creds)

def get_sheet():
    sheet_id = os.environ.get("SHEET_ID", "")
    if not sheet_id:
        raise RuntimeError("缺少 SHEET_ID Secret")
    print("[INFO] Target SHEET_ID:", sheet_id)
    gc = get_gspread_client()
    return gc.open_by_key(sheet_id)

def upsert_df(ws, df, stamp_text):
    """安全寫入：清空→A1時間→從A2寫表頭+資料"""
    ws.clear()
    ws.update("A1", f"資料截至 (Asia/Taipei): {stamp_text}")
    set_with_dataframe(ws, df, row=2, include_index=False, include_column_header=True)

def get_or_create_ws(sh, title, rows=2000, cols=30):
    titles = [w.title for w in sh.worksheets()]
    if title in titles:
        return sh.worksheet(title)
    return sh.add_worksheet(title=title, rows=rows, cols=cols)

# -----------------------
# 代號 ↔ 公司名稱（可擴充）
# -----------------------
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

# 金融股集合（含 28xx 與上面列出者）
FIN_TICKERS = set([t for t in TICKER_NAME_MAP if t.startswith("28")])
FIN_TICKERS.update({
    "5871.TW","5876.TW"
})

# -----------------------
# 讀取 config（若存在）
# -----------------------
def load_config_tickers():
    """優先讀 config.json 的 tickers；沒有就用一份常見清單（含金融/非金融）"""
    fallback = [
        "2330.TW","2317.TW","2454.TW","2303.TW","2308.TW","2412.TW","3711.TW","3008.TW",
        "2379.TW","2382.TW","2395.TW","2408.TW","1216.TW","1402.TW","1301.TW","1326.TW",
        "1101.TW","1102.TW","2002.TW","3481.TW",
        "2881.TW","2882.TW","2884.TW","2885.TW","2891.TW","2892.TW","5871.TW"
    ]
    cfg_path = "config.json"
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                ticks = cfg.get("tickers") or cfg.get("TW50") or []
                if isinstance(ticks, list) and ticks:
                    print(f"[INFO] Load {len(ticks)} tickers from config.json")
                    return ticks
        except Exception as e:
            print("[WARN] 讀取 config.json 失敗，使用內建清單。", e)
    print(f"[INFO] Use fallback tickers: {len(fallback)}")
    return fallback

# -----------------------
# 指標計算
# -----------------------
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # 移動平均
    df["SMA20"]  = df["Close"].rolling(20, min_periods=20).mean()
    df["SMA50"]  = df["Close"].rolling(50, min_periods=50).mean()
    df["SMA200"] = df["Close"].rolling(200, min_periods=200).mean()
    # RSI14
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
    return df

def last_row_with_meta(ticker: str, name: str, period="12mo", interval="1d"):
    """抓單一代號，回傳含指標的最後一列（含 Ticker/Name）"""
    try:
        df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
        if df.empty:
            return None
        # yfinance 有時會回 MultiIndex 欄位
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]
        df = df[["Open","High","Low","Close","Volume"]].copy()
        df = add_indicators(df)
        row = df.tail(1).copy()
        row.insert(0, "股票代號", ticker)
        row.insert(1, "公司名稱", TICKER_NAME_MAP.get(ticker, ""))
        return row
    except Exception as e:
        print(f"[WARN] 下載失敗 {ticker}: {e}")
        return None

# -----------------------
# Top/Hot 表格
# -----------------------
def build_colored_top5(df_nonfin: pd.DataFrame) -> pd.DataFrame:
    """從非金融挑 Hot20 → Top5 + 訊號與建議區間（中文欄名）"""
    hot20 = df_nonfin.sort_values("Volume", ascending=False).head(20).copy()
    top5  = hot20.sort_values(["Volume","RSI14"], ascending=[False, True]).head(5).copy()

    # 建議區間
    top5["建議進場下界"] = top5["BB_Lower"]
    top5["建議進場上界"] = top5["BB_Mid"]
    top5["建議出場下界"] = top5["BB_Mid"]
    top5["建議出場上界"] = top5["BB_Upper"]

    # 訊號（保守版）
    def decide_signal(r):
        if pd.notna(r["RSI14"]) and pd.notna(r["BB_Lower"]) and r["RSI14"] < 40 and r["Close"] <= r["BB_Lower"]:
            return "買進"
        if pd.notna(r["RSI14"]) and pd.notna(r["BB_Upper"]) and r["RSI14"] > 60 and r["Close"] >= r["BB_Upper"]:
            return "賣出"
        return "觀望"

    top5["訊號"] = top5.apply(decide_signal, axis=1)

    # 欄位順序（中文）
    cols = [
        "股票代號","公司名稱","Close","RSI14","訊號",
        "建議進場下界","建議進場上界","建議出場下界","建議出場上界",
        "Open","High","Low","Volume","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"
    ]
    # 只取存在的欄位
    cols = [c for c in cols if c in top5.columns]
    top5 = top5[cols].copy()
    top5 = top5.rename(columns={"Close":"收盤價"})
    return top5, hot20

# -----------------------
# 主流程
# -----------------------
def main():
    stamp = taipei_now_str()
    sh = get_sheet()

    tickers = load_config_tickers()

    rows = []
    for t in tickers:
        name = TICKER_NAME_MAP.get(t, "")
        r = last_row_with_meta(t, name, period="12mo", interval="1d")
        if r is not None:
            rows.append(r)

    if not rows:
        raise RuntimeError("沒有成功抓到任何代號的資料")

    df_all = pd.concat(rows, ignore_index=True)
    # 分金融/非金融
    is_fin = df_all["股票代號"].isin(FIN_TICKERS) | df_all["股票代號"].str.startswith("28")
    df_fin = df_all[is_fin].copy()
    df_nonfin = df_all[~is_fin].copy()

    # 排序與欄位命名（全量表）
    def tidy(df):
        # 英文技術欄保留原名，主要顯示中文在 Top5
        order = ["股票代號","公司名稱","Open","High","Low","Close","Volume",
                 "RSI14","SMA20","SMA50","SMA200","BB_Lower","BB_Mid","BB_Upper"]
        order = [c for c in order if c in df.columns]
        out = df[order].copy()
        return out

    df_fin_all    = tidy(df_fin)
    df_nonfin_all = tidy(df_nonfin)

    # Top10（非金融，示意用 RSI、Volume 排序）
    top10 = df_nonfin.sort_values(["RSI14","Volume"], ascending=[True, False]).head(10).copy()

    # Hot20 / Top5（非金融）
    top5, hot20 = build_colored_top5(df_nonfin)

    # === 寫入各分頁 ===
    for title, data in [
        ("TW50_fin",    df_fin_all),
        ("TW50_nonfin", df_nonfin_all),
        ("Top10_nonfin",top10),
        ("Hot20_nonfin",hot20),
        ("Top5_hot20",  top5),
    ]:
        ws = get_or_create_ws(sh, title)
        upsert_df(ws, data, stamp)
        time.sleep(0.3)  # 放緩一點，避免 429/暫時錯誤

    print("✅ 全部分頁更新完成")

if __name__ == "__main__":
    main()
