import os
import json
import requests
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo  # 台灣時區

# ---------- 基本設定 ----------
HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")
TZ_TAIPEI = ZoneInfo("Asia/Taipei")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# （可自行補齊）股票代號 → 名稱 對照
NAME_MAP = {
    "2330": "台積電", "2317": "鴻海", "2454": "聯發科", "2308": "台達電",
    "2881": "富邦金", "2882": "國泰金", "2382": "廣達", "2886": "兆豐金",
    "2884": "玉山金", "2885": "元大金", "2412": "中華電", "1303": "南亞",
    "6505": "台塑化", "2002": "中鋼", "1216": "統一", "2891": "中信金",
    "5871": "中租-KY", "2880": "華南金", "3711": "日月光投控", "2892": "第一金",
    "2883": "開發金", "3481": "群創", "5880": "合庫金", "1402": "遠東新",
    "2887": "台新金", "2888": "新光金", "3045": "台灣大", "1605": "華新",
    "4938": "和碩", "1101": "台泥", "9910": "豐泰", "1326": "台化",
    "2889": "國票金", "2890": "永豐金", "2395": "研華", "2303": "聯電",
    "2897": "王道銀行", "1590": "亞德客-KY", "2408": "南亞科", "2603": "長榮",
    "3034": "聯詠", "2379": "瑞昱", "1102": "亞泥", "2609": "陽明",
    "5876": "上海商銀",
    # 可持續擴充…
}

# ---------- 指標 ----------
def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def rsi_wilder(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def bbands(series: pd.Series, length: int = 20, k: float = 2.0):
    basis = series.rolling(length, min_periods=length).mean()
    dev   = series.rolling(length, min_periods=length).std(ddof=0)
    upper = basis + k * dev
    lower = basis - k * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# ---------- FinMind ----------
FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"

def finmind_get(dataset: str, params: dict) -> pd.DataFrame:
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise RuntimeError("環境變數 FINMIND_TOKEN 未設定，請在 GitHub Secrets 建立 FINMIND_TOKEN。")
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(FINMIND_BASE, params={"dataset": dataset, **params}, headers=headers, timeout=30)
    data = resp.json()
    if data.get("status") != 200:
        raise RuntimeError(f"FinMind API 回傳錯誤: {data}")
    return pd.DataFrame(data["data"])

def fetch_tw_stock(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = finmind_get("TaiwanStockPrice", {
        "data_id": ticker, "start_date": start_date, "end_date": end_date
    })
    if df.empty:
        return pd.DataFrame()
    df.rename(columns={
        "date": "Date", "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "Trading_Volume": "Volume"
    }, inplace=True)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# ---------- 指標 + 結構整理 ----------
def add_indicators(df: pd.DataFrame, ticker: str, cfg: dict) -> pd.DataFrame:
    close = df["Close"].astype(float)

    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"]  = basis
    df[f"BB_{bb_len}_Upper"]  = upper
    df[f"BB_{bb_len}_Lower"]  = lower
    df[f"BB_{bb_len}_Width"]  = width

    df.insert(0, "Ticker", ticker)
    df["Name"] = df["Ticker"].astype(str).str.replace(".TW", "", regex=False).map(NAME_MAP).fillna("")

    df.reset_index(inplace=True)

    # 日期轉字串，避免 JSON 序列化錯誤
    if "Date" in df.columns and pd.api.types.is_datetime64_any_dtype(df["Date"]):
        df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    cols = (
        ["Date","Ticker","Name","Open","High","Low","Close","Volume",f"RSI_{rsi_len}"] +
        [f"SMA_{w}" for w in cfg.get("sma_windows", [20, 50, 200])] +
        [f"BB_{bb_len}_Basis", f"BB_{bb_len}_Upper", f"BB_{bb_len}_Lower", f"BB_{bb_len}_Width"]
    )
    return df.loc[:, [c for c in cols if c in df.columns]]

# ---------- 訊號/趨勢 & Top10 ----------
def add_signals(df: pd.DataFrame) -> pd.DataFrame:
    """新增短線/長期標註欄位"""
    df = df.copy()
    # 短線：超賣買進/超買賣出
    df["ShortSignal"] = np.where(
        (df["RSI_14"] < 30) | (df["Close"] < df["BB_20_Lower"]), "Buy",
        np.where((df["RSI_14"] > 70) | (df["Close"] > df["BB_20_Upper"]), "Sell", "")
    )
    # 長期趨勢
    df["LongTrend"] = np.where(
        (df["Close"] > df["SMA_200"]) & (df["SMA_20"] > df["SMA_50"]), "Uptrend",
        np.where((df["Close"] < df["SMA_200"]) & (df["SMA_20"] < df["SMA_50"]), "Downtrend", "Neutral")
    )
    return df

def build_top10(df: pd.DataFrame) -> pd.DataFrame:
    """
    排序邏輯：
      1) 短線 Buy 優先
      2) 長期 Uptrend 次之
      3) RSI 由小到大（越低越先）
    然後取不重複前 10 檔。
    """
    d = add_signals(df)
    d["__buy"] = (d["ShortSignal"] == "Buy").astype(int)
    d["__up"]  = (d["LongTrend"] == "Uptrend").astype(int)
    d = d.sort_values(by=["__buy", "__up", "RSI_14"], ascending=[False, False, True])
    d = d.drop_duplicates(subset=["Ticker"])
    keep_cols = [
        "Date","Ticker","Name","Close","RSI_14","SMA_20","SMA_50","SMA_200",
        "BB_20_Lower","BB_20_Upper","ShortSignal","LongTrend"
    ]
    top10 = d.loc[:, [c for c in keep_cols if c in d.columns]].head(10)
    return top10

# ---------- Google Sheets ----------
def gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

def write_dataframe(ws, df: pd.DataFrame):
    """清空 → A1 寫台灣時間 → A2 寫資料"""
    ws.clear()
    update_time = datetime.now(TZ_TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
    ws.update("A1", [[f"Last Update (Asia/Taipei): {update_time}"]])
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.update("A2", values)

# ---------- 主流程 ----------
def main():
    cfg = load_cfg()
    start_date = cfg.get("start_date")
    end_date   = cfg.get("end_date")

    tickers = cfg.get("tickers", [])
    if not tickers:
        raise RuntimeError("tickers 為空（目前不自動抓ETF成分股），請在 config.json 填入股票代號列表。")

    frames = []
    for t in tickers:
        raw = fetch_tw_stock(t, start_date, end_date)
        if not raw.empty:
            frames.append(add_indicators(raw, t, cfg))

    if not frames:
        raise RuntimeError("沒有任何股票資料被成功抓取，請檢查代號或日期。")

    out = pd.concat(frames, ignore_index=True)

    gc = gspread_client()
    sh = gc.open_by_key(cfg["sheet_id"])

    # 主表
    try:
        ws_main = sh.worksheet(cfg["worksheet"])
    except gspread.WorksheetNotFound:
        ws_main = sh.add_worksheet(title=cfg["worksheet"], rows="2000", cols="60")
    write_dataframe(ws_main, out)

    # Top10
    top10_name = cfg.get("worksheet_top10", "Top10")
    try:
        ws_top10 = sh.worksheet(top10_name)
    except gspread.WorksheetNotFound:
        ws_top10 = sh.add_worksheet(title=top10_name, rows="500", cols="40")
    write_dataframe(ws_top10, build_top10(out))

    print("Top10 代號：", build_top10(out)["Ticker"].tolist())
    print(f"Done. Rows: {len(out)} | Cols: {len(out.columns)}")

if __name__ == "__main__":
    main()
