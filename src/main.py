# src/main.py
import os, json, time, requests
from typing import List
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ===== 指標 =====
def sma(s: pd.Series, w: int) -> pd.Series:
    return s.rolling(window=w, min_periods=w).mean()

def rsi_wilder(s: pd.Series, n: int = 14) -> pd.Series:
    d = s.diff()
    gain = d.clip(lower=0)
    loss = -d.clip(upper=0)
    ag = gain.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    al = loss.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    rs = ag / al.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def bbands(s: pd.Series, n: int = 20, k: float = 2.0):
    basis = s.rolling(n, min_periods=n).mean()
    dev = s.rolling(n, min_periods=n).std(ddof=0)
    upper = basis + k * dev
    lower = basis - k * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# ===== FinMind =====
def _finmind_get(dataset: str, params: dict) -> pd.DataFrame:
    token = os.environ.get("FINMIND_TOKEN", "")
    if not token:
        raise RuntimeError("FINMIND_TOKEN 未設定（Settings→Secrets→Actions 新增 FINMIND_TOKEN）")
    url = "https://api.finmindtrade.com/api/v4/data"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, params={"dataset": dataset, **params}, headers=headers, timeout=30)
    j = r.json()
    if j.get("status") != 200:
        raise RuntimeError(f"FinMind error: {j}")
    return pd.DataFrame(j.get("data", []))

def fetch_tw_stock(ticker: str, start_date: str, end_date: str, retries: int = 3, pause: float = 0.6) -> pd.DataFrame:
    """逐檔抓價：加入重試&間隔，降低被限流機率。"""
    for i in range(1, retries + 1):
        try:
            df = _finmind_get("TaiwanStockPrice", {
                "data_id": ticker,
                "start_date": start_date,
                "end_date": end_date
            })
            if df.empty:
                return df
            df.rename(columns={
                "date":"Date","open":"Open","high":"High","low":"Low",
                "close":"Close","Trading_Volume":"Volume"
            }, inplace=True)
            df["Date"] = pd.to_datetime(df["Date"])
            return df
        except Exception as e:
            if i == retries:
                print(f"[WARN] {ticker} 抓取失敗（已重試 {retries} 次）：{e}")
                return pd.DataFrame()
            time.sleep(pause)  # 等一下再重試
    return pd.DataFrame()

def get_etf_components(etf_code: str, on_date: str) -> List[str]:
    df = _finmind_get("TaiwanETFComponent", {
        "data_id": etf_code,
        "start_date": on_date,
        "end_date": on_date
    })
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"])
    latest = df["date"].max()
    codes = df.loc[df["date"] == latest, "stock_id"].astype(str).tolist()
    return sorted([c for c in codes if c.isdigit() and len(c) == 4])

# ===== 指標打包 =====
def add_indicators(df: pd.DataFrame, ticker: str, cfg) -> pd.DataFrame:
    close = df["Close"].astype(float)

    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    df.insert(0, "Ticker", ticker)
    df.reset_index(inplace=True)

    cols = ["Date","Ticker","Open","High","Low","Close","Volume", f"RSI_{rsi_len}"] \
        + [f"SMA_{w}" for w in cfg.get("sma_windows", [20, 50, 200])] \
        + [f"BB_{bb_len}_Basis", f"BB_{bb_len}_Upper", f"BB_{bb_len}_Lower", f"BB_{bb_len}_Width"]
    return df.loc[:, [c for c in cols if c in df.columns]]

# ===== Sheets =====
def gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError("找不到 Service Account 憑證（GOOGLE_APPLICATION_CREDENTIALS）")
    creds = Credentials.from_service_account_file(sa_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)

def write_dataframe(ws, df: pd.DataFrame):
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.clear()
    ws.update(values, value_input_option="RAW")

# ===== Main =====
def main():
    cfg = load_cfg()

    # 期間：只抓最近 N 天（避免資料太多）
    end_date = cfg.get("end_date", pd.Timestamp.today().strftime("%Y-%m-%d"))
    lookback_days = int(cfg.get("lookback_days", 90))
    start_date = cfg.get("start_date")
    if not start_date:
        start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    # 股票清單
    if cfg.get("ticker_mode", "list") == "auto_etf":
        tickers = get_etf_components(cfg.get("etf", "0050"), end_date)
        print(f"{cfg.get('etf','0050')} 成分股 {len(tickers)} 檔，抓取區間 {start_date} ~ {end_date}")
    else:
        tickers = cfg.get("tickers", [])
        print(f"手動清單 {len(tickers)} 檔，抓取區間 {start_date} ~ {end_date}")

    frames = []
    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] 抓 {t} ...")
        raw = fetch_tw_stock(t, start_date, end_date)
        if raw.empty:
            print(f"  -> 無資料，跳過")
            continue
        frames.append(add_indicators(raw, t, cfg))
        time.sleep(0.6)  # 節流，避免被限流

    if not frames:
        raise RuntimeError("沒有任何股票資料")

    out = pd.concat(frames, ignore_index=True)

    # 轉字串避免 JSON 序列化錯誤
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")

    gc = gspread_client()
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws = sh.worksheet(cfg["worksheet"])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=cfg["worksheet"], rows="200", cols="26")

    write_dataframe(ws, out)
    print(f"Done. Rows: {len(out)} | Cols: {len(out.columns)}")

if __name__ == "__main__":
    main()
