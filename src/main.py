import os, json, requests, time, sys
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(HERE, "..", "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(length, min_periods=length).mean()
    ma_down = down.rolling(length, min_periods=length).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def sma(series: pd.Series, length: int = 20) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()

def bbands(series: pd.Series, length: int = 20, std: float = 2.0):
    basis = series.rolling(length).mean()
    dev = series.rolling(length).std(ddof=0)
    upper = basis + std * dev
    lower = basis - std * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

def finmind_get(dataset: str, params: dict, retries: int = 3, pause: float = 1.0) -> pd.DataFrame:
    token = os.environ.get("FINMIND_TOKEN", "")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = "https://api.finmindtrade.com/api/v4/data"
    for i in range(1, retries + 1):
        try:
            r = requests.get(url, params={"dataset": dataset, **params}, headers=headers, timeout=60)
            if r.status_code in (429, 503):  # 節流/暫時不可用
                print(f"[FinMind] {r.status_code}，第 {i}/{retries} 次重試…")
                time.sleep(pause * i)
                continue
            r.raise_for_status()
            j = r.json()
            if j.get("status") != 200:
                print(f"[FinMind] 回傳非 200 狀態：{j}")
                time.sleep(pause * i)
                continue
            return pd.DataFrame(j.get("data", []))
        except Exception as e:
            print(f"[FinMind] 例外：{e}，第 {i}/{retries} 次重試…")
            time.sleep(pause * i)
    return pd.DataFrame()

def fetch_tw_stock(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = finmind_get("TaiwanStockPrice", {
        "data_id": ticker, "start_date": start_date, "end_date": end_date
    })
    if df.empty:
        return df
    df.rename(columns={
        "date":"Date","stock_id":"Ticker","open":"Open","max":"High","min":"Low",
        "close":"Close","Trading_Volume":"Volume"
    }, inplace=True)
    df["Date"] = pd.to_datetime(df["Date"])
    return df

def add_indicators(df: pd.DataFrame, cfg) -> pd.DataFrame:
    if df.empty: return df
    close = df["Close"].astype(float)
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI({rsi_len})"] = rsi(close, rsi_len)
    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA({int(w)})"] = sma(close, int(w))
    bb_len = int(cfg.get("bb_length", 20)); bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width
    return df

def gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS 未設或找不到檔案")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)

def write_dataframe(ws, df: pd.DataFrame):
    values = [df.columns.tolist()] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.clear()
    ws.update(values, value_input_option="RAW")

def main():
    cfg = load_cfg()

    # 時間區間（僅 lookback_days，自動推算）
    end_date = pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d")
    lookback_days = int(cfg.get("lookback_days", 90))
    start_date = (pd.to_datetime(end_date) - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    tickers = cfg.get("tickers", [])
    print(f"[Diag] 時區=Asia/Taipei | 期間 {start_date} ~ {end_date}")
    print(f"[Diag] Tickers={len(tickers)}：{', '.join(tickers[:8])}{' ...' if len(tickers)>8 else ''}")

    frames = []
    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] 抓取 {t} …")
        raw = fetch_tw_stock(t, start_date, end_date)
        if raw.empty:
            print(f"  -> 無資料（可能是假日/更新未到/節流），跳過 {t}")
            continue
        frames.append(add_indicators(raw, cfg))
        time.sleep(0.8)  # 節流更保守

    if not frames:
        print("[Diag] 本次沒有任何有效資料可寫入（不視為失敗）。")
        # 給一個空訊息並正常退出，避免紅叉
        sys.exit(0)

    out = pd.concat(frames, ignore_index=True)
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%Y-%m-%d")

    print(f"[Diag] 合併完成 Rows={len(out)} Cols={len(out.columns)}")

    gc = gspread_client()
    sh = gc.open_by_key(cfg["sheet_id"])
    try:
        ws = sh.worksheet(cfg["sheet_name"])
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=cfg["sheet_name"], rows="200", cols="26")
    write_dataframe(ws, out)
    print(f"✅ 寫入完成 Rows={len(out)} Cols={len(out.columns)}")

if __name__ == "__main__":
    main()
