import os, json
from datetime import datetime
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# 讀 config（可選）
cfg = {}
if os.path.exists("config.json"):
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

# 先吃 Secrets，再退回 config
SHEET_ID = os.getenv("SHEET_ID", cfg.get("sheet_id", "")) or cfg.get("sheet_id", "")
WORKSHEET = cfg.get("worksheet", "TW50_test")

creds_raw = os.getenv("GOOGLE_CREDENTIALS_JSON") or cfg.get("google_credentials_json")
if not creds_raw:
    raise RuntimeError("找不到 Google 憑證：請設定 secrets.GOOGLE_SERVICE_ACCOUNT_JSON 或在 config.json 放 google_credentials_json")

creds_info = json.loads(creds_raw) if isinstance(creds_raw, str) else creds_raw
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
gc = gspread.authorize(creds)

# 股票清單（無則給預設三檔）
tickers = cfg.get("tickers") or ["2330.TW", "2454.TW", "2317.TW"]

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["Close"].astype(float)

    # SMA
    df["SMA_20"]  = close.rolling(20).mean()
    df["SMA_50"]  = close.rolling(50).mean()
    df["SMA_200"] = close.rolling(200).mean()

    # RSI(14) - 穩定版
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.rolling(14, min_periods=14).mean()
    avg_loss = loss.rolling(14, min_periods=14).mean().replace(0, 1e-9)
    rs = avg_gain / avg_loss
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # 布林通道(20,2)
    ma20  = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    df["BB_20_Basis"] = ma20
    df["BB_20_Upper"] = ma20 + 2 * std20
    df["BB_20_Lower"] = ma20 - 2 * std20
    df["BB_20_Width"] = (df["BB_20_Upper"] - df["BB_20_Lower"]) / df["BB_20_Basis"]

    return df

def add_signals(df: pd.DataFrame) -> pd.DataFrame:
    df["LongTrend"] = (df["SMA_20"] > df["SMA_200"]).map({True: "Bullish", False: "Neutral"})
    def _short(r):
        if pd.isna(r["RSI_14"]): return "Neutral"
        if r["RSI_14"] < 30: return "Buy"
        if r["RSI_14"] > 70: return "Sell"
        return "Neutral"
    df["ShortTrend"] = df.apply(_short, axis=1)
    df["EntryZone"]  = df["Close"] < df["BB_20_Lower"]
    df["ExitZone"]   = df["Close"] > df["BB_20_Upper"]
    df["ShortSignal"]= df.apply(lambda r: "Buy" if r["EntryZone"] else ("Sell" if r["ExitZone"] else "Hold"), axis=1)
    return df

def main():
    sheet_id = SHEET_ID or cfg.get("sheet_id", "")
    if not sheet_id:
        raise RuntimeError("缺少 SHEET_ID：請在 repo secrets 設定 SHEET_ID，或在 config.json 內填 sheet_id")

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(WORKSHEET)

    frames = []
    for t in tickers:
        print(f"下載 {t} ...")
        df = yf.download(t, period="6mo", interval="1d", auto_adjust=True)
        if df.empty:
            print(f"{t} 無資料，略過")
            continue
        df = df.reset_index().rename(columns={"Date":"Date"})
        df["Ticker"] = t
        df = add_indicators(df)
        df = add_signals(df)
        frames.append(df)

    if not frames:
        print("沒有抓到任何資料")
        return

    out = pd.concat(frames, ignore_index=True)
    out.insert(0, "Last Update (Asia/Taipei)", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 直接覆蓋整張分頁
    ws.clear()
    ws.update([out.columns.tolist()] + out.fillna("").values.tolist())
    print(f"✅ Updated → {WORKSHEET}")

if __name__ == "__main__":
    main()
