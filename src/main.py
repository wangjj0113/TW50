import os
import json
import datetime as dt
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# ====================
# 讀取設定檔
# ====================
def _load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    mode = os.getenv("MODE", cfg.get("mode", "dev"))
    cfg["mode"] = "prod" if mode == "prod" else "dev"
    return cfg

# ====================
# 選擇工作表
# ====================
def _pick_sheet(cfg, page_key):
    env = "prod" if cfg["mode"] == "prod" else "dev"
    name = cfg["sheets"][env][page_key]

    # 防呆：DEV 模式禁用正式表；PROD 禁用 test 表
    if env == "dev" and name.lower().endswith("tw50"):
        raise RuntimeError("DEV 模式禁止寫入正式 TW50 表")
    if env == "prod" and name.lower().endswith("test"):
        raise RuntimeError("PROD 模式禁止寫入 test 表")
    return name

# ====================
# 指標計算
# ====================
def add_indicators(df):
    df["RSI_14"] = ta_rsi(df["Close"], 14).reindex(df.index).fillna(0)
    df["SMA_20"] = df["Close"].rolling(20).mean().reindex(df.index).fillna(0)
    df["SMA_50"] = df["Close"].rolling(50).mean().reindex(df.index).fillna(0)
    df["SMA_200"] = df["Close"].rolling(200).mean().reindex(df.index).fillna(0)

    bb = df["Close"].rolling(20)
    df["BB_20_Basis"] = bb.mean().reindex(df.index).fillna(0)
    df["BB_20_Upper"] = (bb.mean() + 2 * bb.std()).reindex(df.index).fillna(0)
    df["BB_20_Lower"] = (bb.mean() - 2 * bb.std()).reindex(df.index).fillna(0)
    df["BB_20_Width"] = (df["BB_20_Upper"] - df["BB_20_Lower"]).reindex(df.index).fillna(0)

    return df

# RSI 計算
def ta_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ====================
# 主程式
# ====================
def main():
    cfg = _load_cfg()
    tickers = cfg["tickers"]

    # 下載資料
    df = yf.download(tickers, period="6mo", interval="1d", group_by="ticker", auto_adjust=True)

    all_data = []
    for t in tickers:
        sub = df[t].copy()
        sub["Ticker"] = t
        sub.reset_index(inplace=True)
        sub = add_indicators(sub)
        all_data.append(sub)

    result = pd.concat(all_data, ignore_index=True)

    # 連線 Google Sheets
    creds = Credentials.from_service_account_info(json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")))
    client = gspread.authorize(creds)

    sheet_name = _pick_sheet(cfg, "tw50")
    sheet = client.open_by_key(cfg["sheet_id"]).worksheet(sheet_name)

    sheet.clear()
    sheet.update([result.columns.values.tolist()] + result.values.tolist())

    print("✅ Data updated to Google Sheets:", sheet_name)

if __name__ == "__main__":
    main()
