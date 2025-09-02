# src/main.py
import json
import os
from datetime import datetime
import pytz
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from FinMind.finmind import FinMind

# 計算 RSI
def calc_rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# 計算布林通道
def calc_bbands(series: pd.Series, length: int, n_std: float) -> tuple:
    basis = series.rolling(length).mean()
    dev = n_std * series.rolling(length).std()
    upper = basis + dev
    lower = basis - dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# 擷取個股資料並計算指標
def get_stock_data(api: FinMind, cfg: dict, ticker: str) -> pd.DataFrame:
    # 抓取歷史價格
    raw = api.taiwan_stock_price(
        dataset="TaiwanStockPrice",
        data_id=ticker,
        start_date=cfg["start_date"],
        end_date=cfg["end_date"],
    )
    if not raw:
        raise RuntimeError(f"No data fetched for {ticker}")
    df = pd.DataFrame(raw)
    # 計算技術指標
    df["RSI_14"] = calc_rsi(df["close"], cfg["rsi_length"])
    for w in cfg["sma_windows"]:
        df[f"SMA_{w}"] = df["close"].rolling(window=w).mean()
    basis, upper, lower, width = calc_bbands(df["close"], cfg["bb_length"], cfg["bb_std"])
    df[f"BB_{cfg['bb_length']}_Basis"] = basis
    df[f"BB_{cfg['bb_length']}_Upper"] = upper
    df[f"BB_{cfg['bb_length']}_Lower"] = lower
    df[f"BB_{cfg['bb_length']}_Width"] = width
    df["ticker"] = ticker
    return df

# 取得股票代號對應名稱
def get_stock_name(api: FinMind, ticker: str) -> str:
    info = api.taiwan_stock_info(
        dataset="TaiwanStockInfo",
        data_id=ticker
    )
    return info[0]["stock_name"] if info else ticker

# 寫入 Google Sheets
def write_to_sheets(ws, df: pd.DataFrame, title_cell: str, title_text: str):
    # 標題寫在第一列第一欄
    ws.update(title_cell, title_text)
    # 將 dataframe 欄位和資料轉成列表並一次寫入
    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    # 第二列第一欄開始寫入
    ws.update("A2", data)

def main() -> None:
    # 讀取設定檔
    with open("config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    # 初始化 FinMind 客戶端
    api = FinMind()
    api.login_by_token(cfg.get("finmind_token"))
    # 抓取每檔股票資料
    frames = []
    names_map = {}
    for ticker in cfg["tickers"]:
        df = get_stock_data(api, cfg, ticker)
        name = get_stock_name(api, ticker)
        names_map[ticker] = name
        frames.append(df)
    # 合併全部股票資料
    all_df = pd.concat(frames).reset_index(drop=True)
    # 換成公司名稱欄位
    all_df.insert(1, "Name", all_df["ticker"].map(names_map))
    # 依日期排序
    all_df.sort_values(by=["date", "ticker"], inplace=True)
    # 建立推薦欄位：RSI < 30 -> Buy；> 70 -> Sell；其他 -> Hold
    def classify(row):
        rsi = row["RSI_14"]
        if rsi < 30:
            return "Buy"
        if rsi > 70:
            return "Sell"
        return "Hold"
    all_df["Recommend"] = all_df.apply(classify, axis=1)
    # 生成 Top10 清單：以 RSI 由高到低排序取得前10檔最新日期資料
    latest_date = all_df["date"].max()
    latest_df = all_df[all_df["date"] == latest_date]
    top10 = latest_df.nlargest(10, "RSI_14").copy().reset_index(drop=True)
    # 寫入 Google Sheets
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"], scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(cfg["sheet_id"])
    ws_main = spreadsheet.worksheet(cfg["worksheet"])
    ws_top = spreadsheet.worksheet(cfg["worksheet_top10"])
    # 更新工作表資料與更新時間
    now_taipei = datetime.now(pytz.timezone("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    write_to_sheets(
        ws_main,
        all_df,
        "A1",
        f"Last Update (Taipei): {now_taipei}"
    )
    write_to_sheets(
        ws_top,
        top10,
        "A1",
        f"Last Update (Taipei): {now_taipei}"
    )

if __name__ == "__main__":
    main()
