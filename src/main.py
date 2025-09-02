import os
import json
import requests
import pandas as pd
import numpy as np
import pandas_ta as ta
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime


def fetch_data(token, ticker, start_date, end_date):
    """呼叫 FinMind API 下載單一股票日行情。"""
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "token": token,
    }
    res = requests.get(url, params=params)
    data = res.json()
    if data["status"] != 200:
        print(f"Error fetching {ticker}: {data.get('msg')}")
        return pd.DataFrame()
    df = pd.DataFrame(data["data"])
    if df.empty:
        return df
    # 更名欄位
    df = df.rename(
        columns={
            "date": "Date",
            "open": "Open",
            "max": "High",
            "min": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    return df


def calculate_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """計算 RSI、SMA 及布林通道等指標。"""
    close = df["Close"]
    rsi_len = cfg["rsi_length"]
    df[f"F_RSI_{rsi_len}"] = ta.rsi(close, length=rsi_len)
    # 計算多個 SMA
    for w in cfg["sma_windows"]:
        df[f"SMA_{w}"] = ta.sma(close, length=w)
    # 計算布林通道
    bb_len = cfg.get("bb_length", 20)
    bb_std = cfg.get("bb_std", 2)
    bb = ta.bbands(close, length=bb_len, std=bb_std)
    # pandas-ta 產生的名稱格式為 BBM_{length}_0.0 等
    df[f"BB_{bb_len}_Basis"] = bb[f"BBM_{bb_len}_0.0"]
    df[f"BB_{bb_len}_Upper"] = bb[f"BBU_{bb_len}_{bb_std}"]
    df[f"BB_{bb_len}_Lower"] = bb[f"BBL_{bb_len}_{bb_std}"]
    df[f"BB_{bb_len}_Width"] = (
        df[f"BB_{bb_len}_Upper"] - df[f"BB_{bb_len}_Lower"]
    )
    return df


def write_to_sheet(
    sh: gspread.Spreadsheet,
    df_all: pd.DataFrame,
    top10_df: pd.DataFrame,
    update_time: str,
    cfg: dict,
) -> None:
    """寫入主工作表與 Top10 工作表，並添加 Last Update。"""
    # 主工作表
    ws_main = sh.worksheet(cfg["worksheet"])
    values_main = []
    # 第 1 列放 Last Update 與時間
    values_main.append(["Last Update", update_time])
    # 第 2 列放標題
    values_main.append(df_all.columns.tolist())
    # 後續列放資料
    values_main += (
        df_all.astype(object).where(pd.notna(df_all), "")
        .values.tolist()
    )
    ws_main.clear()
    ws_main.update("A1", values_main)

    # Top10 工作表
    ws_top = sh.worksheet(cfg["worksheet_top10"])
    values_top = []
    values_top.append(["Last Update", update_time])
    values_top.append(top10_df.columns.tolist())
    values_top += (
        top10_df.astype(object).where(pd.notna(top10_df), "")
        .values.tolist()
    )
    ws_top.clear()
    ws_top.update("A1", values_top)


def main() -> None:
    # 讀取設定檔
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    token = os.environ.get("FINMIND_TOKEN")
    if not token:
        raise RuntimeError(
            "環境變數 FINMIND_TOKEN 尚未設定，請在 GitHub Secrets 或系統環境設定 API Token。"
        )

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not sa_path:
        raise RuntimeError(
            "環境變數 GOOGLE_APPLICATION_CREDENTIALS 尚未設定，請在工作流程前寫入服務帳號檔案路徑。"
        )

    # 使用 google-auth 建立 gspread 授權
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)

    # 打開 Google Sheet
    sh = gc.open_by_key(cfg["sheet_id"])

    # 下載各個股票資料並計算指標
    all_dfs = []
    for ticker in cfg["tickers"]:
        df = fetch_data(
            token=token,
            ticker=ticker,
            start_date=cfg["start_date"],
            end_date=cfg["end_date"],
        )
        if df.empty:
            continue
        df = calculate_indicators(df, cfg)
        # 加入 Ticker 欄位
        df.insert(0, "Ticker", ticker)
        all_dfs.append(df)

    if not all_dfs:
        raise RuntimeError("所有代號均未取得資料，請檢查設定與 Token。")

    # 合併資料
    df_all = (
        pd.concat(all_dfs, ignore_index=True)
        .sort_values(["Ticker", "Date"])
        .reset_index(drop=True)
    )

    # 取得每支股票最後一日資料
    last_df = (
        df_all.sort_values("Date")
        .groupby("Ticker")
        .tail(1)
        .reset_index(drop=True)
    )

    rsi_len = cfg["rsi_length"]
    # 建立買賣訊號
    conditions = [
        (last_df[f"F_RSI_{rsi_len}"] < 30)
        & (last_df["Close"] > last_df["SMA_20"]),
        (last_df[f"F_RSI_{rsi_len}"] < 50)
        & (last_df["Close"] > last_df["SMA_20"]),
        (last_df[f"F_RSI_{rsi_len}"] > 70)
        & (last_df["Close"] < last_df["SMA_20"]),
    ]
    choices = ["Strong Buy", "Buy", "Sell"]
    last_df["Signal"] = np.select(
        conditions, choices, default="Neutral"
    )
    # 將訊號轉為分數以便排序
    score_map = {"Strong Buy": 3, "Buy": 2, "Neutral": 1, "Sell": 0}
    last_df["Score"] = last_df["Signal"].map(score_map)

    # 下載股票名稱
    info_res = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={"dataset": "TaiwanStockInfo", "token": token},
    )
    if info_res.json().get("status") == 200:
        info_df = pd.DataFrame(info_res.json()["data"])
        name_map = (
            info_df.set_index("stock_id")["stock_name"].to_dict()
        )
        last_df["Name"] = last_df["Ticker"].map(name_map)
    else:
        last_df["Name"] = last_df["Ticker"]

    # 取出前 10 名
    top10_df = (
        last_df.sort_values("Score", ascending=False)
        .head(10)
        .loc[
            :,
            [
                "Ticker",
                "Name",
                "Signal",
                "Score",
                f"F_RSI_{rsi_len}",
                "SMA_20",
                "Close",
            ],
        ]
    ).reset_index(drop=True)

    # 更新時間戳
    update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 寫入工作表
    write_to_sheet(sh, df_all, top10_df, update_time, cfg)


if __name__ == "__main__":
    main()
