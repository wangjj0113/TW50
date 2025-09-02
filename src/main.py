import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials


# -----------------------------
# 指標：RSI / SMA / 布林
# -----------------------------
def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    out = 100 - (100 / (1 + rs))
    return out


def sma(close: pd.Series, window: int) -> pd.Series:
    return close.rolling(window=window, min_periods=window).mean()


def bbands(close: pd.Series, window: int = 20, nstd: float = 2.0):
    basis = close.rolling(window=window, min_periods=window).mean()
    stdev = close.rolling(window=window, min_periods=window).std()
    upper = basis + nstd * stdev
    lower = basis - nstd * stdev
    width = upper - lower
    return basis, upper, lower, width


# -----------------------------
# FinMind API 小工具
# -----------------------------
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"


def fm_get(dataset: str, params: dict) -> pd.DataFrame:
    """呼叫 FinMind REST，回傳 DataFrame；無資料則回傳空表。"""
    q = {"dataset": dataset}
    q.update(params)
    r = requests.get(FINMIND_URL, params=q, timeout=30)
    r.raise_for_status()
    j = r.json()
    data = j.get("data", [])
    return pd.DataFrame(data)


def fetch_price(ticker: str, start_date: str, end_date: str, token: str) -> pd.DataFrame:
    df = fm_get(
        "TaiwanStockPrice",
        {"data_id": ticker, "start_date": start_date, "end_date": end_date, "token": token},
    )
    if df.empty:
        return df
    df = df.rename(
        columns={
            "date": "Date",
            "stock_id": "Ticker",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    df["Date"] = pd.to_datetime(df["Date"])
    return df


def fetch_name_map(tickers: list[str], token: str) -> dict:
    """用 TaiwanStockInfo 把代號 → 公司名"""
    name_map = {}
    for t in tickers:
        df = fm_get("TaiwanStockInfo", {"data_id": t, "token": token})
        name = df.iloc[0]["stock_name"] if not df.empty else t
        name_map[t] = name
    return name_map


# -----------------------------
# 技術指標/建議
# -----------------------------
def add_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    close = df["Close"]

    # RSI
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi(close, rsi_len)

    # SMA
    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    # BBands
    bb_len = int(cfg.get("bb_length", 20))   # <- 強制整數，避免 '20.0'
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    # 短線：RSI<30 或 Close<BB下軌 → Buy；RSI>70 或 Close>BB上軌 → Sell；其餘 Neutral
    df["ShortSignal"] = np.where(
        (df[f"RSI_{rsi_len}"] < 30) | (df["Close"] < df[f"BB_{bb_len}_Lower"]),
        "Buy",
        np.where(
            (df[f"RSI_{rsi_len}"] > 70) | (df["Close"] > df[f"BB_{bb_len}_Upper"]),
            "Sell",
            "Neutral",
        ),
    )

    # 長期：在 SMA200 之上且 SMA20 > SMA50 → Uptrend；反之 → Downtrend；其他 Neutral
    if "SMA_20" in df and "SMA_50" in df and "SMA_200" in df:
        df["LongTrend"] = np.where(
            (df["Close"] > df["SMA_200"]) & (df["SMA_20"] > df["SMA_50"]),
            "Uptrend",
            np.where(
                (df["Close"] < df["SMA_200"]) & (df["SMA_20"] < df["SMA_50"]),
                "Downtrend",
                "Neutral",
            ),
        )
    else:
        df["LongTrend"] = "Neutral"

    return df


# -----------------------------
# Google Sheets
# -----------------------------
def get_gspread_client():
    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def write_dataframe(ws, df: pd.DataFrame):
    """清空 → A1 寫台北時間 → A2 寫表頭+資料，確保所有值可序列化"""
    df = df.copy()

    # datetime → 字串
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        else:
            # 保險把 object 裡的 Timestamp 也轉掉
            df[col] = df[col].apply(
                lambda x: x.strftime("%Y-%m-%d") if isinstance(x, (pd.Timestamp, datetime)) else x
            )

    # NaN → ""
    df = df.astype(object).where(pd.notna(df), "")

    # 清空工作表
    ws.clear()

    # A1 寫最後更新（台北時間）
    now_tw = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    ws.update(range_name="A1", values=[[f"Last Update (Asia/Taipei): {now_tw}"]])

    # A2 寫資料
    values = [df.columns.tolist()] + df.values.tolist()
    ws.update(range_name="A2", values=values)


# -----------------------------
# 主流程
# -----------------------------
def main():
    # 讀設定
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    tickers = cfg["tickers"]
    start_date = cfg.get("start_date", "2025-01-01")
    end_date = cfg.get("end_date", "2025-12-31")
    sheet_id = cfg["sheet_id"]
    ws_name = cfg.get("worksheet", "TW50")
    ws_top = cfg.get("worksheet_top10", "Top10")

    token = os.environ.get("FINMIND_TOKEN", cfg.get("finmind_token", ""))

    # 取公司名稱
    name_map = fetch_name_map(tickers, token)

    # 取價格+指標
    all_frames = []
    for t in tickers:
        df = fetch_price(t, start_date, end_date, token)
        if df.empty:
            print(f"[WARN] No data for {t}")
            continue

        df = add_indicators(df, cfg)
        df.insert(1, "Name", df["Ticker"].map(name_map).fillna(df["Ticker"]))
        all_frames.append(df)

    if not all_frames:
        raise RuntimeError("No data fetched for any ticker.")

    result = pd.concat(all_frames, ignore_index=True)
    result.sort_values(["Date", "Ticker"], inplace=True)

    # 產 Top10（用最新日期、RSI 高到低）
    rsi_col = f"RSI_{int(cfg.get('rsi_length', 14))}"
    latest_date = result["Date"].max()
    latest = result[result["Date"] == latest_date].copy()
    # 每檔只留最新一列（保險）
    latest = latest.sort_values(rsi_col, ascending=False).drop_duplicates("Ticker", keep="first")
    top10 = latest.head(10).reset_index(drop=True)

    # 寫入 Google Sheets
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    ws_main = sh.worksheet(ws_name)
    write_dataframe(ws_main, result)

    ws_t10 = sh.worksheet(ws_top)
    write_dataframe(ws_t10, top10)


if __name__ == "__main__":
    main()
