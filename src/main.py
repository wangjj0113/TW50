# src/main.py
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials


# -------- Config --------
def load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)


cfg = load_cfg()


# -------- FinMind helpers --------
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
HEADERS = {"Authorization": f"Bearer {cfg.get('finmind_token','')}"}


def fm_get(dataset: str, **params) -> dict:
    p = {"dataset": dataset}
    p.update(params)
    r = requests.get(FINMIND_URL, params=p, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


_name_cache: dict[str, str] = {}


def fetch_stock_name(ticker: str) -> str:
    if ticker in _name_cache:
        return _name_cache[ticker]
    try:
        j = fm_get("TaiwanStockInfo", data_id=str(ticker))
        if j.get("data"):
            name = j["data"][0].get("stock_name") or ""
            _name_cache[ticker] = name
            return name
    except Exception:
        pass
    return ""


def fetch_price_df(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    j = fm_get(
        "TaiwanStockPrice",
        data_id=str(ticker),
        start_date=start_date,
        end_date=end_date,
    )
    data = j.get("data", [])
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)
    # 型別與排序
    for col in ["open", "max", "min", "close", "Trading_Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)
    return df


# -------- TA (SMA, RSI, BBands) --------
def calc_rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1 / length, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = roll_up / roll_down
    rsi = 100 - (100 / (1 + rs))
    return rsi


def calc_indicators(
    df: pd.DataFrame,
    rsi_len: int,
    sma_windows: list[int],
    bb_len: int,
    bb_std: float,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    close = out["close"]

    # RSI
    out[f"RSI_{rsi_len}"] = calc_rsi(close, rsi_len)

    # SMAs
    for w in sma_windows:
        out[f"SMA_{w}"] = close.rolling(w).mean()

    # Bollinger
    basis = close.rolling(bb_len).mean()
    stdev = close.rolling(bb_len).std()
    upper = basis + bb_std * stdev
    lower = basis - bb_std * stdev
    out["BB_20_Basis"] = basis
    out["BB_20_Upper"] = upper
    out["BB_20_Lower"] = lower
    out["BB_20_Width"] = upper - lower

    # ---- 進階欄位（把你表格公式邏輯寫進來）----
    # 長期趨勢：看 Close vs SMA_200（±0.5% 視為 Neutral）
    tol = 0.005
    sma200 = out.get("SMA_200")
    if sma200 is not None:
        diff200 = (close - sma200) / sma200
        cond_up = diff200 > tol
        cond_down = diff200 < -tol
        out["LongTrend"] = pd.Series("Neutral", index=out.index)
        out.loc[cond_up, "LongTrend"] = "Uptrend"
        out.loc[cond_down, "LongTrend"] = "Downtrend"
    else:
        out["LongTrend"] = "Neutral"

    # 短期趨勢：SMA_20 vs SMA_50（±0.5% 視為 Neutral）
    sma20 = out.get("SMA_20")
    sma50 = out.get("SMA_50")
    if sma20 is not None and sma50 is not None:
        diff_short = (sma20 - sma50) / sma50
        cond_up = diff_short > tol
        cond_down = diff_short < -tol
        out["ShortTrend"] = pd.Series("Neutral", index=out.index)
        out.loc[cond_up, "ShortTrend"] = "Uptrend"
        out.loc[cond_down, "ShortTrend"] = "Downtrend"
    else:
        out["ShortTrend"] = "Neutral"

    # 進場/出場區間
    # 進場：BB_20_Lower ~ SMA_50
    out["EntryZone"] = False
    if "BB_20_Lower" in out and "SMA_50" in out:
        out["EntryZone"] = (close >= out["BB_20_Lower"]) & (close <= out["SMA_50"])

    # 出場：SMA_200 ~ BB_20_Upper
    out["ExitZone"] = False
    if "BB_20_Upper" in out and "SMA_200" in out:
        out["ExitZone"] = (close >= out["SMA_200"]) & (close <= out["BB_20_Upper"])

    # 短線訊號：RSI/BB 上下軌
    rsi_col = f"RSI_{rsi_len}"
    out["ShortSignal"] = "Hold"
    if rsi_col in out:
        overbought = (out[rsi_col] > 70) | (close > out["BB_20_Upper"])
        oversold = (out[rsi_col] < 30) | (close < out["BB_20_Lower"])
        out.loc[oversold, "ShortSignal"] = "Buy"
        out.loc[overbought, "ShortSignal"] = "Sell"

    # 把訊號轉成人話（你手機截圖那種）
    out["短線建議"] = out["ShortSignal"].map(
        {"Buy": "短線：買入", "Sell": "短線：賣出", "Hold": "短線：觀望"}
    )

    # 長線建議：綜合 LongTrend + RSI
    def long_advice(row):
        lt = row["LongTrend"]
        rsi = row.get(rsi_col, pd.NA)
        try:
            rsi = float(rsi)
        except Exception:
            return "長線：中立"
        if lt == "Uptrend" and 30 <= rsi <= 70:
            return "長線：持有"
        if lt == "Downtrend" and rsi < 30:
            return "長線：觀望/分批"
        if lt == "Downtrend" and rsi > 70:
            return "長線：迴避"
        return "長線：中立"

    out["長線建議"] = out.apply(long_advice, axis=1)

    return out


# -------- Google Sheets --------
def gsheet_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    import os

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    return gspread.authorize(creds)


def write_dataframe(ws, df: pd.DataFrame):
    """把 df 寫到 sheet，從 A2 起，A1 留給 Last Update（台灣時間）。"""
    if df.empty:
        print("⚠️ No data to write.")
        return

    rsi_len = int(cfg.get("rsi_length", 14))

    cols = (
        [
            "Date",
            "Ticker",
            "Name",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            f"RSI_{rsi_len}",
            "SMA_20",
            "SMA_50",
            "SMA_200",
            "BB_20_Basis",
            "BB_20_Upper",
            "BB_20_Lower",
            "BB_20_Width",
            "LongTrend",
            "ShortTrend",
            "EntryZone",
            "ExitZone",
            "ShortSignal",
            "短線建議",
            "長線建議",
        ]
    )

    # 僅保留真的存在的欄位
    cols = [c for c in cols if c in df.columns]

    # 先清掉 A2 以下，避免殘影
    ws.batch_clear(["A2:Z999999"])

    values = [cols] + df.loc[:, cols].astype(object).where(pd.notna(df), "").values.tolist()
    ws.update("A2", values, value_input_option="RAW")

    # 台灣時間時間戳
    tw_now = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    ws.update("A1", [[f"Last Update (Asia/Taipei): {tw_now}"]])


# -------- Main --------
def main():
    tickers = cfg["tickers"]
    start_date = cfg["start_date"]
    end_date = cfg["end_date"]

    frames = []
    for t in tickers:
        df = fetch_price_df(t, start_date, end_date)
        if df.empty:
            print(f"⚠️ No data for {t}")
            continue
        name = fetch_stock_name(t)
        df = calc_indicators(
            df,
            rsi_len=int(cfg.get("rsi_length", 14)),
            sma_windows=list(cfg.get("sma_windows", [20, 50, 200])),
            bb_len=int(cfg.get("bb_length", 20)),
            bb_std=float(cfg.get("bb_std", 2)),
        )
        # 欄位整理
        df.rename(
            columns={
                "date": "Date",
                "open": "Open",
                "max": "High",
                "min": "Low",
                "close": "Close",
                "Trading_Volume": "Volume",
            },
            inplace=True,
        )
        df.insert(1, "Ticker", t)
        df.insert(2, "Name", name)
        frames.append(df)

    if not frames:
        raise RuntimeError("No data fetched for any ticker.")

    final_df = pd.concat(frames, ignore_index=True)

    client = gsheet_client()
    sh = client.open_by_key(cfg["sheet_id"])
    ws = sh.worksheet(cfg["worksheet"])
    write_dataframe(ws, final_df)
    print("✅ Done.")


if __name__ == "__main__":
    main()
