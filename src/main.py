import os, json
from typing import List
import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials

# repo 結構：
#   /config.json   ← 在 repo 根目錄
#   /src/main.py   ← 這隻程式
HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------- 技術指標 ----------
def sma(s: pd.Series, window: int) -> pd.Series:
    return s.rolling(window=window, min_periods=window).mean()

def rsi_wilder(s: pd.Series, length: int = 14) -> pd.Series:
    delta = s.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    avg_loss = loss.ewm(alpha=1/length, adjust=False, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def bbands(s: pd.Series, length: int = 20, k: float = 2.0):
    basis = s.rolling(length, min_periods=length).mean()
    dev = s.rolling(length, min_periods=length).std(ddof=0)
    upper = basis + k * dev
    lower = basis - k * dev
    width = (upper - lower) / basis
    return basis, upper, lower, width

# ---------- 資料抓取 + 指標計算 ----------
def fetch_with_indicators(ticker: str, period: str, interval: str, cfg) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if df.empty:
        return pd.DataFrame()
    df = df.rename(columns=str.title)  # Open High Low Close Volume
    close = df["Close"].astype(float)

    # RSI
    rsi_len = int(cfg.get("rsi_length", 14))
    df[f"RSI_{rsi_len}"] = rsi_wilder(close, rsi_len)

    # SMAs
    for w in cfg.get("sma_windows", [20, 50, 200]):
        df[f"SMA_{w}"] = sma(close, int(w))

    # BBands
    bb_len = int(cfg.get("bb_length", 20))
    bb_std = float(cfg.get("bb_std", 2))
    basis, upper, lower, width = bbands(close, bb_len, bb_std)
    df[f"BB_{bb_len}_Basis"] = basis
    df[f"BB_{bb_len}_Upper"] = upper
    df[f"BB_{bb_len}_Lower"] = lower
    df[f"BB_{bb_len}_Width"] = width

    df.insert(0, "Ticker", ticker)
    df.reset_index(inplace=True)  # 把 Date 變成欄位

    cols = [
        "Date","Ticker","Open","High","Low","Close","Volume",
        f"RSI_{rsi_len}"
    ] + [f"SMA_{w}" for w in cfg.get("sma_windows",]()
