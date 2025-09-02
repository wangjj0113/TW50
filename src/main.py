import os
import json
import requests
import numpy as np
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo  # << 用台灣時區

# 讀取 config.json
HERE = os.path.dirname(os.path.abspath(__file__))
CFG_PATH = os.path.join(os.path.dirname(HERE), "config.json")

def load_cfg():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# ===== 技術指標 =====
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
    lower =
