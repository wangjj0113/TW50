# -*- coding: utf-8 -*-
"""
TW50 分組 + Hot20 + Top5_hot20(含Signal) 主程式
輸出分頁：
  1) TW50_fin       金融股完整表（存股參考）
  2) TW50_nonfin    非金融完整表（操作池）
  3) Top10_nonfin   非金融 Top10（RSI↓, Volume↓）
  4) Hot20_nonfin   非金融「最新一筆成交量前20」快照
  5) Top5_hot20     Hot20內再篩前5（含 Signal=Buy/Sell/Neutral）
Notes:
  - 需要 GitHub Secrets: GCP_SERVICE_ACCOUNT_JSON（完整 JSON）
  - 每個分頁 A1 會寫入台北時區更新時間
"""

import os
import json
from typing import Dict, List

import numpy as np
import pandas as pd
import yfinance as yf
import gspread
from gspread_dataframe import set_with_dataframe


# ========= 金融股名單（0050常見金融） =========
FIN_TICKERS = {
    "2880.TW","2881.TW","2882.TW","2883.TW","2884.TW","2885.TW",
    "2886.TW","2887.TW","2888.TW","2889.TW","2890.TW","2891.TW",
    "2892.TW","2897.TW","2898.TW","2899.TW","5871.TW","5876.TW"
}

# ========= 股票名稱對照（可自行擴充） =========
TICKER_NAME_MAP = {
    "2330.TW": "台積電","2317.TW": "鴻海","6505.TW": "台塑化","2454.TW": "聯發科",
    "2412.TW": "中華電","2881.TW": "富邦金","2882.TW": "國泰金","2308.TW": "台達電",
    "2002.TW": "中鋼","2303.TW": "聯電","1303.TW": "南亞","1326.TW": "台化",
    "2886.TW": "兆豐金","2884.TW": "玉山金","2885.TW": "元大金","2891.TW": "中信金",
    "2880.TW": "華南金","2883.TW": "開發金","2887.TW": "台新金","2888.TW": "新光金",
    "2892.TW": "第一金","2890.TW": "永豐金","5871.TW": "中租-KY","1216.TW": "統一",
    "1101.TW": "台泥","1102.TW": "亞泥","9904.TW": "寶成","2889.TW": "國票金",
    "2897.TW": "王道銀行","3008.TW": "大立光","3045.TW": "台灣大","4904.TW": "遠傳",
    "3711.TW": "日月光投控","2899.TW": "永豐金控","5876.TW": "上海商銀","9910.TW": "豐泰",
    "2603.TW": "長榮","2609.TW": "陽明","2615.TW": "萬海","2633.TW": "台灣高鐵",
    "2898.TW": "安泰銀","1402.TW": "遠東新","1590.TW": "亞德客-KY","2379.TW": "瑞昱",
    "2382.TW": "廣達","2395.TW": "研華","2408.TW": "南亞科","3006.TW": "晶豪科","3481.TW": "群創"
}


# ========= 基本設定 =========
def load_config(cfg_path: str = "config.json") -> Dict:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("period", "12mo")
    cfg.setdefault("interval", "1d")
    return cfg

def taipei_now_str() -> str:
    return pd.Timestamp.now(tz="Asia/Taipei").strftime("%Y-%m-%d %H:%M")


# ========= 抓價 + 指標 =========
def fetch_history(ticker: str, period: str, interval: str) -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        level1_vals = list(df.columns.levels[1])
        df = df.xs(level1_vals[0], axis=1, level=1)
    df = df.rename(columns=str.title)
    df.index.name = "Date"
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # 均線
    out["SMA
