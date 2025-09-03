# === 內建標的（暫時放這裡；之後可改外部清單或 API）===
TICKERS = ["2330", "2317", "2454"]  # 測試用；要跑完整 TW50 再自行補齊

def _with_tw_suffix(ts):
    return [t if t.endswith(".TW") else f"{t}.TW" for t in ts]

import os, json, datetime as dt
import pandas as pd
import math

# ----------------------
# 基礎設定/防呆/時間
# ----------------------
def _load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    mode = os.getenv("MODE", cfg.get("mode", "dev"))
    cfg["mode"] = "prod" if mode == "prod" else "dev"
    return cfg

def _pick_sheet(cfg, page_key):  # page_key: "tw50" or "top10"
    env = "prod" if cfg["mode"] == "prod" else "dev"
    name = cfg["sheets"][env][page_key]
    # 防呆：dev 禁寫正式；prod 禁寫 _test
    if env == "dev" and name in ("TW50", "Top10"):
        raise RuntimeError("DEV 模式禁止寫入正式分頁")
    if env == "prod" and name.endswith("_test"):
        raise RuntimeError("PROD 模式不應寫入 _test 分頁")
    return name

def _tw_now():
    tz = dt.timezone(dt.timedelta(hours=8))  # Asia/Taipei
    return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def _fmt_range(a, b):
    # 任何一個是 NaN 就回 "-"
    if a is None or b is None or (isinstance(a, float) and math.isnan(a)) or (isinstance(b, float) and math.isnan(b)):
        return "-"
    lo, hi = (a, b) if a <= b else (b, a)
    # 四捨五入到 2 位，去掉多餘小數 0
    def _fmt(x):
        v = round(float(x), 2)
        s = f"{v:.2f}"
        return s.rstrip("0").rstrip(".")
    return f"{_fmt(lo)}~{_fmt(hi)}"

# ----------------------
# 資料抓取與技術指標
# ----------------------
def fetch_prices(tickers, cfg):
    import yfinance as yf
    start = cfg.get("start_date")
    end   = cfg.get("end_date")
    data = []
    for t in tickers:
        df = yf.download(t, start=start, end=end, interval="1d", auto_adjust=False)
        if df.empty:
            continue
        df = df.rename(columns={
            "Open":"開盤", "High":"最高", "Low":"最低", "Close":"收盤", "Volume":"成交量"
        })
        df["代號"] = t.replace(".TW", "")
        df["日期"] = df.index.strftime("%Y-%m-%d")
        data.append(df.reset_index(drop=True))
    return pd.concat(data, ignore_index=True) if data else pd.Dat_
