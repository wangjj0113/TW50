# === 內建標的（暫時放這裡；之後可改外部清單或 API）===
TICKERS = ["2330", "2317", "2454"]  # 測試用；要跑完整 TW50 再自行補齊

def _with_tw_suffix(ts):
    return [t if t.endswith(".TW") else f"{t}.TW" for t in ts]

import os, json, datetime as dt
import pandas as pd

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
    tz = dt.timezone(dt.timedelta(hours=8))  # Asia/Taipei（簡化）
    return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

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
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def add_indicators(df, cfg):
    if df.empty:
        return df
    df = df.sort_values(["代號","日期"]).copy()

    sma_w = cfg["sma_windows"]
    rsi_n = cfg["rsi_length"]
    bb_n  = cfg["bb_length"]
    bb_k  = 2  # 標準差倍數固定用 2

    def _group(g: pd.DataFrame):
        # SMA
        g["SMA20"]  = g["收盤"].rolling(sma_w[0]).mean()
        g["SMA50"]  = g["收盤"].rolling(sma_w[1]).mean()
        g["SMA200"] = g["收盤"].rolling(sma_w[2]).mean()
        # RSI (簡化版)
        delta = g["收盤"].diff()
        up = delta.clip(lower=0).rolling(rsi_n).mean()
        down = (-delta.clip(upper=0)).rolling(rsi_n).mean()
        rs = up / down
        g["RSI14"] = 100 - (100 / (1 + rs))
        # 布林
        ma = g["收盤"].rolling(bb_n).mean()
        std = g["收盤"].rolling(bb_n).std()
        g["BB_Mid"] = ma
        g["BB_Up"]  = ma + bb_k*std
        g["BB_Low"] = ma - bb_k*std
        g["BB_Width"] = (g["BB_Up"] - g["BB_Low"]) / ma

        # 中文化趨勢
        g["短線趨勢"] = ["上升" if a>b else "下降" if a<b else "中立"
                      for a,b in zip(g["SMA20"], g["SMA50"])]
        g["長線趨勢"] = ["上升" if a>b else "下降" if a<b else "中立"
                      for a,b in zip(g["SMA50"], g["SMA200"])]

        # 建議（簡版：test 驗證用）
        def _short_suggest(r):
            if pd.isna(r): return "觀望"
            if r < 30: return "買入"
            if r > 70: return "賣出"
            return "觀望"
        g["短線建議"] = [ _short_suggest(r) for r in g["RSI14"] ]
        g["長線建議"] = [ "持有" if (s50 > s200) else "觀望" if pd.notna(s50) and pd.notna(s200) else "中立"
                        for s50, s200 in zip(g["SMA50"], g["SMA200"]) ]
        return g

    return df.groupby("代號", group_keys=False).apply(_group)

# ----------------------
# Top10：短線=買入 → RSI 由低到高 → 取前10（無買入時保底取 RSI 最低10 檔）
# ----------------------
def build_top10(df):
    if df.empty:
        return pd.DataFrame()
    latest_date = df["日期"].max()
    latest = df[df["日期"] == latest_date].copy()

    buy = latest[latest["短線建議"] == "買入"].copy()
    if buy.empty:
        pick = latest.sort_values("RSI14", ascending=True).head(10)
    else:
        pick = buy.sort_values("RSI14", ascending=True).head(10)

    cols = ["日期","代號","收盤","RSI14","SMA20","SMA50","SMA200",
            "短線趨勢","長線趨勢","短線建議","長線建議"]
    return pick.reindex(columns=cols)

# ----------------------
# Google Sheets 輸出（先清→寫資料→最後補 A1 時間戳）
# ----------------------
def write_to_sheet(df, cfg, page_key):
    import gspread
    from google.oauth2.service_account import Credentials

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("找不到 GOOGLE_CREDENTIALS_JSON 環境變數（請於 GitHub Secrets 設定）")
    creds = Credentials.from_service_account_info(json.loads(creds_json))
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])

    sheet_name = _pick_sheet(cfg, page_key)
    ws = sh.worksheet(sheet_name)

    # 先整張清空（避免殘留舊資料）
    ws.clear()

    if df.empty:
        ws.update("A2", [["無資料"]])
    else:
        out = df.copy()
        values = [out.columns.tolist()] + out.fillna("").values.tolist()
        ws.update("A2", values, value_input_option="RAW")

    # 最後再寫 A1 時間戳，避免被 clear 蓋掉
    ws.update_acell("A1", f"最後更新（台北）：{_tw_now()}")

# ----------------------
# main（強制 dev，保證只寫 _test 分頁）
# ----------------------
def main():
    cfg = _load_cfg()
    cfg["mode"] = "dev"   # ← 強制 dev，這個版本永遠只寫 _test

    tickers = _with_tw_suffix(TICKERS)

    # 1) 抓價
    base = fetch_prices(tickers, cfg)
    if base.empty:
        raise RuntimeError("取價失敗或無資料")

    # 2) 指標與中文欄位
    base = add_indicators(base, cfg)

    # 3) 寫 TW50_test
    write_to_sheet(base, cfg, "tw50")

    # 4) 寫 Top10_test
    top10 = build_top10(base)
    top10_name = _pick_sheet(cfg, "top10")
    print("MODE =", cfg["mode"], "| Top10 target =", top10_name)
    write_to_sheet(top10, cfg, "top10")

if __name__ == "__main__":
    main()
