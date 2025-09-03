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
    return pd.concat(data, ignore_index=True) if data else pd.DataFrame()

def add_indicators(df, cfg):
    if df.empty:
        return df
    df = df.sort_values(["代號","日期"]).copy()

    sma_w = cfg["sma_windows"]
    rsi_n = cfg["rsi_length"]
    bb_n  = cfg["bb_length"]
    bb_k  = 2  # 標準差倍數用 2

    def _group(g: pd.DataFrame):
        # SMA
        g["SMA20"]  = g["收盤"].rolling(sma_w[0]).mean()
        g["SMA50"]  = g["收盤"].rolling(sma_w[1]).mean()
        g["SMA200"] = g["收盤"].rolling(sma_w[2]).mean()
        # RSI (簡化)
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
# Top10（含：建議理由、進/出場區間）
#   規則：優先挑「短線=買入」，依 RSI 由低到高取前 10；
#         若無買入，改取 RSI 最低的前 10 當保底。
#   區間：進場 = [布林下軌 ~ SMA20]；出場 = [SMA200 ~ 布林上軌]
# ----------------------
def _reason_and_ranges(row):
    rsi   = row.get("RSI14")
    close = row.get("收盤")
    s20   = row.get("SMA20")
    s50   = row.get("SMA50")
    s200  = row.get("SMA200")
    bl    = row.get("BB_Low")
    bu    = row.get("BB_Up")
    short = row.get("短線建議")

    # 建議理由（人話）
    if short == "買入":
        if pd.notna(rsi) and rsi < 30:
            reason = "RSI<30：超賣反彈機會"
        elif pd.notna(close) and pd.notna(s20) and close <= s20:
            reason = "回測SMA20：短線支撐附近"
        else:
            reason = "技術面轉強：可分批布局"
    elif short == "賣出":
        if pd.notna(rsi) and rsi > 70:
            reason = "RSI>70：超買風險"
        elif pd.notna(close) and pd.notna(s20) and close < s20:
            reason = "跌破SMA20：短線轉弱"
        else:
            reason = "獲利了結：保守減碼"
    else:
        reason = "訊號不足：觀望"

    # 區間
    entry = _fmt_range(bl, s20) if short == "買入" else "-"
    # 出場區間給目標帶/停利帶：一律用 [SMA200 ~ 布林上軌]
    exit_  = _fmt_range(s200, bu)

    return reason, entry, exit_

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

    # 生成理由與區間
    reasons, entries, exits = [], [], []
    for _, r in pick.iterrows():
        reason, en, ex = _reason_and_ranges(r)
        reasons.append(reason); entries.append(en); exits.append(ex)
    pick["建議理由"] = reasons
    pick["建議進場區間"] = entries
    pick["建議出場區間"] = exits

    cols = ["日期","代號","收盤","RSI14","SMA20","SMA50","SMA200",
            "短線趨勢","長線趨勢","短線建議","長線建議",
            "建議理由","建議進場區間","建議出場區間"]
    return pick.reindex(columns=cols)

# ----------------------
# Google Sheets 輸出（自動建立分頁；先清→寫→最後補 A1）
# ----------------------
def write_to_sheet(df, cfg, page_key):
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import WorksheetNotFound

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("找不到 GOOGLE_CREDENTIALS_JSON 環境變數（請於 GitHub Secrets 設定）")
    creds = Credentials.from_service_account_info(json.loads(creds_json))
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])

    sheet_name = _pick_sheet(cfg, page_key)
    try:
        ws = sh.worksheet(sheet_name)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="2000", cols="40")

    # 先整張清空（避免殘留）
    ws.clear()

    if df.empty:
        ws.update("A2", [["無資料"]])
    else:
        out = df.copy()
        values = [out.columns.tolist()] + out.fillna("").values.tolist()
        ws.update("A2", values, value_input_option="RAW")

    # A1 加上 DEV/PROD 標記
    prefix = "[DEV] " if cfg["mode"] == "dev" else "[PROD] "
    ws.update_acell("A1", f"{prefix}最後更新（台北）：{_tw_now()}")

# ----------------------
# main（強制 dev，保證只寫 _test 分頁；Top10 一定寫）
# ----------------------
def main():
    cfg = _load_cfg()
    cfg["mode"] = "dev"   # ← 強制 dev，永遠只寫 _test

    tickers = _with_tw_suffix(TICKERS)

    # 1) 抓價
    base = fetch_prices(tickers, cfg)
    print(f"[INFO] Fetched tickers: {len(tickers)} rows={len(base)}")
    if base.empty:
        raise RuntimeError("取價失敗或無資料")

    # 2) 指標與中文欄位
    base = add_indicators(base, cfg)

    # 3) 寫 TW50_test
    write_to_sheet(base, cfg, "tw50")

    # 4) 寫 Top10_test（不判斷，直接寫）
    latest_date = base["日期"].max() if not base.empty else "N/A"
    top10 = build_top10(base)
    print(f"[INFO] Latest date={latest_date} | Top10 rows={len(top10)}")
    top10_name = _pick_sheet(cfg, "top10")
    print("MODE =", cfg["mode"], "| Top10 target =", top10_name)
    write_to_sheet(top10, cfg, "top10")

if __name__ == "__main__":
    main()
