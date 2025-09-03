# -*- coding: utf-8 -*-
"""
TW50 / Top10 自動化（test 版）
- 強制 dev：只寫 *_test 分頁
- 分頁不存在會自動建立
- Top10 會輸出：建議理由 / 建議進場區間 / 建議出場區間
- 全程印出 [DEBUG] 日誌，方便在 GitHub Actions 追蹤
"""

import os, json, math
import datetime as dt
import pandas as pd

# ---------- 公用 ----------

def _tw_now_str():
    tz = dt.timezone(dt.timedelta(hours=8))  # Asia/Taipei
    return dt.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def _fmt_range(a, b):
    def _isnan(x):
        return x is None or (isinstance(x, float) and math.isnan(x))
    if _isnan(a) or _isnan(b):
        return "-"
    lo, hi = (a, b) if a <= b else (b, a)
    def _fmt(x):
        v = round(float(x), 2)
        s = f"{v:.2f}"
        return s.rstrip("0").rstrip(".")
    return f"{_fmt(lo)}~{_fmt(hi)}"

def _with_tw_suffix(ts):
    return [t if t.endswith(".TW") else f"{t}.TW" for t in ts]

# ---------- 設定 / 防呆 ----------

def _load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    # 讀 MODE（環境變數優先），但 test 版最終仍強制 dev
    mode_env = os.getenv("MODE", cfg.get("mode", "dev"))
    cfg["mode"] = "prod" if mode_env == "prod" else "dev"
    return cfg

def _pick_sheet(cfg, page_key):  # "tw50" or "top10"
    env = "prod" if cfg["mode"] == "prod" else "dev"
    name = cfg["sheets"][env][page_key]
    # 防呆：dev 禁寫正式；prod 禁寫 _test
    if env == "dev" and name in ("TW50", "Top10"):
        raise RuntimeError("DEV 模式禁止寫入正式分頁")
    if env == "prod" and name.endswith("_test"):
        raise RuntimeError("PROD 模式不應寫入 _test 分頁")
    return name

# ---------- 抓價 / 指標 ----------

def fetch_prices(tickers, cfg):
    import yfinance as yf
    start = cfg.get("start_date")
    end   = cfg.get("end_date")
    all_rows = []
    for t in tickers:
        print(f"[DEBUG] 下載 {t} ...")
        df = yf.download(t, start=start, end=end, interval="1d", auto_adjust=False)
        if df.empty:
            print(f"[DEBUG] {t} 無資料，略過")
            continue
        df = df.rename(columns={
            "Open":"開盤","High":"最高","Low":"最低","Close":"收盤","Volume":"成交量"
        })
        df["代號"] = t.replace(".TW", "")
        df["日期"] = df.index.strftime("%Y-%m-%d")
        all_rows.append(df.reset_index(drop=True))
    out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    print(f"[DEBUG] 下載完成：{len(out)} 筆")
    return out

def add_indicators(df, cfg):
    if df.empty:
        return df
    df = df.sort_values(["代號","日期"]).copy()

    sma_w = cfg["sma_windows"]   # [20,50,200]
    rsi_n = cfg["rsi_length"]    # 14
    bb_n  = cfg["bb_length"]     # 20
    bb_k  = 2

    def _group(g: pd.DataFrame):
        g = g.sort_values("日期").reset_index(drop=True)

        # SMA —— 加 .values 避免「多欄塞單欄」錯誤
        g["SMA20"]  = g["收盤"].rolling(sma_w[0]).mean().values
        g["SMA50"]  = g["收盤"].rolling(sma_w[1]).mean().values
        g["SMA200"] = g["收盤"].rolling(sma_w[2]).mean().values

        # RSI (簡化)
        delta = g["收盤"].diff()
        up = delta.clip(lower=0).rolling(rsi_n).mean()
        down = (-delta.clip(upper=0)).rolling(rsi_n).mean()
        rs = up / down
        g["RSI14"] = (100 - (100 / (1 + rs))).values

        # 布林
        ma = g["收盤"].rolling(bb_n).mean()
        std = g["收盤"].rolling(bb_n).std()
        g["BB_Mid"]   = ma.values
        g["BB_Up"]    = (ma + bb_k*std).values
        g["BB_Low"]   = (ma - bb_k*std).values
        with pd.option_context('mode.use_inf_as_na', True):
            width = (g["BB_Up"] - g["BB_Low"]) / ma
        g["BB_Width"] = width.values

        # 中文趨勢
        g["短線趨勢"] = ["上升" if a>b else "下降" if a<b else "中立"
                        for a,b in zip(g["SMA20"], g["SMA50"])]
        g["長線趨勢"] = ["上升" if a>b else "下降" if a<b else "中立"
                        for a,b in zip(g["SMA50"], g["SMA200"])]

        # 建議（簡版）
        def _short_suggest(r):
            if pd.isna(r): return "觀望"
            if r < 30: return "買入"
            if r > 70: return "賣出"
            return "觀望"
        g["短線建議"] = [_short_suggest(r) for r in g["RSI14"]]
        g["長線建議"] = ["持有" if (s50 > s200) else "觀望" if pd.notna(s50) and pd.notna(s200) else "中立"
                          for s50, s200 in zip(g["SMA50"], g["SMA200"])]
        return g

    out = df.groupby("代號", group_keys=False).apply(_group)
    print(f"[DEBUG] 指標計算完成：{len(out)} 筆")
    return out

# ---------- Top10（含理由/區間） ----------

def _reason_and_ranges(row):
    rsi   = row.get("RSI14")
    close = row.get("收盤")
    s20   = row.get("SMA20")
    s200  = row.get("SMA200")
    bl    = row.get("BB_Low")
    bu    = row.get("BB_Up")
    short = row.get("短線建議")

    if short == "買入":
        if pd.notna(rsi) and rsi < 30:
            reason = "RSI<30：超賣反彈機會"
        elif pd.notna(close) and pd.notna(s20) and close <= s20:
            reason = "回測SMA20：短線支撐附近"
        else:
            reason = "技術面轉強：可分批布局"
        entry = _fmt_range(bl, s20)
    elif short == "賣出":
        if pd.notna(rsi) and rsi > 70:
            reason = "RSI>70：超買風險"
        elif pd.notna(close) and pd.notna(s20) and close < s20:
            reason = "跌破SMA20：短線轉弱"
        else:
            reason = "獲利了結：保守減碼"
        entry = "-"
    else:
        reason = "訊號不足：觀望"
        entry = "-"

    exit_ = _fmt_range(s200, bu)  # 目標/停利帶
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
    out = pick.reindex(columns=cols)
    print(f"[DEBUG] Top10 準備完成：{len(out)} 列（最新日 {latest_date}）")
    return out

# ---------- Google Sheets 輸出 ----------

def write_to_sheet(df, cfg, page_key):
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.exceptions import WorksheetNotFound

    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("找不到 GOOGLE_CREDENTIALS_JSON（請在 GitHub Secrets 設定）")

    creds = Credentials.from_service_account_info(json.loads(creds_json))
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])

    sheet_name = _pick_sheet(cfg, page_key)
    try:
        ws = sh.worksheet(sheet_name)
    except WorksheetNotFound:
        print(f"[DEBUG] 分頁 {sheet_name} 不存在，建立中 ...")
        ws = sh.add_worksheet(title=sheet_name, rows="2000", cols="40")

    ws.clear()
    if df.empty:
        ws.update("A2", [["無資料"]])
    else:
        vals = [df.columns.tolist()] + df.fillna("").values.tolist()
        ws.update("A2", vals, value_input_option="RAW")

    prefix = "[DEV] " if cfg["mode"] == "dev" else "[PROD] "
    ts = _tw_now_str()
    ws.update_acell("A1", f"{prefix}最後更新（台北）：{ts}")
    print(f"[DEBUG] 已寫入分頁：{sheet_name} | A1={ts}")

# ---------- main ----------

def main():
    try:
        print("[DEBUG] 程式開始")

        cfg = _load_cfg()
        # 強制 dev（test 版保護）
        cfg["mode"] = "dev"

        # 讀取 tickers：優先 config.json，否則用少量測試清單
        cfg_tickers = cfg.get("tickers", [])
        if not cfg_tickers:
            cfg_tickers = ["2330","2317","2454"]
        tickers = _with_tw_suffix(cfg_tickers)

        print(f"[DEBUG] 模式={cfg['mode']} | sheet_id={cfg['sheet_id']}")
        print(f"[DEBUG] 目標分頁 dev.tw50={cfg['sheets']['dev']['tw50']} dev.top10={cfg['sheets']['dev']['top10']}")
        print(f"[DEBUG] 標的數量={len(tickers)} | 前3個={tickers[:3]}")

        # 1) 抓價
        base = fetch_prices(tickers, cfg)
        if base.empty:
            raise RuntimeError("取價失敗或無資料")

        # 2) 指標
        base = add_indicators(base, cfg)

        # 3) 寫 TW50_test
        write_to_sheet(base, cfg, "tw50")

        # 4) 寫 Top10_test
        top10 = build_top10(base)
        write_to_sheet(top10, cfg, "top10")

        print("[DEBUG] 程式結束（全部完成）")

    except Exception as e:
        import traceback
        print("[ERROR]", str(e))
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
