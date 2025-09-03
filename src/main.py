# ====== 這段開始可以直接貼到 src/main.py（覆蓋原本主流程與寫入）======

import os
import json
import io
import gspread
from datetime import datetime, timezone, timedelta
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import pandas as pd

# ---------- 工具：讀 config 與選工作表 ----------
def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def pick_sheet_names(cfg, mode: str):
    """
    cfg["sheets"] 必須像這樣：
    {
      "prod": {"TW50": "TW50", "Top10": "Top10"},
      "dev":  {"TW50": "TW50_test", "Top10": "Top10_test"}
    }
    """
    mode = (mode or "dev").lower()
    tables = cfg["sheets"]["dev" if mode == "dev" else "prod"]
    return tables["TW50"], tables["Top10"]

# ---------- 連線 Google Sheet ----------
def connect_google_sheet(cfg):
    # 支援 secrets 或明文檔（以 secrets 優先）
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        info = json.loads(sa_json)
    else:
        # 若你是放在 repo 內，路徑用 cfg["service_account_file"] 指到 .json
        with open(cfg["service_account_file"], "r", encoding="utf-8") as f:
            info = json.load(f)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet_id"])
    return sh

def get_or_create_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        # 建立空工作表（1000x26 只是初值，之後 set_with_dataframe 會自動調整）
        return sh.add_worksheet(title=title, rows=1000, cols=26)

# ---------- 寫入（安全版：不清空、不寫空） ----------
def safe_write_dataframe(ws, df: pd.DataFrame, note_ts: bool = True):
    if df is None or df.empty:
        print(f"[WARN] DataFrame 是空的，跳過寫入：{ws.title}")
        return

    # 轉成純文字/數值，避免物件型態
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype(str)

    # 直接覆蓋（不 clear）
    set_with_dataframe(ws, df, include_index=False, include_column_header=True)

    # 在 A1 寫 Last Update 註記
    if note_ts:
        tz = timezone(timedelta(hours=8))  # Asia/Taipei
        ts = datetime.now(tz).strftime("Last Update (Asia/Taipei): %Y-%m-%d %H:%M:%S")
        ws.update_acell("A1", ts)

# ---------- Top10 建置（強韌版） ----------
def build_top10(df_tw50: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if df_tw50 is None or df_tw50.empty:
        return pd.DataFrame()

    df = df_tw50.copy()

    # 正規化欄位大小寫與名稱
    cols = {c.lower(): c for c in df.columns}
    # 需要的欄位鍵
    need_map = {}
    for k in ["ticker", "date", "shortsignal", "rsi_14"]:
        # 在 columns 中找對應（忽略大小寫）
        cand = [c for c in df.columns if c.lower() == k]
        if cand:
            need_map[k] = cand[0]
    # 至少要有 ticker 與 date
    if "ticker" not in need_map or "date" not in need_map:
        print("[WARN] 缺少必要欄位（Ticker/Date），回傳空 Top10。")
        return pd.DataFrame()

    # 型別一致化
    df[need_map["ticker"]] = df[need_map["ticker"]].astype(str).str.strip()

    # 取各股票最新一筆
    # 若 Date 是字串，先嘗試 parse
    try:
        df["_dt_"] = pd.to_datetime(df[need_map["date"]], errors="coerce")
    except Exception:
        df["_dt_"] = pd.to_datetime(df[need_map["date"]].astype(str), errors="coerce")
    df = df.dropna(subset=["_dt_"])
    last = df.sort_values("_dt_").groupby(need_map["ticker"], as_index=False).tail(1)

    # 先用 ShortSignal == 'Buy'
    if "shortsignal" in need_map:
        top = last[last[need_map["shortsignal"]].astype(str).str.lower() == "buy"]
        if top.empty:
            # 再用 RSI_14 由大到小
            if "rsi_14" in need_map:
                # 非數值轉 NaN
                top = last.copy()
                top["__rsi__"] = pd.to_numeric(top[need_map["rsi_14"]], errors="coerce")
                top = top.sort_values("__rsi__", ascending=False).head(top_n)
                top = top.drop(columns=["__rsi__"])
            else:
                # 沒有 RSI 就直接取前 N 檔
                top = last.head(top_n)
        else:
            top = top.head(top_n)
    else:
        # 沒 ShortSignal 欄位
        if "rsi_14" in need_map:
            top = last.copy()
            top["__rsi__"] = pd.to_numeric(top[need_map["rsi_14"]], errors="coerce")
            top = top.sort_values("__rsi__", ascending=False).head(top_n)
            top = top.drop(columns=["__rsi__"])
        else:
            top = last.head(top_n)

    # 防呆：如果還是空，至少回傳 last 的前 N 檔，避免整張表為空
    if top.empty:
        print("[WARN] Top10 篩選結果為空，回傳最近一筆的前 N 檔避免空白。")
        top = last.head(top_n)

    # 選幾個常用欄位（存在才保留）
    prefer_cols = ["Date", "Ticker", "Name", "Close", "RSI_14", "SMA_20", "SMA_50", "SMA_200",
                   "BB_20_Lower", "BB_20_Upper", "ShortSignal", "LongTrend"]
    final_cols = [c for c in prefer_cols if c in top.columns]
    if not final_cols:
        final_cols = list(top.columns)
    top = top.loc[:, final_cols]
    return top.reset_index(drop=True)

# ---------- 主流程 ----------
def main():
    mode = (os.getenv("MODE", "dev") or "dev").lower()
    print(f"[INFO] MODE={mode}")

    cfg = load_config("config.json")
    tw_sheet_name, top_sheet_name = pick_sheet_names(cfg, mode)
    print(f"[INFO] 對應工作表：TW50={tw_sheet_name}, Top10={top_sheet_name}")

    # 你前面應該已有：抓價、算指標 -> 產生 df_tw50
    # 這裡假設你有個函式 build_tw50_dataframe(cfg) 回傳 df_tw50
    # df_tw50 = build_tw50_dataframe(cfg)
    # 若你目前是其他名稱，就把它改成 df_tw50
    # ----------------------------
    # !!! 把這行換成你實際產生 TW50 DataFrame 的變數 !!!
    # 例如：df_tw50 = result_df
    # ----------------------------
    raise_if_missing = False  # 防止真的忘記替換
    if raise_if_missing:
        raise RuntimeError("請把 df_tw50 指向你產生的 TW50 DataFrame 變數。")

    # ========= 示範（請用你自己的 df_tw50 替換）=========
    # 下方只是示範空 df（避免誤清表），真實跑要改成你的實際資料
    df_tw50 = pd.DataFrame()
    # ================================================

    # 印出前幾列檢查
    print("[DEBUG] df_tw50 預覽：")
    try:
        print(df_tw50.head())
    except Exception as e:
        print(f"[DEBUG] 無法列印 head(): {e}")

    # 連 Google Sheet
    sh = connect_google_sheet(cfg)
    ws_tw = get_or_create_worksheet(sh, tw_sheet_name)
    ws_top = get_or_create_worksheet(sh, top_sheet_name)

    # 寫 TW50（安全）
    safe_write_dataframe(ws_tw, df_tw50, note_ts=True)

    # 產生 Top10（強韌）
    df_top10 = build_top10(df_tw50, top_n=10)
    print("[DEBUG] df_top10 預覽：")
    try:
        print(df_top10.head())
    except Exception as e:
        print(f"[DEBUG] 無法列印 head(): {e}")

    # 寫 Top10（安全）
    safe_write_dataframe(ws_top, df_top10, note_ts=True)

if __name__ == "__main__":
    main()

# ====== 這段結束 ======
