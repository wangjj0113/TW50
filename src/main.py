import os
import io
import json
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials


def tw_now(tz="Asia/Taipei"):
    # 簡單處理台北時間（不裝第三方套件）
    utc_now = datetime.now(timezone.utc)
    taipei = timezone(timedelta(hours=8))
    return utc_now.astimezone(taipei).strftime("%Y-%m-%d %H:%M:%S")


def load_cfg():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    # 驗證必要欄位
    missing = []
    for k in ["sheet_id", "service_account_env_key", "sheets"]:
        if not cfg.get(k):
            missing.append(k)
    if missing:
        raise RuntimeError(f"config.json 缺少欄位: {', '.join(missing)}")

    mode = os.getenv("MODE", cfg.get("mode", "dev"))
    if mode not in cfg["sheets"]:
        raise RuntimeError(f"MODE={mode} 無對應 sheets 設定，請檢查 config.json")

    cfg["mode"] = mode
    return cfg


def connect_google_sheet(cfg):
    env_key = cfg["service_account_env_key"]
    sa_json_raw = os.getenv(env_key)
    if not sa_json_raw:
        raise RuntimeError(
            f"GitHub Secrets 未設定 {env_key}，請到 Settings → Secrets and variables → Actions 新增"
        )

    try:
        sa_info = json.loads(sa_json_raw)
    except json.JSONDecodeError:
        raise RuntimeError(f"{env_key} 不是有效的 JSON 文字，請確認 Secrets 內容")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(sa_info, scopes=scopes)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(cfg["sheet_id"])
    return sh


def ensure_worksheet(sh, title):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        # 沒有就自動建立
        return sh.add_worksheet(title=title, rows=2000, cols=30)


def write_timestamp(ws, label="Last Update"):
    ws.update("A1", [[label + " (Asia/Taipei):", tw_now()]])


def write_tickers(ws, tickers):
    # 標題列
    ws.update("A3", [["Ticker"]])
    # 清單
    if tickers:
        values = [[t] for t in tickers]
        ws.update(f"A4:A{3+len(values)}", values)


def main():
    print("[INFO] MODE=", os.getenv("MODE", "dev"))
    cfg = load_cfg()
    env = cfg["mode"]
    print(f"[INFO] 當前目標表: TW50={cfg['sheets'][env]['tw50']}, Top10={cfg['sheets'][env]['top10']}")
    print(f"[INFO] 將寫入的 tickers: {cfg.get('tickers', [])}")

    sh = connect_google_sheet(cfg)

    # 取得分頁
    tw50_ws = ensure_worksheet(sh, cfg["sheets"][env]["tw50"])
    top10_ws = ensure_worksheet(sh, cfg["sheets"][env]["top10"])

    # 寫入時間戳與 tickers（驗證用）
    write_timestamp(tw50_ws)
    write_tickers(tw50_ws, cfg.get("tickers", []))

    write_timestamp(top10_ws)
    write_tickers(top10_ws, cfg.get("tickers", []))

    print("[OK] 已成功寫入 TW50 與 Top10 分頁（時間戳與 Tickers）。")


if __name__ == "__main__":
    main()
