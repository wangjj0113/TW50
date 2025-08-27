# -*- coding: utf-8 -*-
# 【台股遠征計畫 v1.5.2 - 網路強化版】
# 修正日誌：
# v1.5.2: 針對 v1.5.1 的網路超時問題進行修正。
#         1. 為 requests.get 增加明確的 timeout=30 參數，延長等待時間。
#         2. 為整個 get_tw_stock_info 函式增加 @retry 裝飾器，
#            當下載失敗時，會自動重試最多3次，極大提高成功率。

import os
import json
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import gspread
from google.oauth2.service_account import Credentials
import pytz
from datetime import datetime
from retrying import retry
import requests
from io import StringIO

# --- 核心設定 ---
TAIPEI_TZ = pytz.timezone('Asia/Taipei')
TWSE_L_URL = 'https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv'
TWSE_O_URL = 'https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv'

# --- 升級 v1.5.2: 獲取台股基本資料 (增加重試與超時 ) ---
@retry(stop_max_attempt_number=3, wait_fixed=5000) # 失敗後等5秒再重試，最多3次
def get_tw_stock_info():
    print("步驟 1/4: 正在從證交所下載最新的上市櫃公司名單 (網路強化版)...")
    try:
        # 增加 timeout=30，將等待時間延長到30秒
        res_l = requests.get(TWSE_L_URL, timeout=30)
        res_o = requests.get(TWSE_O_URL, timeout=30)
        res_l.raise_for_status()
        res_o.raise_for_status()

        print("...下載成功，正在進行資料清洗...")
        df_l = pd.read_csv(StringIO(res_l.text))
        df_o = pd.read_csv(StringIO(res_o.text))
        df_all = pd.concat([df_l, df_o], ignore_index=True)
        
        df_all = df_all[['公司代號', '公司簡稱', '產業別']]
        df_all.dropna(inplace=True)
        df_all['公司代號'] = df_all['公司代號'].apply(lambda x: str(int(x)) if isinstance(x, float) else str(x))
        df_all = df_all[df_all['公司代號'].str.isdigit()]

        stock_info_map = df_all.set_index('公司代號')
        
        print(f"✅ 成功整合 {len(stock_info_map)} 家上市櫃公司基本資料。")
        return stock_info_map

    except requests.exceptions.Timeout:
        print("❌ 錯誤：下載時發生超時，將觸發自動重試...")
        raise # 必須拋出異常，@retry 才會捕獲並重試
    except Exception as e:
        print(f"❌ 錯誤：下載或處理台股基本資料時失敗: {e}，將觸發自動重試...")
        raise

# --- Google Sheets 連線 (無變動) ---
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def connect_to_google_sheet():
    # ... (程式碼與上一版完全相同) ...
    print("步驟 3/4: 準備初始化 Google Sheets 客戶端...")
    try:
        creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not creds_json: raise ValueError("錯誤：環境變數 GOOGLE_SERVICE_ACCOUNT_JSON 未設定。")
        creds_dict = json.loads(creds_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope )
        gc = gspread.authorize(creds)
        sheet_id = os.getenv('SHEET_ID')
        if not sheet_id: raise ValueError("錯誤：環境變數 SHEET_ID 未設定。")
        spreadsheet = gc.open_by_key(sheet_id)
        print("✅ Google Sheets 連線成功！")
        return spreadsheet
    except Exception as e:
        print(f"❌ 初始化 Google Sheets 客戶端時發生錯誤: {e}")
        raise

# --- 核心分析函數 (無變動) ---
def analyze_stock(ticker, stock_info_map):
    # ... (程式碼與上一版完全相同) ...
    stock_code = ticker.replace('.TW', '')
    print(f"--- 開始分析 {stock_code} ---")
    try:
        info = stock_info_map.loc[stock_code]
        stock_name = info['公司簡稱']
        industry = info['產業別']
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        if hist.empty:
            print(f"警告：無法獲取 {ticker} 的歷史數據。跳過分析。")
            return None
        hist.ta.strategy(ta.Strategy(name="King's Analysis", ta=[{"kind": "sma", "length": 20}, {"kind": "sma", "length": 50}, {"kind": "sma", "length": 200}, {"kind": "rsi"}, {"kind": "bbands", "length": 20, "std": 2.0},]))
        latest_data = hist.iloc[-1]
        report = {"掃描時間(TW)": datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S'), "產業類別": industry, "股票代號": stock_code, "股票名稱": stock_name, "當前股價": latest_data['Close'], "RSI(14)": latest_data['RSI_14'], "SMA(20)": latest_data['SMA_20'], "SMA(50)": latest_data['SMA_50'], "SMA(200)": latest_data['SMA_200'], "布林上軌": latest_data['BBU_20_2.0'], "布林下軌": latest_data['BBL_20_2.0'],}
        print(f"✅ 成功分析 {stock_name}({stock_code})。")
        return report
    except KeyError:
        print(f"警告：在證交所名單中找不到 {stock_code} 的基本資料。可能為ETF或特殊股票，跳過。")
        return None
    except Exception as e:
        print(f"❌ 分析 {ticker} 時發生未知錯誤: {e}")
        return None

# --- 主控流程 (無變動) ---
def main():
    # ... (程式碼與上一版完全相同) ...
    print("==============================================")
    print(f"【台股遠征計畫 v1.5.2】啟動於 {datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    print("==============================================")
    try:
        stock_info_map = get_tw_stock_info()
        if stock_info_map is None:
            print("❌ 在多次重試後，仍無法獲取基本資料，任務終止。")
            return
        print("\n步驟 2/4: 正在讀取 'taiwan_scan_list.json'...")
        with open('taiwan_scan_list.json', 'r', encoding='utf-8') as f:
            stock_list_config = json.load(f)
        stock_list = stock_list_config.get("stocks", [])
        if not stock_list:
            print("❌ 錯誤：'taiwan_scan_list.json' 中未找到股票清單或清單為空。")
            return
        print(f"✅ 成功讀取 {len(stock_list)} 支待分析股票。")
        all_reports = []
        for stock_code in stock_list:
            ticker = f"{stock_code}.TW"
            report = analyze_stock(ticker, stock_info_map)
            if report:
                all_reports.append(report)
        if not all_reports:
            print("⚠️ 任務完成，但未能成功分析任何股票。請檢查日誌中的警告訊息。")
            return
        print(f"\n--- 分析完畢，總共成功生成 {len(all_reports)} 份報告 ---")
        spreadsheet = connect_to_google_sheet()
        worksheet_name = f"王者報告_{datetime.now(TAIPEI_TZ).strftime('%Y%m%d')}"
        print(f"步驟 4/4: 準備寫入資料至工作表: '{worksheet_name}'...")
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            print(f"工作表 '{worksheet_name}' 已存在，將清空並寫入新數據。")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="30")
            print(f"工作表 '{worksheet_name}' 不存在，已成功創建。")
        df = pd.DataFrame(all_reports)
        column_order = ["掃描時間(TW)", "產業類別", "股票代號", "股票名稱", "當前股價", "RSI(14)", "SMA(20)", "SMA(50)", "SMA(200)", "布林上軌", "布林下軌"]
        df = df[column_order]
        data_to_write = [df.columns.values.tolist()] + df.values.tolist()
        worksheet.update(data_to_write, range_name='A1')
        print(f"✅ 成功將 {len(df)} 筆數據寫入 '{worksheet_name}'！")
        print("🎉 任務圓滿成功！")
    except Exception as e:
        print(f"❌ 主流程發生致命錯誤: {e}")

if __name__ == "__main__":
    main()
