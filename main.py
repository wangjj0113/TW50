# -*- coding: utf-8 -*-
# 【台股遠征計畫 v3.0 - 靈活清單版】
# 修正日誌：
# v3.0: 第一階段基礎建設完成！本版本恢復了系統的靈活性。
#       移除硬編碼的股票清單，改回讀取外部的 `taiwan_scan_list.json` 檔案。
#       使用者現在可以透過修改 JSON 檔案，自由定義要分析的股票範圍。

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

# --- 核心設定 ---
TAIPEI_TZ = pytz.timezone('Asia/Taipei')
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data?dataset=TaiwanStockInfo"

# --- 獲取台股基本資料 (已驗證穩定 ) ---
@retry(stop_max_attempt_number=3, wait_fixed=3000)
def get_tw_stock_info():
    print("步驟 1/4: 正在從 FinMind API 獲取台股基本資料...")
    try:
        res = requests.get(FINMIND_API_URL, timeout=30)
        res.raise_for_status()
        data = res.json()
        if data['status'] != 200: raise Exception("FinMind API 回應狀態碼非 200")
        df = pd.DataFrame(data['data'])
        df = df[['stock_id', 'stock_name', 'industry_category']]
        df.rename(columns={'stock_id': '公司代號', 'stock_name': '公司簡稱', 'industry_category': '產業別'}, inplace=True)
        df = df[~df['產業別'].isin(['', '其他'])].dropna()
        stock_info_map = df.set_index('公司代號')
        print(f"✅ 成功整合 {len(stock_info_map)} 家公司基本資料。")
        return stock_info_map
    except Exception as e:
        print(f"❌ 錯誤：從 FinMind API 獲取資料時失敗: {e}，將觸發自動重試...")
        raise

# --- Google Sheets 連線 (已驗證穩定) ---
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def connect_to_google_sheet():
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

# --- 核心分析函數 (已驗證穩定) ---
def analyze_stock(ticker, stock_info_map):
    stock_code = ticker.replace('.TW', '')
    print(f"--- 開始分析 {stock_code} ---")
    try:
        info = stock_info_map.loc[stock_code]
        stock_name = info['公司簡稱']
        industry = info['產業別']
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        if hist.empty: return None
        hist.ta.strategy(ta.Strategy(name="King's Analysis", ta=[{"kind": "sma", "length": 20}, {"kind": "sma", "length": 50}, {"kind": "sma", "length": 200}, {"kind": "rsi"}, {"kind": "bbands", "length": 20, "std": 2.0},]))
        latest_data = hist.iloc[-1]
        report = {"掃描時間(TW)": datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S'), "產業類別": industry, "股票代號": stock_code, "股票名稱": stock_name, "當前股價": latest_data['Close'], "RSI(14)": latest_data['RSI_14'], "SMA(20)": latest_data['SMA_20'], "SMA(50)": latest_data['SMA_50'], "SMA(200)": latest_data['SMA_200'], "布林上軌": latest_data['BBU_20_2.0'], "布林下軌": latest_data['BBL_20_2.0'],}
        print(f"✅ 成功分析 {stock_name}({stock_code})。")
        return report
    except KeyError: return None
    except Exception as e: return None

# --- 主控流程 (v3.0 靈活清單版) ---
def main():
    print("==============================================")
    print(f"【台股遠征計畫 v3.0】啟動於 {datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    print("==============================================")
    try:
        stock_info_map = get_tw_stock_info()
        if stock_info_map is None: return

        # --- 關鍵修改 v3.0：恢復讀取 JSON 檔案 ---
        print("\n步驟 2/4: 正在讀取 'taiwan_scan_list.json'...")
        with open('taiwan_scan_list.json', 'r', encoding='utf-8') as f:
            stock_list_config = json.load(f)
        stock_list = stock_list_config.get("stocks", [])
        # --- 修改結束 ---
        
        if not stock_list:
            print("❌ 錯誤：'taiwan_scan_list.json' 中未找到股票清單或清單為空。")
            return
        print(f"✅ 成功讀取 {len(stock_list)} 支待分析股票。")
        
        all_reports = []
        for stock_code in stock_list:
            ticker = f"{stock_code}.TW"
            report = analyze_stock(ticker, stock_info_map)
            if report: all_reports.append(report)
                
        if not all_reports:
            print("⚠️ 任務完成，但未能成功分析任何股票。")
            return
            
        print(f"\n--- 分析完畢，總共成功生成 {len(all_reports)} 份報告 ---")
        
        spreadsheet = connect_to_google_sheet()
        worksheet_name = f"王者報告_{datetime.now(TAIPEI_TZ).strftime('%Y%m%d')}"
        
        print(f"步驟 4/4: 準備寫入資料至工作表: '{worksheet_name}'...")
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=str(len(all_reports) + 50), cols="30")
            
        df = pd.DataFrame(all_reports)
        column_order = ["掃描時間(TW)", "產業類別", "股票代號", "股票名稱", "當前股價", "RSI(14)", "SMA(20)", "SMA(50)", "SMA(200)", "布林上軌", "布林下軌"]
        df = df[column_order]
        df_to_write = df.astype(str)
        data_to_write = [df_to_write.columns.values.tolist()] + df_to_write.values.tolist()
        
        worksheet.update(data_to_write, range_name='A1')
        print(f"✅ 成功將 {len(df)} 筆數據寫入 '{worksheet_name}'！")
        print("🎉🎉🎉 任務圓滿成功！🎉🎉🎉")

    except Exception as e:
        print(f"❌ 主流程發生致命錯誤: {e}")

if __name__ == "__main__":
    main()
