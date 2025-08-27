# -*- coding: utf-8 -*-
# 【台股遠征計畫 v1.3 - 王者報告・最終修正版】
# 修正日誌：
# v1.1: 初始版本
# v1.2: 修正了因 yfinance 數據格式變動，導致 pandas-ta 計算布林通道 (BBU) 等指標時出錯的問題。
#       透過在獲取數據後，明確地將索引轉換為有時區的 datetime 物件，確保數據格式的兼容性。
# v1.3: 增加更詳細的日誌輸出，並優化寫入邏輯。

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

# --- 核心設定 ---
# 台灣時區
TAIPEI_TZ = pytz.timezone('Asia/Taipei')

# --- Google Sheets 連線 (帶重試機制) ---
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def connect_to_google_sheet():
    print("準備初始化 Google Sheets 客戶端...")
    try:
        creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if not creds_json:
            raise ValueError("錯誤：環境變數 GOOGLE_SERVICE_ACCOUNT_JSON 未設定。")
        
        creds_dict = json.loads(creds_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_dict, scopes=scope )
        gc = gspread.authorize(creds)
        
        sheet_id = os.getenv('SHEET_ID')
        if not sheet_id:
            raise ValueError("錯誤：環境變數 SHEET_ID 未設定。")
            
        spreadsheet = gc.open_by_key(sheet_id)
        print("Google Sheets 連線成功！")
        return spreadsheet
    except Exception as e:
        print(f"初始化 Google Sheets 客戶端時發生錯誤: {e}")
        raise

# --- 核心分析函數 ---
def analyze_stock(ticker):
    print(f"--- 開始分析 {ticker} ---")
    try:
        stock = yf.Ticker(ticker)
        # 獲取足夠長的歷史數據以計算200MA
        hist = stock.history(period="1y")

        if hist.empty:
            print(f"警告：無法獲取 {ticker} 的歷史數據。跳過分析。")
            return None

        # --- 關鍵修正 v1.2 ---
        # 確保索引是帶有時區的 datetime 物件，以兼容 pandas-ta
        hist.index = pd.to_datetime(hist.index).tz_convert(TAIPEI_TZ)
        
        # 計算技術指標
        custom_strategy = ta.Strategy(
            name="King's Analysis",
            description="綜合技術指標分析",
            ta=[
                {"kind": "sma", "length": 20},
                {"kind": "sma", "length": 50},
                {"kind": "sma", "length": 200},
                {"kind": "rsi"},
                {"kind": "bbands", "length": 20, "std": 2.0},
            ]
        )
        hist.ta.strategy(custom_strategy)

        # 獲取最新一天（最後一行）的數據
        latest_data = hist.iloc[-1]

        # 準備報告字典
        report = {
            "股票代號": ticker.replace('.TW', ''),
            "分析時間": datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            "當前價格": latest_data['Close'],
            "RSI(14)": latest_data['RSI_14'],
            "SMA(20)": latest_data['SMA_20'],
            "SMA(50)": latest_data['SMA_50'],
            "SMA(200)": latest_data['SMA_200'],
            "布林上軌": latest_data['BBU_20_2.0'],
            "布林下軌": latest_data['BBL_20_2.0'],
        }
        
        # 價格位置判斷
        price_position = []
        if latest_data['Close'] > latest_data['SMA_200']: price_position.append("高於年線")
        if latest_data['Close'] < latest_data['SMA_200']: price_position.append("低於年線")
        if latest_data['Close'] > latest_data['SMA_50']: price_position.append("高於季線")
        if latest_data['Close'] < latest_data['SMA_50']: price_position.append("低於季線")
        if latest_data['Close'] > latest_data['SMA_20']: price_position.append("高於月線")
        if latest_data['Close'] < latest_data['SMA_20']: price_position.append("低於月線")
        report["價格位置"] = "、".join(price_position) if price_position else "均線糾結"

        # RSI 狀態判斷
        rsi_status = "中性"
        if latest_data['RSI_14'] > 70: rsi_status = "超買"
        if latest_data['RSI_14'] < 30: rsi_status = "超賣"
        report["RSI狀態"] = rsi_status
        
        print(f"成功分析 {ticker}。當前價格: {report['當前價格']:.2f}")
        return report

    except Exception as e:
        print(f"分析 {ticker} 時發生未知錯誤: {e}")
        return None

# --- 主控流程 ---
def main():
    print("==============================================")
    print(f"【台股王者報告 v1.3】啟動於 {datetime.now(TAIPEI_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    print("==============================================")

    try:
        with open('taiwan_scan_list.json', 'r', encoding='utf-8') as f:
            stock_list_config = json.load(f)
        
        stock_list = stock_list_config.get("stocks", [])
        if not stock_list:
            print("錯誤：'taiwan_scan_list.json' 中未找到股票清單或清單為空。")
            return
            
        print(f"成功讀取 {len(stock_list)} 支股票清單。")

        all_reports = []
        for stock_code in stock_list:
            ticker = f"{stock_code}.TW"
            report = analyze_stock(ticker)
            if report:
                all_reports.append(report)
        
        if not all_reports:
            print("任務完成，但未能成功分析任何股票。請檢查日誌中的警告訊息。")
            return

        print(f"\n--- 分析完畢，總共成功生成 {len(all_reports)} 份報告 ---")
        
        # 連線到 Google Sheet 並寫入數據
        spreadsheet = connect_to_google_sheet()
        
        # 決定工作表名稱
        worksheet_name = f"王者報告_{datetime.now(TAIPEI_TZ).strftime('%Y%m%d')}"
        
        print(f"準備寫入資料至工作表: '{worksheet_name}'...")
        
        # 檢查工作表是否存在，不存在則創建
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            print(f"工作表 '{worksheet_name}' 已存在，將清空並寫入新數據。")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="100", cols="20")
            print(f"工作表 '{worksheet_name}' 不存在，已成功創建。")

        # 將報告轉換為 DataFrame 並寫入
        df = pd.DataFrame(all_reports)
        # 調整欄位順序
        column_order = ["股票代號", "當前價格", "RSI(14)", "RSI狀態", "價格位置", "布林上軌", "布林下軌", "SMA(20)", "SMA(50)", "SMA(200)", "分析時間"]
        df = df[column_order]
        
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"成功將 {len(df)} 筆數據寫入 '{worksheet_name}'！")
        print("任務圓滿成功！")

    except Exception as e:
        print(f"主流程發生致命錯誤: {e}")

if __name__ == "__main__":
    main()
