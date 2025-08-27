import os
import json
from datetime import datetime
import pytz
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import pandas_ta as ta
from retrying import retry

# --- 常數設定 ---
TAIWAN_TIMEZONE = pytz.timezone('Asia/Taipei')
TARGET_SHEET_NAME = "King_Report_TW_v1"
SCAN_LIST_FILE = "taiwan_scan_list.json"

# --- Google Sheets 客戶端初始化 ---
def get_gspread_client():
    try:
        creds_json_str = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON']
        creds_info = json.loads(creds_json_str)
        creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"] )
        return gspread.authorize(creds)
    except Exception as e:
        raise Exception(f"初始化 Google Sheets 客戶端時發生錯誤: {e}")

# --- 獲取或創建工作表 ---
def get_or_create_worksheet(client, sheet_id, worksheet_title, headers):
    try:
        spreadsheet = client.open_by_key(sheet_id)
        try:
            worksheet = spreadsheet.worksheet(worksheet_title)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_title, rows="1000", cols="50")
            worksheet.append_row(headers, value_input_option='USER_ENTERED')
        return worksheet
    except Exception as e:
        raise Exception(f"處理 Google Sheet ({worksheet_title}) 時發生錯誤: {e}")

# --- 帶重試機制的數據獲取 ---
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_stock_data_with_retry(ticker_symbol):
    print(f"  正在嘗試獲取 {ticker_symbol} 的數據...")
    stock = yf.Ticker(ticker_symbol)
    # 獲取更長的歷史數據以確保技術指標計算的準確性
    hist = stock.history(period="1y")
    if hist.empty:
        print(f"  警告：無法獲取 {ticker_symbol} 的歷史數據。")
        return None
    return hist

# --- 核心分析引擎 ---
def analyze_stock(ticker_info, hist):
    try:
        # 確保數據足夠計算所有指標
        if len(hist) < 60:
            return {"error": "數據長度不足"}

        # 使用 pandas_ta 計算所有需要的指標
        hist.ta.sma(length=5, append=True)
        hist.ta.sma(length=20, append=True)
        hist.ta.sma(length=60, append=True)
        hist.ta.macd(append=True)
        hist.ta.rsi(append=True)
        hist.ta.bbands(append=True)
        
        # 獲取最新數據
        latest = hist.iloc[-1]
        
        # --- 正股機會評分 ---
        stock_score = 50  # 基準分
        stock_reasons = []

        # 趨勢判斷 (均線)
        if latest['SMA_5'] > latest['SMA_20'] > latest['SMA_60']:
            stock_score += 20
            stock_reasons.append("多頭排列(+20)")
        elif latest['SMA_5'] < latest['SMA_20'] < latest['SMA_60']:
            stock_score -= 20
            stock_reasons.append("空頭排列(-20)")
        
        # 動能判斷 (MACD)
        if latest['MACD_12_26_9'] > latest['MACDs_12_26_9']:
            stock_score += 10
            stock_reasons.append("MACD黃金交叉(+10)")
        else:
            stock_score -= 10
            stock_reasons.append("MACD死亡交叉(-10)")

        # 過熱判斷 (RSI)
        if latest['RSI_14'] > 75:
            stock_score -= 10
            stock_reasons.append("RSI超買(-10)")
        elif latest['RSI_14'] < 25:
            stock_score += 10
            stock_reasons.append("RSI超賣(+10)")

        # --- 選擇權機會評分 (此處為台股簡化版，主要基於波動率) ---
        option_score = 50 # 基準分
        option_reasons = []
        
        # 波動率分析 (布林帶寬度)
        bb_width = (latest['BBU_20_2.0'] - latest['BBL_20_2.0']) / latest['BBM_20_2.0']
        if bb_width > bb_width_history_percentile(hist, 0.75): # 帶寬大於75%分位
            option_score += 20
            option_reasons.append("波動放大(+20)")
        elif bb_width < bb_width_history_percentile(hist, 0.25): # 帶寬小於25%分位
            option_score -= 15
            option_reasons.append("波動收縮(-15)")

        return {
            "ticker": ticker_info['ticker'],
            "name": ticker_info['name'],
            "category": ticker_info['category'],
            "price": latest['Close'],
            "stock_score": int(stock_score),
            "stock_reasons": ", ".join(stock_reasons) or "無",
            "option_score": int(option_score),
            "option_reasons": ", ".join(option_reasons) or "無",
            "sma5": latest['SMA_5'],
            "sma20": latest['SMA_20'],
            "sma60": latest['SMA_60'],
            "rsi": latest['RSI_14'],
            "macd": latest['MACD_12_26_9'],
            "bb_upper": latest['BBU_20_2.0'],
            "bb_lower": latest['BBL_20_2.0'],
        }
    except Exception as e:
        return {"error": f"分析時出錯: {str(e)}"}

def bb_width_history_percentile(hist, percentile):
    """計算歷史布林帶寬度的百分位數"""
    bb_width_hist = (hist['BBU_20_2.0'] - hist['BBL_20_2.0']) / hist['BBM_20_2.0']
    return bb_width_hist.quantile(percentile)

# --- 主函數 ---
def main():
    print("--- 啟動台股「王者分析系統」v1.0 ---")
    try:
        # 讀取掃描列表
        with open(SCAN_LIST_FILE, 'r', encoding='utf-8') as f:
            tickers_info = json.load(f)
        print(f"成功讀取 {len(tickers_info)} 支股票從 {SCAN_LIST_FILE}")

        # 初始化Google Sheets
        client = get_gspread_client()
        sheet_id = os.environ['SHEET_ID']
        
        headers = [
            "掃描時間(TW)", "產業類別", "股票代號", "股票名稱", "當前股價",
            "正股機會分", "評分原因(正股)", "選擇權機會分", "評分原因(選擇權)",
            "5日線", "20日線", "60日線", "RSI(14)", "MACD", "布林上軌", "布林下軌"
        ]
        worksheet = get_or_create_worksheet(client, sheet_id, TARGET_SHEET_NAME, headers)
        worksheet.clear()
        worksheet.append_row(headers, value_input_option='USER_ENTERED')

        all_results = []
        for ticker_info in tickers_info:
            ticker_symbol = ticker_info['ticker']
            print(f"\n正在分析 {ticker_info['name']} ({ticker_symbol})...")
            try:
                hist = fetch_stock_data_with_retry(ticker_symbol)
                if hist is None:
                    print(f"  跳過 {ticker_symbol}，因無法獲取數據。")
                    continue
                
                analysis_result = analyze_stock(ticker_info, hist)

                if "error" in analysis_result:
                    print(f"  分析 {ticker_symbol} 時發生錯誤: {analysis_result['error']}")
                    continue

                new_row = [
                    datetime.now(TAIWAN_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                    analysis_result['category'],
                    analysis_result['ticker'],
                    analysis_result['name'],
                    f"{analysis_result['price']:.2f}",
                    analysis_result['stock_score'],
                    analysis_result['stock_reasons'],
                    analysis_result['option_score'],
                    analysis_result['option_reasons'],
                    f"{analysis_result['sma5']:.2f}",
                    f"{analysis_result['sma20']:.2f}",
                    f"{analysis_result['sma60']:.2f}",
                    f"{analysis_result['rsi']:.2f}",
                    f"{analysis_result['macd']:.2f}",
                    f"{analysis_result['bb_upper']:.2f}",
                    f"{analysis_result['bb_lower']:.2f}",
                ]
                all_results.append(new_row)
                print(f"  {ticker_info['name']} 分析完畢。正股分數: {analysis_result['stock_score']}")

            except Exception as e:
                print(f"  處理 {ticker_symbol} 時發生未預期的嚴重錯誤: {e}")

        if all_results:
            worksheet.append_rows(all_results, value_input_option='USER_ENTERED')
            print(f"\n--- 任務完成！成功分析 {len(all_results)} 支股票 ---")
            print(f"報告已生成在您的 Google Sheet 中，工作表名稱為: '{TARGET_SHEET_NAME}'")
        else:
            print("\n--- 任務完成，但未能成功分析任何股票。請檢查日誌。 ---")

    except FileNotFoundError:
        print(f"錯誤：找不到掃描名單檔案 {SCAN_LIST_FILE}。請確保檔案存在於專案根目錄。")
    except Exception as e:
        print(f"程式執行時發生致命錯誤: {e}")

if __name__ == "__main__":
    main()
