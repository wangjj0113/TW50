# TA to Google Sheets (No pandas-ta)

穩定版：不依賴 `pandas-ta`，僅用 `pandas + numpy` 計算 SMA(20/50/200)、RSI(14)、布林通道(20, 2σ)，在 GitHub Actions（Ubuntu）直接跑，結果寫入 Google Sheets。

## 使用步驟
1. **建立 Google Service Account** 並將該帳號的 email 加入你的 Google Sheet 編輯權限。
2. 在 GitHub 專案 → Settings → Secrets and variables → Actions → New repository secret：
   - 建立 `GOOGLE_SERVICE_ACCOUNT_JSON`，內容貼上整份 Service Account JSON。
3. 編輯 `config.json`：
   - `sheet_id` 改成你的 Sheet ID。
   - `tickers`（台股記得加 `.TW`）。
4. 手動觸發或等排程：Actions → Workflows → **TA-to-Sheets** → Run workflow。

## 版本釘死
- `numpy==1.26.4`、`pandas==2.2.2` 等，避免與 NumPy 2.x 相容性問題。

## 常見問題
- **Worksheet 不存在**：腳本會自動建立。
- **沒資料**：檢查代號是否正確（台股 `.TW`）、期間/頻率是否支援。
- **權限**：確認已把 Sheet 分享給 Service Account。

