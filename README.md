# Quant System

An automated quantitative trading system that combines Machine Learning models (XGBoost), cloud-based ETL pipelines, and real-time notifications to trade financial options on US market assets.

---

## Overview

The system extracts options chains from Yahoo Finance, stores them in a Data Lake on Google Cloud Storage, and applies pre-trained predictive models to the configured assets. Alerts and results are delivered via Telegram.

---

## Operative Assets

| Ticker | Type   | ML Model                      |
|--------|--------|-------------------------------|
| SPY    | ETF    | `models/xgboost_spy.pkl`      |
| QQQ    | ETF    | `models/xgboost_qqq.pkl`      |
| NVDA   | Stock  | `models/xgboost_nvda.pkl`     |
| TSLA   | Stock  | `models/xgboost_tsla.pkl`     |
| AAPL   | Stock  | `models/xgboost_aapl.pkl`     |

---

## Architecture

```
Yahoo Finance (yfinance)
        │
        ▼
 etl_opciones.py          ← Extracts options chain (calls & puts)
        │                    Processes the next 3 expiration dates
        │                    Adds ticker and capture_date fields
        ▼
Google Cloud Storage      ← Data Lake - Bronze Layer
  gs://datalake-quant-451704/
  └── opciones/bronce/{TICKER}_{YYYYMMDD}.parquet
        │
        ▼
Google BigQuery           ← Analytics and SQL queries
        │
        ▼
  daily_trader.py         ← Daily execution: signals and orders
        │
        ▼
  Telegram Notifications  ← Alerts on pipeline completion
```

---

## Project Structure

```
sistema-quant/
├── config.json             # Operative assets and model paths
├── requirements.txt        # Python dependencies
├── gcp_credentials.json    # GCP credentials (never commit to version control)
├── models/                 # Trained XGBoost models (.pkl)
│   ├── xgboost_spy.pkl
│   ├── xgboost_qqq.pkl
│   ├── xgboost_nvda.pkl
│   ├── xgboost_tsla.pkl
│   └── xgboost_aapl.pkl
└── scripts/
    ├── etl_opciones.py     # ETL pipeline: extraction and upload to Data Lake
    └── daily_trader.py     # Daily trader: signal generation and execution
```

---

## Requirements

- Python 3.9+
- Google Cloud Platform account with the following services enabled:
  - Cloud Storage
  - BigQuery
- Configured Telegram bot

### Install dependencies

```bash
pip install -r requirements.txt
```

---

## Configuration

### 1. GCP Credentials

Place your service account credentials file at the root of the project:

```
sistema-quant/gcp_credentials.json
```

> **Important:** This file must never be pushed to a public repository. Add `gcp_credentials.json` to your `.gitignore`.

### 2. Telegram environment variables

```bash
# Windows (PowerShell)
$env:TELEGRAM_BOT_TOKEN = "your_token_here"
$env:TELEGRAM_CHAT_ID   = "your_chat_id_here"
```

```bash
# Linux / macOS
export TELEGRAM_BOT_TOKEN="your_token_here"
export TELEGRAM_CHAT_ID="your_chat_id_here"
```

### 3. Asset configuration (`config.json`)

Edit `config.json` to add or remove assets and point to the corresponding models:

```json
{
  "activos_operativos": {
    "SPY": { "modelo": "models/xgboost_spy.pkl" }
  }
}
```

---

## Usage

### Run the options ETL pipeline

Downloads the options chains for all configured assets and uploads them to the Bronze layer of the Data Lake:

```bash
python scripts/etl_opciones.py
```

**Expected output:**

```
[INFO] Found 5 assets in config.json: ['SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL']
[INFO] STARTING EXTRACTION FOR: SPY
[INFO] Downloading data for SPY from Yahoo Finance...
       -> Processing expiration: 2026-03-20
       -> Processing expiration: 2026-03-27
       -> Processing expiration: 2026-04-17
[SUCCESS] File saved to Data Lake: gs://datalake-quant-451704/opciones/bronce/SPY_20260310.parquet
...
[INFO] Pipeline completed successfully for all assets.
```

### Run the daily trader

```bash
python scripts/daily_trader.py
```

---

## Data Lake — Medallion Architecture

Data is organized following the Medallion architecture:

| Layer  | GCS Path                                      | Description                        |
|--------|-----------------------------------------------|------------------------------------|
| Bronze | `opciones/bronce/{TICKER}_{YYYYMMDD}.parquet` | Raw data directly from the source  |
| Silver | `opciones/plata/...`                          | Cleaned and normalized data        |
| Gold   | `opciones/oro/...`                            | Aggregated features for ML models  |

---

## Dependencies

| Library                | Purpose                                      |
|------------------------|----------------------------------------------|
| `yfinance`             | Market data and options chain download       |
| `pandas`               | Data manipulation and transformation         |
| `pyarrow`              | Parquet format serialization                 |
| `google-cloud-storage` | File upload to Data Lake (GCS)               |
| `google-cloud-bigquery`| Data loading and querying in BigQuery        |
| `scipy`                | Statistical and quantitative calculations    |
| `requests`             | Telegram API notifications                   |

---

## Security

- GCP credentials and Telegram tokens must **never** be hardcoded in the source code.
- Use environment variables or a secrets manager (e.g., Google Secret Manager) in production.
- Add the following to your `.gitignore`:
  ```
  gcp_credentials.json
  *.pkl
  *.parquet
  ```

---

## License

Private use. All rights reserved.
