# Quant System

An automated quantitative trading system built on multi-source ETL pipelines, cloud-based data storage, and real-time Telegram notifications, targeting financial options on US market assets.

---

## Overview

The system runs a set of daily ETL pipelines that collect options chains, historical prices, macroeconomic indicators, earnings calendars, and news sentiment for the configured assets. All data lands in a Bronze-layer Data Lake on Google Cloud Storage in Parquet format, ready for downstream analytics in BigQuery and ML-based signal generation.

---

## Operative Assets

| Ticker | Type  | ML Model                  |
|--------|-------|---------------------------|
| SPY    | ETF   | `models/xgboost_spy.pkl`  |
| QQQ    | ETF   | `models/xgboost_qqq.pkl`  |
| NVDA   | Stock | `models/xgboost_nvda.pkl` |
| TSLA   | Stock | `models/xgboost_tsla.pkl` |
| AAPL   | Stock | `models/xgboost_aapl.pkl` |

---

## Architecture

```
 Yahoo Finance (yfinance)          FRED API              Hugging Face (FinBERT)
        │                             │                           │
        ├─────────────────────────────┤                           │
        ▼                             ▼                           ▼
 etl_opciones.py            etl_macro.py                etl_sentiment.py
 etl_precios.py             etl_earnings.py
        │                             │                           │
        └──────────────────┬──────────┘───────────────────────────┘
                           ▼
             Google Cloud Storage  ─── Data Lake (Bronze Layer)
               gs://datalake-quant-451704/
               ├── opciones/bronce/{TICKER}_{YYYYMMDD}.parquet
               ├── precios/bronce/{TICKER}_{YYYYMMDD}.parquet
               ├── macro/bronce/macro_{YYYYMMDD}.parquet
               ├── earnings/bronce/earnings_{YYYYMMDD}.parquet
               └── sentimiento/bronce/sentiment_{YYYYMMDD}.parquet
                           │
                           ▼
                    Google BigQuery  ─── Analytics and SQL queries
                           │
                           ▼
                    daily_trader.py  ─── Signal generation (in development)
                           │
                           ▼
               notificar_pipeline.py ─── Telegram pipeline summary
```

---

## Project Structure

```
sistema-quant/
├── config.json                  # Operative assets and model paths
├── requirements.txt             # Python dependencies
├── gcp_credentials.json         # GCP credentials (never commit to version control)
├── models/                      # Trained XGBoost models (.pkl)
│   ├── xgboost_spy.pkl
│   ├── xgboost_qqq.pkl
│   ├── xgboost_nvda.pkl
│   ├── xgboost_tsla.pkl
│   └── xgboost_aapl.pkl
└── scripts/
    ├── etl_opciones.py          # Options chains (calls & puts) → GCS
    ├── etl_precios.py           # Daily OHLCV prices → GCS
    ├── etl_macro.py             # Macro indicators (FRED + Yahoo) → GCS
    ├── etl_earnings.py          # Earnings calendar + EPS estimates → GCS
    ├── etl_sentiment.py         # News sentiment via FinBERT → GCS
    ├── notificar_pipeline.py    # Telegram pipeline completion notification
    └── daily_trader.py          # Daily trader: signal generation (in development)
```

---

## Requirements

- Python 3.9+
- Google Cloud Platform account with the following services enabled:
  - Cloud Storage
  - BigQuery
- Telegram bot configured
- FRED API key (free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html))
- Hugging Face API token (for FinBERT sentiment inference)

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

### 2. Environment variables

All secrets are read from environment variables. Set them before running any pipeline:

```powershell
# Windows (PowerShell)
$env:TELEGRAM_BOT_TOKEN = "your_token_here"
$env:TELEGRAM_CHAT_ID   = "your_chat_id_here"
$env:FRED_API_KEY        = "your_fred_key_here"
$env:HF_API_TOKEN        = "your_huggingface_token_here"
```

```bash
# Linux / macOS
export TELEGRAM_BOT_TOKEN="your_token_here"
export TELEGRAM_CHAT_ID="your_chat_id_here"
export FRED_API_KEY="your_fred_key_here"
export HF_API_TOKEN="your_huggingface_token_here"
```

| Variable            | Required by              | Description                              |
|---------------------|--------------------------|------------------------------------------|
| `TELEGRAM_BOT_TOKEN`| `notificar_pipeline.py`  | Bot token from @BotFather                |
| `TELEGRAM_CHAT_ID`  | `notificar_pipeline.py`  | Target chat/channel ID                   |
| `FRED_API_KEY`      | `etl_macro.py`           | API key for Federal Reserve FRED data    |
| `HF_API_TOKEN`      | `etl_sentiment.py`       | Hugging Face token for FinBERT inference |

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

Run each pipeline independently. All scripts auto-detect credentials from `gcp_credentials.json` in local environments, or use Application Default Credentials in production (e.g., Cloud Run, GCE).

### Options chains

Downloads calls & puts for the next 3 expiration dates per asset:

```bash
python scripts/etl_opciones.py
```

GCS destination: `opciones/bronce/{TICKER}_{YYYYMMDD}.parquet`

---

### Daily prices

Downloads the OHLCV daily candle for each asset:

```bash
python scripts/etl_precios.py
```

GCS destination: `precios/bronce/{TICKER}_{YYYYMMDD}.parquet`

---

### Macroeconomic indicators

Fetches the following series and stores a single daily row:

| Field                  | Source       | Series/Ticker |
|------------------------|--------------|---------------|
| `ten_year_rate`        | FRED         | DGS10         |
| `inflacion_cpi`        | FRED         | CPIAUCSL      |
| `desempleo`            | FRED         | UNRATE        |
| `fed_balance`          | FRED         | WALCL         |
| `expectativa_inflacion`| FRED         | T10YIE        |
| `vix`                  | Yahoo Finance| ^VIX          |
| `dxy`                  | Yahoo Finance| DX-Y.NYB      |

```bash
python scripts/etl_macro.py
```

GCS destination: `macro/bronce/macro_{YYYYMMDD}.parquet`

---

### Earnings calendar

Retrieves the next earnings date and EPS estimate for each asset (ETFs return `null`):

```bash
python scripts/etl_earnings.py
```

GCS destination: `earnings/bronce/earnings_{YYYYMMDD}.parquet`

---

### News sentiment

Pulls up to 10 recent news headlines per asset from Yahoo Finance and scores them using the [FinBERT](https://huggingface.co/ProsusAI/finbert) model via the Hugging Face Inference API. The net sentiment score is `positive_score - negative_score`, averaged across all headlines.

```bash
python scripts/etl_sentiment.py
```

GCS destination: `sentimiento/bronce/sentiment_{YYYYMMDD}.parquet`

---

### Pipeline completion notification

Sends a Telegram message with the total pipeline duration. Pass the ISO 8601 start timestamp via `--inicio`:

```bash
python scripts/notificar_pipeline.py --inicio 2026-03-10T14:00:00Z
```

**Example output (Telegram message):**
```
Pipeline Diario Completado con éxito
Duración total: 3m 42s
Finalizado: 2026-03-10 14:03:42 UTC
```

---

## Data Lake — Medallion Architecture

| Layer  | GCS Path                                           | Description                       |
|--------|----------------------------------------------------|-----------------------------------|
| Bronze | `opciones/bronce/{TICKER}_{YYYYMMDD}.parquet`      | Raw options chain data            |
| Bronze | `precios/bronce/{TICKER}_{YYYYMMDD}.parquet`       | Raw OHLCV daily prices            |
| Bronze | `macro/bronce/macro_{YYYYMMDD}.parquet`            | Raw macro indicators              |
| Bronze | `earnings/bronce/earnings_{YYYYMMDD}.parquet`      | Raw earnings calendar             |
| Bronze | `sentimiento/bronce/sentiment_{YYYYMMDD}.parquet`  | Raw FinBERT sentiment scores      |
| Silver | *(pending)*                                        | Cleaned and normalized data       |
| Gold   | *(pending)*                                        | Aggregated features for ML models |

---

## Dependencies

| Library                 | Purpose                                           |
|-------------------------|---------------------------------------------------|
| `yfinance`              | Options chains, prices, news, earnings data       |
| `pandas`                | Data manipulation and transformation              |
| `pyarrow`               | Parquet format serialization                      |
| `google-cloud-storage`  | File upload to Data Lake (GCS)                    |
| `google-cloud-bigquery` | Data loading and querying in BigQuery             |
| `requests`              | FRED API, Hugging Face API, Telegram API calls    |
| `scipy`                 | Statistical and quantitative calculations         |

---

## Security

- GCP credentials and all API tokens must **never** be hardcoded in the source code.
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
