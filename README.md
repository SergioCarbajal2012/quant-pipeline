# Sistema Quant

Sistema de trading cuantitativo automatizado que combina modelos de Machine Learning (XGBoost), pipelines ETL en la nube y notificaciones en tiempo real para operar opciones financieras sobre activos del mercado estadounidense.

---

## Descripcion General

El sistema extrae cadenas de opciones desde Yahoo Finance, las almacena en un Data Lake en Google Cloud Storage y aplica modelos predictivos entrenados sobre los activos configurados. Las alertas y resultados se envian por Telegram.

---

## Activos Operativos

| Ticker | Tipo      | Modelo ML                     |
|--------|-----------|-------------------------------|
| SPY    | ETF       | `models/xgboost_spy.pkl`      |
| QQQ    | ETF       | `models/xgboost_qqq.pkl`      |
| NVDA   | Accion    | `models/xgboost_nvda.pkl`     |
| TSLA   | Accion    | `models/xgboost_tsla.pkl`     |
| AAPL   | Accion    | `models/xgboost_aapl.pkl`     |

---

## Arquitectura

```
Yahoo Finance (yfinance)
        │
        ▼
 etl_opciones.py          ← Extrae cadena de opciones (calls & puts)
        │                    Procesa las 3 proximas expiraciones
        │                    Agrega ticker, fecha_captura
        ▼
Google Cloud Storage      ← Data Lake - Capa Bronce
  gs://datalake-quant-451704/
  └── opciones/bronce/{TICKER}_{YYYYMMDD}.parquet
        │
        ▼
Google BigQuery           ← Analitica y consultas SQL
        │
        ▼
  daily_trader.py         ← Ejecucion diaria: senales y ordenes
        │
        ▼
  Notificaciones Telegram  ← Alertas al finalizar pipeline
```

---

## Estructura del Proyecto

```
sistema-quant/
├── config.json             # Activos operativos y rutas de modelos
├── requirements.txt        # Dependencias Python
├── gcp_credentials.json    # Credenciales GCP (no incluir en control de versiones)
├── models/                 # Modelos XGBoost entrenados (.pkl)
│   ├── xgboost_spy.pkl
│   ├── xgboost_qqq.pkl
│   ├── xgboost_nvda.pkl
│   ├── xgboost_tsla.pkl
│   └── xgboost_aapl.pkl
└── scripts/
    ├── etl_opciones.py     # Pipeline ETL: extraccion y carga al Data Lake
    └── daily_trader.py     # Trader diario: generacion de senales y ejecucion
```

---

## Requisitos

- Python 3.9+
- Cuenta de Google Cloud Platform con los siguientes servicios habilitados:
  - Cloud Storage
  - BigQuery
- Bot de Telegram configurado

### Instalacion de dependencias

```bash
pip install -r requirements.txt
```

---

## Configuracion

### 1. Credenciales GCP

Coloca tu archivo de credenciales de servicio en la raiz del proyecto:

```
sistema-quant/gcp_credentials.json
```

> **Importante:** Este archivo nunca debe subirse a un repositorio publico. Agrega `gcp_credentials.json` a tu `.gitignore`.

### 2. Variables de entorno para Telegram

```bash
# Windows (PowerShell)
$env:TELEGRAM_BOT_TOKEN = "tu_token_aqui"
$env:TELEGRAM_CHAT_ID   = "tu_chat_id_aqui"
```

```bash
# Linux / macOS
export TELEGRAM_BOT_TOKEN="tu_token_aqui"
export TELEGRAM_CHAT_ID="tu_chat_id_aqui"
```

### 3. Configuracion de activos (`config.json`)

Edita `config.json` para agregar o quitar activos y apuntar a los modelos correspondientes:

```json
{
  "activos_operativos": {
    "SPY": { "modelo": "models/xgboost_spy.pkl" }
  }
}
```

---

## Uso

### Ejecutar el pipeline ETL de opciones

Descarga las cadenas de opciones de todos los activos configurados y los sube a la capa Bronce del Data Lake:

```bash
python scripts/etl_opciones.py
```

**Salida esperada:**

```
[INFO] Se encontraron 5 activos en config.json: ['SPY', 'QQQ', 'NVDA', 'TSLA', 'AAPL']
[INFO] INICIANDO EXTRACCION PARA: SPY
[INFO] Descargando datos para SPY desde Yahoo Finance...
       -> Procesando expiracion: 2026-03-20
       -> Procesando expiracion: 2026-03-27
       -> Procesando expiracion: 2026-04-17
[EXITO] Archivo guardado en Data Lake: gs://datalake-quant-451704/opciones/bronce/SPY_20260310.parquet
...
[INFO] Pipeline finalizado exitosamente para todos los activos.
```

### Ejecutar el trader diario

```bash
python scripts/daily_trader.py
```

---

## Data Lake — Arquitectura Medallion

Los datos se organizan siguiendo la arquitectura Medallion:

| Capa     | Ruta GCS                                      | Descripcion                        |
|----------|-----------------------------------------------|------------------------------------|
| Bronce   | `opciones/bronce/{TICKER}_{YYYYMMDD}.parquet` | Datos crudos directamente de origen |
| Plata    | `opciones/plata/...`                          | Datos limpios y normalizados        |
| Oro      | `opciones/oro/...`                            | Agregados y features para modelos   |

---

## Dependencias

| Libreria               | Uso                                       |
|------------------------|-------------------------------------------|
| `yfinance`             | Descarga de datos de mercado y opciones   |
| `pandas`               | Manipulacion y transformacion de datos    |
| `pyarrow`              | Serializacion en formato Parquet          |
| `google-cloud-storage` | Subida de archivos al Data Lake (GCS)     |
| `google-cloud-bigquery`| Carga y consulta de datos en BigQuery     |
| `scipy`                | Calculos estadisticos y cuantitativos     |
| `requests`             | Notificaciones via Telegram API           |

---

## Seguridad

- Las credenciales de GCP y los tokens de Telegram **nunca** deben estar hardcodeados en el codigo.
- Usa variables de entorno o un gestor de secretos (e.g., Google Secret Manager) en produccion.
- Agrega al `.gitignore`:
  ```
  gcp_credentials.json
  *.pkl
  *.parquet
  ```

---

## Licencia

Uso privado. Todos los derechos reservados.
