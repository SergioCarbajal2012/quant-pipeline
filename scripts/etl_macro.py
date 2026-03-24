import os
import json
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timezone
from google.cloud import storage

def configurar_autenticacion_local():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_credenciales = os.path.join(ruta_base, 'gcp_credentials.json')
    if os.path.exists(ruta_credenciales):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales

def extraer_serie_fred(api_key, series_id):
    """Extrae el valor mas reciente de cualquier serie de la FRED."""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=asc&limit=60"
    try:
        response = requests.get(url)
        data = response.json()

        observaciones = data.get('observations', [])
        if not observaciones:
            return None, None

        df_obs = pd.DataFrame(observaciones)
        if df_obs.empty or 'value' not in df_obs.columns or 'date' not in df_obs.columns:
            return None, None

        df_obs['date'] = pd.to_datetime(df_obs['date'], errors='coerce')
        df_obs['value'] = pd.to_numeric(df_obs['value'], errors='coerce')
        df_obs = df_obs[['date', 'value']].dropna(subset=['date'])

        if df_obs.empty:
            return None, None

        # Arrastra el ultimo valor valido para cubrir huecos puntuales de publicacion.
        df_obs['value'] = df_obs['value'].ffill()

        if df_obs['value'].dropna().empty:
            return None, None

        ultima_fila = df_obs.iloc[-1]
        return float(ultima_fila['value']), ultima_fila['date'].date()
    except Exception as e:
        print(f"[ERROR] Fallo al extraer {series_id} de FRED: {e}")
        return None, None

def extraer_yahoo_macro():
    """Extrae VIX y DXY usando Yahoo Finance."""
    tickers = {"vix": "^VIX", "dxy": "DX-Y.NYB"}
    resultados = {}
    fechas = []
    for nombre, ticker in tickers.items():
        try:
            data = yf.Ticker(ticker).history(period="15d")
            if not data.empty:
                data = data.ffill()
                close_series = data['Close'].ffill()
                if close_series.dropna().empty:
                    resultados[nombre] = None
                    continue

                resultados[nombre] = float(close_series.iloc[-1])
                fechas.append(pd.to_datetime(data.index).max().date())
        except:
            resultados[nombre] = None
    return resultados, fechas

def main():
    configurar_autenticacion_local()
    fred_key = os.environ.get("FRED_API_KEY")
    bucket_name = "datalake-quant-451704"

    if not fred_key:
        print("[ERROR] No se encontro FRED_API_KEY en las variables de entorno.")
        return

    # Diccionario de series FRED que queremos monitorear
    series_fred = {
        "ten_year_rate": "DGS10",
        "inflacion_cpi": "CPIAUCSL",
        "desempleo": "UNRATE",
        "fed_balance": "WALCL",
        "expectativa_inflacion": "T10YIE"
    }

    datos_finales = {}
    fechas_observadas = []

    # 1. Extraer datos de FRED
    for nombre_col, series_id in series_fred.items():
        print(f"[INFO] Extrayendo {series_id}...")
        valor, fecha_obs = extraer_serie_fred(fred_key, series_id)
        datos_finales[nombre_col] = valor
        if fecha_obs is not None:
            fechas_observadas.append(fecha_obs)

    # 2. Extraer datos de Yahoo Finance
    print("[INFO] Extrayendo VIX y DXY...")
    yahoo_data, yahoo_fechas = extraer_yahoo_macro()
    datos_finales.update(yahoo_data)
    fechas_observadas.extend(yahoo_fechas)

    fecha_logica_mercado = max(fechas_observadas) if fechas_observadas else (pd.Timestamp.utcnow() - pd.tseries.offsets.BDay(1)).date()
    datos_finales["fecha"] = fecha_logica_mercado.strftime('%Y-%m-%d')
    fecha_str = fecha_logica_mercado.strftime('%Y%m%d')

    # 3. Guardar en Parquet y subir a GCS
    df_macro = pd.DataFrame([datos_finales])
    archivo_temp = f"temp_macro_{fecha_str}.parquet"
    
    try:
        df_macro.to_parquet(archivo_temp, index=False)
        
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(f"macro/bronce/macro_{fecha_str}.parquet")
        blob.upload_from_filename(archivo_temp)
        
        print(f"[EXITO] Datos macro guardados en gs://{bucket_name}/macro/bronce/")
    finally:
        if os.path.exists(archivo_temp):
            os.remove(archivo_temp)

if __name__ == "__main__":
    main()