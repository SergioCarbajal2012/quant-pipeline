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

def notificar_telegram(mensaje):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
        try:
            requests.post(url, json=payload)
        except:
            pass

def extraer_serie_fred(api_key, series_id):
    """Extrae el valor mas reciente de cualquier serie de la FRED."""
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&sort_order=desc&limit=1"
    try:
        response = requests.get(url)
        data = response.json()
        # Tomamos el valor y lo convertimos a float
        value = data['observations'][0]['value']
        # La FRED a veces devuelve '.' si el dato no esta disponible aun
        return float(value) if value != '.' else None
    except Exception as e:
        print(f"[ERROR] Fallo al extraer {series_id} de FRED: {e}")
        return None

def extraer_yahoo_macro():
    """Extrae VIX y DXY usando Yahoo Finance."""
    tickers = {"vix": "^VIX", "dxy": "DX-Y.NYB"}
    resultados = {}
    for nombre, ticker in tickers.items():
        try:
            data = yf.Ticker(ticker).history(period="1d")
            if not data.empty:
                resultados[nombre] = data['Close'].iloc[-1]
        except:
            resultados[nombre] = None
    return resultados

def main():
    configurar_autenticacion_local()
    fred_key = os.environ.get("FRED_API_KEY")
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')

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

    datos_finales = {
        "fecha": datetime.now(timezone.utc).strftime('%Y-%m-%d')
    }

    # 1. Extraer datos de FRED
    for nombre_col, series_id in series_fred.items():
        print(f"[INFO] Extrayendo {series_id}...")
        valor = extraer_serie_fred(fred_key, series_id)
        datos_finales[nombre_col] = valor

    # 2. Extraer datos de Yahoo Finance
    print("[INFO] Extrayendo VIX y DXY...")
    yahoo_data = extraer_yahoo_macro()
    datos_finales.update(yahoo_data)

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
        
        notificar_telegram(
            "*Pipeline Macro (Capa Bronce)*\n"
            "Status: COMPLETADO\n"
            f"Variables: {', '.join(datos_finales.keys())}"
        )
    finally:
        if os.path.exists(archivo_temp):
            os.remove(archivo_temp)

if __name__ == "__main__":
    main()