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
        print(f"[INFO] Credenciales locales cargadas desde: {ruta_credenciales}")
    else:
        print("[WARN] No se encontro gcp_credentials.json local. Asumiendo entorno de produccion.")

def cargar_configuracion():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_config = os.path.join(ruta_base, 'config.json')

    try:
        with open(ruta_config, 'r') as archivo:
            return json.load(archivo)
    except FileNotFoundError:
        print("[ERROR] No se encontro config.json")
        return None

def notificar_telegram(mensaje):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("[WARN] Credenciales de Telegram no encontradas. Saltando notificacion.")
        return
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": mensaje,
        "parse_mode": "Markdown"
    }
    
    try:
        respuesta = requests.post(url, json=payload)
        respuesta.raise_for_status()
        print("[INFO] Notificacion de Telegram enviada correctamente.")
    except Exception as e:
        print(f"[ERROR] Fallo al enviar notificacion de Telegram: {e}")

def extraer_precio_diario(ticker_symbol):
    print(f"[INFO] Descargando vela diaria (OHLCV) para {ticker_symbol}...")
    ticker = yf.Ticker(ticker_symbol)
    
    # Descargamos solo la vela del dia actual
    df_precio = ticker.history(period="1d")
    
    if df_precio.empty:
        print(f"[WARN] No se encontraron datos de precio para {ticker_symbol} hoy.")
        return None
        
    df_precio = df_precio.reset_index()
    
    # Estandarizamos los nombres de las columnas
    df_precio.rename(columns={
        'Date': 'fecha',
        'Open': 'apertura',
        'High': 'maximo',
        'Low': 'minimo',
        'Close': 'cierre',
        'Volume': 'volumen'
    }, inplace=True)
    
    # Limpiamos el formato de fecha (evitamos problemas de zonas horarias en Parquet)
    if pd.api.types.is_datetime64_any_dtype(df_precio['fecha']):
        df_precio['fecha'] = df_precio['fecha'].dt.strftime('%Y-%m-%d')
        
    df_precio['ticker'] = ticker_symbol
    
    columnas_finales = ['fecha', 'ticker', 'apertura', 'maximo', 'minimo', 'cierre', 'volumen']
    df_final = df_precio[columnas_finales]
    
    return df_final

def subir_a_datalake(df, bucket_name, ruta_destino):
    print(f"[INFO] Comprimiendo datos de precio a formato Parquet...")
    archivo_temporal = f"temp_precio_{df['ticker'].iloc[0]}.parquet"
    
    df.to_parquet(archivo_temporal, engine='pyarrow', index=False)
    
    print(f"[INFO] Conectando a Cloud Storage (Bucket: {bucket_name})...")
    cliente_storage = storage.Client()
    bucket = cliente_storage.bucket(bucket_name)
    blob = bucket.blob(ruta_destino)
    
    blob.upload_from_filename(archivo_temporal)
    
    os.remove(archivo_temporal)
    print(f"[EXITO] Archivo guardado: gs://{bucket_name}/{ruta_destino}")

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Pipeline ETL de Precios")
    configurar_autenticacion_local()
    
    config = cargar_configuracion()
    if not config or "activos_operativos" not in config:
        print("[ERROR] Configuracion invalida o vacia.")
        return

    activos = list(config["activos_operativos"].keys())
    bucket_datalake = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    
    print(f"[INFO] Procesando precios para {len(activos)} activos: {activos}\n")
    
    activos_procesados = 0
    
    for ticker in activos:
        print(f"----------------------------------------")
        df_precio = extraer_precio_diario(ticker)
        
        if df_precio is not None:
            ruta_gcs = f"precios/bronce/{ticker}_{fecha_str}.parquet"
            subir_a_datalake(df_precio, bucket_datalake, ruta_gcs)
            activos_procesados += 1
            
    # Notificacion limpia de cierre
    mensaje_alerta = (
        "*Pipeline ETL Precios (Capa Bronce)*\n"
        f"Total de activos procesados: {activos_procesados}\n"
        "Estado: COMPLETADO"
    )
    notificar_telegram(mensaje_alerta)
    
    print("\n[INFO] Pipeline de precios finalizado exitosamente.")

if __name__ == "__main__":
    main()