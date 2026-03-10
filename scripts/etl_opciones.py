import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage

def configurar_autenticacion_local():
    # Encuentra la ruta base del proyecto para ubicar la llave JSON
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_credenciales = os.path.join(ruta_base, 'gcp_credentials.json')
    
    # Si el archivo existe localmente, lo usamos (GitHub Actions usara su propia boveda)
    if os.path.exists(ruta_credenciales):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales
        print(f"[INFO] Credenciales locales cargadas desde: {ruta_credenciales}")
    else:
        print("[WARN] No se encontro gcp_credentials.json local. Asumiendo entorno de produccion (GitHub Actions).")

def extraer_cadena_opciones(ticker_symbol):
    print(f"[INFO] Descargando datos para {ticker_symbol} desde Yahoo Finance...")
    ticker = yf.Ticker(ticker_symbol)
    expiraciones = ticker.options

    if not expiraciones:
        print(f"[ERROR] No se encontraron contratos de opciones para {ticker_symbol}")
        return None

    todas_las_opciones = []
    
    # Para el MVP, descargamos solo las 3 fechas de expiracion mas proximas 
    # para no saturar la API ni demorar la ejecucion.
    for fecha_exp in expiraciones[:3]:
        print(f"       -> Procesando expiracion: {fecha_exp}")
        cadena = ticker.option_chain(fecha_exp)
        
        # Procesar Calls
        calls = cadena.calls
        calls['tipo'] = 'call'
        calls['fecha_expiracion'] = fecha_exp
        
        # Procesar Puts
        puts = cadena.puts
        puts['tipo'] = 'put'
        puts['fecha_expiracion'] = fecha_exp
        
        todas_las_opciones.extend([calls, puts])

    # Unificar todo en un solo DataFrame
    df_final = pd.concat(todas_las_opciones, ignore_index=True)
    df_final['fecha_captura'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    df_final['ticker'] = ticker_symbol
    
    print(f"[INFO] Extraccion completa. Total de contratos procesados: {len(df_final)}")
    return df_final

def subir_a_datalake(df, bucket_name, ruta_destino):
    print(f"[INFO] Comprimiendo datos a formato Parquet...")
    archivo_temporal = "temp_opciones.parquet"
    
    # Convertir a Parquet
    df.to_parquet(archivo_temporal, engine='pyarrow', index=False)
    
    print(f"[INFO] Conectando a Google Cloud Storage (Bucket: {bucket_name})...")
    cliente_storage = storage.Client()
    bucket = cliente_storage.bucket(bucket_name)
    blob = bucket.blob(ruta_destino)
    
    # Subir el archivo
    blob.upload_from_filename(archivo_temporal)
    
    # Limpiar archivo temporal local
    os.remove(archivo_temporal)
    print(f"[EXITO] Archivo guardado en Data Lake: gs://{bucket_name}/{ruta_destino}")

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Pipeline ETL de Opciones")
    configurar_autenticacion_local()
    
    # Configuracion inicial
    bucket_datalake = "datalake-quant-451704"
    ticker_prueba = "NVDA"
    
    # 1. Extraer (Extract)
    df_opciones = extraer_cadena_opciones(ticker_prueba)
    
    if df_opciones is not None:
        # 2. Cargar a Bronce (Load)
        fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
        ruta_gcs = f"opciones/bronce/{ticker_prueba}_{fecha_str}.parquet"
        subir_a_datalake(df_opciones, bucket_datalake, ruta_gcs)
        
    print("[INFO] Pipeline finalizado.")

if __name__ == "__main__":
    main()