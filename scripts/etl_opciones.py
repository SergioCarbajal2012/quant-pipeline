import os
import json
import yfinance as yf
import pandas as pd
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

def extraer_cadena_opciones(ticker_symbol):
    print(f"[INFO] Descargando datos para {ticker_symbol} desde Yahoo Finance...")
    ticker = yf.Ticker(ticker_symbol)
    expiraciones = ticker.options

    if not expiraciones:
        print(f"[ERROR] No se encontraron contratos de opciones para {ticker_symbol}")
        return None

    todas_las_opciones = []
    
    for fecha_exp in expiraciones[:3]:
        print(f"       -> Procesando expiracion: {fecha_exp}")
        cadena = ticker.option_chain(fecha_exp)
        
        calls = cadena.calls
        calls['tipo'] = 'call'
        calls['fecha_expiracion'] = fecha_exp
        
        puts = cadena.puts
        puts['tipo'] = 'put'
        puts['fecha_expiracion'] = fecha_exp
        
        todas_las_opciones.extend([calls, puts])

    df_final = pd.concat(todas_las_opciones, ignore_index=True)
    df_final['fecha_captura'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    df_final['ticker'] = ticker_symbol
    
    print(f"[INFO] Extraccion completa para {ticker_symbol}. Total de contratos: {len(df_final)}")
    return df_final

def subir_a_datalake(df, bucket_name, ruta_destino):
    print(f"[INFO] Comprimiendo datos a formato Parquet...")
    archivo_temporal = f"temp_{df['ticker'].iloc[0]}.parquet"
    
    df.to_parquet(archivo_temporal, engine='pyarrow', index=False)
    
    print(f"[INFO] Conectando a Google Cloud Storage (Bucket: {bucket_name})...")
    cliente_storage = storage.Client()
    bucket = cliente_storage.bucket(bucket_name)
    blob = bucket.blob(ruta_destino)
    
    blob.upload_from_filename(archivo_temporal)
    
    os.remove(archivo_temporal)
    print(f"[EXITO] Archivo guardado en Data Lake: gs://{bucket_name}/{ruta_destino}")

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Pipeline ETL de Opciones")
    configurar_autenticacion_local()
    
    config = cargar_configuracion()
    if not config or "activos_operativos" not in config:
        print("[ERROR] Configuracion invalida o vacia.")
        return

    activos = list(config["activos_operativos"].keys())
    bucket_datalake = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    
    print(f"[INFO] Se encontraron {len(activos)} activos en config.json: {activos}\n")
    
    for ticker in activos:
        print(f"========================================")
        print(f"[INFO] INICIANDO EXTRACCION PARA: {ticker}")
        print(f"========================================")
        
        df_opciones = extraer_cadena_opciones(ticker)
        
        if df_opciones is not None:
            ruta_gcs = f"opciones/bronce/{ticker}_{fecha_str}.parquet"
            subir_a_datalake(df_opciones, bucket_datalake, ruta_gcs)
            
    print("\n[INFO] Pipeline finalizado exitosamente para todos los activos.")

if __name__ == "__main__":
    main()