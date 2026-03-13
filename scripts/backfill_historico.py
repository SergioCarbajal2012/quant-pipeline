import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from google.cloud import bigquery, storage

# Configuración de tablas
PROYECTO = "resonant-forge-451704-v6"
DATASET = "sistema_quant"
TABLA_PLATA = f"{PROYECTO}.{DATASET}.plata_diaria"
BUCKET_NAME = "datalake-quant-451704" # Ajusta si tu bucket tiene otro nombre

def obtener_puntos_corte(client):
    """Obtiene la fecha mínima existente en Plata para cada ticker"""
    query = f"SELECT ticker, MIN(fecha) as min_fecha FROM `{TABLA_PLATA}` GROUP BY ticker"
    try:
        df = client.query(query).to_dataframe()
        return dict(zip(df['ticker'], df['min_fecha']))
    except Exception as e:
        print(f"[ERROR] No se pudo consultar la tabla Plata: {e}")
        return {}

def ejecutar_backfill():
    client_bq = bigquery.Client()
    client_gcs = storage.Client()
    bucket = client_gcs.bucket(BUCKET_NAME)
    
    # Cargar activos desde el config
    ruta_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(ruta_base, 'config.json'), 'r') as f:
        config = json.load(f)
    
    cortes = obtener_puntos_corte(client_bq)
    hoy = datetime.now()
    inicio_historico = "2022-01-01"

    for ticker in config['activos_operativos'].keys():
        # Si el ticker existe en Plata, terminamos el backfill el día anterior a su llegada
        # Si no existe, descargamos hasta hoy
        fecha_fin = cortes.get(ticker)
        if fecha_fin:
            fecha_fin_str = (pd.to_datetime(fecha_fin) - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            fecha_fin_str = hoy.strftime('%Y-%m-%d')

        print(f"--- PROCESANDO HISTORICO: {ticker} ({inicio_historico} -> {fecha_fin_str}) ---")
        
        try:
            df = yf.download(ticker, start=inicio_historico, end=fecha_fin_str, progress=False)
            if df.empty:
                print(f"    [SKIP] No hay datos para {ticker}")
                continue
            
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            df['ticker'] = ticker
            
            # Columnas vacías para mantener compatibilidad con el esquema futuro
            df['total_gex'] = None
            df['sentimiento_score'] = 0.0
            df['dias_para_earnings'] = None
            
            # Guardar y subir a GCS
            temp_file = f"hist_{ticker}.parquet"
            df.to_parquet(temp_file, index=False)
            
            blob = bucket.blob(f"historico/precios/{ticker}_hist.parquet")
            blob.upload_from_filename(temp_file)
            os.remove(temp_file)
            print(f"    [EXITO] {len(df)} dias guardados en Data Lake.")
            
        except Exception as e:
            print(f"    [ERROR] Fallo en {ticker}: {e}")

if __name__ == "__main__":
    ejecutar_backfill()