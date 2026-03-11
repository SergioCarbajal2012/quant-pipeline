import os
import json
import numpy as np
import pandas as pd
from scipy.stats import norm
from datetime import datetime, timezone
from google.cloud import storage

def configurar_autenticacion_local():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_credenciales = os.path.join(ruta_base, 'gcp_credentials.json')
    if os.path.exists(ruta_credenciales):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales

def cargar_configuracion():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_config = os.path.join(ruta_base, 'config.json')
    try:
        with open(ruta_config, 'r') as archivo:
            return json.load(archivo)
    except FileNotFoundError:
        return None

def descargar_parquet_gcs(bucket_name, ruta_blob, archivo_temp):
    """Descarga un archivo desde GCS a local temporalmente para leerlo."""
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(ruta_blob)
        if blob.exists():
            blob.download_to_filename(archivo_temp)
            df = pd.read_parquet(archivo_temp)
            os.remove(archivo_temp)
            return df
        return None
    except Exception as e:
        print(f"[ERROR] Fallo al descargar {ruta_blob}: {e}")
        return None

def subir_parquet_gcs(df, bucket_name, ruta_destino):
    archivo_temporal = "temp_upload.parquet"
    df.to_parquet(archivo_temporal, engine='pyarrow', index=False)
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(ruta_destino)
    blob.upload_from_filename(archivo_temporal)
    os.remove(archivo_temporal)

def calcular_griegas(df):
    """Aplica Black-Scholes vectorizado a todo el DataFrame."""
    # Evitar divisiones por cero en volatilidad o tiempo
    df['impliedVolatility'] = df['impliedVolatility'].replace(0, np.nan)
    df = df.dropna(subset=['impliedVolatility']).copy()
    
    # Variables base
    S = df['spot_price']
    K = df['strike']
    r = df['risk_free_rate']
    sigma = df['impliedVolatility']
    
    # Tiempo a expiración en años (T). Si vence hoy, asignamos 1 día (1/365) para evitar T=0
    df['dias_expiracion'] = (pd.to_datetime(df['fecha_expiracion']).dt.tz_localize(None) - pd.to_datetime(df['fecha_captura']).dt.tz_localize(None)).dt.days
    df['T'] = np.where(df['dias_expiracion'] <= 0, 1.0 / 365.0, df['dias_expiracion'] / 365.0)
    T = df['T']

    # Black-Scholes d1 y d2
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    # Densidad y distribución normal
    N_d1 = norm.cdf(d1)
    N_d2 = norm.cdf(d2)
    N_neg_d1 = norm.cdf(-d1)
    N_neg_d2 = norm.cdf(-d2)
    pdf_d1 = norm.pdf(d1)

    # 1. GAMMA (Igual para Call y Put)
    df['Gamma'] = pdf_d1 / (S * sigma * np.sqrt(T))

    # 2. VEGA (Igual para Call y Put, expresado en porcentaje)
    df['Vega'] = (S * pdf_d1 * np.sqrt(T)) / 100

    # Separar la lógica para Calls y Puts usando máscaras booleanas
    es_call = df['tipo'] == 'call'
    es_put = df['tipo'] == 'put'

    # 3. DELTA
    df.loc[es_call, 'Delta'] = N_d1[es_call]
    df.loc[es_put, 'Delta'] = N_d1[es_put] - 1

    # 4. THETA (Anualizado, lo dividimos entre 365 para ver la caída diaria)
    theta_call = (- (S * pdf_d1 * sigma) / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * N_d2) / 365
    theta_put = (- (S * pdf_d1 * sigma) / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * N_neg_d2) / 365
    
    df.loc[es_call, 'Theta'] = theta_call[es_call]
    df.loc[es_put, 'Theta'] = theta_put[es_put]

    return df

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Transformación Capa Plata (Griegas)")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    
    if not config:
        return

    activos = list(config["activos_operativos"].keys())
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')

    # 1. Cargar el contexto Macro (Tasa libre de riesgo)
    ruta_macro = f"macro/bronce/macro_{fecha_str}.parquet"
    df_macro = descargar_parquet_gcs(bucket_name, ruta_macro, "temp_macro.parquet")
    
    if df_macro is None or df_macro.empty:
        print("[ERROR] Faltan datos macro de hoy. No se pueden calcular las Griegas.")
        return
        
    risk_free_rate = df_macro['ten_year_rate'].iloc[0]
    print(f"[INFO] Tasa Libre de Riesgo (Bono 10Y): {risk_free_rate:.4f}")

    # 2. Iterar sobre cada activo para procesar su Capa Plata
    for ticker in activos:
        print(f"----------------------------------------")
        print(f"[INFO] Procesando Capa Plata para {ticker}...")
        
        # Leer Bronce (Precios)
        ruta_precio = f"precios/bronce/{ticker}_{fecha_str}.parquet"
        df_precio = descargar_parquet_gcs(bucket_name, ruta_precio, f"temp_px_{ticker}.parquet")
        
        # Leer Bronce (Opciones)
        ruta_opciones = f"opciones/bronce/{ticker}_{fecha_str}.parquet"
        df_opciones = descargar_parquet_gcs(bucket_name, ruta_opciones, f"temp_opt_{ticker}.parquet")

        if df_precio is None or df_opciones is None:
            print(f"[WARN] Faltan datos base para {ticker}. Saltando...")
            continue

        spot_price = df_precio['cierre'].iloc[-1]
        
        # Inyectar el Spot Price y la Tasa Macro a la tabla de opciones
        df_opciones['spot_price'] = spot_price
        df_opciones['risk_free_rate'] = risk_free_rate
        
        # Calcular la matemática pesada
        df_plata = calcular_griegas(df_opciones)
        
        # Limpiar columnas innecesarias y estructurar la tabla final
        columnas_finales = [
            'fecha_captura', 'ticker', 'fecha_expiracion', 'tipo', 'strike', 
            'spot_price', 'lastPrice', 'bid', 'ask', 'volume', 'openInterest', 
            'impliedVolatility', 'dias_expiracion', 'Delta', 'Gamma', 'Theta', 'Vega'
        ]
        
        # Manejo de columnas faltantes por si Yahoo no trajo volumen en contratos inactivos
        for col in columnas_finales:
            if col not in df_plata.columns:
                df_plata[col] = 0
                
        df_plata = df_plata[columnas_finales]
        
        # Guardar en la Capa Plata
        ruta_plata = f"opciones/plata/{ticker}_{fecha_str}.parquet"
        subir_parquet_gcs(df_plata, bucket_name, ruta_plata)
        print(f"[EXITO] Griegas calculadas y guardadas: gs://{bucket_name}/{ruta_plata}")

if __name__ == "__main__":
    main()