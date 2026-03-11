import os
import json
import numpy as np
import pandas as pd
from scipy.stats import norm
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud import bigquery

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
    """Descarga un archivo desde GCS y devuelve un DataFrame. Retorna None si no existe."""
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

def calcular_total_gex(df_opciones, spot_price, risk_free_rate):
    """Calcula el Gamma Exposure (GEX) total comprimiendo miles de contratos."""
    df = df_opciones.copy()
    df['impliedVolatility'] = df['impliedVolatility'].replace(0, np.nan)
    df = df.dropna(subset=['impliedVolatility'])
    
    if df.empty:
        return 0.0

    S = spot_price
    K = df['strike']
    r = risk_free_rate
    sigma = df['impliedVolatility']
    
    # Manejo de fechas para el tiempo T en años
    hoy = pd.to_datetime(df['fecha_captura'].iloc[0]).tz_localize(None)
    fecha_exp = pd.to_datetime(df['fecha_expiracion']).dt.tz_localize(None)
    dias = (fecha_exp - hoy).dt.days
    T = np.where(dias <= 0, 1.0 / 365.0, dias / 365.0)

    # Fórmula Black-Scholes para Gamma
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    pdf_d1 = norm.pdf(d1)
    Gamma = pdf_d1 / (S * sigma * np.sqrt(T))

    # Open Interest (rellenar nulos con 0)
    OI = df['openInterest'].fillna(0)

    # GEX del contrato = Gamma * Open Interest * 100 * Spot Price
    gex_contrato = Gamma * OI * 100 * S
    
    es_call = df['tipo'] == 'call'
    es_put = df['tipo'] == 'put'

    # GEX Total (Calls suman liquidez, Puts restan)
    total_gex = gex_contrato[es_call].sum() - gex_contrato[es_put].sum()
    
    return total_gex

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Transformación Capa Plata")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    
    if not config:
        print("[ERROR] No se pudo cargar config.json")
        return

    activos = list(config["activos_operativos"].keys())
    bucket_name = "datalake-quant-451704"
    fecha_hoy = datetime.now(timezone.utc).strftime('%Y%m%d')
    fecha_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    # Variables globales para Data Warehouse
    proyecto_bq = "resonant-forge-451704-v6"
    dataset_bq = "sistema_quant"
    tabla_bq = "plata_diaria"
    tabla_destino = f"{proyecto_bq}.{dataset_bq}.{tabla_bq}"

    # 1. Cargar datos generales (Macro, Sentimiento, Earnings)
    df_macro = descargar_parquet_gcs(bucket_name, f"macro/bronce/macro_{fecha_hoy}.parquet", "temp_m.parquet")
    df_sent = descargar_parquet_gcs(bucket_name, f"sentimiento/bronce/sentiment_{fecha_hoy}.parquet", "temp_s.parquet")
    df_earn = descargar_parquet_gcs(bucket_name, f"earnings/bronce/earnings_{fecha_hoy}.parquet", "temp_e.parquet")

    if df_macro is None or df_macro.empty:
        print("[ERROR] Datos Macro no encontrados. Abortando Capa Plata.")
        return

    tasa_10y = float(df_macro['ten_year_rate'].iloc[0]) if 'ten_year_rate' in df_macro else 0.0
    vix = float(df_macro['vix'].iloc[0]) if 'vix' in df_macro else 0.0
    dxy = float(df_macro['dxy'].iloc[0]) if 'dxy' in df_macro else 0.0

    filas_plata = []

    # 2. Iterar por activo para armar la súper fila
    print(f"[INFO] Ensamblando features diarios para {len(activos)} activos...\n")
    for ticker in activos:
        print(f" -> Procesando {ticker}...")
        
        # Precios
        df_px = descargar_parquet_gcs(bucket_name, f"precios/bronce/{ticker}_{fecha_hoy}.parquet", f"t_px_{ticker}.parquet")
        if df_px is None or df_px.empty:
            print(f"    [WARN] Sin datos de precio. Saltando activo.")
            continue
            
        apertura = float(df_px['apertura'].iloc[-1])
        maximo = float(df_px['maximo'].iloc[-1])
        minimo = float(df_px['minimo'].iloc[-1])
        cierre = float(df_px['cierre'].iloc[-1])
        volumen = int(df_px['volumen'].iloc[-1])

        # Opciones y GEX
        df_opc = descargar_parquet_gcs(bucket_name, f"opciones/bronce/{ticker}_{fecha_hoy}.parquet", f"t_op_{ticker}.parquet")
        total_gex = 0.0
        if df_opc is not None and not df_opc.empty:
            total_gex = calcular_total_gex(df_opc, cierre, tasa_10y)

        # Sentimiento
        sent_score = 0.0
        if df_sent is not None and not df_sent.empty:
            filtro_s = df_sent[df_sent['ticker'] == ticker]
            if not filtro_s.empty:
                sent_score = float(filtro_s['sentiment_score'].iloc[0])

        # Earnings
        dias_earnings = None
        if df_earn is not None and not df_earn.empty:
            filtro_e = df_earn[df_earn['ticker'] == ticker]
            if not filtro_e.empty and pd.notna(filtro_e['proximo_reporte'].iloc[0]):
                fecha_rep = pd.to_datetime(filtro_e['proximo_reporte'].iloc[0]).tz_localize(None)
                hoy = datetime.now().replace(tzinfo=None)
                dias_earnings = int((fecha_rep - hoy).days)

        # 3. Ensamblar fila
        filas_plata.append({
            "fecha": pd.to_datetime(fecha_iso).date(),
            "ticker": ticker,
            "apertura": apertura,
            "maximo": maximo,
            "minimo": minimo,
            "cierre": cierre,
            "volumen": volumen,
            "total_gex": float(total_gex),
            "tasa_10y": tasa_10y,
            "vix": vix,
            "dxy": dxy,
            "sentimiento_score": sent_score,
            "dias_para_earnings": dias_earnings
        })

    if not filas_plata:
        print("[WARN] No se procesaron filas para BigQuery.")
        return

    # 4. Inserción en BigQuery (Data Warehouse)
    df_final = pd.DataFrame(filas_plata)
    # Convertimos los NaN en tipos de pandas compatibles con BQ nulos
    df_final['dias_para_earnings'] = df_final['dias_para_earnings'].astype('Int64')

    print(f"\n[INFO] Escribiendo {len(df_final)} filas en BigQuery ({tabla_destino})...")
    cliente_bq = bigquery.Client()
    
    # Configurar el trabajo para que haga APPEND
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    
    job = cliente_bq.load_table_from_dataframe(df_final, tabla_destino, job_config=job_config)
    job.result() # Esperar a que termine
    
    print("[EXITO] Capa Plata materializada correctamente en BigQuery.")

if __name__ == "__main__":
    main()