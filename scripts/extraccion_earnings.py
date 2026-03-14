import os
import json
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone
from google.cloud import storage

def obtener_fecha_logica_mercado():
    """Obtiene la ultima sesion de mercado usando SPY como ancla."""
    try:
        df_calendario = yf.Ticker("SPY").history(period="10d")
        if not df_calendario.empty:
            return pd.to_datetime(df_calendario.index).max().date()
    except Exception as e:
        print(f"[WARN] No se pudo resolver fecha logica via SPY: {e}")

    return (pd.Timestamp.utcnow() - pd.tseries.offsets.BDay(1)).date()

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

def obtener_proximo_reporte(ticker):
    try:
        tk = yf.Ticker(ticker)
        calendario = tk.calendar
        
        if calendario and 'Earnings Date' in calendario and calendario['Earnings Date']:
            fecha = calendario['Earnings Date'][0]
            return pd.to_datetime(fecha).date()
        else:
            return None
    except Exception as e:
        print(f"[WARN] No se pudo obtener earnings para {ticker}: {e}")
        return None

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Extraccion de Earnings (Metodo Seguro)")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    
    if not config:
        print("[ERROR] No se pudo cargar config.json")
        return

    activos = list(config["activos_operativos"].keys())
    resultados = []
    fecha_logica_mercado = obtener_fecha_logica_mercado()

    for ticker in activos:
        print(f"Obteniendo fecha de reporte para {ticker}...")
        fecha_reporte = obtener_proximo_reporte(ticker)
        
        resultados.append({
            "ticker": ticker,
            "proximo_reporte": fecha_reporte
        })
        
        if fecha_reporte:
            print(f"    -> {fecha_reporte}")
        else:
            print(f"    -> N/A (Fondo o no disponible)")

    df = pd.DataFrame(resultados)
    df['fecha_captura'] = fecha_logica_mercado.strftime('%Y-%m-%d')
    
    bucket_name = "datalake-quant-451704"
    fecha_str = fecha_logica_mercado.strftime('%Y%m%d')
    ruta_gcs = f"earnings/bronce/earnings_{fecha_str}.parquet"
    
    archivo_temporal = "temp_earnings.parquet"
    df.to_parquet(archivo_temporal, index=False)
    
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(ruta_gcs)
    blob.upload_from_filename(archivo_temporal)
    os.remove(archivo_temporal)
    print(f"[EXITO] Calendario de Earnings guardado en gs://{bucket_name}/{ruta_gcs}")

if __name__ == "__main__":
    main()