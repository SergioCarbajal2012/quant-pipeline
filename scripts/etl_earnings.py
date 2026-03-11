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

def cargar_configuracion():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_config = os.path.join(ruta_base, 'config.json')
    try:
        with open(ruta_config, 'r') as archivo:
            return json.load(archivo)
    except FileNotFoundError:
        return None

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

def extraer_proximo_reporte(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    try:
        # yfinance devuelve un DataFrame con las fechas de reportes pasados y futuros
        df_earnings = ticker.get_earnings_dates(limit=10)
        
        if df_earnings is not None and not df_earnings.empty:
            ahora = pd.Timestamp.now(tz='UTC')
            
            # Filtramos solo las fechas que aun no han ocurrido
            futuros = df_earnings[df_earnings.index > ahora].sort_index()
            
            if not futuros.empty:
                siguiente_reporte = futuros.iloc[0]
                eps_est = siguiente_reporte.get("EPS Estimate")
                
                return {
                    "fecha_reporte": futuros.index[0].strftime('%Y-%m-%d'),
                    "eps_estimado": float(eps_est) if pd.notna(eps_est) else None
                }
    except Exception as e:
        # Los ETFs como SPY o QQQ caeran aqui, o si Yahoo Finance no tiene el dato
        pass
        
    return {"fecha_reporte": None, "eps_estimado": None}

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Pipeline ETL de Earnings")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    fecha_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if not config or "activos_operativos" not in config:
        print("[ERROR] Configuracion invalida.")
        return

    activos = list(config["activos_operativos"].keys())
    datos_earnings = []
    
    print(f"[INFO] Buscando fechas de reportes para {len(activos)} activos...\n")
    
    for ticker in activos:
        print(f"----------------------------------------")
        print(f"[INFO] Consultando calendario para {ticker}...")
        
        resultado = extraer_proximo_reporte(ticker)
        
        if resultado["fecha_reporte"]:
            print(f"       -> Proximo reporte: {resultado['fecha_reporte']} (EPS Est: {resultado['eps_estimado']})")
        else:
            print(f"       -> Sin fecha de reporte proxima (Probablemente sea un ETF o no hay datos).")
            
        datos_earnings.append({
            "fecha_captura": fecha_iso,
            "ticker": ticker,
            "proximo_reporte": resultado["fecha_reporte"],
            "eps_estimado": resultado["eps_estimado"]
        })

    # Guardar en Parquet y subir a GCS
    df_earnings = pd.DataFrame(datos_earnings)
    archivo_temp = f"temp_earnings_{fecha_str}.parquet"
    
    try:
        df_earnings.to_parquet(archivo_temp, index=False)
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(f"earnings/bronce/earnings_{fecha_str}.parquet")
        blob.upload_from_filename(archivo_temp)
        
        print(f"\n[EXITO] Calendario guardado en gs://{bucket_name}/earnings/bronce/")
        
        notificar_telegram(
            "*Pipeline Earnings (Capa Bronce)*\n"
            f"Activos procesados: {len(activos)}\n"
            "Status: COMPLETADO"
        )
    finally:
        if os.path.exists(archivo_temp):
            os.remove(archivo_temp)

if __name__ == "__main__":
    main()