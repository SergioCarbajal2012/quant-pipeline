import os
import json
import time
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

def analizar_sentimiento_hf(titulares, hf_token):
    API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"
    headers = {"Authorization": f"Bearer {hf_token}"}
    
    if not titulares:
        return 0.0

    # Hugging Face a veces necesita despertar el modelo, intentamos hasta 3 veces
    for intento in range(3):
        try:
            response = requests.post(API_URL, headers=headers, json={"inputs": titulares})
            resultado = response.json()
            
            # Si el modelo esta cargando, la API nos dice cuanto esperar
            if isinstance(resultado, dict) and "error" in resultado and "estimated_time" in resultado:
                tiempo_espera = resultado["estimated_time"]
                print(f"[WARN] Modelo FinBERT cargando en la nube. Esperando {tiempo_espera:.1f} segundos...")
                time.sleep(tiempo_espera + 1)
                continue
                
            if isinstance(resultado, list) and len(resultado) > 0:
                score_total = 0
                for predicciones in resultado:
                    # FinBERT devuelve: [{'label': 'positive', 'score': 0.8}, ...]
                    if isinstance(predicciones, list):
                        dic_scores = {item['label']: item['score'] for item in predicciones}
                        # Sentimiento Neto = Positivo - Negativo
                        score_neto = dic_scores.get('positive', 0) - dic_scores.get('negative', 0)
                        score_total += score_neto
                
                # Devolvemos el promedio de todos los titulares
                return score_total / len(titulares) 
            
            return 0.0
        except Exception as e:
            print(f"[ERROR] Fallo en la API de Hugging Face: {e}")
            time.sleep(5)
            
    return 0.0

def extraer_noticias_yfinance(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    noticias = ticker.news
    titulares = []
    
    # Extraemos maximo 10 titulares recientes para no saturar
    for noticia in noticias[:10]: 
        if 'title' in noticia:
            titulares.append(noticia['title'])
            
    return titulares

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Pipeline ETL de Sentimiento")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    hf_token = os.environ.get("HF_API_TOKEN")
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    fecha_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if not config or "activos_operativos" not in config:
        print("[ERROR] Configuracion invalida o vacia.")
        return

    if not hf_token:
        print("[ERROR] No se encontro HF_API_TOKEN en las variables de entorno.")
        return

    activos = list(config["activos_operativos"].keys())
    datos_sentimiento = []
    
    print(f"[INFO] Analizando sentimiento para {len(activos)} activos...\n")
    
    for ticker in activos:
        print(f"----------------------------------------")
        print(f"[INFO] Procesando noticias para {ticker}...")
        titulares = extraer_noticias_yfinance(ticker)
        
        if titulares:
            print(f"       -> {len(titulares)} titulares encontrados. Evaluando con FinBERT...")
            score = analizar_sentimiento_hf(titulares, hf_token)
            print(f"       -> Score neto diario: {score:.4f}")
        else:
            print(f"       -> No se encontraron noticias recientes.")
            score = 0.0
            
        datos_sentimiento.append({
            "fecha": fecha_iso,
            "ticker": ticker,
            "sentiment_score": score,
            "num_noticias": len(titulares)
        })

    # Guardar en Parquet y subir a GCS
    df_sentimiento = pd.DataFrame(datos_sentimiento)
    archivo_temp = f"temp_sentiment_{fecha_str}.parquet"
    
    try:
        df_sentimiento.to_parquet(archivo_temp, index=False)
        cliente = storage.Client()
        bucket = cliente.bucket(bucket_name)
        blob = bucket.blob(f"sentimiento/bronce/sentiment_{fecha_str}.parquet")
        blob.upload_from_filename(archivo_temp)
        
        print(f"\n[EXITO] Datos guardados en gs://{bucket_name}/sentimiento/bronce/")
    finally:
        if os.path.exists(archivo_temp):
            os.remove(archivo_temp)

if __name__ == "__main__":
    main()