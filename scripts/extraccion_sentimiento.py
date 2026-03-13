import os
import json
import time
import requests
import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET
import email.utils # Vital para parsear las fechas del RSS
from datetime import datetime, timezone
from google.cloud import storage

HF_TOKEN = os.environ.get("HF_TOKEN")
API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"

def configurar_autenticacion_local():
    # ... (mantiene la misma lógica de credenciales)
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_credenciales = os.path.join(ruta_base, 'gcp_credentials.json')
    if os.path.exists(ruta_credenciales):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales

def obtener_noticias_frescas_rss(ticker):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    noticias_validas = []
    ahora = datetime.now(timezone.utc)
    
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        xml_data = urllib.request.urlopen(req, timeout=10).read()
        root = ET.fromstring(xml_data)
        
        for item in root.findall('.//item'):
            titulo = item.find('title').text
            fecha_raw = item.find('pubDate').text
            
            # Parsear fecha RFC822 a objeto datetime
            fecha_dt = email.utils.parsedate_to_datetime(fecha_raw)
            
            # Calcular antigüedad
            diferencia = ahora - fecha_dt
            horas_antiguedad = diferencia.total_seconds() / 3600
            
            # FILTRO CRÍTICO: Solo noticias de las últimas 24 horas
            if horas_antiguedad <= 24:
                noticias_validas.append(titulo)
        
        return noticias_validas[:10] # Máximo 10, pero solo de las últimas 24h
    except Exception as e:
        print(f"[WARN] Fallo RSS para {ticker}: {e}")
        return []

def analizar_sentimiento(textos):
    if not HF_TOKEN or not textos:
        return None
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    for intento in range(3):
        try:
            response = requests.post(API_URL, headers=headers, json={"inputs": textos}, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif 'estimated_time' in response.text:
                time.sleep(response.json().get('estimated_time', 20))
            else:
                time.sleep(5)
        except:
            time.sleep(5)
    return None

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Sentimiento Pro (Filtro 24h + Intensidad)")
    configurar_autenticacion_local()
    
    # Cargar configuración (asumimos que existe config.json en la raíz)
    ruta_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(ruta_base, 'config.json'), 'r') as f:
        config = json.load(f)
    
    activos = list(config["activos_operativos"].keys())
    resultados = []

    for ticker in activos:
        print(f"Procesando {ticker}...")
        titulares_frescos = obtener_noticias_frescas_rss(ticker)
        
        if not titulares_frescos:
            print(f"    -> [INFO] 0 noticias en las últimas 24h.")
            resultados.append({"ticker": ticker, "sentiment_score": 0.0, "total_noticias": 0})
            continue

        # Contextualización (Prompt)
        titulares_con_contexto = [f"Regarding {ticker}: {t}" for t in titulares_frescos]
        
        analisis = analizar_sentimiento(titulares_con_contexto)
        score_acumulado = 0.0
        
        if analisis:
            for i, res in enumerate(analisis):
                mejor_etiqueta = max(res, key=lambda x: x['score']) if isinstance(res, list) else res
                label, score = mejor_etiqueta['label'], mejor_etiqueta['score']
                
                # Relevancia (usa el titular original sin el prefijo para la búsqueda)
                relevancia = 1.5 if ticker in titulares_frescos[i].upper() else 0.5
                
                if label == 'positive':
                    score_acumulado += (score * relevancia)
                elif label == 'negative':
                    score_acumulado -= (score * relevancia)
        
        intensidad_final = round(score_acumulado * 100, 2)
        print(f"    -> Intensidad (24h): {intensidad_final} ({len(titulares_frescos)} noticias)")
        
        resultados.append({
            "ticker": ticker, 
            "sentiment_score": intensidad_final, 
            "total_noticias": len(titulares_frescos)
        })

    # Guardado en GCS (Parquet)
    df = pd.DataFrame(resultados)
    df['fecha_captura'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    ruta_gcs = f"sentimiento/bronce/sentiment_{fecha_str}.parquet"
    
    archivo_temporal = "temp_sentiment.parquet"
    df.to_parquet(archivo_temporal, index=False)
    storage.Client().bucket(bucket_name).blob(ruta_gcs).upload_from_filename(archivo_temporal)
    os.remove(archivo_temporal)
    print(f"[EXITO] Sentimiento filtrado guardado.")

if __name__ == "__main__":
    main()