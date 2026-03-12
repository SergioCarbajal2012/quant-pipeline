import os
import json
import time
import requests
import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from google.cloud import storage

HF_TOKEN = os.environ.get("HF_TOKEN")
API_URL = "https://router.huggingface.co/hf-inference/models/ProsusAI/finbert"

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
    with open(ruta_config, 'r') as archivo:
        return json.load(archivo)

def obtener_noticias_rss(ticker):
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        xml_data = urllib.request.urlopen(req, timeout=10).read()
        root = ET.fromstring(xml_data)
        titulares = [item.find('title').text for item in root.findall('.//item')]
        return titulares[:10]
    except Exception as e:
        print(f"[WARN] Fallo al extraer RSS para {ticker}: {e}")
        return []

def analizar_sentimiento(textos):
    if not HF_TOKEN:
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
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Sentimiento Pro (Intensidad + Contexto)")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    activos = list(config["activos_operativos"].keys())
    
    resultados = []
    for ticker in activos:
        print(f"Analizando {ticker}...")
        titulares_originales = obtener_noticias_rss(ticker)
        
        if not titulares_originales:
            resultados.append({"ticker": ticker, "sentiment_score": 0.0, "total_noticias": 0})
            continue

        # --- AQUI ESTA EL PROMPT (CONTEXTO) ---
        # Prependemos el ticker a cada titular para que la IA sepa de quién hablamos
        titulares_con_contexto = [f"Regarding {ticker}: {t}" for t in titulares_originales]
            
        analisis = analizar_sentimiento(titulares_con_contexto)
        score_acumulado = 0.0
        
        if analisis:
            for i, res in enumerate(analisis):
                mejor_etiqueta = max(res, key=lambda x: x['score']) if isinstance(res, list) else res
                label, score = mejor_etiqueta['label'], mejor_etiqueta['score']
                
                # Relevancia personalizada
                relevancia = 1.5 if ticker in titulares_originales[i].upper() else 0.5
                
                if label == 'positive':
                    score_acumulado += (score * relevancia)
                elif label == 'negative':
                    score_acumulado -= (score * relevancia)
        
        # Escala de Intensidad (x100) para evitar dilución
        intensidad_final = round(score_acumulado * 100, 2)
        print(f"    -> Intensidad: {intensidad_final}")
        
        resultados.append({
            "ticker": ticker, 
            "sentiment_score": intensidad_final, 
            "total_noticias": len(titulares_originales)
        })

    # Guardar en GCS
    df = pd.DataFrame(resultados)
    df['fecha_captura'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    ruta_gcs = f"sentimiento/bronce/sentiment_{fecha_str}.parquet"
    
    archivo_temporal = "temp_sentiment.parquet"
    df.to_parquet(archivo_temporal, index=False)
    cliente = storage.Client()
    blob = cliente.bucket(bucket_name).blob(ruta_gcs)
    blob.upload_from_filename(archivo_temporal)
    os.remove(archivo_temporal)
    print(f"[EXITO] Sentimiento Pro guardado.")

if __name__ == "__main__":
    main()