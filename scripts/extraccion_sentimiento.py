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
API_URL = "https://api-inference.huggingface.co/models/ProsusAI/finbert"

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
        print("    [ERROR] No se encontro HF_TOKEN en las variables de entorno.")
        return None
        
    headers = {"Authorization": f"Bearer {HF_TOKEN}"}
    for intento in range(3):
        try:
            response = requests.post(API_URL, headers=headers, json={"inputs": textos}, timeout=15)
            if response.status_code == 200:
                return response.json()
            elif 'estimated_time' in response.text:
                espera = response.json().get('estimated_time', 20)
                print(f"    [INFO] FinBERT despertando. Esperando {espera:.1f}s...")
                time.sleep(espera)
            else:
                print(f"    [ERROR API] Intento {intento+1}. Codigo: {response.status_code}. Detalle: {response.text}")
                time.sleep(5)
        except Exception as e:
            print(f"    [ERROR RED] Excepcion en intento {intento+1}: {e}")
            time.sleep(5)
    return None

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Iniciando Extraccion Sentimiento (RSS + Relevancia)")
    configurar_autenticacion_local()
    config = cargar_configuracion()
    activos = list(config["activos_operativos"].keys())
    
    resultados = []
    
    for ticker in activos:
        print(f"Procesando noticias para {ticker}...")
        titulares = obtener_noticias_rss(ticker)
        
        if not titulares:
            print(f"    [WARN] Cero noticias encontradas para {ticker}.")
            resultados.append({"ticker": ticker, "sentiment_score": 0.0, "total_noticias": 0})
            continue
            
        analisis = analizar_sentimiento(titulares)
        score_acumulado = 0.0
        
        if analisis:
            for i, res in enumerate(analisis):
                # Extraemos la etiqueta de mayor confianza (manejo de listas anidadas de HF)
                if isinstance(res, list):
                    mejor_etiqueta = max(res, key=lambda x: x['score'])
                else:
                    mejor_etiqueta = res
                    
                label = mejor_etiqueta['label']
                score = mejor_etiqueta['score']
                
                # Multiplicador de relevancia
                titular_upper = titulares[i].upper()
                if ticker in titular_upper:
                    relevancia = 1.5
                else:
                    relevancia = 0.5
                
                if label == 'positive':
                    valor_final = score * relevancia
                elif label == 'negative':
                    valor_final = -score * relevancia
                else:
                    valor_final = 0.0
                    
                score_acumulado += valor_final
                
        score_diario = score_acumulado / len(titulares) if titulares else 0.0
        print(f"    -> Score Ajustado: {score_diario:.4f} (en {len(titulares)} titulares)")
        
        resultados.append({
            "ticker": ticker,
            "sentiment_score": round(score_diario, 4),
            "total_noticias": len(titulares)
        })

    df = pd.DataFrame(resultados)
    df['fecha_captura'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    
    bucket_name = "datalake-quant-451704"
    fecha_str = datetime.now(timezone.utc).strftime('%Y%m%d')
    ruta_gcs = f"sentimiento/bronce/sentiment_{fecha_str}.parquet"
    
    archivo_temporal = "temp_sentiment.parquet"
    df.to_parquet(archivo_temporal, index=False)
    
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(ruta_gcs)
    blob.upload_from_filename(archivo_temporal)
    os.remove(archivo_temporal)
    print(f"[EXITO] Sentimiento guardado en gs://{bucket_name}/{ruta_gcs}")

if __name__ == "__main__":
    main()