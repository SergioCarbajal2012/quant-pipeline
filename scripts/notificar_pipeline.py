import os
import sys
import json
import argparse
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas as pd


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


def contar_ceros_historico(cliente_bq, tabla_destino, fecha_hoy):
    query = f"""
    WITH dias_historicos AS (
      SELECT
        fecha,
        COUNTIF(IFNULL(sentimiento_score, 0.0) = 0.0) AS ceros_por_dia
      FROM `{tabla_destino}`
      WHERE fecha < @fecha_hoy
      GROUP BY fecha
      ORDER BY fecha DESC
      LIMIT 7
    )
    SELECT IFNULL(AVG(ceros_por_dia), 0.0) AS promedio_historico
    FROM dias_historicos
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("fecha_hoy", "DATE", fecha_hoy)
        ]
    )
    filas = list(cliente_bq.query(query, job_config=job_config).result())
    if not filas:
        return 0.0
    return float(filas[0]["promedio_historico"] or 0.0)


def contar_metricas_hoy(cliente_bq, tabla_destino, fecha_hoy, run_id):
    query = f"""
    SELECT
      COUNT(1) AS total_activos,
      COUNTIF(IFNULL(sentimiento_score, 0.0) = 0.0) AS ceros_hoy,
      MAX(IF(IFNULL(tasa_10y, 0.0) = 0.0 OR IFNULL(vix, 0.0) = 0.0 OR IFNULL(dxy, 0.0) = 0.0, 1, 0)) AS macro_error
    FROM `{tabla_destino}`
    WHERE fecha = @fecha_hoy
      AND (@run_id IN ('Local', 'local_run', '') OR ejecucion_id = @run_id)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("fecha_hoy", "DATE", fecha_hoy),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    filas = list(cliente_bq.query(query, job_config=job_config).result())
    if not filas:
        return 0, 0, 0

    fila = filas[0]
    total_activos = int(fila["total_activos"] or 0)
    ceros_hoy = int(fila["ceros_hoy"] or 0)
    macro_error = int(fila["macro_error"] or 0)
    return total_activos, ceros_hoy, macro_error

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inicio", required=True)
    parser.add_argument("--run_id", required=False, default="Local")
    parser.add_argument("--errores", required=False, default="")
    args = parser.parse_args()

    try:
        configurar_autenticacion_local()
        inicio = datetime.fromisoformat(args.inicio.replace("Z", "+00:00"))
        fin = datetime.now(timezone.utc)
        duracion = fin - inicio
        minutos, segundos = divmod(int(duracion.total_seconds()), 60)
        duracion_texto = f"{minutos}m {segundos}s"

        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        fecha_hoy = pd.Timestamp.utcnow().date()

        config = cargar_configuracion()
        total_esperado = len(config.get("activos_operativos", {}))

        proyecto_bq = "resonant-forge-451704-v6"
        dataset_bq = "sistema_quant"
        tabla_bq = "plata_diaria"
        tabla_destino = f"{proyecto_bq}.{dataset_bq}.{tabla_bq}"

        cliente_bq = bigquery.Client()
        promedio_historico_ceros = contar_ceros_historico(cliente_bq, tabla_destino, fecha_hoy)
        total_hoy, ceros_hoy, macro_error = contar_metricas_hoy(cliente_bq, tabla_destino, fecha_hoy, str(args.run_id))

        alertas = []
        umbral_anomalia = promedio_historico_ceros + 5.0

        if ceros_hoy > umbral_anomalia:
            alertas.append(
                f"[WARNING] Sentiment Anomaly: {ceros_hoy} assets with 0.0 score detected today. "
                f"Historical 7-day average is {promedio_historico_ceros:.2f}."
            )

        umbral_minimo_activos = int(total_esperado * 0.9)
        if total_hoy < umbral_minimo_activos:
            alertas.append(
                f"[CRITICAL] Data Loss: Only {total_hoy} assets processed out of expected total ({total_esperado})."
            )

        if macro_error == 1:
            alertas.append("[CRITICAL] Macro Indicators Error: 0.0 values detected.")

        if args.errores:
            alertas.append(f"[WARNING] Sentiment API errors reported for tickers: {args.errores}.")

        if not alertas:
            mensaje = f"[INFO] Pipeline execution completed successfully. Duration: {duracion_texto}."
        else:
            lineas_alerta = "\n".join(alertas)
            mensaje = (
                f"[INFO] Pipeline execution completed with data quality alerts. Duration: {duracion_texto}.\n"
                "--- DATA QUALITY ALERTS ---\n"
                f"{lineas_alerta}"
            )

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": mensaje}
        requests.post(url, json=payload, timeout=10).raise_for_status()

    except Exception as e:
        print(f"[ERROR] Fallo al enviar notificación: {e}")

if __name__ == "__main__":
    main()