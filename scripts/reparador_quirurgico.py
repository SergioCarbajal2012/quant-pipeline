import argparse
import os
from datetime import timedelta

import pandas as pd
import yfinance as yf
from google.cloud import bigquery, storage

from transformacion_plata import calcular_total_gex

PROYECTO = "resonant-forge-451704-v6"
DATASET = "sistema_quant"
TABLA_PLATA = f"{PROYECTO}.{DATASET}.plata_diaria"
BUCKET_NAME = "datalake-quant-451704"


def configurar_autenticacion_local():
    ruta_script = os.path.abspath(__file__)
    ruta_base = os.path.dirname(os.path.dirname(ruta_script))
    ruta_credenciales = os.path.join(ruta_base, "gcp_credentials.json")
    if os.path.exists(ruta_credenciales):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales
        print(f"[INFO] Credenciales cargadas desde: {ruta_credenciales}")
    else:
        print("[WARN] gcp_credentials.json no encontrado. Se asume entorno con credenciales inyectadas.")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Reparacion quirurgica de filas en plata_diaria con cierre=0 o dxy=0. "
            "Permite filtrar por lista de fechas o rango."
        )
    )
    parser.add_argument(
        "--fechas",
        type=str,
        default="",
        help="Lista de fechas separadas por coma. Ejemplo: 2025-01-03,2025-01-04",
    )
    parser.add_argument("--fecha-inicio", type=str, default="", help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--fecha-fin", type=str, default="", help="Fecha fin YYYY-MM-DD")
    return parser.parse_args()


def validar_fecha(valor):
    if not valor:
        return None
    return pd.to_datetime(valor).date()


def construir_query_objetivo(args):
    base_query = (
        f"SELECT fecha, ticker, apertura, maximo, minimo, cierre, volumen, "
        f"dxy, tasa_10y, vix, total_gex "
        f"FROM `{TABLA_PLATA}` "
        f"WHERE (cierre = 0 OR dxy = 0)"
    )

    parametros = []
    lista_fechas = [f.strip() for f in args.fechas.split(",") if f.strip()]

    if lista_fechas:
        fechas = [pd.to_datetime(f).date() for f in lista_fechas]
        base_query += " AND fecha IN UNNEST(@fechas)"
        parametros.append(bigquery.ArrayQueryParameter("fechas", "DATE", fechas))
    elif args.fecha_inicio and args.fecha_fin:
        fecha_inicio = validar_fecha(args.fecha_inicio)
        fecha_fin = validar_fecha(args.fecha_fin)
        base_query += " AND fecha BETWEEN @fecha_inicio AND @fecha_fin"
        parametros.append(bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio))
        parametros.append(bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin))
    elif args.fecha_inicio:
        fecha_inicio = validar_fecha(args.fecha_inicio)
        base_query += " AND fecha >= @fecha_inicio"
        parametros.append(bigquery.ScalarQueryParameter("fecha_inicio", "DATE", fecha_inicio))
    elif args.fecha_fin:
        fecha_fin = validar_fecha(args.fecha_fin)
        base_query += " AND fecha <= @fecha_fin"
        parametros.append(bigquery.ScalarQueryParameter("fecha_fin", "DATE", fecha_fin))

    base_query += " ORDER BY fecha, ticker"
    return base_query, parametros


def descargar_ventana_15d(ticker, fecha_objetivo):
    fecha_fin = pd.Timestamp(fecha_objetivo) + pd.Timedelta(days=1)
    try:
        df = yf.Ticker(ticker).history(period="15d", end=fecha_fin)
        if df.empty:
            print(f"[WARN] Yahoo devolvio vacio para {ticker} ({fecha_objetivo}).")
            return None

        idx = pd.to_datetime(df.index)
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
        df.index = idx
        return df.sort_index()
    except Exception as e:
        print(f"[ERROR] Fallo al descargar Yahoo para {ticker}: {e}")
        return None


def obtener_fila_asof(df, fecha_objetivo):
    if df is None or df.empty:
        return None
    corte = df.loc[df.index.date <= fecha_objetivo]
    if corte.empty:
        return None
    return corte.iloc[-1]


def obtener_precio_curado(ticker, fecha_objetivo):
    df_hist = descargar_ventana_15d(ticker, fecha_objetivo)
    if df_hist is None or df_hist.empty:
        return None

    cols_precio = ["Open", "High", "Low", "Close"]
    cols_disponibles = [col for col in cols_precio if col in df_hist.columns]
    if cols_disponibles:
        df_hist[cols_disponibles] = df_hist[cols_disponibles].ffill()

    if "Volume" in df_hist.columns:
        df_hist["Volume"] = df_hist["Volume"].fillna(0)

    fila = obtener_fila_asof(df_hist, fecha_objetivo)
    if fila is None:
        return None

    return {
        "apertura": float(fila.get("Open", 0.0) or 0.0),
        "maximo": float(fila.get("High", 0.0) or 0.0),
        "minimo": float(fila.get("Low", 0.0) or 0.0),
        "cierre": float(fila.get("Close", 0.0) or 0.0),
        "volumen": int(float(fila.get("Volume", 0.0) or 0.0)),
    }


def obtener_macro_curado(fecha_objetivo):
    mapa = {
        "vix": "^VIX",
        "dxy": "DX-Y.NYB",
        "tasa_10y": "^TNX",
    }
    resultado = {"vix": None, "dxy": None, "tasa_10y": None}

    for campo, ticker in mapa.items():
        df_hist = descargar_ventana_15d(ticker, fecha_objetivo)
        if df_hist is None or df_hist.empty:
            print(f"[WARN] No hubo macro para {campo}/{ticker} en {fecha_objetivo}.")
            continue

        if "Close" in df_hist.columns:
            df_hist["Close"] = df_hist["Close"].ffill()

        fila = obtener_fila_asof(df_hist, fecha_objetivo)
        if fila is None:
            print(f"[WARN] Sin fila as-of para {campo}/{ticker} en {fecha_objetivo}.")
            continue

        val = fila.get("Close", None)
        if pd.notna(val):
            resultado[campo] = float(val)

    return resultado


def descargar_opciones_dia(bucket, ticker, fecha_objetivo):
    fecha_str = pd.Timestamp(fecha_objetivo).strftime("%Y%m%d")
    ruta_blob = f"opciones/bronce/{ticker}_{fecha_str}.parquet"
    blob = bucket.blob(ruta_blob)

    if not blob.exists():
        print(f"[WARN] No existe archivo de opciones: gs://{BUCKET_NAME}/{ruta_blob}")
        return None

    temp_file = f"temp_opc_{ticker}_{fecha_str}.parquet"
    try:
        blob.download_to_filename(temp_file)
        df = pd.read_parquet(temp_file)
        return df
    except Exception as e:
        print(f"[ERROR] Fallo al descargar/leer opciones para {ticker} {fecha_objetivo}: {e}")
        return None
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)


def recalcular_gex(bucket, ticker, fecha_objetivo, cierre, tasa_10y, gex_actual):
    df_opciones = descargar_opciones_dia(bucket, ticker, fecha_objetivo)
    if df_opciones is None or df_opciones.empty:
        print(f"[WARN] GEX se conserva por falta de opciones para {ticker} {fecha_objetivo}.")
        return gex_actual

    try:
        return float(calcular_total_gex(df_opciones, cierre, tasa_10y))
    except Exception as e:
        print(f"[ERROR] Fallo al recalcular GEX para {ticker} {fecha_objetivo}: {e}")
        return gex_actual


def actualizar_fila_plata(client, fecha, ticker, valores):
    query = f"""
    UPDATE `{TABLA_PLATA}`
    SET
      apertura = @apertura,
      maximo = @maximo,
      minimo = @minimo,
      cierre = @cierre,
      volumen = @volumen,
      dxy = @dxy,
      tasa_10y = @tasa_10y,
      vix = @vix,
      total_gex = @total_gex
    WHERE fecha = @fecha
      AND ticker = @ticker
    """

    parametros = [
        bigquery.ScalarQueryParameter("apertura", "FLOAT64", float(valores["apertura"])),
        bigquery.ScalarQueryParameter("maximo", "FLOAT64", float(valores["maximo"])),
        bigquery.ScalarQueryParameter("minimo", "FLOAT64", float(valores["minimo"])),
        bigquery.ScalarQueryParameter("cierre", "FLOAT64", float(valores["cierre"])),
        bigquery.ScalarQueryParameter("volumen", "INT64", int(valores["volumen"])),
        bigquery.ScalarQueryParameter("dxy", "FLOAT64", float(valores["dxy"])),
        bigquery.ScalarQueryParameter("tasa_10y", "FLOAT64", float(valores["tasa_10y"])),
        bigquery.ScalarQueryParameter("vix", "FLOAT64", float(valores["vix"])),
        bigquery.ScalarQueryParameter("total_gex", "FLOAT64", float(valores["total_gex"])),
        bigquery.ScalarQueryParameter("fecha", "DATE", fecha),
        bigquery.ScalarQueryParameter("ticker", "STRING", ticker),
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=parametros)
    client.query(query, job_config=job_config).result()


def safe_or_default(nuevo, actual, default=0.0):
    if nuevo is None or pd.isna(nuevo):
        if actual is None or pd.isna(actual):
            return default
        return float(actual)
    return float(nuevo)


def safe_int_or_default(nuevo, actual, default=0):
    if nuevo is None or pd.isna(nuevo):
        if actual is None or pd.isna(actual):
            return int(default)
        return int(float(actual))
    return int(float(nuevo))


def main():
    print("[INFO] Iniciando reparador quirurgico de plata_diaria...")
    configurar_autenticacion_local()
    args = parse_args()

    client_bq = bigquery.Client()
    client_gcs = storage.Client()
    bucket = client_gcs.bucket(BUCKET_NAME)

    query, parametros = construir_query_objetivo(args)
    print("[INFO] Query objetivo construida. Buscando filas problematicas...")

    job_config = bigquery.QueryJobConfig(query_parameters=parametros)
    df_objetivo = client_bq.query(query, job_config=job_config).to_dataframe()

    if df_objetivo.empty:
        print("[INFO] No hay filas con cierre=0 o dxy=0 para el filtro indicado.")
        return

    print(f"[INFO] Filas candidatas encontradas: {len(df_objetivo)}")

    reparadas = 0
    errores = 0

    for i, fila in df_objetivo.iterrows():
        fecha = pd.to_datetime(fila["fecha"]).date()
        ticker = str(fila["ticker"])

        print("\n--------------------------------------------------")
        print(f"[INFO] Procesando {i + 1}/{len(df_objetivo)} -> {ticker} @ {fecha}")

        try:
            precios = obtener_precio_curado(ticker, fecha)
            macro = obtener_macro_curado(fecha)

            apertura = safe_or_default(precios.get("apertura") if precios else None, fila["apertura"], 0.0)
            maximo = safe_or_default(precios.get("maximo") if precios else None, fila["maximo"], 0.0)
            minimo = safe_or_default(precios.get("minimo") if precios else None, fila["minimo"], 0.0)
            cierre = safe_or_default(precios.get("cierre") if precios else None, fila["cierre"], 0.0)
            volumen = safe_int_or_default(precios.get("volumen") if precios else None, fila["volumen"], 0)

            dxy = safe_or_default(macro.get("dxy"), fila["dxy"], 0.0)
            tasa_10y = safe_or_default(macro.get("tasa_10y"), fila["tasa_10y"], 0.0)
            vix = safe_or_default(macro.get("vix"), fila["vix"], 0.0)

            gex_actual = safe_or_default(None, fila["total_gex"], 0.0)

            total_gex = recalcular_gex(
                bucket=bucket,
                ticker=ticker,
                fecha_objetivo=fecha,
                cierre=cierre,
                tasa_10y=tasa_10y,
                gex_actual=gex_actual,
            )

            payload = {
                "apertura": apertura,
                "maximo": maximo,
                "minimo": minimo,
                "cierre": cierre,
                "volumen": volumen,
                "dxy": dxy,
                "tasa_10y": tasa_10y,
                "vix": vix,
                "total_gex": total_gex,
            }

            print(f"[INFO] Payload de actualizacion: {payload}")
            actualizar_fila_plata(client_bq, fecha, ticker, payload)
            print("[EXITO] UPDATE aplicado (sentimiento_score no se toca).")
            reparadas += 1

        except Exception as e:
            print(f"[ERROR] Fallo en {ticker} {fecha}: {e}")
            errores += 1

    print("\n================ RESUMEN ================")
    print(f"[INFO] Filas candidatas: {len(df_objetivo)}")
    print(f"[INFO] Filas reparadas: {reparadas}")
    print(f"[INFO] Filas con error: {errores}")
    print("[INFO] Reparador quirurgico finalizado.")


if __name__ == "__main__":
    main()
