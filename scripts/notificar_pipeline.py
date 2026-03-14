import os
import sys
import argparse
import requests
from datetime import datetime, timezone

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inicio", required=True)
    parser.add_argument("--run_id", required=False, default="Local")
    parser.add_argument("--errores", required=False, default="") # Nuevo argumento
    args = parser.parse_args()

    try:
        inicio = datetime.fromisoformat(args.inicio.replace("Z", "+00:00"))
        fin = datetime.now(timezone.utc)
        duracion = fin - inicio
        minutos, segundos = divmod(int(duracion.total_seconds()), 60)

        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        mensaje = (
            "*Pipeline Diario Completado*\n" # Quitamos "con exito" fijo
            f"Duracion total: {minutos}m {segundos}s\n"
            f"ID de Ejecucion: `{args.run_id}`\n"
        )

        # Si hay errores de sentimiento, los añadimos brevemente
        if args.errores:
            mensaje += f"\nAlerta API en: `{args.errores}`\n"
        
        mensaje += f"Finalizado: {fin.strftime('%Y-%m-%d %H:%M:%S')} UTC"

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10).raise_for_status()

    except Exception as e:
        print(f"[ERROR] Fallo al enviar notificación: {e}")

if __name__ == "__main__":
    main()