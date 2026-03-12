import os
import sys
import argparse
import requests
from datetime import datetime, timezone

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inicio", required=True)
    parser.add_argument("--run_id", required=False, default="Local")
    args = parser.parse_args()

    try:
        inicio = datetime.fromisoformat(args.inicio.replace("Z", "+00:00"))
        fin = datetime.now(timezone.utc)
        duracion = fin - inicio
        minutos, segundos = divmod(int(duracion.total_seconds()), 60)

        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        mensaje = (
            "*Pipeline Diario Completado con exito*\n"
            f"Duracion total: {minutos}m {segundos}s\n"
            f"ID de Ejecucion: `{args.run_id}`\n"
            f"Finalizado: {fin.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
        requests.post(url, json=payload, timeout=10).raise_for_status()

    except Exception as e:
        print(f"[ERROR] Fallo al enviar notificacion: {e}")

if __name__ == "__main__":
    main()