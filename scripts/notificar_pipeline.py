import os
import sys
import argparse
import requests
from datetime import datetime, timezone


def main():
    parser = argparse.ArgumentParser(description="Envia notificacion final del pipeline a Telegram.")
    parser.add_argument(
        "--inicio",
        required=True,
        help="Timestamp ISO 8601 del inicio del pipeline (UTC). Ejemplo: 2026-03-10T14:00:00Z"
    )
    args = parser.parse_args()

    try:
        inicio = datetime.fromisoformat(args.inicio.replace("Z", "+00:00"))
        fin = datetime.now(timezone.utc)
        duracion = fin - inicio
        minutos, segundos = divmod(int(duracion.total_seconds()), 60)

        token = os.environ.get("TELEGRAM_BOT_TOKEN")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            print("[ERROR] TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID no configurados.")
            sys.exit(1)

        mensaje = (
            "*Pipeline Diario Completado con éxito*\n"
            f"Duración total: {minutos}m {segundos}s\n"
            f"Finalizado: {fin.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}

        respuesta = requests.post(url, json=payload, timeout=10)
        respuesta.raise_for_status()
        print("[INFO] Notificacion enviada correctamente.")

    except Exception as e:
        print(f"[ERROR] Fallo al enviar notificacion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
