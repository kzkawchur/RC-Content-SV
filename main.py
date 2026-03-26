import logging
import os
import threading

import requests
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("WebhookBot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 8080))
WEBHOOK_URL = os.environ["WEBHOOK_URL"].rstrip("/")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEBHOOK_PATH = "/webhook"
FULL_WEBHOOK_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

app = Flask(__name__)


def tg_api(method: str, payload: dict | None = None):
    url = f"{API_BASE}/{method}"
    try:
        r = requests.post(url, json=payload or {}, timeout=30)
        logger.info("Telegram API %s -> %s", method, r.text[:500])
        return r
    except Exception:
        logger.exception("Telegram API call failed: %s", method)
        raise


def send_message(chat_id: int, text: str):
    return tg_api("sendMessage", {"chat_id": chat_id, "text": text})


def set_webhook():
    try:
        tg_api("deleteWebhook", {"drop_pending_updates": True})
        tg_api("setWebhook", {"url": FULL_WEBHOOK_URL})
        info = requests.get(f"{API_BASE}/getWebhookInfo", timeout=30)
        logger.info("Webhook info: %s", info.text)
    except Exception:
        logger.exception("Failed to set webhook")


@app.get("/")
def home():
    return "Webhook bot is running!"


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "mode": "webhook",
            "webhook_url": FULL_WEBHOOK_URL,
        }
    )


@app.post(WEBHOOK_PATH)
def webhook():
    data = request.get_json(silent=True) or {}
    logger.info("Incoming update: %s", str(data)[:2000])

    message = data.get("message") or data.get("edited_message")
    if not message:
        return jsonify({"ok": True, "ignored": True})

    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    text = message.get("text", "")

    if not chat_id:
        return jsonify({"ok": True, "ignored": True})

    if text == "/start":
        send_message(
            chat_id,
            "Webhook bot is alive.\n\nCommands:\n/start\n/ping",
        )
    elif text == "/ping":
        send_message(chat_id, "pong")
    else:
        send_message(chat_id, f"got: {text or '[non-text]'}")

    return jsonify({"ok": True})


def boot():
    set_webhook()


if __name__ == "__main__":
    threading.Thread(target=boot, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)