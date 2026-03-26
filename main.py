import logging
import os
import threading
import time
from typing import Any

import requests
from flask import Flask, jsonify, request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("RenderWebhookBot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", 8080))
WEBHOOK_URL = os.environ["WEBHOOK_URL"].strip().rstrip("/")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEBHOOK_PATH = "/telegram/webhook"
FULL_WEBHOOK_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

app = Flask(__name__)


def tg_get(method: str, params: dict[str, Any] | None = None) -> requests.Response:
    url = f"{API_BASE}/{method}"
    r = requests.get(url, params=params or {}, timeout=30)
    logger.info("Telegram GET %s -> %s", method, r.text[:700])
    return r


def tg_post(method: str, payload: dict[str, Any] | None = None) -> requests.Response:
    url = f"{API_BASE}/{method}"
    r = requests.post(url, json=payload or {}, timeout=30)
    logger.info("Telegram POST %s -> %s", method, r.text[:700])
    return r


def send_message(chat_id: int, text: str) -> None:
    try:
        tg_post("sendMessage", {"chat_id": chat_id, "text": text})
    except Exception:
        logger.exception("send_message failed")


def setup_commands() -> None:
    try:
        tg_post(
            "setMyCommands",
            {
                "commands": [
                    {"command": "start", "description": "Start the bot"},
                    {"command": "ping", "description": "Health check"},
                ]
            },
        )
    except Exception:
        logger.exception("setMyCommands failed")


def delete_webhook() -> None:
    try:
        tg_post("deleteWebhook", {"drop_pending_updates": True})
    except Exception:
        logger.exception("deleteWebhook failed")


def set_webhook_once() -> bool:
    try:
        delete_webhook()
        time.sleep(1)

        resp = tg_post(
            "setWebhook",
            {
                "url": FULL_WEBHOOK_URL,
                "allowed_updates": ["message", "edited_message"],
            },
        )
        ok = resp.ok and '"ok":true' in resp.text.replace(" ", "").lower()

        info = tg_get("getWebhookInfo")
        logger.info("Webhook info after set: %s", info.text[:1000])

        if ok:
            setup_commands()
            return True
        return False
    except Exception:
        logger.exception("set_webhook_once failed")
        return False


def setup_webhook_with_retry() -> None:
    logger.info("Starting webhook setup. Target: %s", FULL_WEBHOOK_URL)

    # Render public URL sometimes needs a little time to become reachable.
    for attempt in range(1, 13):
        logger.info("Webhook setup attempt %s/12", attempt)
        if set_webhook_once():
            logger.info("Webhook setup succeeded")
            return
        time.sleep(5)

    logger.error("Webhook setup failed after all retries")


@app.get("/")
def home():
    return "Webhook bot is running"


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "mode": "webhook",
            "webhook_url": FULL_WEBHOOK_URL,
        }
    )


@app.get("/setup-webhook")
def manual_setup_webhook():
    ok = set_webhook_once()
    return jsonify({"ok": ok, "webhook_url": FULL_WEBHOOK_URL})


@app.get("/webhook-info")
def webhook_info():
    try:
        r = tg_get("getWebhookInfo")
        return app.response_class(
            response=r.text,
            status=r.status_code,
            mimetype="application/json",
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post(WEBHOOK_PATH)
def telegram_webhook():
    data = request.get_json(silent=True) or {}
    logger.info("Incoming update: %s", str(data)[:3000])

    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return jsonify({"ok": True, "ignored": "no-message"})

    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = msg.get("text", "")

    if not chat_id:
        return jsonify({"ok": True, "ignored": "no-chat-id"})

    if text == "/start":
        send_message(
            chat_id,
            "Webhook bot is alive.\n\nCommands:\n/start\n/ping\n\nSend any normal text and I will echo it back.",
        )
    elif text == "/ping":
        send_message(chat_id, "pong")
    elif text:
        send_message(chat_id, f"got: {text}")
    else:
        send_message(chat_id, "I received a non-text message.")

    return jsonify({"ok": True})


if __name__ == "__main__":
    threading.Thread(target=setup_webhook_with_retry, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)