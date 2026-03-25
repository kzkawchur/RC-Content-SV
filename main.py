import asyncio
import logging
import os
import threading

import requests
from flask import Flask
from pyrogram import Client, filters, idle
from pyrogram.types import Message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("TestBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 8080))

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Test bot is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": "true"}, timeout=20)
        logger.info("deleteWebhook: %s", r.text)
    except Exception:
        logger.exception("deleteWebhook failed")

app = Client(
    "render-test-bot-v1",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
    workers=1,
)

@app.on_message(filters.incoming)
async def debug_all(_, message: Message):
    text = message.text or message.caption or "<non-text>"
    logger.info(
        "RECEIVED | chat_id=%s | chat_type=%s | from_user=%s | text=%s",
        message.chat.id,
        message.chat.type,
        getattr(message.from_user, "id", None),
        text
    )

@app.on_message(filters.command("start"))
async def start_cmd(_, message: Message):
    await message.reply_text("Bot is alive.")

@app.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    await message.reply_text("pong")

async def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    delete_webhook()

    await app.start()
    me = await app.get_me()
    logger.info("Bot logged in as: @%s", me.username)
    logger.info("Test bot fully running")

    await idle()

if __name__ == "__main__":
    asyncio.run(main())