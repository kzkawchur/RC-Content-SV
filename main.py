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
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ResetBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
PORT = int(os.environ.get("PORT", 8080))

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Reset bot running"

@flask_app.get("/health")
def health():
    return {"status": "ok"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": "true"}, timeout=20)
        logger.info("deleteWebhook response: %s", r.text)
    except Exception:
        logger.exception("deleteWebhook failed")

bot = Client(
    "reset-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

@bot.on_message(filters.private & ~filters.service)
async def private_debug(client: Client, message: Message):
    logger.info("PRIVATE UPDATE | chat_id=%s | text=%s", message.chat.id, message.text)
    if message.text == "/start":
        await message.reply_text("reset ok")
    else:
        await message.reply_text("got your message")

async def main():
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("Flask started on port %s", PORT)

    delete_webhook()

    await bot.start()
    me = await bot.get_me()
    logger.info("Bot logged in as: @%s", me.username)

    await idle()

if __name__ == "__main__":
    asyncio.run(main())