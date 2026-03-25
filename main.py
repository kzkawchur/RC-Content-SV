import asyncio
import logging
import os
import shutil
import threading
from urllib.parse import urlparse

import requests
import yt_dlp
from flask import Flask
from pyrogram import Client as PyroClient
import pyrogram.errors as pyro_errors
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, MediaStream

if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MUSO_V3")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ.get("SESSION_STRING") or os.environ.get("STRING_SESSION")

if not SESSION_STRING:
    raise RuntimeError("Missing SESSION_STRING or STRING_SESSION")

PORT = int(os.environ.get("PORT", 8080))
YT_USER_AGENT = os.environ.get(
    "YT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

RAW_COOKIES_FILE = os.environ.get("COOKIES_FILE", "/etc/secrets/cookies.txt")
RUNTIME_COOKIES_FILE = "/tmp/cookies.txt"

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "MUSO V3 is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok", "version": "MUSO_V3"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def delete_webhook():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": "true"}, timeout=20)
        logger.info("deleteWebhook response: %s", r.text)
    except Exception:
        logger.exception("Failed to delete webhook")

def prepare_cookies_file():
    try:
        if os.path.exists(RAW_COOKIES_FILE):
            shutil.copyfile(RAW_COOKIES_FILE, RUNTIME_COOKIES_FILE)
            logger.info("Copied cookies file to writable path: %s", RUNTIME_COOKIES_FILE)
        else:
            logger.warning("Cookies source file not found: %s", RAW_COOKIES_FILE)
    except Exception:
        logger.exception("Failed to prepare cookies file")

def is_url(text: str) -> bool:
    try:
        p = urlparse(text.strip())
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False

def is_group_chat(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in ("group", "supergroup"))

def build_ydl_opts() -> dict:
    return {
        "quiet": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,
        "default_search": "ytsearch1",
        "nocheckcertificate": True,
        "geo_bypass": True,
        "geo_bypass_country": "US",
        "cookiefile": RUNTIME_COOKIES_FILE if os.path.exists(RUNTIME_COOKIES_FILE) else None,
        "http_headers": {
            "User-Agent": YT_USER_AGENT
        },
        "extractor_args": {
            "youtube": {
                "player_client": ["android"],
                "player_skip": ["configs"],
            }
        },
        "format": "bestaudio[acodec!=none]/best[acodec!=none]/best",
    }

def extract_audio_info(query: str) -> dict:
    search_term = query.strip()
    if not is_url(search_term):
        search_term = f"ytsearch1:{search_term}"

    with yt_dlp.YoutubeDL(build_ydl_opts()) as ydl:
        info = ydl.extract_info(search_term, download=False)

        if not info:
            raise ValueError("No results found.")

        if "entries" in info:
            entries = info.get("entries") or []
            if not entries:
                raise ValueError("No results found.")
            info = entries[0]

        title = info.get("title") or "Unknown Title"
        webpage_url = info.get("webpage_url") or info.get("original_url") or query

        formats = info.get("formats") or []
        stream_url = None

        for f in formats:
            if not f.get("url"):
                continue
            if f.get("acodec") in (None, "none"):
                continue
            stream_url = f["url"]
            break

        if not stream_url and info.get("url") and info.get("acodec") not in (None, "none"):
            stream_url = info["url"]

        if not stream_url:
            raise ValueError("MUSO V3 could not extract a playable audio stream.")

        return {
            "title": title,
            "webpage_url": webpage_url,
            "stream_url": stream_url,
        }

voice_user = PyroClient(
    "voice-user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
)

call_py = PyTgCalls(voice_user)
tg_app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "MUSO V3 is online.\n\n"
        "Commands:\n"
        "/ping\n"
        "/play <song name or YouTube link>\n"
        "/stop"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text("pong - MUSO V3")

async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /play command only works in groups."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n/play <song name or YouTube link>"
        )
        return

    chat_id = update.effective_chat.id
    query = " ".join(context.args).strip()
    status = await update.effective_message.reply_text("MUSO V3: Searching...")

    try:
        info = await asyncio.to_thread(extract_audio_info, query)

        try:
            await call_py.leave_call(chat_id)
        except Exception:
            pass

        await status.edit_text("MUSO V3: Starting voice chat stream...")

        await call_py.play(
            chat_id,
            MediaStream(
                info["stream_url"],
                audio_quality=AudioQuality.LOW,
            ),
        )

        await status.edit_text(
            f"MUSO V3: Now playing\n{info['title']}\n{info['webpage_url']}"
        )

    except Exception as e:
        logger.exception("play_cmd failed")
        await status.edit_text(f"MUSO V3 play failed:\n{e}")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /stop command only works in groups."
        )
        return

    try:
        await call_py.leave_call(update.effective_chat.id)
        await update.effective_message.reply_text("MUSO V3 stopped the stream.")
    except Exception as e:
        logger.exception("stop_cmd failed")
        await update.effective_message.reply_text(f"MUSO V3 stop failed:\n{e}")

async def setup_bot_commands(app: Application) -> None:
    await app.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("ping", "Health check"),
        BotCommand("play", "Play music in voice chat"),
        BotCommand("stop", "Stop the current stream"),
    ])

async def main() -> None:
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    delete_webhook()
    prepare_cookies_file()

    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("ping", ping_cmd))
    tg_app.add_handler(CommandHandler("play", play_cmd))
    tg_app.add_handler(CommandHandler("stop", stop_cmd))

    await voice_user.start()
    logger.info("Voice user started")

    await call_py.start()
    logger.info("PyTgCalls started")

    async with tg_app:
        await setup_bot_commands(tg_app)
        await tg_app.start()
        await tg_app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message"],
        )

        me_bot = await tg_app.bot.get_me()
        logger.info("Bot logged in as: @%s", me_bot.username)
        logger.info("MUSO V3 fully running")

        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())