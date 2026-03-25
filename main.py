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
logger = logging.getLogger("MUSO_SIMPLE")

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

# -----------------------------
# Flask keep-alive
# -----------------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "MUSO Simple Bot is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok", "mode": "simple"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

# -----------------------------
# Helpers
# -----------------------------
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

def is_youtube_url(text: str) -> bool:
    try:
        p = urlparse(text.strip())
        host = (p.netloc or "").lower()
        return "youtube.com" in host or "youtu.be" in host
    except Exception:
        return False

# -----------------------------
# yt-dlp
# -----------------------------
def build_ydl_opts() -> dict:
    return {
        "quiet": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,
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
    }

def extract_audio_info(youtube_url: str) -> dict:
    with yt_dlp.YoutubeDL(build_ydl_opts()) as ydl:
        info = ydl.extract_info(youtube_url, download=False)

        if not info:
            raise ValueError("No media information found.")

        title = info.get("title") or "Unknown Title"
        webpage_url = info.get("webpage_url") or info.get("original_url") or youtube_url

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
            raise ValueError(
                "Could not extract playable audio from this YouTube link. "
                "Try another direct YouTube video link."
            )

        return {
            "title": title,
            "webpage_url": webpage_url,
            "stream_url": stream_url,
        }

# -----------------------------
# Voice side
# -----------------------------
voice_user = PyroClient(
    "voice-user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
)

call_py = PyTgCalls(voice_user)

# -----------------------------
# Bot side
# -----------------------------
tg_app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Hello! I am MUSO Simple.\n\n"
        "Commands:\n"
        "/ping\n"
        "/play <direct YouTube link>\n"
        "/stop\n\n"
        "Important:\n"
        "- /play currently supports direct YouTube links only\n"
        "- Start a voice chat before using /play\n"
        "- Make the bot an admin\n"
        "- Make sure the user session account is also in the group"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text("pong - MUSO Simple")

async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /play command can only be used in a group or supergroup."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n/play <direct YouTube link>"
        )
        return

    query = " ".join(context.args).strip()
    if not is_url(query) or not is_youtube_url(query):
        await update.effective_message.reply_text(
            "Please send a direct YouTube video link.\n\n"
            "Example:\n"
            "/play https://youtu.be/xxxxxxxxxxx"
        )
        return

    chat_id = update.effective_chat.id
    status = await update.effective_message.reply_text("Checking YouTube link...")

    try:
        info = await asyncio.to_thread(extract_audio_info, query)

        try:
            await call_py.leave_call(chat_id)
        except Exception:
            pass

        await status.edit_text("Starting voice chat stream...")

        await call_py.play(
            chat_id,
            MediaStream(
                info["stream_url"],
                audio_quality=AudioQuality.LOW,
            ),
        )

        await status.edit_text(
            f"Now playing:\n{info['title']}\n{info['webpage_url']}"
        )

    except Exception as e:
        logger.exception("play_cmd failed")
        await status.edit_text(f"Play failed:\n{e}")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /stop command can only be used in a group or supergroup."
        )
        return

    try:
        await call_py.leave_call(update.effective_chat.id)
        await update.effective_message.reply_text("Stopped the stream.")
    except Exception as e:
        logger.exception("stop_cmd failed")
        await update.effective_message.reply_text(f"Stop failed:\n{e}")

async def setup_bot_commands(app: Application) -> None:
    await app.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("ping", "Health check"),
        BotCommand("play", "Play direct YouTube link"),
        BotCommand("stop", "Stop the current stream"),
    ])

# -----------------------------
# Main
# -----------------------------
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
        logger.info("MUSO Simple fully running")

        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())