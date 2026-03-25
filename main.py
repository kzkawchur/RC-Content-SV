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

# py-tgcalls compatibility shim
if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MUSO_DOCKER_FINAL")

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
    return "MUSO Docker Final is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok", "mode": "docker-final"}

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
            os.chmod(RUNTIME_COOKIES_FILE, 0o600)
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

def base_ydl_opts() -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
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
        "youtube_include_dash_manifest": True,
        "youtube_include_hls_manifest": True,
    }

def choose_best_audio_format(info: dict) -> str | None:
    formats = info.get("formats") or []
    candidates = []

    for f in formats:
        url = f.get("url")
        if not url:
            continue

        acodec = f.get("acodec")
        if acodec in (None, "none"):
            continue

        vcodec = f.get("vcodec") or ""
        protocol = (f.get("protocol") or "").lower()
        ext = (f.get("ext") or "").lower()
        abr = f.get("abr") or 0
        asr = f.get("asr") or 0
        filesize = f.get("filesize") or 0

        audio_only = 3 if vcodec == "none" else 1
        proto_score = 3 if protocol in ("https", "http") else 2 if "m3u8" in protocol else 1
        ext_score = 3 if ext in ("m4a", "webm", "mp4") else 1

        score = (audio_only, proto_score, ext_score, abr, asr, filesize)
        candidates.append((score, url))

    if not candidates and info.get("url") and info.get("acodec") not in (None, "none"):
        return info["url"]

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def extract_audio_info(query: str) -> dict:
    search_term = query.strip()

    # Step 1: resolve search query into a YouTube URL
    if is_url(search_term):
        video_url = search_term
    else:
        search_opts = base_ydl_opts()
        search_opts["extractor_args"] = {
            "youtube": {
                "player_client": ["android", "web"],
                "player_skip": ["configs"],
            }
        }

        with yt_dlp.YoutubeDL(search_opts) as ydl:
            info = ydl.extract_info(f"ytsearch1:{search_term}", download=False)
            if not info or "entries" not in info or not info["entries"]:
                raise ValueError("No results found.")
            first = info["entries"][0]
            video_url = (
                first.get("webpage_url")
                or first.get("original_url")
                or first.get("url")
            )

        if not video_url:
            raise ValueError("Could not resolve the search query into a YouTube URL.")

    # Step 2: try multiple YouTube client profiles
    attempt_clients = [
        ["android", "web", "ios"],
        ["tv_embedded", "android"],
        ["web"],
    ]

    errors = []

    for clients in attempt_clients:
        try:
            opts = base_ydl_opts()
            opts["extractor_args"] = {
                "youtube": {
                    "player_client": clients,
                    "player_skip": ["configs"],
                }
            }

            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(video_url, download=False)

            if not info:
                raise ValueError("No media information found.")

            title = info.get("title") or "Unknown Title"
            webpage_url = info.get("webpage_url") or info.get("original_url") or video_url

            stream_url = choose_best_audio_format(info)
            if not stream_url:
                raise ValueError("No playable audio format found.")

            return {
                "title": title,
                "webpage_url": webpage_url,
                "stream_url": stream_url,
            }

        except Exception as e:
            errors.append(f"{clients}: {e}")

    raise ValueError("YouTube extraction failed. " + " | ".join(errors[-2:]))

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
        "Hello! I am MUSO Docker Final.\n\n"
        "Commands:\n"
        "/ping\n"
        "/play <song name or YouTube link>\n"
        "/stop\n\n"
        "Tips:\n"
        "- Direct YouTube links are usually more reliable than search.\n"
        "- Start a voice chat before using /play.\n"
        "- Make the bot an admin.\n"
        "- Make sure the user session account is also in the group."
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text("pong - MUSO Docker Final")

async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /play command can only be used in a group or supergroup."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n/play <song name or YouTube link>"
        )
        return

    chat_id = update.effective_chat.id
    query = " ".join(context.args).strip()
    status = await update.effective_message.reply_text("Searching YouTube...")

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
        BotCommand("play", "Play music in voice chat"),
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
        logger.info("MUSO Docker Final fully running")

        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())