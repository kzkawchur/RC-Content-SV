import asyncio

# Pyrogram startup fix
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import logging
import os
import threading
from urllib.parse import urlparse

from flask import Flask
from pyrogram import Client, filters
import pyrogram.errors as pyro_errors
from pyrogram.errors import FloodWait
from pyrogram.types import Message

# Compatibility shim for py-tgcalls -> pyrogram error import mismatch
if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

from pytgcalls import PyTgCalls, idle
from pytgcalls.types import AudioQuality, MediaStream
import yt_dlp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("MusicBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ.get("SESSION_STRING") or os.environ.get("STRING_SESSION")

if not SESSION_STRING:
    raise RuntimeError("Missing SESSION_STRING or STRING_SESSION")

PORT = int(os.environ.get("PORT", 8080))
COOKIES_FILE = os.environ.get("COOKIES_FILE", "cookies.txt")
YT_USER_AGENT = os.environ.get(
    "YT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Telegram Music Bot is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

bot = Client(
    "music-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=int(os.environ.get("BOT_WORKERS", 4)),
)

user = Client(
    "music-user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
)

call_py = PyTgCalls(user)
ACTIVE_STREAMS = {}

def is_url(text: str) -> bool:
    try:
        parsed = urlparse(text.strip())
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

def build_ydl_opts():
    return {
        "format": "bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,
        "default_search": "ytsearch1",
        "nocheckcertificate": True,
        "geo_bypass": True,
        "geo_bypass_country": "US",
        "cookiefile": COOKIES_FILE if os.path.exists(COOKIES_FILE) else None,
        "http_headers": {
            "User-Agent": YT_USER_AGENT
        },
    }

def extract_audio_info(query: str):
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
        webpage_url = info.get("webpage_url") or info.get("url")

        if webpage_url and webpage_url != info.get("url"):
            info = ydl.extract_info(webpage_url, download=False)

        stream_url = info.get("url")
        webpage_url = info.get("webpage_url") or webpage_url

        if not stream_url:
            raise ValueError("Could not extract audio stream URL.")

        return {
            "title": title,
            "webpage_url": webpage_url,
            "stream_url": stream_url,
        }

@bot.on_message(filters.command("start"))
async def start_cmd(_, message: Message):
    await message.reply_text(
        "🎵 **Telegram Music Bot is alive!**\n\n"
        "**Commands:**\n"
        "`/play <YouTube link or search>`\n"
        "`/stop`\n\n"
        "Bot and user account must both be in the group.\n"
        "Voice chat must be started first."
    )

@bot.on_message(filters.command("play") & (filters.group | filters.supergroup))
async def play_cmd(_, message: Message):
    if len(message.command) < 2:
        await message.reply_text("Usage:\n`/play <YouTube link or search>`")
        return

    query = message.text.split(None, 1)[1].strip()
    chat_id = message.chat.id
    status = await message.reply_text("🔎 Searching YouTube...")

    try:
        info = await asyncio.to_thread(extract_audio_info, query)

        try:
            await call_py.leave_call(chat_id)
        except Exception:
            pass

        await status.edit_text("⏳ Starting stream in voice chat...")

        await call_py.play(
            chat_id,
            MediaStream(
                info["stream_url"],
                audio_quality=AudioQuality.LOW,
            ),
        )

        ACTIVE_STREAMS[chat_id] = {
            "title": info["title"],
            "url": info["webpage_url"],
        }

        await status.edit_text(
            f"▶️ **Now Playing:** {info['title']}\n"
            f"🔗 {info['webpage_url']}"
        )

    except FloodWait as e:
        await status.edit_text(f"⏳ Flood wait: {e.value} seconds.")
    except Exception as e:
        logger.exception("Play command failed")
        await status.edit_text(f"❌ Failed to play.\n`{e}`")

@bot.on_message(filters.command("stop") & (filters.group | filters.supergroup))
async def stop_cmd(_, message: Message):
    chat_id = message.chat.id
    try:
        await call_py.leave_call(chat_id)
        ACTIVE_STREAMS.pop(chat_id, None)
        await message.reply_text("⏹️ Stopped streaming and left the voice chat.")
    except Exception as e:
        logger.exception("Stop command failed")
        await message.reply_text(f"❌ Failed to stop.\n`{e}`")

async def main():
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask keep-alive server started on port %s", PORT)

    await bot.start()
    logger.info("Bot client started")

    await user.start()
    logger.info("User client started")

    await call_py.start()
    logger.info("PyTgCalls started")

    me_bot = await bot.get_me()
    me_user = await user.get_me()

    logger.info("Bot logged in as: @%s", me_bot.username)
    logger.info("User logged in as: %s", me_user.first_name)

    await idle()

if __name__ == "__main__":
    asyncio.run(main())