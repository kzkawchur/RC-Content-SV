import asyncio
import logging
import os
import threading
from contextlib import suppress
from urllib.parse import parse_qs, urlparse

from flask import Flask
from pyrogram import Client, filters
from pyrogram.errors import ChatAdminRequired, FloodWait
from pyrogram.types import Message
from pytgcalls import PyTgCalls, idle
from pytgcalls.types import AudioQuality, MediaStream
import yt_dlp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("music-bot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]
PORT = int(os.environ.get("PORT", 8080))
COOKIES_FILE = os.environ.get("COOKIES_FILE", "cookies.txt")
YT_USER_AGENT = os.environ.get(
    "YT_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

# Lower resource usage for small free instances.
BOT_WORKERS = int(os.environ.get("BOT_WORKERS", "4"))

bot = Client(
    "music_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=BOT_WORKERS,
    in_memory=True,
)

user = Client(
    "music_user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
    workers=1,
)

calls = PyTgCalls(user)
chat_locks: dict[int, asyncio.Lock] = {}
current_tracks: dict[int, dict] = {}

web_app = Flask(__name__)


@web_app.get("/")
def root():
    return {
        "ok": True,
        "service": "Telegram Music Bot",
        "status": "running",
    }


@web_app.get("/healthz")
def healthz():
    return {"ok": True}


@web_app.get("/ping")
def ping():
    return "pong", 200


def run_web_server() -> None:
    web_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


def get_lock(chat_id: int) -> asyncio.Lock:
    if chat_id not in chat_locks:
        chat_locks[chat_id] = asyncio.Lock()
    return chat_locks[chat_id]


def is_youtube_url(text: str) -> bool:
    lowered = text.lower()
    return any(
        domain in lowered
        for domain in (
            "youtube.com/watch",
            "youtu.be/",
            "youtube.com/shorts/",
            "youtube.com/live/",
            "music.youtube.com/",
        )
    )


def normalize_youtube_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower()

    if "youtu.be" in host:
        video_id = parsed.path.strip("/")
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    if parsed.path.startswith("/shorts/"):
        video_id = parsed.path.split("/shorts/", 1)[1].split("/", 1)[0]
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    if parsed.path.startswith("/live/"):
        video_id = parsed.path.split("/live/", 1)[1].split("/", 1)[0]
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    query = parse_qs(parsed.query)
    video_id = query.get("v", [None])[0]
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"

    return url


class YTDLPSourceError(Exception):
    pass


def build_ytdlp_options() -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "extract_flat": False,
        "default_search": "ytsearch1",
        "source_address": "0.0.0.0",
        "geo_bypass": True,
        "nocheckcertificate": True,
        "http_headers": {
            "User-Agent": YT_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        },
        # Lower bandwidth and memory pressure on free hosting.
        "format": "bestaudio[acodec=opus]/bestaudio[ext=webm]/bestaudio/best",
        "extractor_args": {
            "youtube": {
                "player_client": ["web"],
                "player_skip": ["configs"],
            }
        },
    }

    if os.path.isfile(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
        logger.info("Using cookies file: %s", COOKIES_FILE)
    else:
        logger.warning("cookies.txt not found at %s; continuing without cookies", COOKIES_FILE)

    return opts


def resolve_source(query_or_url: str) -> dict:
    target = normalize_youtube_url(query_or_url) if is_youtube_url(query_or_url) else query_or_url
    ydl_opts = build_ytdlp_options()

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info(target, download=False)
        except Exception as exc:
            raise YTDLPSourceError(f"yt-dlp failed: {exc}") from exc

    if info is None:
        raise YTDLPSourceError("No result found.")

    if "entries" in info:
        entries = [entry for entry in info.get("entries", []) if entry]
        if not entries:
            raise YTDLPSourceError("No search result found.")
        info = entries[0]

    stream_url = info.get("url")
    webpage_url = info.get("webpage_url") or info.get("original_url") or target
    title = info.get("title") or "Unknown title"
    duration = info.get("duration")
    is_live = bool(info.get("is_live"))

    if not stream_url:
        formats = info.get("formats") or []
        for fmt in formats:
            candidate = fmt.get("url")
            if candidate and fmt.get("acodec") != "none":
                stream_url = candidate
                break

    if not stream_url:
        raise YTDLPSourceError("No playable audio stream URL found.")

    return {
        "title": title,
        "stream_url": stream_url,
        "webpage_url": webpage_url,
        "duration": duration,
        "is_live": is_live,
    }


def format_duration(seconds: int | None) -> str:
    if not seconds:
        return "Live/Unknown"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


async def ensure_voice_chat_permissions(message: Message) -> None:
    chat_member = await bot.get_chat_member(message.chat.id, "me")
    user_member = await user.get_chat_member(message.chat.id, "me")

    if not chat_member.privileges:
        raise PermissionError(
            "Bot must be an admin in this chat. Give it permission to delete/manage voice chats if needed."
        )

    if user_member.status not in {"administrator", "member", "owner"}:
        raise PermissionError("The SESSION_STRING account must be in the chat/group.")


@bot.on_message(filters.command("start") & ~filters.private)
async def start_group_handler(_: Client, message: Message):
    await message.reply_text(
        "**Music Bot is ready.**\n\n"
        "Commands:\n"
        "`/play <YouTube link or search query>`\n"
        "`/stop`\n\n"
        "Requirements:\n"
        "• Start a Telegram Voice Chat in the group first\n"
        "• Add both the bot and the SESSION_STRING account to the group\n"
        "• Make the bot admin"
    )


@bot.on_message(filters.command("start") & filters.private)
async def start_private_handler(_: Client, message: Message):
    await message.reply_text(
        "Send commands inside your group or supergroup voice chat.\n\n"
        "Use:\n"
        "`/play <YouTube link or search query>`\n"
        "`/stop`"
    )


@bot.on_message(filters.command("play") & ~filters.private)
async def play_handler(_: Client, message: Message):
    chat_id = message.chat.id
    query = message.text.split(maxsplit=1)[1].strip() if len(message.command) > 1 else ""

    if not query:
        return await message.reply_text("Usage: `/play <YouTube link or search query>`", quote=True)

    status = await message.reply_text("🔎 Searching and preparing stream...", quote=True)

    async with get_lock(chat_id):
        try:
            await ensure_voice_chat_permissions(message)
            source = await asyncio.to_thread(resolve_source, query)

            media = MediaStream(
                source["stream_url"],
                audio_quality=AudioQuality.LOW,
            )

            # Replaces any currently active stream in this chat.
            await calls.play(chat_id, media)
            current_tracks[chat_id] = source

            text = (
                "▶️ **Now streaming**\n"
                f"**Title:** {source['title']}\n"
                f"**Duration:** {format_duration(source['duration'])}\n"
                f"**Source:** {source['webpage_url']}"
            )
            await status.edit_text(text, disable_web_page_preview=True)

        except FloodWait as e:
            await status.edit_text(f"Telegram flood wait. Retry after {e.value} seconds.")
        except PermissionError as e:
            await status.edit_text(str(e))
        except ChatAdminRequired:
            await status.edit_text("Bot needs admin rights in this chat.")
        except Exception as e:
            logger.exception("Play failed")
            await status.edit_text(
                "❌ Failed to start streaming.\n\n"
                f"Reason: `{type(e).__name__}: {e}`\n\n"
                "Tips:\n"
                "• Make sure a Voice Chat is already running\n"
                "• Make sure the SESSION_STRING account is inside the group\n"
                "• Check cookies.txt if YouTube gives 403"
            )


@bot.on_message(filters.command("stop") & ~filters.private)
async def stop_handler(_: Client, message: Message):
    chat_id = message.chat.id

    async with get_lock(chat_id):
        try:
            await calls.leave_call(chat_id)
            current_tracks.pop(chat_id, None)
            await message.reply_text("⏹️ Stopped streaming and left the voice chat.")
        except Exception as e:
            logger.exception("Stop failed")
            await message.reply_text(f"Nothing was playing or stop failed: `{type(e).__name__}: {e}`")


async def start_services() -> None:
    await bot.start()
    logger.info("Bot client started")

    await user.start()
    logger.info("User client started")

    await calls.start()
    logger.info("PyTgCalls started")


async def stop_services() -> None:
    with suppress(Exception):
        for chat_id in list(current_tracks.keys()):
            with suppress(Exception):
                await calls.leave_call(chat_id)

    with suppress(Exception):
        await calls.stop()
    with suppress(Exception):
        await user.stop()
    with suppress(Exception):
        await bot.stop()


async def main() -> None:
    threading.Thread(target=run_web_server, daemon=True).start()
    logger.info("Flask keep-alive server started on port %s", PORT)

    try:
        await start_services()
        logger.info("All services started successfully")
        await idle()
    finally:
        await stop_services()


if __name__ == "__main__":
    asyncio.run(main())
