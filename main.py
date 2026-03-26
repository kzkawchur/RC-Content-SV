import asyncio
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, Any

import requests
from flask import Flask, jsonify
import pyrogram.errors as pyro_errors
from pyrogram import Client, filters, idle
from pyrogram.types import BotCommand, Message
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream

if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("PollingLibraryMusicBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
SESSION_STRING = (os.environ.get("SESSION_STRING") or os.environ.get("STRING_SESSION") or "").strip()
PORT = int(os.environ.get("PORT", 8080))

if not SESSION_STRING:
    raise RuntimeError("Missing SESSION_STRING or STRING_SESSION")

DB_PATH = os.environ.get("DB_PATH", "music_library.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/music_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Polling Library Music Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "mode": "polling"})

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def delete_webhook() -> None:
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": "true"}, timeout=30)
        logger.info("deleteWebhook response: %s", r.text[:700])
    except Exception:
        logger.exception("deleteWebhook failed")

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with db_connect() as conn:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                file_id TEXT NOT NULL,
                original_name TEXT,
                mime_type TEXT,
                added_by INTEGER,
                created_at INTEGER NOT NULL
            )
            '''
        )
        conn.commit()

def normalize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", " ", name)
    return name

def add_song_to_db(
    name: str,
    file_id: str,
    original_name: Optional[str],
    mime_type: Optional[str],
    added_by: Optional[int],
) -> None:
    song_name = normalize_name(name)
    with db_connect() as conn:
        conn.execute(
            '''
            INSERT OR REPLACE INTO songs (name, file_id, original_name, mime_type, added_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (song_name, file_id, original_name, mime_type, added_by, int(time.time())),
        )
        conn.commit()

def get_song(name: str) -> Optional[sqlite3.Row]:
    song_name = normalize_name(name)
    with db_connect() as conn:
        return conn.execute("SELECT * FROM songs WHERE name = ?", (song_name,)).fetchone()

def search_songs(keyword: str, limit: int = 10):
    q = f"%{normalize_name(keyword)}%"
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM songs WHERE name LIKE ? ORDER BY name ASC LIMIT ?",
            (q, limit),
        ).fetchall()

def list_songs(limit: int = 50):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM songs ORDER BY name ASC LIMIT ?",
            (limit,),
        ).fetchall()

def delete_song(name: str) -> bool:
    song_name = normalize_name(name)
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM songs WHERE name = ?", (song_name,))
        conn.commit()
        return cur.rowcount > 0

def is_private_chat(message: Message) -> bool:
    return bool(message.chat and message.chat.type == "private")

def is_group_chat(message: Message) -> bool:
    return bool(message.chat and message.chat.type in ("group", "supergroup"))

def split_long_text(text: str, limit: int = 3500) -> list[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip()
    if text:
        parts.append(text)
    return parts

def safe_file_ext(original_name: Optional[str], mime_type: Optional[str]) -> str:
    if original_name and "." in original_name:
        ext = Path(original_name).suffix.lower()
        if ext:
            return ext
    if mime_type:
        if "mpeg" in mime_type or "mp3" in mime_type:
            return ".mp3"
        if "ogg" in mime_type:
            return ".ogg"
        if "aac" in mime_type:
            return ".aac"
        if "wav" in mime_type:
            return ".wav"
        if "m4a" in mime_type or "mp4" in mime_type:
            return ".m4a"
        if "flac" in mime_type:
            return ".flac"
        if "video/" in mime_type:
            return ".mp4"
    return ".mp3"

def replied_media(message: Message):
    if not message.reply_to_message:
        return None
    rep = message.reply_to_message
    if rep.audio:
        return rep.audio, "audio"
    if rep.voice:
        return rep.voice, "voice"
    if rep.video:
        return rep.video, "video"
    if rep.document and rep.document.mime_type:
        mt = rep.document.mime_type
        if mt.startswith("audio/") or mt.startswith("video/"):
            return rep.document, "document"
    return None

bot = Client(
    "bot-client",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

user = Client(
    "voice-user",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
)

call_py = PyTgCalls(user)
ACTIVE_STREAMS: dict[int, dict[str, Any]] = {}

async def cleanup_chat_file(chat_id: int) -> None:
    info = ACTIVE_STREAMS.get(chat_id)
    if not info:
        return
    path = info.get("local_path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
            logger.info("Removed temp file: %s", path)
        except Exception:
            logger.exception("Failed to remove temp file: %s", path)

async def resolve_voice_chat_id(chat_id: int) -> int:
    chat_id = int(chat_id)
    try:
        resolved_chat = await user.get_chat(chat_id)
        logger.info("Resolved voice peer directly: raw=%s resolved=%s", chat_id, resolved_chat.id)
        return resolved_chat.id
    except Exception:
        logger.exception("Direct get_chat failed, trying dialogs scan")
    async for dialog in user.get_dialogs(limit=500):
        try:
            if int(dialog.chat.id) == chat_id:
                logger.info("Resolved voice peer from dialogs: raw=%s resolved=%s", chat_id, dialog.chat.id)
                return int(dialog.chat.id)
        except Exception:
            continue
    raise RuntimeError(f"Could not resolve peer for chat id {chat_id}")

async def play_saved_song(chat_id: int, song_name: str, status_message: Message) -> None:
    try:
        chat_id = await resolve_voice_chat_id(chat_id)
    except Exception as e:
        logger.exception("Failed to resolve peer")
        await status_message.edit_text(
            f"Play failed:\nCould not resolve this group for the session account.\n{e}"
        )
        return

    row = get_song(song_name)
    if not row:
        await status_message.edit_text("Song not found in library.")
        return

    ext = safe_file_ext(row["original_name"], row["mime_type"])
    local_path = TMP_DIR / f"{abs(chat_id)}_{int(time.time())}{ext}"

    try:
        await status_message.edit_text(f"Preparing: {row['name']}")
        await bot.download_media(row["file_id"], file_name=str(local_path))

        try:
            await call_py.leave_call(chat_id)
        except Exception:
            pass

        await cleanup_chat_file(chat_id)

        await status_message.edit_text("Starting voice chat stream...")
        await call_py.play(chat_id, MediaStream(str(local_path)))

        ACTIVE_STREAMS[chat_id] = {
            "name": row["name"],
            "local_path": str(local_path),
        }

        await status_message.edit_text(f"Now playing: {row['name']}")
    except Exception as e:
        logger.exception("play_saved_song failed")
        try:
            if local_path.exists():
                local_path.unlink(missing_ok=True)
        except Exception:
            pass
        await status_message.edit_text(f"Play failed:\n{e}")

async def stop_current_stream(chat_id: int) -> None:
    try:
        chat_id = await resolve_voice_chat_id(chat_id)
    except Exception:
        logger.exception("Failed to resolve peer for stop")

    try:
        await call_py.leave_call(chat_id)
    except Exception:
        logger.exception("leave_call failed")

    await cleanup_chat_file(chat_id)
    ACTIVE_STREAMS.pop(chat_id, None)

@bot.on_message(filters.command("start"))
async def start_cmd(_, message: Message):
    await message.reply_text(
        "Polling Library Music Bot is alive.\n\n"
        "Commands:\n"
        "/start\n"
        "/ping\n"
        "/addsong <name>\n"
        "/listsongs\n"
        "/searchsong <keyword>\n"
        "/delsong <name>\n"
        "/play <name>\n"
        "/stop\n"
        "/nowplaying\n\n"
        "How to use:\n"
        "1. Send or forward an audio/video file to the bot in private chat\n"
        "2. Reply to that file with /addsong <name>\n"
        "3. In your group, start voice chat and use /play <name>"
    )

@bot.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    await message.reply_text("pong")

@bot.on_message(filters.command("addsong"))
async def addsong_cmd(_, message: Message):
    if not is_private_chat(message):
        await message.reply_text("Use /addsong only in private chat.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage:\n/addsong <name>")
        return

    replied = replied_media(message)
    if not replied:
        await message.reply_text(
            "Reply to an audio, voice, video, or audio/video document with /addsong <name>."
        )
        return

    media, media_type = replied
    song_name = " ".join(message.command[1:]).strip()
    if not song_name:
        await message.reply_text("Please provide a valid song name.")
        return

    file_id = media.file_id
    original_name = getattr(media, "file_name", None)
    mime_type = getattr(media, "mime_type", None)
    added_by = message.from_user.id if message.from_user else None

    add_song_to_db(song_name, file_id, original_name, mime_type, added_by)
    await message.reply_text(
        f"Saved successfully.\nName: {normalize_name(song_name)}\nType: {media_type}"
    )

@bot.on_message(filters.command("listsongs"))
async def listsongs_cmd(_, message: Message):
    if not is_private_chat(message):
        await message.reply_text("Use /listsongs in private chat.")
        return

    rows = list_songs(limit=100)
    if not rows:
        await message.reply_text("No songs saved yet.")
        return

    text = "Saved songs:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
    for part in split_long_text(text):
        await message.reply_text(part)

@bot.on_message(filters.command("searchsong"))
async def searchsong_cmd(_, message: Message):
    if not is_private_chat(message):
        await message.reply_text("Use /searchsong in private chat.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage:\n/searchsong <keyword>")
        return

    keyword = " ".join(message.command[1:]).strip()
    rows = search_songs(keyword, limit=20)
    if not rows:
        await message.reply_text("No matching songs found.")
        return

    text = "Search results:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
    await message.reply_text(text)

@bot.on_message(filters.command("delsong"))
async def delsong_cmd(_, message: Message):
    if not is_private_chat(message):
        await message.reply_text("Use /delsong only in private chat.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage:\n/delsong <name>")
        return

    name = " ".join(message.command[1:]).strip()
    ok = delete_song(name)
    if ok:
        await message.reply_text(f"Deleted: {normalize_name(name)}")
    else:
        await message.reply_text("Song not found.")

@bot.on_message(filters.command("nowplaying"))
async def nowplaying_cmd(_, message: Message):
    if not is_group_chat(message):
        await message.reply_text("Use /nowplaying in a group voice chat.")
        return

    info = ACTIVE_STREAMS.get(message.chat.id)
    if not info:
        await message.reply_text("Nothing is playing right now.")
        return

    await message.reply_text(f"Now playing: {info['name']}")

@bot.on_message(filters.command("play"))
async def play_cmd(_, message: Message):
    if not is_group_chat(message):
        await message.reply_text("Use /play in a group or supergroup.")
        return

    if len(message.command) < 2:
        await message.reply_text("Usage:\n/play <saved song name>")
        return

    name = " ".join(message.command[1:]).strip()
    status = await message.reply_text("Queued...")
    asyncio.create_task(play_saved_song(message.chat.id, name, status))

@bot.on_message(filters.command("stop"))
async def stop_cmd(_, message: Message):
    if not is_group_chat(message):
        await message.reply_text("Use /stop in a group or supergroup.")
        return

    await stop_current_stream(message.chat.id)
    await message.reply_text("Stopped the stream.")

async def main():
    init_db()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    delete_webhook()

    await user.start()
    logger.info("User client started")

    loaded = 0
    async for _ in user.get_dialogs(limit=300):
        loaded += 1
    logger.info("Preloaded dialogs: %s", loaded)

    await call_py.start()
    logger.info("PyTgCalls started")

    await bot.start()
    logger.info("Bot client started")

    try:
        await bot.set_bot_commands([
            BotCommand("start", "Start the bot"),
            BotCommand("ping", "Health check"),
            BotCommand("addsong", "Reply to media and save it"),
            BotCommand("listsongs", "Show saved songs"),
            BotCommand("searchsong", "Search saved songs"),
            BotCommand("delsong", "Delete a saved song"),
            BotCommand("play", "Play a saved song"),
            BotCommand("stop", "Stop current stream"),
            BotCommand("nowplaying", "Show current song"),
        ])
    except Exception:
        logger.exception("Failed to set bot commands")

    me_bot = await bot.get_me()
    me_user = await user.get_me()
    logger.info("Bot logged in as: @%s", me_bot.username)
    logger.info("User logged in as: %s", me_user.first_name)
    logger.info("Polling music bot is running")

    await idle()

    await bot.stop()
    await call_py.stop()
    await user.stop()

if __name__ == "__main__":
    asyncio.run(main())
