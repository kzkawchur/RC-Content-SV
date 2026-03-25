import asyncio
import logging
import os
import re
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from flask import Flask
from pyrogram import Client as PyroClient
import pyrogram.errors as pyro_errors
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioQuality, MediaStream
from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# -----------------------------
# Compatibility shim
# -----------------------------
if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("LibraryMusicBot")

# -----------------------------
# Env
# -----------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ.get("SESSION_STRING") or os.environ.get("STRING_SESSION")

if not SESSION_STRING:
    raise RuntimeError("Missing SESSION_STRING or STRING_SESSION")

PORT = int(os.environ.get("PORT", 8080))
DB_PATH = os.environ.get("DB_PATH", "music_library.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/music_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Flask keep-alive
# -----------------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Telegram Library Music Bot is running!"

@flask_app.get("/health")
def health():
    return {"status": "ok", "mode": "library"}

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

# -----------------------------
# DB
# -----------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                file_id TEXT NOT NULL,
                original_name TEXT,
                mime_type TEXT,
                added_by INTEGER,
                created_at INTEGER NOT NULL
            )
        """)
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
        conn.execute("""
            INSERT OR REPLACE INTO songs (name, file_id, original_name, mime_type, added_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (song_name, file_id, original_name, mime_type, added_by, int(time.time())))
        conn.commit()

def get_song(name: str) -> Optional[sqlite3.Row]:
    song_name = normalize_name(name)
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM songs WHERE name = ?",
            (song_name,)
        ).fetchone()
        return row

def search_songs(keyword: str, limit: int = 10):
    q = f"%{normalize_name(keyword)}%"
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM songs WHERE name LIKE ? ORDER BY name ASC LIMIT ?",
            (q, limit)
        ).fetchall()
        return rows

def list_songs(limit: int = 50):
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM songs ORDER BY name ASC LIMIT ?",
            (limit,)
        ).fetchall()
        return rows

def delete_song(name: str) -> bool:
    song_name = normalize_name(name)
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM songs WHERE name = ?", (song_name,))
        conn.commit()
        return cur.rowcount > 0

# -----------------------------
# Helpers
# -----------------------------
def delete_webhook():
    import requests
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook"
        r = requests.get(url, params={"drop_pending_updates": "true"}, timeout=20)
        logger.info("deleteWebhook response: %s", r.text)
    except Exception:
        logger.exception("Failed to delete webhook")

def is_group_chat(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in ("group", "supergroup"))

def is_private_chat(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type == "private")

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
        if "x-m4a" in mime_type or "m4a" in mime_type or "mp4" in mime_type:
            return ".m4a"
        if "flac" in mime_type:
            return ".flac"

    return ".mp3"

def replied_audio_message(update: Update):
    msg = update.effective_message
    if not msg or not msg.reply_to_message:
        return None

    rep = msg.reply_to_message
    if rep.audio:
        return rep.audio, "audio"
    if rep.document and rep.document.mime_type and rep.document.mime_type.startswith("audio/"):
        return rep.document, "document"
    return None

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

ACTIVE_STREAMS: dict[int, dict] = {}

async def cleanup_chat_file(chat_id: int):
    info = ACTIVE_STREAMS.get(chat_id)
    if not info:
        return
    path = info.get("local_path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            logger.exception("Failed to remove temp file: %s", path)

# -----------------------------
# Bot side
# -----------------------------
tg_app: Application = ApplicationBuilder().token(BOT_TOKEN).build()

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "Hello! I am your Telegram Library Music Bot.\n\n"
        "Main commands:\n"
        "/ping\n"
        "/addsong <name>  (reply to an audio file in private chat)\n"
        "/play <name>\n"
        "/stop\n"
        "/listsongs\n"
        "/searchsong <keyword>\n"
        "/delsong <name>\n\n"
        "How to use:\n"
        "1. Send or forward an audio file to me in private chat\n"
        "2. Reply to that file with /addsong <name>\n"
        "3. In your group, start voice chat and use /play <name>\n\n"
        "Requirements for group playback:\n"
        "- Voice chat must already be started\n"
        "- Bot should be admin\n"
        "- The user session account must be inside the group"
    )

async def ping_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text("pong")

async def addsong_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        await update.effective_message.reply_text(
            "Use /addsong only in private chat with the bot."
        )
        return

    if not context.args:
        await update.effective_message.reply_text(
            "Usage:\n/addsong <name>\n\nReply to an audio file with this command."
        )
        return

    replied = replied_audio_message(update)
    if not replied:
        await update.effective_message.reply_text(
            "Reply to an audio file or audio document with /addsong <name>."
        )
        return

    media, media_type = replied
    song_name = " ".join(context.args).strip()
    if not song_name:
        await update.effective_message.reply_text("Please provide a valid song name.")
        return

    file_id = media.file_id
    original_name = getattr(media, "file_name", None)
    mime_type = getattr(media, "mime_type", None)
    added_by = update.effective_user.id if update.effective_user else None

    add_song_to_db(song_name, file_id, original_name, mime_type, added_by)

    await update.effective_message.reply_text(
        f"Saved successfully.\nName: {normalize_name(song_name)}\nType: {media_type}"
    )

async def listsongs_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    rows = list_songs(limit=50)
    if not rows:
        await update.effective_message.reply_text("No songs saved yet.")
        return

    text = "Saved songs:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
    await update.effective_message.reply_text(text)

async def searchsong_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.effective_message.reply_text("Usage:\n/searchsong <keyword>")
        return

    keyword = " ".join(context.args).strip()
    rows = search_songs(keyword)
    if not rows:
        await update.effective_message.reply_text("No matching songs found.")
        return

    text = "Search results:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
    await update.effective_message.reply_text(text)

async def delsong_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        await update.effective_message.reply_text(
            "Use /delsong only in private chat."
        )
        return

    if not context.args:
        await update.effective_message.reply_text("Usage:\n/delsong <name>")
        return

    name = " ".join(context.args).strip()
    ok = delete_song(name)
    if ok:
        await update.effective_message.reply_text(f"Deleted: {normalize_name(name)}")
    else:
        await update.effective_message.reply_text("Song not found.")

async def play_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /play command can only be used in a group or supergroup."
        )
        return

    if not context.args:
        await update.effective_message.reply_text("Usage:\n/play <saved song name>")
        return

    name = " ".join(context.args).strip()
    row = get_song(name)

    if not row:
        similar = search_songs(name, limit=5)
        if similar:
            text = "Song not found. Similar songs:\n\n" + "\n".join(f"- {r['name']}" for r in similar)
            await update.effective_message.reply_text(text)
        else:
            await update.effective_message.reply_text("Song not found in library.")
        return

    chat_id = update.effective_chat.id
    status = await update.effective_message.reply_text(f"Preparing: {row['name']}")

    ext = safe_file_ext(row["original_name"], row["mime_type"])
    local_path = TMP_DIR / f"{chat_id}_{int(time.time())}{ext}"

    try:
        file_obj = await context.bot.get_file(row["file_id"])
        await file_obj.download_to_drive(custom_path=str(local_path))

        try:
            await call_py.leave_call(chat_id)
            await cleanup_chat_file(chat_id)
        except Exception:
            pass

        await status.edit_text("Starting voice chat stream...")

        await call_py.play(
            chat_id,
            MediaStream(
                str(local_path),
                audio_quality=AudioQuality.LOW,
            ),
        )

        ACTIVE_STREAMS[chat_id] = {
            "name": row["name"],
            "local_path": str(local_path),
        }

        await status.edit_text(f"Now playing: {row['name']}")

    except Exception as e:
        logger.exception("play_cmd failed")
        try:
            if local_path.exists():
                local_path.unlink(missing_ok=True)
        except Exception:
            pass
        await status.edit_text(f"Play failed:\n{e}")

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /stop command can only be used in a group or supergroup."
        )
        return

    chat_id = update.effective_chat.id

    try:
        await call_py.leave_call(chat_id)
    except Exception:
        logger.exception("leave_call failed")

    await cleanup_chat_file(chat_id)
    ACTIVE_STREAMS.pop(chat_id, None)
    await update.effective_message.reply_text("Stopped the stream.")

async def nowplaying_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_group_chat(update):
        await update.effective_message.reply_text(
            "The /nowplaying command can only be used in a group or supergroup."
        )
        return

    info = ACTIVE_STREAMS.get(update.effective_chat.id)
    if not info:
        await update.effective_message.reply_text("Nothing is playing right now.")
        return

    await update.effective_message.reply_text(f"Now playing: {info['name']}")

async def setup_bot_commands(app: Application) -> None:
    await app.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("ping", "Health check"),
        BotCommand("addsong", "Reply to an audio file and save it"),
        BotCommand("play", "Play a saved song in voice chat"),
        BotCommand("stop", "Stop the current stream"),
        BotCommand("listsongs", "Show saved songs"),
        BotCommand("searchsong", "Search saved songs"),
        BotCommand("delsong", "Delete a saved song"),
        BotCommand("nowplaying", "Show current song"),
    ])

# -----------------------------
# Main
# -----------------------------
async def main() -> None:
    init_db()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    delete_webhook()

    tg_app.add_handler(CommandHandler("start", start_cmd))
    tg_app.add_handler(CommandHandler("ping", ping_cmd))
    tg_app.add_handler(CommandHandler("addsong", addsong_cmd))
    tg_app.add_handler(CommandHandler("listsongs", listsongs_cmd))
    tg_app.add_handler(CommandHandler("searchsong", searchsong_cmd))
    tg_app.add_handler(CommandHandler("delsong", delsong_cmd))
    tg_app.add_handler(CommandHandler("play", play_cmd))
    tg_app.add_handler(CommandHandler("stop", stop_cmd))
    tg_app.add_handler(CommandHandler("nowplaying", nowplaying_cmd))

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
        logger.info("Library Music Bot fully running")

        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
