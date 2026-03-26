import asyncio
import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional

import edge_tts
from flask import Flask, jsonify
from pyrogram import Client, filters
from pyrogram.types import Message

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("WelcomeVoiceBot")

# -----------------------------
# Env
# -----------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", 8080))

DB_PATH = os.environ.get("DB_PATH", "welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/welcome_voice_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

VOICE_NAME = os.environ.get("VOICE_NAME", "bn-BD-NabanitaNeural")

# -----------------------------
# Flask health server for Render
# -----------------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Welcome Voice Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "bot": "welcome_voice_bot"})

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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS welcome_state (
                chat_id INTEGER PRIMARY KEY,
                last_text_message_id INTEGER,
                last_voice_message_id INTEGER
            )
            """
        )
        conn.commit()

def get_last_welcome(chat_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM welcome_state WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()

def save_last_welcome(chat_id: int, text_message_id: Optional[int], voice_message_id: Optional[int]) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO welcome_state (chat_id, last_text_message_id, last_voice_message_id)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                last_text_message_id = excluded.last_text_message_id,
                last_voice_message_id = excluded.last_voice_message_id
            """,
            (chat_id, text_message_id, voice_message_id),
        )
        conn.commit()

# -----------------------------
# Bot client
# -----------------------------
app = Client(
    "welcome-voice-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# -----------------------------
# Helpers
# -----------------------------
async def delete_previous_welcome(client: Client, chat_id: int) -> None:
    row = get_last_welcome(chat_id)
    if not row:
        return

    for mid in (row["last_text_message_id"], row["last_voice_message_id"]):
        if mid:
            try:
                await client.delete_messages(chat_id, mid)
            except Exception:
                logger.exception("Failed deleting previous welcome message %s in chat %s", mid, chat_id)

async def make_voice_file(text: str, output_path: Path) -> None:
    communicate = edge_tts.Communicate(text=text, voice=VOICE_NAME)
    await communicate.save(str(output_path))

def clean_name(name: str) -> str:
    return (name or "বন্ধু").replace("\n", " ").strip()[:50]

# -----------------------------
# Welcome handler
# -----------------------------
@app.on_message(filters.new_chat_members)
async def welcome_new_members(client: Client, message: Message):
    if not message.new_chat_members:
        return

    chat_id = message.chat.id

    for member in message.new_chat_members:
        # Bot নিজে join করলে ignore
        if member.is_bot:
            me = await client.get_me()
            if member.id == me.id:
                continue

        first_name = clean_name(member.first_name)
        mention_name = member.mention(first_name)

        # আগের welcome delete
        await delete_previous_welcome(client, chat_id)

        welcome_text = (
            f"🌸 স্বাগতম {mention_name}!\n"
            f"তোমাকে আমাদের গ্রুপে পেয়ে খুব ভালো লাগছে।"
        )

        voice_text = (
            f"{first_name}, তোমাকে আমাদের গ্রুপে স্বাগতম। "
            f"তোমাকে পেয়ে খুব ভালো লাগছে।"
        )

        # text welcome
        sent_text = await message.reply_text(
            welcome_text,
            disable_web_page_preview=True
        )

        # voice welcome
        voice_path = TMP_DIR / f"welcome_{chat_id}_{member.id}.mp3"
        sent_voice = None

        try:
            await make_voice_file(voice_text, voice_path)
            sent_voice = await client.send_voice(
                chat_id=chat_id,
                voice=str(voice_path),
                reply_to_message_id=message.id,
                caption=f"🎤 Voice welcome for {first_name}"
            )
        except Exception:
            logger.exception("Failed to generate/send voice welcome for user %s", member.id)
        finally:
            try:
                if voice_path.exists():
                    voice_path.unlink()
            except Exception:
                logger.exception("Failed to remove temp voice file")

        save_last_welcome(
            chat_id=chat_id,
            text_message_id=sent_text.id if sent_text else None,
            voice_message_id=sent_voice.id if sent_voice else None,
        )

# -----------------------------
# Simple commands
# -----------------------------
@app.on_message(filters.command("start"))
async def start_cmd(_, message: Message):
    await message.reply_text(
        "Welcome Voice Bot is alive.\n\n"
        "নতুন member join করলে আমি আগের welcome delete করে "
        "নতুন member-কে text + voice welcome দেব।"
    )

@app.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    await message.reply_text("pong")

# -----------------------------
# Main
# -----------------------------
def main():
    init_db()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    logger.info("Starting Welcome Voice Bot")
    app.run()

if __name__ == "__main__":
    main()