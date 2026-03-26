import asyncio
import logging
import os
import random
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import edge_tts
from flask import Flask, jsonify
from pyrogram import Client, filters
from pyrogram.types import Message
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("SweetWelcomeVoiceBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", 8080))

DB_PATH = os.environ.get("DB_PATH", "welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/welcome_voice_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

VOICE_NAME = os.environ.get("VOICE_NAME", "bn-BD-NabanitaNeural")
VOICE_RATE = os.environ.get("VOICE_RATE", "-18%")
VOICE_PITCH = os.environ.get("VOICE_PITCH", "+3Hz")
VOICE_VOLUME = os.environ.get("VOICE_VOLUME", "+8%")
TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME", "Asia/Dhaka")

WELCOME_DELETE_AFTER = int(os.environ.get("WELCOME_DELETE_AFTER", "90"))
JOIN_COOLDOWN_SECONDS = int(os.environ.get("JOIN_COOLDOWN_SECONDS", "18"))
REJOIN_IGNORE_SECONDS = int(os.environ.get("REJOIN_IGNORE_SECONDS", "300"))

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "Sweet Welcome Voice Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "bot": "sweet_welcome_voice_bot"})

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

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
                last_voice_message_id INTEGER,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS join_memory (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, user_id)
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
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO welcome_state (chat_id, last_text_message_id, last_voice_message_id, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                last_text_message_id = excluded.last_text_message_id,
                last_voice_message_id = excluded.last_voice_message_id,
                updated_at = excluded.updated_at
            """,
            (chat_id, text_message_id, voice_message_id, now_ts),
        )
        conn.commit()

def get_last_join_time(chat_id: int, user_id: int) -> int:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT joined_at FROM join_memory WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        ).fetchone()
        return int(row["joined_at"]) if row else 0

def save_join_time(chat_id: int, user_id: int) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO join_memory (chat_id, user_id, joined_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET joined_at = excluded.joined_at
            """,
            (chat_id, user_id, now_ts),
        )
        conn.commit()

app = Client(
    "sweet-welcome-voice-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

chat_last_welcome_ts: dict[int, float] = {}

def clean_name(name: str) -> str:
    if not name:
        return "বন্ধু"
    return name.replace("\n", " ").strip()[:40]

def get_local_time() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE_NAME))

def get_day_phase() -> str:
    hour = get_local_time().hour
    if 5 <= hour < 12:
        return "morning"
    if 12 <= hour < 17:
        return "day"
    if 17 <= hour < 21:
        return "evening"
    return "night"

def build_welcome_text(first_name: str, mention_name: str) -> tuple[str, str]:
    phase = get_day_phase()

    text_templates = {
        "morning": [
            f"🌼 শুভ সকাল {mention_name}!\nআমাদের গ্রুপে তোমাকে স্বাগতম।",
            f"✨ {mention_name}, সকালের সুন্দর শুভেচ্ছা। গ্রুপে তোমাকে পেয়ে ভালো লাগছে।",
        ],
        "day": [
            f"🌸 স্বাগতম {mention_name}!\nআমাদের গ্রুপে তোমাকে পেয়ে খুব ভালো লাগছে।",
            f"💫 হ্যালো {mention_name}!\nতোমাকে আমাদের গ্রুপে আন্তরিক স্বাগতম।",
        ],
        "evening": [
            f"🌙 শুভ সন্ধ্যা {mention_name}!\nগ্রুপে তোমাকে স্বাগতম।",
            f"✨ {mention_name}, সন্ধ্যার মিষ্টি শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।",
        ],
        "night": [
            f"🌌 শুভ রাত্রি {mention_name}!\nআমাদের গ্রুপে তোমাকে স্বাগতম।",
            f"💙 {mention_name}, রাতের শান্ত শুভেচ্ছা। গ্রুপে তোমাকে পেয়ে ভালো লাগছে।",
        ],
    }

    voice_templates = {
        "morning": [
            f"{first_name}, শুভ সকাল। আমাদের গ্রুপে তোমাকে আন্তরিক স্বাগতম।",
            f"হ্যালো {first_name}, সকালের সুন্দর শুভেচ্ছা। তোমাকে পেয়ে খুব ভালো লাগছে।",
        ],
        "day": [
            f"{first_name}, তোমাকে আমাদের গ্রুপে আন্তরিক স্বাগতম। তোমাকে পেয়ে খুব ভালো লাগছে।",
            f"হ্যালো {first_name}, আমাদের গ্রুপে তোমাকে পেয়ে সত্যিই ভালো লাগছে। স্বাগতম।",
        ],
        "evening": [
            f"{first_name}, শুভ সন্ধ্যা। আমাদের গ্রুপে তোমাকে স্বাগতম। আশা করি এখানে ভালো সময় কাটাবে।",
            f"হ্যালো {first_name}, সন্ধ্যার মিষ্টি শুভেচ্ছা। গ্রুপে তোমাকে পেয়ে ভালো লাগছে।",
        ],
        "night": [
            f"{first_name}, শুভ রাত্রি। আমাদের গ্রুপে তোমাকে আন্তরিক স্বাগতম।",
            f"হ্যালো {first_name}, রাতের শান্ত শুভেচ্ছা। গ্রুপে তোমাকে পেয়ে ভালো লাগছে।",
        ],
    }

    return random.choice(text_templates[phase]), random.choice(voice_templates[phase])

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

async def schedule_delete_message(client: Client, chat_id: int, message_id: int, delay: int) -> None:
    try:
        await asyncio.sleep(delay)
        await client.delete_messages(chat_id, message_id)
    except Exception:
        logger.exception("Failed auto-deleting message %s in chat %s", message_id, chat_id)

async def make_voice_file(text: str, output_path: Path) -> None:
    communicate = edge_tts.Communicate(
        text=text,
        voice=VOICE_NAME,
        rate=VOICE_RATE,
        pitch=VOICE_PITCH,
        volume=VOICE_VOLUME,
    )
    await communicate.save(str(output_path))

def should_skip_for_spam(chat_id: int, user_id: int) -> bool:
    now_ts = time.time()
    last_chat_ts = chat_last_welcome_ts.get(chat_id, 0)
    if now_ts - last_chat_ts < JOIN_COOLDOWN_SECONDS:
        return True
    last_user_ts = get_last_join_time(chat_id, user_id)
    if now_ts - last_user_ts < REJOIN_IGNORE_SECONDS:
        return True
    return False

def mark_welcomed(chat_id: int, user_id: int) -> None:
    chat_last_welcome_ts[chat_id] = time.time()
    save_join_time(chat_id, user_id)

@app.on_message(filters.new_chat_members)
async def welcome_new_members(client: Client, message: Message):
    if not message.new_chat_members:
        return

    me = await client.get_me()
    target_member = None
    for member in message.new_chat_members:
        if member.is_bot and member.id == me.id:
            continue
        if not member.is_bot:
            target_member = member

    if target_member is None:
        return

    chat_id = message.chat.id
    user_id = target_member.id
    first_name = clean_name(target_member.first_name)
    mention_name = target_member.mention(first_name)

    if should_skip_for_spam(chat_id, user_id):
        logger.info("Skipped welcome due to anti-spam rules | chat_id=%s user_id=%s", chat_id, user_id)
        return

    text_welcome, voice_text = build_welcome_text(first_name, mention_name)
    await delete_previous_welcome(client, chat_id)

    sent_text = None
    sent_voice = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user_id}_{int(time.time())}.mp3"

    try:
        sent_text = await message.reply_text(
            text_welcome,
            disable_web_page_preview=True
        )

        await make_voice_file(voice_text, voice_path)

        sent_voice = await client.send_voice(
            chat_id=chat_id,
            voice=str(voice_path),
            reply_to_message_id=message.id,
            caption=f"🎤 Welcome voice for {first_name}",
        )

        mark_welcomed(chat_id, user_id)

        save_last_welcome(
            chat_id=chat_id,
            text_message_id=sent_text.id if sent_text else None,
            voice_message_id=sent_voice.id if sent_voice else None,
        )

        if sent_text:
            asyncio.create_task(schedule_delete_message(client, chat_id, sent_text.id, WELCOME_DELETE_AFTER))
        if sent_voice:
            asyncio.create_task(schedule_delete_message(client, chat_id, sent_voice.id, WELCOME_DELETE_AFTER))

    except Exception:
        logger.exception("Failed welcome flow in chat %s for user %s", chat_id, user_id)
    finally:
        try:
            if voice_path.exists():
                voice_path.unlink()
        except Exception:
            logger.exception("Failed removing temp voice file")

@app.on_message(filters.command("start"))
async def start_cmd(_, message: Message):
    await message.reply_text(
        "Sweet Welcome Voice Bot is alive.\n\n"
        "নতুন member join করলে আমি:\n"
        "- নাম ধরে সুন্দর welcome দেব\n"
        "- Bangla sweet female voice পাঠাব\n"
        "- আগের welcome delete করব\n"
        "- spam কমানোর জন্য cooldown রাখব\n"
        "- কিছুক্ষণ পরে welcome auto-delete করব"
    )

@app.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    now_local = get_local_time().strftime("%I:%M %p")
    await message.reply_text(f"pong | {TIMEZONE_NAME} | {now_local}")

@app.on_message(filters.command("testwelcome"))
async def testwelcome_cmd(_, message: Message):
    first_name = clean_name(message.from_user.first_name if message.from_user else "বন্ধু")
    mention_name = message.from_user.mention(first_name) if message.from_user else first_name
    text_welcome, voice_text = build_welcome_text(first_name, mention_name)

    sent_text = await message.reply_text(text_welcome)
    voice_path = TMP_DIR / f"test_{message.chat.id}_{int(time.time())}.mp3"

    try:
        await make_voice_file(voice_text, voice_path)
        sent_voice = await app.send_voice(
            chat_id=message.chat.id,
            voice=str(voice_path),
            reply_to_message_id=message.id,
            caption="🎤 Test welcome voice",
        )
        asyncio.create_task(schedule_delete_message(app, message.chat.id, sent_text.id, WELCOME_DELETE_AFTER))
        asyncio.create_task(schedule_delete_message(app, message.chat.id, sent_voice.id, WELCOME_DELETE_AFTER))
    finally:
        try:
            if voice_path.exists():
                voice_path.unlink()
        except Exception:
            logger.exception("Failed removing temp test file")

def main():
    init_db()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    logger.info("Starting Sweet Welcome Voice Bot")
    app.run()

if __name__ == "__main__":
    main()
