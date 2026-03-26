import asyncio
import logging
import os
import random
import sqlite3
import string
import threading
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import edge_tts
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFont
from pyrogram import Client, filters
from pyrogram.types import Message
from zoneinfo import ZoneInfo

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MayaWelcomeBot")

# -----------------------------
# Env
# -----------------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", 8080))

DB_PATH = os.environ.get("DB_PATH", "maya_welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/maya_welcome_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

# Voice tuning
VOICE_NAME = os.environ.get("VOICE_NAME", "bn-BD-NabanitaNeural")
VOICE_RATE = os.environ.get("VOICE_RATE", "-6%")
VOICE_PITCH = os.environ.get("VOICE_PITCH", "+0Hz")
VOICE_VOLUME = os.environ.get("VOICE_VOLUME", "+0%")

TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME", "Asia/Dhaka")

# Anti-spam / cleanup
WELCOME_DELETE_AFTER = int(os.environ.get("WELCOME_DELETE_AFTER", "90"))
JOIN_COOLDOWN_SECONDS = int(os.environ.get("JOIN_COOLDOWN_SECONDS", "15"))
REJOIN_IGNORE_SECONDS = int(os.environ.get("REJOIN_IGNORE_SECONDS", "300"))
SECRET_CODE_TTL_SECONDS = int(os.environ.get("SECRET_CODE_TTL_SECONDS", "600"))

# Broadcast permission: comma-separated Telegram user IDs
SUPER_ADMINS = {
    int(x.strip())
    for x in os.environ.get("SUPER_ADMINS", "").split(",")
    if x.strip().isdigit()
}

BOT_NAME = os.environ.get("BOT_NAME", "Maya")

# -----------------------------
# Flask health server for Render
# -----------------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return f"{BOT_NAME} Welcome Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "bot": BOT_NAME})

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
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                activated INTEGER NOT NULL DEFAULT 0,
                activated_by INTEGER,
                custom_welcome TEXT,
                voice_enabled INTEGER NOT NULL DEFAULT 1,
                delete_service INTEGER NOT NULL DEFAULT 1,
                last_primary_msg_id INTEGER,
                last_voice_msg_id INTEGER,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_codes (
                user_id INTEGER PRIMARY KEY,
                code TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                created_at INTEGER NOT NULL
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

def ensure_group(chat_id: int, title: str) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO groups (chat_id, title, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                updated_at = excluded.updated_at
            """,
            (chat_id, title or "", now_ts),
        )
        conn.commit()

def get_group(chat_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()

def activate_group(chat_id: int, title: str, activated_by: int) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO groups (chat_id, title, activated, activated_by, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                activated = 1,
                activated_by = excluded.activated_by,
                updated_at = excluded.updated_at
            """,
            (chat_id, title or "", activated_by, now_ts),
        )
        conn.commit()

def set_group_value(chat_id: int, field: str, value) -> None:
    allowed = {"custom_welcome", "voice_enabled", "delete_service", "last_primary_msg_id", "last_voice_msg_id", "updated_at", "title"}
    if field not in allowed:
        raise ValueError("Invalid field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()

def get_activated_groups() -> list[int]:
    with db_connect() as conn:
        rows = conn.execute("SELECT chat_id FROM groups WHERE activated = 1").fetchall()
        return [int(r["chat_id"]) for r in rows]

def set_admin_code(user_id: int, code: str, expires_at: int) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO admin_codes (user_id, code, expires_at, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                code = excluded.code,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at
            """,
            (user_id, code, expires_at, now_ts),
        )
        conn.commit()

def validate_admin_code(user_id: int, code: str) -> bool:
    now_ts = int(time.time())
    with db_connect() as conn:
        row = conn.execute(
            "SELECT code, expires_at FROM admin_codes WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not row:
            return False
        return row["code"] == code and int(row["expires_at"]) >= now_ts

def clear_admin_code(user_id: int) -> None:
    with db_connect() as conn:
        conn.execute("DELETE FROM admin_codes WHERE user_id = ?", (user_id,))
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

# -----------------------------
# Bot client
# -----------------------------
app = Client(
    "maya-welcome-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# -----------------------------
# Runtime state
# -----------------------------
chat_last_welcome_ts: dict[int, float] = {}

# -----------------------------
# Helpers
# -----------------------------
def clean_name(name: str) -> str:
    if not name:
        return "বন্ধু"
    return name.replace("\n", " ").strip()[:40]

def ascii_name(name: str) -> str:
    s = (name or "").encode("ascii", "ignore").decode().strip()
    return s[:18] if s else "FRIEND"

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

def build_welcome_copy(first_name: str, mention_name: str, group_title: str, custom_text: Optional[str]) -> tuple[str, str]:
    phase = get_day_phase()
    safe_group = group_title or "আমাদের গ্রুপ"

    if custom_text:
        text_welcome = (
            custom_text
            .replace("{name}", mention_name)
            .replace("{group}", safe_group)
            .replace("{phase}", phase)
        )
    else:
        templates = {
            "morning": [
                f"🌼 শুভ সকাল {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।",
                f"✨ {mention_name}, সকালের মিষ্টি শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
            ],
            "day": [
                f"🌸 স্বাগতম {mention_name}!\n{safe_group} এ তোমাকে পেয়ে খুব ভালো লাগছে।",
                f"💫 হ্যালো {mention_name}!\n{safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
            ],
            "evening": [
                f"🌙 শুভ সন্ধ্যা {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।",
                f"✨ {mention_name}, সন্ধ্যার সুন্দর শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
            ],
            "night": [
                f"🌌 শুভ রাত্রি {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।",
                f"💙 {mention_name}, রাতের শান্ত শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
            ],
        }
        text_welcome = random.choice(templates[phase])

    voice_templates = {
        "morning": [
            f"{first_name}, শুভ সকাল। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
            f"হ্যালো {first_name}, সকালের সুন্দর শুভেচ্ছা। তোমাকে পেয়ে খুব ভালো লাগছে।",
        ],
        "day": [
            f"{first_name}, তোমাকে {safe_group} এ আন্তরিক স্বাগতম। তোমাকে পেয়ে খুব ভালো লাগছে।",
            f"হ্যালো {first_name}, {safe_group} এ তোমাকে পেয়ে সত্যিই ভালো লাগছে। স্বাগতম।",
        ],
        "evening": [
            f"{first_name}, শুভ সন্ধ্যা। {safe_group} এ তোমাকে স্বাগতম। আশা করি এখানে ভালো সময় কাটাবে।",
            f"হ্যালো {first_name}, সন্ধ্যার মিষ্টি শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।",
        ],
        "night": [
            f"{first_name}, শুভ রাত্রি। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
            f"হ্যালো {first_name}, রাতের শান্ত শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।",
        ],
    }
    voice_text = random.choice(voice_templates[phase])
    return text_welcome, voice_text

def is_super_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in SUPER_ADMINS)

async def is_group_admin(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        status_text = getattr(member.status, "value", str(member.status)).lower()
        return any(x in status_text for x in ("administrator", "owner", "creator"))
    except Exception:
        logger.exception("Failed to check admin status for user %s in chat %s", user_id, chat_id)
        return False

def generate_secret_code(length: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))

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

async def delete_previous_welcome(client: Client, chat_id: int) -> None:
    group = get_group(chat_id)
    if not group:
        return
    for mid in (group["last_primary_msg_id"], group["last_voice_msg_id"]):
        if mid:
            try:
                await client.delete_messages(chat_id, int(mid))
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

def build_cover_bytes(first_name: str, group_title: str) -> BytesIO:
    width, height = 1280, 720
    phase = get_day_phase()

    palette = {
        "morning": ((255, 214, 102), (255, 122, 89)),
        "day": ((93, 224, 230), (88, 104, 245)),
        "evening": ((139, 92, 246), (236, 72, 153)),
        "night": ((30, 41, 59), (59, 130, 246)),
    }
    c1, c2 = palette[phase]

    img = Image.new("RGB", (width, height), c1)
    draw = ImageDraw.Draw(img)

    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(c1[0] * (1 - blend) + c2[0] * blend)
        g = int(c1[1] * (1 - blend) + c2[1] * blend)
        b = int(c1[2] * (1 - blend) + c2[2] * blend)
        draw.line((0, y, width, y), fill=(r, g, b))

    # Decorative circles
    draw.ellipse((65, 65, 245, 245), fill=(255, 255, 255))
    draw.ellipse((1040, 110, 1220, 290), fill=(255, 255, 255))
    draw.ellipse((965, 500, 1160, 695), fill=(255, 255, 255))

    # Dark glass panel
    draw.rounded_rectangle((90, 120, 1190, 610), radius=36, fill=(20, 20, 30))

    # Fonts
    title_font = ImageFont.load_default()
    mid_font = ImageFont.load_default()
    small_font = ImageFont.load_default()

    ascii_user = ascii_name(first_name)
    ascii_group = ascii_name(group_title or "GROUP")

    draw.text((150, 190), "WELCOME", fill=(255, 255, 255), font=title_font)
    draw.text((150, 290), ascii_user, fill=(255, 215, 120), font=mid_font)
    draw.text((150, 370), f"TO {ascii_group}", fill=(220, 220, 255), font=small_font)
    draw.text((150, 470), BOT_NAME.upper(), fill=(180, 255, 225), font=small_font)

    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

# -----------------------------
# Service message handler
# -----------------------------
@app.on_message(filters.service)
async def service_handler(client: Client, message: Message):
    chat = message.chat
    if not chat or chat.type not in ("group", "supergroup"):
        return

    ensure_group(chat.id, chat.title or "")

    group = get_group(chat.id)
    if not group or int(group["activated"]) != 1:
        return

    # Delete Telegram's default join/leave message if enabled
    if int(group["delete_service"]) == 1:
        try:
            await client.delete_messages(chat.id, message.id)
        except Exception:
            logger.exception("Failed deleting service message in chat %s", chat.id)

    # Join welcome
    if message.new_chat_members:
        me = await client.get_me()
        target_member = None

        for member in message.new_chat_members:
            if member.is_bot and member.id == me.id:
                continue
            if not member.is_bot:
                target_member = member

        if target_member is None:
            return

        user_id = target_member.id
        first_name = clean_name(target_member.first_name)
        mention_name = target_member.mention(first_name)

        if should_skip_for_spam(chat.id, user_id):
            logger.info("Skipped welcome due to anti-spam rules | chat_id=%s user_id=%s", chat.id, user_id)
            return

        await delete_previous_welcome(client, chat.id)

        text_welcome, voice_text = build_welcome_copy(
            first_name=first_name,
            mention_name=mention_name,
            group_title=chat.title or "আমাদের গ্রুপ",
            custom_text=group["custom_welcome"],
        )

        primary_message = None
        voice_message = None
        voice_path = TMP_DIR / f"welcome_{chat.id}_{user_id}_{int(time.time())}.mp3"

        try:
            cover = build_cover_bytes(first_name, chat.title or "GROUP")
            primary_message = await client.send_photo(
                chat_id=chat.id,
                photo=cover,
                caption=text_welcome,
            )

            if int(group["voice_enabled"]) == 1:
                await make_voice_file(voice_text, voice_path)
                voice_message = await client.send_voice(
                    chat_id=chat.id,
                    voice=str(voice_path),
                    caption=f"🎤 {BOT_NAME} welcome voice",
                )

            mark_welcomed(chat.id, user_id)

            set_group_value(chat.id, "last_primary_msg_id", primary_message.id if primary_message else None)
            set_group_value(chat.id, "last_voice_msg_id", voice_message.id if voice_message else None)
            set_group_value(chat.id, "updated_at", int(time.time()))

            if primary_message:
                asyncio.create_task(schedule_delete_message(client, chat.id, primary_message.id, WELCOME_DELETE_AFTER))
            if voice_message:
                asyncio.create_task(schedule_delete_message(client, chat.id, voice_message.id, WELCOME_DELETE_AFTER))

        except Exception:
            logger.exception("Failed welcome flow in chat %s for user %s", chat.id, user_id)
        finally:
            try:
                if voice_path.exists():
                    voice_path.unlink()
            except Exception:
                logger.exception("Failed removing temp voice file")

    # Leave event: only default service message delete, no extra chatter
    elif message.left_chat_member:
        logger.info("Member left in chat %s", chat.id)

# -----------------------------
# Commands
# -----------------------------
@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if message.chat.type == "private":
        code_help = (
            f"আমি {BOT_NAME} 🌸\n\n"
            "Main private commands:\n"
            "/getcode - group activation code নাও\n"
            "/ping - bot alive check\n"
            "/broadcast <text> - owner broadcast\n"
            "/myid - তোমার user id দেখাবে\n\n"
            "Group setup steps:\n"
            "1. আমাকে group-এ add করো\n"
            "2. আমাকে Delete Messages permission দাও\n"
            "3. personal chat-এ /getcode দাও\n"
            "4. group-এ /activate CODE দাও\n\n"
            "তারপর আমি join/leave service message delete করব,\n"
            "cover welcome + sweet voice দিব।"
        )
        await message.reply_text(code_help)
    else:
        await message.reply_text(
            f"{BOT_NAME} is here.\n"
            "এই group-এ use করতে:\n"
            "1. Admin personal chat-এ /getcode নেবে\n"
            "2. Group-এ /activate CODE দেবে"
        )

@app.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    now_local = get_local_time().strftime("%I:%M %p")
    await message.reply_text(f"pong | {TIMEZONE_NAME} | {now_local}")

@app.on_message(filters.command("myid"))
async def myid_cmd(_, message: Message):
    user_id = message.from_user.id if message.from_user else 0
    await message.reply_text(f"Your user ID: {user_id}")

@app.on_message(filters.command("getcode") & filters.private)
async def getcode_cmd(_, message: Message):
    if not message.from_user:
        return
    code = generate_secret_code()
    expires_at = int(time.time()) + SECRET_CODE_TTL_SECONDS
    set_admin_code(message.from_user.id, code, expires_at)
    mins = max(1, SECRET_CODE_TTL_SECONDS // 60)
    await message.reply_text(
        f"🔐 Secret activation code: `{code}`\n\n"
        f"এই code শুধু group admin use করবে.\n"
        f"Group-এ লিখবে:\n`/activate {code}`\n\n"
        f"Code {mins} মিনিট valid থাকবে."
    )

@app.on_message(filters.command("activate"))
async def activate_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /activate শুধু group-এ.")
        return

    if not message.from_user:
        return

    if len(message.command) < 2:
        await message.reply_text("Usage:\n/activate YOURCODE")
        return

    is_admin = await is_group_admin(client, message.chat.id, message.from_user.id)
    if not is_admin:
        await message.reply_text("Only group admins can activate me.")
        return

    code = message.command[1].strip().upper()
    if not validate_admin_code(message.from_user.id, code):
        await message.reply_text("Invalid or expired code. Personal chat-এ /getcode দাও.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    activate_group(message.chat.id, message.chat.title or "", message.from_user.id)
    clear_admin_code(message.from_user.id)

    await message.reply_text(
        f"✅ {BOT_NAME} activated successfully.\n\n"
        "এখন আমি:\n"
        "- join/leave service message delete করব\n"
        "- সুন্দর cover welcome পাঠাব\n"
        "- sweet voice welcome দেব\n"
        "- spam control maintain করব"
    )

@app.on_message(filters.command("voice"))
async def voice_toggle_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /voice in group.")
        return
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text("Only group admins can use this command.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text("এই group এখনো activated না. আগে /activate CODE দাও.")
        return

    if len(message.command) < 2:
        current = "ON" if int(group["voice_enabled"]) == 1 else "OFF"
        await message.reply_text(f"Usage:\n/voice on\n/voice off\n\nCurrent: {current}")
        return

    value = message.command[1].strip().lower()
    if value not in ("on", "off"):
        await message.reply_text("Usage:\n/voice on\n/voice off")
        return

    set_group_value(message.chat.id, "voice_enabled", 1 if value == "on" else 0)
    await message.reply_text(f"Voice welcome: {value.upper()}")

@app.on_message(filters.command("deleteservice"))
async def deleteservice_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /deleteservice in group.")
        return
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text("Only group admins can use this command.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text("এই group এখনো activated না. আগে /activate CODE দাও.")
        return

    if len(message.command) < 2:
        current = "ON" if int(group["delete_service"]) == 1 else "OFF"
        await message.reply_text(f"Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}")
        return

    value = message.command[1].strip().lower()
    if value not in ("on", "off"):
        await message.reply_text("Usage:\n/deleteservice on\n/deleteservice off")
        return

    set_group_value(message.chat.id, "delete_service", 1 if value == "on" else 0)
    await message.reply_text(f"Delete service message: {value.upper()}")

@app.on_message(filters.command("setwelcome"))
async def setwelcome_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /setwelcome in group.")
        return
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text("Only group admins can use this command.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text("এই group এখনো activated না. আগে /activate CODE দাও.")
        return

    text = message.text or ""
    parts = text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text(
            "Usage:\n/setwelcome your text\n\n"
            "Available placeholders:\n"
            "{name} = user mention\n"
            "{group} = group title\n"
            "{phase} = morning/day/evening/night"
        )
        return

    custom_text = parts[1].strip()[:600]
    set_group_value(message.chat.id, "custom_welcome", custom_text)
    await message.reply_text("Custom welcome text saved successfully.")

@app.on_message(filters.command("resetwelcome"))
async def resetwelcome_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /resetwelcome in group.")
        return
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text("Only group admins can use this command.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    set_group_value(message.chat.id, "custom_welcome", None)
    await message.reply_text("Custom welcome reset done.")

@app.on_message(filters.command("status"))
async def status_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.reply_text("Use /status in group.")
        return
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text("Only group admins can use this command.")
        return

    ensure_group(message.chat.id, message.chat.title or "")
    group = get_group(message.chat.id)
    if not group:
        await message.reply_text("No group config found.")
        return

    await message.reply_text(
        f"Bot name: {BOT_NAME}\n"
        f"Activated: {'YES' if int(group['activated']) == 1 else 'NO'}\n"
        f"Voice welcome: {'ON' if int(group['voice_enabled']) == 1 else 'OFF'}\n"
        f"Delete service message: {'ON' if int(group['delete_service']) == 1 else 'OFF'}\n"
        f"Timezone: {TIMEZONE_NAME}\n"
        f"Phase now: {get_day_phase()}"
    )

@app.on_message(filters.command("testwelcome"))
async def testwelcome_cmd(client: Client, message: Message):
    if message.chat.type not in ("group", "supergroup", "private"):
        return

    first_name = clean_name(message.from_user.first_name if message.from_user else "বন্ধু")
    mention_name = message.from_user.mention(first_name) if message.from_user else first_name
    text_welcome, voice_text = build_welcome_copy(
        first_name=first_name,
        mention_name=mention_name,
        group_title=message.chat.title if message.chat and message.chat.title else "আমাদের গ্রুপ",
        custom_text=None,
    )

    cover = build_cover_bytes(first_name, message.chat.title if message.chat else "GROUP")
    primary_message = await client.send_photo(
        chat_id=message.chat.id,
        photo=cover,
        caption=text_welcome,
    )

    voice_path = TMP_DIR / f"test_{message.chat.id}_{int(time.time())}.mp3"
    voice_message = None
    try:
        await make_voice_file(voice_text, voice_path)
        voice_message = await client.send_voice(
            chat_id=message.chat.id,
            voice=str(voice_path),
            caption=f"🎤 {BOT_NAME} test voice",
        )
    except Exception:
        logger.exception("Failed sending test welcome voice")
    finally:
        try:
            if voice_path.exists():
                voice_path.unlink()
        except Exception:
            logger.exception("Failed removing temp test file")

    asyncio.create_task(schedule_delete_message(client, message.chat.id, primary_message.id, WELCOME_DELETE_AFTER))
    if voice_message:
        asyncio.create_task(schedule_delete_message(client, message.chat.id, voice_message.id, WELCOME_DELETE_AFTER))

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    if not message.from_user or not is_super_admin(message.from_user.id):
        await message.reply_text("Broadcast is owner-only.")
        return

    text = message.text or ""
    parts = text.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text("Usage:\n/broadcast your message")
        return

    broadcast_text = parts[1].strip()
    group_ids = get_activated_groups()
    if not group_ids:
        await message.reply_text("No activated groups found.")
        return

    ok_count = 0
    fail_count = 0
    status = await message.reply_text(f"Broadcast started to {len(group_ids)} groups...")

    for gid in group_ids:
        try:
            await client.send_message(gid, broadcast_text)
            ok_count += 1
        except Exception:
            fail_count += 1
            logger.exception("Broadcast failed to group %s", gid)

    await status.edit_text(
        f"Broadcast finished.\n\nSuccess: {ok_count}\nFailed: {fail_count}"
    )

# -----------------------------
# Main
# -----------------------------
def main():
    init_db()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask started on port %s", PORT)

    logger.info("Starting %s", BOT_NAME)
    app.run()

if __name__ == "__main__":
    main()
