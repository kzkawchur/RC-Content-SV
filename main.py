import asyncio
import logging
import os
import random
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import edge_tts
import requests
from flask import Flask, jsonify
import colorsys
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps
from telegram import BotCommand, Update
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MayaWelcomeBot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", "8080"))
DB_PATH = os.environ.get("DB_PATH", "maya_welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/maya_welcome_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

BOT_NAME = os.environ.get("BOT_NAME", "Maya")
TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME", "Asia/Dhaka")
SUPPORT_GROUP_NAME = os.environ.get("SUPPORT_GROUP_NAME", "Support Group")
SUPPORT_GROUP_URL = os.environ.get("SUPPORT_GROUP_URL", "").strip()

VOICE_NAME_BN = os.environ.get("VOICE_NAME_BN", "bn-BD-NabanitaNeural")
VOICE_NAME_EN = os.environ.get("VOICE_NAME_EN", "en-US-JennyNeural")
VOICE_RATE = os.environ.get("VOICE_RATE", "-2%")
VOICE_PITCH = os.environ.get("VOICE_PITCH", "+0Hz")
VOICE_VOLUME = os.environ.get("VOICE_VOLUME", "+0%")

WELCOME_DELETE_AFTER = int(os.environ.get("WELCOME_DELETE_AFTER", "90"))
JOIN_COOLDOWN_SECONDS = int(os.environ.get("JOIN_COOLDOWN_SECONDS", "10"))
REJOIN_IGNORE_SECONDS = int(os.environ.get("REJOIN_IGNORE_SECONDS", "300"))
HOURLY_INTERVAL_SECONDS = int(os.environ.get("HOURLY_INTERVAL_SECONDS", "3600"))
AI_HOURLY_ENABLED = os.environ.get("AI_HOURLY_ENABLED", "true").strip().lower() == "true"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
GROQ_TIMEOUT_SECONDS = int(os.environ.get("GROQ_TIMEOUT_SECONDS", "20"))
AI_BATCH_SIZE = int(os.environ.get("AI_BATCH_SIZE", "8"))
AI_MAX_TEXT_LEN = int(os.environ.get("AI_MAX_TEXT_LEN", "140"))

SUPER_ADMINS = {
    int(x.strip()) for x in os.environ.get("SUPER_ADMINS", "").split(",") if x.strip().isdigit()
}

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
flask_app = Flask(__name__)

recent_hourly_by_chat: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=10))
recent_welcome_keys: dict[str, float] = {}
chat_join_history: dict[int, deque[float]] = defaultdict(lambda: deque(maxlen=20))
LAST_GROQ_STATUS = {
    "configured": bool(GROQ_API_KEY),
    "last_ok": None,
    "last_error": "No check yet",
    "last_checked_at": None,
}

AI_BATCH_CACHE: dict[tuple[str, str], dict] = {}
THEME_NAMES = [
    "gold","neon","soft-pink","royal-blue","night-glow","lavender","pearl","emerald","ruby","sapphire",
    "sunrise","sunset","moonlight","aurora","rose-gold","midnight","ocean","sky","mint","coral",
    "champagne","violet","crystal","plum","ice-blue","amber","pastel","galaxy","velvet","blush",
    "candy","steel","opal","forest","dream","bronze","silver","dusk","dawn","lotus",
    "mist","flame","sand","berry","wave","gloss","noir","halo","frost","petal",
]

@flask_app.get("/")
def home():
    return f"{BOT_NAME} Welcome Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot": BOT_NAME,
        "groq_configured": bool(GROQ_API_KEY),
        "ai_hourly_enabled": AI_HOURLY_ENABLED,
    })

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def tg_post(method: str, payload: dict) -> dict:
    try:
        resp = requests.post(f"{API_BASE}/{method}", json=payload, timeout=30)
        data = resp.json()
        logger.info("Telegram POST %s -> %s", method, str(data)[:500])
        return data
    except Exception:
        logger.exception("tg_post failed: %s", method)
        return {"ok": False}

def send_message_http(chat_id: int, text: str) -> bool:
    data = tg_post("sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    })
    ok = bool(data.get("ok"))
    if not ok:
        record_failure("send_message", chat_id, "", str(data)[:400])
    return ok

def delete_webhook():
    tg_post("deleteWebhook", {"drop_pending_updates": False})

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                language TEXT NOT NULL DEFAULT 'bn',
                custom_welcome TEXT,
                voice_enabled INTEGER NOT NULL DEFAULT 1,
                delete_service INTEGER NOT NULL DEFAULT 1,
                hourly_enabled INTEGER NOT NULL DEFAULT 1,
                voice_choice TEXT NOT NULL DEFAULT 'bd',
                total_welcome_sent INTEGER NOT NULL DEFAULT 0,
                total_hourly_sent INTEGER NOT NULL DEFAULT 0,
                last_ai_success_at INTEGER NOT NULL DEFAULT 0,
                last_fallback_used_at INTEGER NOT NULL DEFAULT 0,
                last_welcome_at INTEGER NOT NULL DEFAULT 0,
                last_milestone_sent INTEGER NOT NULL DEFAULT 0,
                welcome_style TEXT NOT NULL DEFAULT 'auto',
                footer_text TEXT NOT NULL DEFAULT '',
                last_primary_msg_id INTEGER,
                last_voice_msg_id INTEGER,
                last_hourly_at INTEGER NOT NULL DEFAULT 0,
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_generated (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lang TEXT NOT NULL,
                phase TEXT NOT NULL,
                source TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS failure_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                chat_id INTEGER,
                title TEXT,
                error TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )

        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(groups)").fetchall()}
        migrations = {
            "voice_choice": "TEXT NOT NULL DEFAULT 'bd'",
            "total_welcome_sent": "INTEGER NOT NULL DEFAULT 0",
            "total_hourly_sent": "INTEGER NOT NULL DEFAULT 0",
            "last_ai_success_at": "INTEGER NOT NULL DEFAULT 0",
            "last_fallback_used_at": "INTEGER NOT NULL DEFAULT 0",
            "last_welcome_at": "INTEGER NOT NULL DEFAULT 0",
            "last_milestone_sent": "INTEGER NOT NULL DEFAULT 0",
            "welcome_style": "TEXT NOT NULL DEFAULT 'auto'",
            "footer_text": "TEXT NOT NULL DEFAULT ''",
        }
        for col, ddl in migrations.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE groups ADD COLUMN {col} {ddl}")
        conn.commit()

def ensure_group(chat_id: int, title: str):
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO groups (chat_id, title, enabled, updated_at, last_hourly_at)
            VALUES (?, ?, 1, ?, 0)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                updated_at = excluded.updated_at
            """,
            (chat_id, title or "", now_ts),
        )
        conn.commit()

def get_group(chat_id: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()

def get_group_lang(chat_id: int) -> str:
    row = get_group(chat_id)
    lang = (row["language"] if row else "bn") or "bn"
    lang = lang.strip().lower()
    return lang if lang in {"bn", "en"} else "bn"

def set_group_value(chat_id: int, field: str, value):
    allowed = {
        "title",
        "language",
        "custom_welcome",
        "voice_enabled",
        "delete_service",
        "hourly_enabled",
        "enabled",
        "voice_choice",
        "total_welcome_sent",
        "total_hourly_sent",
        "last_ai_success_at",
        "last_fallback_used_at",
        "last_welcome_at",
        "last_milestone_sent",
        "welcome_style",
        "footer_text",
        "last_primary_msg_id",
        "last_voice_msg_id",
        "last_hourly_at",
        "updated_at",
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()

def get_enabled_groups_for_hourly():
    now_ts = int(time.time())
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT * FROM groups
            WHERE enabled = 1
              AND hourly_enabled = 1
              AND (? - last_hourly_at) >= ?
            ORDER BY updated_at DESC
            """,
            (now_ts, HOURLY_INTERVAL_SECONDS),
        ).fetchall()

def get_all_enabled_groups():
    with db_connect() as conn:
        return [int(r["chat_id"]) for r in conn.execute("SELECT chat_id FROM groups WHERE enabled = 1").fetchall()]

def increment_group_counter(chat_id: int, field: str, amount: int = 1):
    allowed = {"total_welcome_sent", "total_hourly_sent"}
    if field not in allowed:
        raise ValueError("Invalid counter field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = COALESCE({field}, 0) + ?, updated_at = ? WHERE chat_id = ?", (amount, int(time.time()), chat_id))
        conn.commit()

def format_ts(ts: int) -> str:
    if not ts:
        return "Never"
    return datetime.fromtimestamp(int(ts), ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d %I:%M:%S %p")

def current_voice_choice(chat_id: int) -> str:
    row = get_group(chat_id)
    choice = (row["voice_choice"] if row and row["voice_choice"] else "bd").strip().lower()
    return choice if choice in {"bd", "in"} else "bd"

def selected_voice_name(lang: str, chat_id: Optional[int] = None) -> str:
    if lang == "en":
        return VOICE_NAME_EN
    choice = current_voice_choice(chat_id or 0) if chat_id else "bd"
    return "bn-IN-TanishaaNeural" if choice == "in" else "bn-BD-NabanitaNeural"



def current_welcome_style(chat_id: int) -> str:
    row = get_group(chat_id)
    value = (row["welcome_style"] if row and row["welcome_style"] else "auto").strip().lower()
    return value if value in {"auto", "random"} or value in THEME_NAMES else "auto"

def current_footer_text(chat_id: int) -> str:
    row = get_group(chat_id)
    return (row["footer_text"] if row and row["footer_text"] else "").strip()[:80]

def theme_palette(style: str, phase: str):
    if style in {"", "auto"}:
        style = phase
    elif style == "random":
        style = random.choice(THEME_NAMES)
    seed = sum(ord(c) for c in style) % 360
    sat = 0.55 + (sum(ord(c) for c in style[::-1]) % 20) / 100
    val1 = 0.24 if phase == "night" else 0.58
    val2 = 0.86 if phase in {"morning", "day"} else 0.72
    def hsv(h, s, v):
        r, g, b = colorsys.hsv_to_rgb((h % 360) / 360.0, max(0, min(1, s)), max(0, min(1, v)))
        return (int(r * 255), int(g * 255), int(b * 255))
    c1 = hsv(seed, sat, val1)
    c2 = hsv(seed + 38, min(1, sat + 0.12), val2)
    glow = hsv(seed + 18, 0.22, 1.0)
    accent = hsv(seed + 10, 0.45, 0.98)
    return c1, c2, glow, accent, style

def list_theme_names_text() -> str:
    return ", ".join(THEME_NAMES)

def increment_group_counter(chat_id: int, field: str, amount: int = 1):
    allowed = {"total_welcome_sent", "total_hourly_sent"}
    if field not in allowed:
        raise ValueError("Invalid counter field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = COALESCE({field}, 0) + ?, updated_at = ? WHERE chat_id = ?", (amount, int(time.time()), chat_id))
        conn.commit()

def save_generated_text(lang: str, phase: str, source: str, text: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO ai_generated (lang, phase, source, text, created_at) VALUES (?, ?, ?, ?, ?)",
            (lang, phase, source, text[:300], int(time.time())),
        )
        conn.commit()

def record_failure(kind: str, chat_id: Optional[int], title: str, error: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO failure_logs (kind, chat_id, title, error, created_at) VALUES (?, ?, ?, ?, ?)",
            (kind[:32], chat_id, (title or "")[:120], (error or "")[:500], int(time.time())),
        )
        conn.commit()

def count_known_groups() -> int:
    with db_connect() as conn:
        row = conn.execute("SELECT COUNT(*) c FROM groups").fetchone()
        return int(row["c"] or 0)

def get_active_groups(limit: int = 20):
    with db_connect() as conn:
        return conn.execute(
            "SELECT chat_id, title, updated_at FROM groups WHERE enabled = 1 ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

def get_recent_failed_groups(limit: int = 15):
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT chat_id, title, MAX(created_at) AS last_time, COUNT(*) AS fail_count
            FROM failure_logs
            WHERE kind IN ('send_message', 'send_photo', 'send_voice', 'broadcast')
            GROUP BY chat_id, title
            ORDER BY last_time DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

def get_recent_ai_errors(limit: int = 10):
    with db_connect() as conn:
        return conn.execute(
            "SELECT error, created_at FROM failure_logs WHERE kind = 'ai' ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

def cleanup_old_temp_files(max_age_seconds: int = 1800):
    now_ts = time.time()
    for p in TMP_DIR.iterdir():
        try:
            if p.is_file() and now_ts - p.stat().st_mtime > max_age_seconds:
                p.unlink(missing_ok=True)
        except Exception:
            pass

def cleanup_loop():
    logger.info("Cleanup loop started")
    while True:
        try:
            cleanup_old_temp_files()
        except Exception:
            logger.exception("cleanup_loop failed")
        time.sleep(600)

def get_batch_pool(lang: str, phase: str):
    key = (lang, phase)
    cached = AI_BATCH_CACHE.get(key)
    now_ts = time.time()
    if cached and now_ts - cached["created_at"] < 900 and cached.get("texts"):
        return cached["texts"], cached["source"]
    ai_lines = groq_generate_batch(lang, phase)
    if ai_lines:
        source = "ai"
        texts = ai_lines
    else:
        source = "fallback"
        texts = build_fallback_messages(lang, phase)
    AI_BATCH_CACHE[key] = {"texts": texts, "source": source, "created_at": now_ts}
    for line in texts[:min(len(texts), 12)]:
        try:
            save_generated_text(lang, phase, source, line)
        except Exception:
            pass
    return texts, source

async def fetch_profile_photo_bytes(bot, user_id: int):
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if not photos or getattr(photos, "total_count", 0) < 1:
            return None
        file = await bot.get_file(photos.photos[0][-1].file_id)
        data = await file.download_as_bytearray()
        return bytes(data)
    except Exception:
        return None

def build_combined_names(members) -> str:
    names = [clean_name(m.first_name) for m in members[:5]]
    if len(members) > 5:
        names.append(f"+{len(members)-5}")
    return ", ".join(names)

def build_burst_text(lang: str, title: str, members) -> str:
    group = title or ("our group" if lang == "en" else "আমাদের গ্রুপ")
    names = build_combined_names(members)
    if lang == "en":
        return f"✨ A warm welcome to {group}!\nNew members: {names}"
    return f"✨ {group} এ আন্তরিক স্বাগতম!\nনতুন সদস্যরা: {names}"

def next_milestone(member_count: int, last_sent: int) -> int:
    for milestone in (100, 500, 1000):
        if member_count >= milestone and milestone > last_sent:
            return milestone
    return 0

def get_last_join_time(chat_id: int, user_id: int) -> int:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT joined_at FROM join_memory WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        ).fetchone()
        return int(row["joined_at"]) if row else 0

def save_join_time(chat_id: int, user_id: int):
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

def support_text() -> str:
    if SUPPORT_GROUP_URL and SUPPORT_GROUP_NAME:
        return f"{SUPPORT_GROUP_NAME} | {SUPPORT_GROUP_URL}"
    if SUPPORT_GROUP_URL:
        return SUPPORT_GROUP_URL
    return SUPPORT_GROUP_NAME

def local_now() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE_NAME))

def phase_now() -> str:
    h = local_now().hour
    if 5 <= h < 12:
        return "morning"
    if 12 <= h < 17:
        return "day"
    if 17 <= h < 21:
        return "evening"
    return "night"

def clean_name(name: str) -> str:
    if not name:
        return "বন্ধু"
    return name.replace("\n", " ").strip()[:40]

def ascii_name(name: str) -> str:
    s = (name or "").encode("ascii", "ignore").decode().strip()
    return s[:24] if s else "FRIEND"

def recent_key(chat_id: int, user_id: int) -> str:
    return f"{chat_id}:{user_id}"

def is_recent_duplicate(chat_id: int, user_id: int) -> bool:
    key = recent_key(chat_id, user_id)
    now_ts = time.time()
    prev = recent_welcome_keys.get(key, 0)
    recent_welcome_keys[key] = now_ts
    return now_ts - prev < 12

def is_join_burst(chat_id: int) -> bool:
    now_ts = time.time()
    hist = chat_join_history[chat_id]
    hist.append(now_ts)
    while hist and now_ts - hist[0] > 25:
        hist.popleft()
    return len(hist) >= 4

def is_super_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in SUPER_ADMINS)

TEXTS = {
    "bn": {
        "start_private": [
            "আমি {bot} 🌸\n\nCommands:\n/ping\n/myid\n/support\n/aistatus\n/broadcast <text>\n\nGroup-এ আমাকে add করলেই আমি auto কাজ শুরু করব। Admin চাইলে /lang, /voice, /deleteservice, /hourly ব্যবহার করতে পারবে।",
            "{bot} ready 🌷\n\nআমি group-এ auto কাজ করি। Commands:\n/ping\n/myid\n/support\n/aistatus\n/broadcast <text>\n\nAdmin হলে /lang bn বা /lang en, /hourly on বা off, /voice on বা off দিতে পারবে।",
        ],
        "start_group": [
            "{bot} ready for this group 🌸\nআমি premium welcome, voice আর premium hourly text handle করব।",
            "{bot} এই group-এ ready আছে 🌷\nআমি join হলে সুন্দর welcome দেব, আর চাইলে premium hourly text-ও পাঠাব।",
        ],
        "only_group_admin": [
            "Only group admins can use this command.",
            "এই command শুধু group admin ব্যবহার করতে পারবে।",
        ],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set_bn": ["ঠিক আছে, এখন থেকে আমি বাংলায় কথা বলব।", "Language changed to বাংলা."],
        "lang_set_en": ["Okay, I will speak in English now.", "Language changed to English."],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "ঠিক আছে, voice welcome এখন {value}।"],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "hourly_usage": ["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],
        "hourly_set": ["Hourly text: {value}", "Premium hourly text mode: {value}"],
        "hourly_now": ["এখনই একটি premium hourly message পাঠালাম।", "ঠিক আছে, এখনই একটি সুন্দর message দিলাম।"],
        "welcome_saved": ["Custom welcome text saved successfully.", "Custom welcome text save হয়ে গেছে।"],
        "welcome_reset": ["Custom welcome reset done.", "Custom welcome reset করা হয়েছে।"],
        "status": ["Bot: {bot}\nLanguage: {lang_name}\nVoice welcome: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTimezone: {tz}\nPhase: {phase}"],
        "aistatus": ["Groq configured: {configured}\nAI hourly enabled: {enabled}\nLast check: {checked}\nLast result: {result}\nModel: {model}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
        "burst_compact": [
            "🌸 {name}, তোমাকে {group} এ স্বাগতম। এই group-এ তোমাকে পেয়ে ভালো লাগছে।",
            "✨ {name}, {group} এ উষ্ণ স্বাগতম। আশা করি সুন্দর সময় কাটবে।",
            "💫 {name}, {group} এ তোমাকে আন্তরিক শুভেচ্ছা।",
            "🌷 {name}, {group} এ তোমাকে পেয়ে groupটা আরও সুন্দর লাগছে।",
        ],
    },
    "en": {
        "start_private": [
            "I am {bot} 🌸\n\nCommands:\n/ping\n/myid\n/support\n/aistatus\n/broadcast <text>\n\nOnce I am added to a group, I start working automatically. Group admins can use /lang, /voice, /deleteservice, and /hourly.",
            "{bot} is ready 🌷\n\nI work automatically in groups. Group admins can use /lang bn or /lang en, /hourly on or off, and /voice on or off.",
        ],
        "start_group": [
            "{bot} is ready for this group 🌸\nI will handle premium welcomes, voice, and premium hourly texts here.",
            "{bot} is now ready in this group 🌷\nI can send elegant welcome messages and premium hourly texts.",
        ],
        "only_group_admin": ["Only group admins can use this command."],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set_bn": ["Language changed to Bangla."],
        "lang_set_en": ["Language changed to English.", "Okay, I will speak in English now."],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "Voice welcome is now {value}."],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "hourly_usage": ["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],
        "hourly_set": ["Hourly text: {value}", "Premium hourly text mode: {value}"],
        "hourly_now": ["I just sent a premium hourly message.", "Okay, I sent one beautiful message right now."],
        "welcome_saved": ["Custom welcome text saved successfully.", "Your custom welcome text has been saved."],
        "welcome_reset": ["Custom welcome has been reset."],
        "status": ["Bot: {bot}\nLanguage: {lang_name}\nVoice welcome: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTimezone: {tz}\nPhase: {phase}"],
        "aistatus": ["Groq configured: {configured}\nAI hourly enabled: {enabled}\nLast check: {checked}\nLast result: {result}\nModel: {model}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
        "burst_compact": [
            "🌸 {name}, welcome to {group}. We are happy to have you here.",
            "✨ {name}, warm welcome to {group}. Hope you enjoy your time here.",
            "💫 {name}, a heartfelt welcome to {group}.",
            "🌷 {name}, glad to see you in {group}. Welcome.",
        ],
    },
}

def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in TEXTS else "bn"
    arr = TEXTS[lang].get(key) or TEXTS["bn"].get(key) or [key]
    return random.choice(arr).format(bot=BOT_NAME, support=support_text(), **kwargs)

BN_PHASE_OPENERS = {
    "morning": [
        "🌼 শুভ সকাল সবাইকে।", "☀️ সকালের সুন্দর শুভেচ্ছা রইল।", "✨ নতুন সকাল মানেই নতুন আলো।",
        "💛 সকালটা হোক কোমল আর সুন্দর।", "🌸 মিষ্টি এক সকালের শুভেচ্ছা।", "🍃 আজকের সকালটা শান্ত হোক।",
        "🕊️ ভালো একটি সকাল সবার জন্য।", "🌤️ আলো ভরা সকাল তোমাদের জন্য।", "🌺 সকালের নরম মায়া ছড়িয়ে থাকুক।",
        "💫 আজকের সকালটা হোক আশাবাদী।", "🌷 সকালের প্রশান্তি সবার হৃদয়ে থাকুক।", "🌞 দিনের শুরুটা হোক সুন্দর।",
        "🍀 শান্ত, পরিষ্কার, সুন্দর এক সকাল।", "🌼 ভালো অনুভূতির একটা সকাল রইল।", "💐 এই সকালটা হোক হাসিমাখা।",
    ],
    "day": [
        "🌷 দিনের শুভেচ্ছা সবাইকে।", "💫 দিনটা যেন সুন্দর কাটে।", "🌸 একটু হাসো, একটু ভালো থাকো।",
        "🍀 আজকের দিনটা হোক দারুণ।", "🌞 উষ্ণ দিনের শুভেচ্ছা রইল।", "✨ নরম এক দিনের শুভেচ্ছা।",
        "🌺 সবার জন্য সুন্দর দিনের বার্তা।", "💐 ভালো থাকুক এই group-এর সবাই।", "🕊️ দিনের মাঝেও শান্তি থাকুক।",
        "🌿 দিনটা হোক সহজ আর সুন্দর।", "🌻 আজকের সময়টা হোক ইতিবাচক।", "💛 এই দিনে থাকুক মমতা।",
        "🍃 স্বস্তির একটি দিন সবার জন্য।", "🌸 ছোট্ট একটু উষ্ণতা ছড়িয়ে দিই।", "✨ ভালো vibe থাকুক চারদিকে।",
    ],
    "evening": [
        "🌙 শুভ সন্ধ্যা সবাইকে।", "✨ সন্ধ্যার নরম শুভেচ্ছা রইল।", "🌆 আজকের সন্ধ্যাটা হোক মিষ্টি।",
        "💜 শান্ত এক সন্ধ্যার শুভেচ্ছা।", "🕯️ সন্ধ্যার আলোয় ভালোবাসা রইল।", "🌃 নরম সন্ধ্যার শুভেচ্ছা সবাইকে।",
        "🍂 সন্ধ্যাটা হোক আরামদায়ক।", "💫 ক্লান্তি ভুলে একটু ভালো থাকো।", "🌸 সন্ধ্যার ছোঁয়ায় মন শান্ত থাকুক।",
        "🌷 এই সন্ধ্যা হোক মোলায়েম আর সুন্দর।", "💐 সবার জন্য নরম এক সন্ধ্যা।", "🌟 আলো-আঁধারির শুভেচ্ছা রইল।",
        "🍃 দিনশেষের সময়টা হোক শান্ত।", "🕊️ শান্ত সন্ধ্যার বার্তা সবার জন্য।", "✨ আজকের সন্ধ্যা হোক স্বস্তিদায়ক।",
    ],
    "night": [
        "🌌 শুভ রাত্রি সবাইকে।", "⭐ রাতের শান্ত শুভেচ্ছা রইল।", "💙 আজকের রাতটা হোক শান্ত।",
        "🌙 মিষ্টি এক রাতের শুভেচ্ছা।", "🕊️ নীরব রাতের কোমল শুভেচ্ছা।", "✨ রাতের শেষে ভালো থেকো সবাই।",
        "🌠 আরামদায়ক একটি রাত কামনা করি।", "💫 সবার জন্য শান্ত রাতের বার্তা।", "🌸 শান্ত ঘুমের শুভেচ্ছা রইল।",
        "🍀 আজকের রাতটা হোক নির্ভার।", "💐 নরম এক শুভ রাত্রি সবার জন্য।", "🌌 মায়াময় রাতের শুভেচ্ছা রইল।",
        "💛 আজ রাতেও মনটা থাকুক হালকা।", "🌷 শান্তির একটি রাত সবার জন্য।", "✨ রাতটা হোক আরাম আর স্বস্তিতে ভরা।",
    ],
}
BN_MIDDLES = [
    "এই group-এর সবার জন্য অনেক শুভকামনা।", "একটু হাসো, একটু স্বস্তিতে থাকো।", "নিজের মনটাকে আজ একটু হালকা রাখো।",
    "আশা করি সময়টা তোমাদের ভালো কাটছে।", "সবাই যেন সুন্দর আর নিরাপদে থাকো।", "দিনের ভিড়ে মনটাও যেন সুন্দর থাকে।",
    "মনে রাখো, শান্ত থাকাও একধরনের শক্তি।", "আজও ভালো কিছুর অপেক্ষা থাকুক।", "সুন্দর কথা, সুন্দর মন—দুটোই জরুরি।",
    "ভালো vibes ছড়িয়ে দাও চারদিকে।", "নিজেকে একটু যত্নে রাখো।", "ক্লান্তি থাকলেও মনটা নরম থাকুক।",
    "সবার জীবনে একটু করে আলো থাকুক।", "ভালো থাকার ছোট্ট কারণও অনেক মূল্যবান।", "আজও মনের ভেতর শান্তি থাকুক।",
    "নিজের প্রতি কোমল থেকো।", "সুন্দর অনুভূতির জন্য বড় কারণ লাগে না।", "স্বস্তির একটু সময় সবাই পাক।",
    "এই group-এ ভালো vibe সবসময় থাকুক।", "নরম, সুন্দর, ভদ্র energy ছড়িয়ে থাকুক।",
]
BN_ENDINGS = [
    "🌷 ভালো থাকো সবাই।", "💫 সুন্দর থাকো সবাই।", "🌼 হাসিখুশি থাকো সবাই।", "💙 শান্তিতে থাকো সবাই।",
    "✨ হৃদয়টা নরম আর সুন্দর থাকুক।", "🕊️ মনটা হোক হালকা আর শান্ত।", "🌸 তোমাদের সবার জন্য রইল শুভেচ্ছা।",
    "🍀 সুন্দর সময় কাটুক সবার।", "💐 শান্তি থাকুক চারপাশে।", "🌙 মনটা থাকুক প্রশান্ত।",
]

EN_PHASE_OPENERS = {
    "morning": [
        "🌼 Good morning everyone.", "☀️ A gentle morning hello to all of you.", "✨ Wishing this group a soft and beautiful morning.",
        "💛 Hope your morning feels light and peaceful.", "🌸 Sending warm morning wishes to everyone.", "🍃 May this morning begin softly for you all.",
        "🕊️ A calm and lovely morning to this group.", "🌤️ Bright morning wishes to everyone here.", "🌺 A graceful morning note for everyone.",
        "💫 Hope today begins with a little peace.", "🌷 Wishing you all a warm morning.", "🌞 A clear and kind morning to this group.",
        "🍀 May this morning feel easy and bright.", "💐 A sweet little morning message for all.", "✨ Gentle morning vibes to everyone here.",
    ],
    "day": [
        "🌷 Hope everyone is having a good day.", "💫 Sending warm daytime vibes to this group.", "🌸 A little beautiful message for your day.",
        "🍀 Wishing everyone a smooth and lovely day.", "🌞 Daytime wishes to all of you.", "✨ Hope today feels a little softer and brighter.",
        "🌺 Sending kindness across the group today.", "💐 A warm little note for everyone here.", "🕊️ Wishing everyone calm energy today.",
        "🌿 May the day stay gentle and kind.", "🌻 A soft little daytime greeting for all.", "💛 Hope the day brings something lovely.",
        "🍃 Sending fresh and peaceful energy.", "🌸 Warm thoughts for everyone in this group.", "✨ May your day keep flowing beautifully.",
    ],
    "evening": [
        "🌙 Good evening everyone.", "✨ Sending peaceful evening wishes to this group.", "🌆 Hope your evening feels calm and gentle.",
        "💜 A soft evening hello to all of you.", "🕯️ Wishing everyone a lovely evening.", "🌃 Evening warmth to this beautiful group.",
        "🍂 Hope the evening brings a little peace.", "💫 A gentle evening message for everyone here.", "🌸 Let the evening feel soft and easy.",
        "🌷 Sending warm evening comfort to all.", "💐 A calm evening note for this group.", "🌟 May your evening feel graceful and light.",
        "🍃 Rest a little and breathe gently.", "🕊️ A peaceful evening vibe to everyone.", "✨ Wishing you all a beautiful sunset mood.",
    ],
    "night": [
        "🌌 Good night everyone.", "⭐ Sending calm night wishes to all of you.", "💙 Hope your night feels peaceful and restful.",
        "🌙 A soft night message for this group.", "🕊️ Wishing everyone a gentle and quiet night.", "✨ End the day with a little peace.",
        "🌠 Warm night wishes to everyone here.", "💫 A peaceful close to the day for all of you.", "🌸 A soft good night to this lovely group.",
        "🍀 May your night feel light and easy.", "💐 Wishing comfort and calm to everyone.", "🌌 Let the night wrap you in peace.",
        "💛 A gentle good night to all.", "🌷 Wishing you all a restful night.", "✨ May your mind feel settled tonight.",
    ],
}
EN_MIDDLES = [
    "Wishing this group a little more peace and softness.", "Hope your heart feels a little lighter today.", "Take a small moment to breathe and smile.",
    "May your day carry a little extra kindness.", "Sending good energy to everyone here.", "Hope things feel a bit easier and brighter.",
    "A small warm message can change a day.", "Keep your heart gentle and your mind steady.", "You all deserve a peaceful moment today.",
    "May this group stay kind, calm, and warm.", "Let this be a reminder to slow down softly.", "A little grace can brighten any hour.",
    "Hope your thoughts feel clear and calm.", "Sending a soft note of comfort to everyone.", "Wishing each of you a beautiful little pause.",
    "Peaceful vibes can make a big difference.", "May something good find you today.", "Keep your energy warm and elegant.",
    "Gentle moments are worth holding onto.", "You are allowed to move through the day softly.",
]
EN_ENDINGS = [
    "🌷 Stay well, everyone.", "💫 Stay beautiful, everyone.", "🌼 Wishing you comfort and peace.", "💙 Take care, everyone.",
    "✨ Keep your vibe soft and bright.", "🕊️ May your mind feel calm.", "🌸 Warm wishes to all of you.",
    "🍀 Hope the rest of your time feels lovely.", "💐 Sending light and warmth to all.", "🌙 Wishing you a peaceful heart.",
]

FALLBACK_CACHE: dict[tuple[str, str], list[str]] = {}

def build_fallback_messages(lang: str, phase: str) -> list[str]:
    key = (lang, phase)
    if key in FALLBACK_CACHE:
        return FALLBACK_CACHE[key]
    result = []
    if lang == "en":
        for a in EN_PHASE_OPENERS[phase]:
            for b in EN_MIDDLES:
                for c in EN_ENDINGS:
                    text = f"{a} {b} {c}".strip()
                    if len(text) <= AI_MAX_TEXT_LEN:
                        result.append(text)
    else:
        for a in BN_PHASE_OPENERS[phase]:
            for b in BN_MIDDLES:
                for c in BN_ENDINGS:
                    text = f"{a} {b} {c}".strip()
                    if len(text) <= AI_MAX_TEXT_LEN:
                        result.append(text)
    seen = set()
    uniq = []
    for x in result:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    random.shuffle(uniq)
    FALLBACK_CACHE[key] = uniq
    return uniq

def sanitize_ai_lines(text: str) -> list[str]:
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        line = re.sub(r"^[\-\*\d\.\)\s]+", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line or len(line) > AI_MAX_TEXT_LEN:
            continue
        lowered = line.lower()
        bad = ["18+", "sex", "sexy", "dating", "kiss", "adult", "nude", "xxx", "porn"]
        if any(b in lowered for b in bad):
            continue
        lines.append(line)
    uniq = []
    seen = set()
    for x in lines:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def _update_groq_status(ok: bool, message: str):
    LAST_GROQ_STATUS["configured"] = bool(GROQ_API_KEY)
    LAST_GROQ_STATUS["last_ok"] = ok
    LAST_GROQ_STATUS["last_error"] = message
    LAST_GROQ_STATUS["last_checked_at"] = local_now().strftime("%Y-%m-%d %I:%M:%S %p")

def groq_generate_batch(lang: str, phase: str) -> list[str]:
    if not AI_HOURLY_ENABLED or not GROQ_API_KEY:
        _update_groq_status(False, "Groq disabled or API key missing")
        return []
    prompt = (
        f"Write {AI_BATCH_SIZE} short premium Telegram group hourly messages in "
        f"{'Bengali' if lang == 'bn' else 'English'}.\n"
        f"Rules:\n"
        f"- warm, elegant, premium, tasteful, group-safe\n"
        f"- non-sexual, non-romantic, non-political, non-religious\n"
        f"- no flirting\n"
        f"- no hashtags\n"
        f"- keep each under {AI_MAX_TEXT_LEN} characters\n"
        f"- suitable for {phase}\n"
        f"- each line must be different\n"
        f"- make them fresh and graceful\n"
        f"Return only the messages, one per line."
    )
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "You write tasteful, premium, short Telegram group texts."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.95,
                "max_tokens": 420,
            },
            timeout=GROQ_TIMEOUT_SECONDS,
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        lines = sanitize_ai_lines(content)
        if lines:
            _update_groq_status(True, f"OK | {len(lines)} lines")
            logger.info("Groq hourly success | lang=%s phase=%s count=%s", lang, phase, len(lines))
            return lines
        _update_groq_status(False, "Groq returned empty/filtered text")
        record_failure("ai", None, "", "Groq returned empty/filtered text")
        return []
    except Exception as e:
        _update_groq_status(False, f"Failed: {e}")
        record_failure("ai", None, "", str(e))
        logger.exception("Groq hourly failed | lang=%s phase=%s", lang, phase)
        return []

def groq_live_check() -> tuple[bool, str]:
    if not GROQ_API_KEY:
        _update_groq_status(False, "API key missing")
        return False, "API key missing"
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "Reply with one short word only."},
                    {"role": "user", "content": "ping"},
                ],
                "temperature": 0,
                "max_tokens": 5,
            },
            timeout=min(GROQ_TIMEOUT_SECONDS, 15),
        )
        data = resp.json()
        if "choices" in data:
            _update_groq_status(True, "Live check passed")
            return True, "Live check passed"
        msg = data.get("error", {}).get("message", "Unknown Groq response")
        _update_groq_status(False, msg)
        record_failure("ai", None, "", msg)
        return False, msg
    except Exception as e:
        _update_groq_status(False, str(e))
        record_failure("ai", None, "", str(e))
        return False, str(e)

def pick_hourly_message(chat_id: int, lang: str, phase: str, pool: list[str]) -> str:
    recent = recent_hourly_by_chat[chat_id]
    choices = [x for x in pool if x not in recent]
    if not choices:
        choices = pool[:] or build_fallback_messages(lang, phase)
    text = random.choice(choices)
    recent.append(text)
    return text

def pick_font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()

def build_cover_bytes(first_name: str, group_title: str, lang: str, style: str = "auto", footer: str = "", profile_bytes: Optional[bytes] = None, member_count: Optional[int] = None) -> BytesIO:
    width, height = 1280, 720
    phase = phase_now()
    c1, c2, glow, accent, resolved_style = theme_palette(style, phase)

    img = Image.new("RGB", (width, height), c1)
    draw = ImageDraw.Draw(img)
    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(c1[0] * (1 - blend) + c2[0] * blend)
        g = int(c1[1] * (1 - blend) + c2[1] * blend)
        b = int(c1[2] * (1 - blend) + c2[2] * blend)
        draw.line((0, y, width, y), fill=(r, g, b))

    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.ellipse((40, 40, 300, 300), fill=(255, 255, 255, 70))
    od.ellipse((980, 70, 1230, 320), fill=(255, 255, 255, 55))
    od.ellipse((910, 460, 1185, 735), fill=(255, 255, 255, 45))
    overlay = overlay.filter(ImageFilter.GaussianBlur(10))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")

    shadow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((90, 95, 1188, 628), radius=48, fill=(0, 0, 0, 95))
    shadow = shadow.filter(ImageFilter.GaussianBlur(18))
    img = Image.alpha_composite(img.convert("RGBA"), shadow).convert("RGB")
    draw = ImageDraw.Draw(img)

    draw.rounded_rectangle((100, 100, 1180, 620), radius=44, fill=(11, 17, 36))
    draw.rounded_rectangle((130, 125, 158, 595), radius=12, fill=accent)

    title_font = pick_font(64, True)
    name_font = pick_font(92, True)
    sub_font = pick_font(36, False)
    mini_font = pick_font(28, True)
    tiny_font = pick_font(22, True)

    group_text = ascii_name(group_title or ("OUR GROUP" if lang == "en" else "GROUP")).upper()
    name_text = ascii_name(first_name).upper()
    draw.text((182, 152), "WELCOME", fill=glow, font=title_font)
    draw.text((182, 252), name_text, fill=(255, 226, 170), font=name_font)
    draw.text((182, 392), f"TO {group_text}", fill=(222, 233, 255), font=sub_font)
    draw.text((182, 480), BOT_NAME.upper(), fill=(176, 255, 223), font=mini_font)

    badge_x, badge_y = 935, 140
    for idx, label in enumerate([phase.upper(), resolved_style.upper()[:12]]):
        x1 = badge_x - (idx * 185)
        draw.rounded_rectangle((x1, badge_y, x1 + 170, badge_y + 44), radius=20, fill=(255, 255, 255))
        draw.text((x1 + 18, badge_y + 10), label, fill=(38, 52, 87), font=tiny_font)

    if member_count:
        mc_text = f"MEMBERS {member_count}"
        draw.rounded_rectangle((880, 545, 1120, 585), radius=18, fill=(255, 255, 255))
        draw.text((900, 554), mc_text, fill=(37, 45, 78), font=tiny_font)

    footer = footer.strip()[:60] if footer else f"Powered by {BOT_NAME}"
    draw.text((182, 552), footer, fill=(214, 229, 255), font=tiny_font)

    if profile_bytes:
        try:
            avatar = Image.open(BytesIO(profile_bytes)).convert("RGB")
            avatar = ImageOps.fit(avatar, (220, 220))
            mask = Image.new("L", (220, 220), 0)
            md = ImageDraw.Draw(mask)
            md.ellipse((0, 0, 220, 220), fill=255)
            ring = Image.new("RGBA", (248, 248), (0, 0, 0, 0))
            rd = ImageDraw.Draw(ring)
            rd.ellipse((0, 0, 248, 248), fill=accent + (255,) if len(accent)==4 else accent)
            ring.paste(avatar, (14, 14), mask)
            img.paste(ring.convert("RGB"), (900, 265))
        except Exception:
            pass

    draw.rounded_rectangle((182, 603, 430, 615), radius=6, fill=(255, 255, 255))
    draw.rounded_rectangle((182, 630, 334, 642), radius=6, fill=(196, 226, 255))
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

async def make_voice_file(text: str, voice_name: str, path: Path):
    communicate = edge_tts.Communicate(
        text=text,
        voice=voice_name,
        rate=VOICE_RATE,
        pitch=VOICE_PITCH,
        volume=VOICE_VOLUME,
    )
    await communicate.save(str(path))

def welcome_texts(lang: str, mention_name: str, first_name: str, group_title: str, custom_text: Optional[str]) -> tuple[str, str]:
    phase = phase_now()
    safe_group = group_title or ("our group" if lang == "en" else "আমাদের গ্রুপ")
    if custom_text:
        text = custom_text.replace("{name}", mention_name).replace("{group}", safe_group).replace("{phase}", phase)
        voice = f"Hello {first_name}, welcome to {safe_group}." if lang == "en" else f"{first_name}, তোমাকে {safe_group} এ স্বাগতম।"
        return text, voice

    if lang == "en":
        bank = {
            "morning": [
                f"🌼 Good morning {mention_name}!\nA graceful welcome to {safe_group}.",
                f"✨ {mention_name}, warm morning wishes and a premium welcome to {safe_group}.",
                f"☀️ Hello {mention_name}!\nA bright and beautiful welcome to {safe_group}.",
                f"🌷 {mention_name}, delighted to welcome you to {safe_group}.",
            ],
            "day": [
                f"🌸 Welcome {mention_name}!\nWe are happy to have you in {safe_group}.",
                f"💫 Hello {mention_name}!\nA warm and elegant welcome to {safe_group}.",
                f"🌷 {mention_name}, glad to see you in {safe_group}. Welcome.",
                f"✨ {mention_name}, your presence makes {safe_group} feel even warmer.",
            ],
            "evening": [
                f"🌙 Good evening {mention_name}!\nWelcome to {safe_group}.",
                f"✨ {mention_name}, lovely evening wishes and welcome to {safe_group}.",
                f"🌆 Hello {mention_name}!\nA calm and gentle welcome to {safe_group}.",
                f"💜 {mention_name}, a graceful evening welcome to {safe_group}.",
            ],
            "night": [
                f"🌌 Good night {mention_name}!\nWelcome to {safe_group}.",
                f"💙 {mention_name}, peaceful night wishes and welcome to {safe_group}.",
                f"⭐ Hello {mention_name}!\nA soft night welcome to {safe_group}.",
                f"✨ {mention_name}, warm night wishes and welcome to {safe_group}.",
            ],
        }
        voice_bank = {
            "morning": [
                f"{first_name}, good morning. A warm welcome to {safe_group}.",
                f"Hello {first_name}, welcome to {safe_group}. We are glad to have you here.",
            ],
            "day": [
                f"{first_name}, welcome to {safe_group}. We are really happy to have you here.",
                f"Hello {first_name}, a warm welcome to {safe_group}.",
            ],
            "evening": [
                f"{first_name}, good evening. Welcome to {safe_group}. Hope you enjoy your time here.",
                f"Hello {first_name}, evening wishes and welcome to {safe_group}.",
            ],
            "night": [
                f"{first_name}, good night. A warm welcome to {safe_group}.",
                f"Hello {first_name}, welcome to {safe_group}. Glad to have you here.",
            ],
        }
    else:
        bank = {
            "morning": [
                f"🌼 শুভ সকাল {mention_name}!\n{safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
                f"✨ {mention_name}, সকালের মিষ্টি শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
                f"☀️ হ্যালো {mention_name}!\nএকটা উজ্জ্বল স্বাগতম রইল {safe_group} এ।",
                f"🌷 {mention_name}, তোমাকে পেয়ে {safe_group} আরও সুন্দর লাগছে।",
            ],
            "day": [
                f"🌸 স্বাগতম {mention_name}!\n{safe_group} এ তোমাকে পেয়ে খুব ভালো লাগছে।",
                f"💫 হ্যালো {mention_name}!\n{safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
                f"🌷 {mention_name}, তোমাকে পেয়ে {safe_group} আরও উষ্ণ লাগছে।",
                f"✨ {mention_name}, তোমার জন্য {safe_group} এ রইল সুন্দর শুভেচ্ছা।",
            ],
            "evening": [
                f"🌙 শুভ সন্ধ্যা {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।",
                f"✨ {mention_name}, সন্ধ্যার সুন্দর শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
                f"🌆 হ্যালো {mention_name}!\nসন্ধ্যার নরম আলোয় তোমাকে {safe_group} এ স্বাগতম।",
                f"💜 {mention_name}, মোলায়েম এক সন্ধ্যার স্বাগতম রইল।",
            ],
            "night": [
                f"🌌 শুভ রাত্রি {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।",
                f"💙 {mention_name}, রাতের শান্ত শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
                f"⭐ হ্যালো {mention_name}!\nরাতের নরম শুভেচ্ছার সাথে তোমাকে স্বাগতম।",
                f"✨ {mention_name}, শান্ত আর মিষ্টি এক স্বাগতম রইল {safe_group} এ।",
            ],
        }
        voice_bank = {
            "morning": [
                f"{first_name}, শুভ সকাল। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।",
                f"হ্যালো {first_name}, সকালের সুন্দর শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।",
            ],
            "day": [
                f"{first_name}, তোমাকে {safe_group} এ আন্তরিক স্বাগতম। তোমাকে পেয়ে ভালো লাগছে।",
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
    return random.choice(bank[phase]), random.choice(voice_bank[phase])

async def require_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    chat = update.effective_chat
    user = update.effective_user
    if not chat or chat.type not in {"group", "supergroup"}:
        await update.effective_message.reply_text("Use this command in group.")
        return False
    ensure_group(chat.id, chat.title or "")
    if not user:
        return False
    member = await context.bot.get_chat_member(chat.id, user.id)
    if member.status not in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
        await update.effective_message.reply_text(t(get_group_lang(chat.id), "only_group_admin"))
        return False
    return True

async def delete_previous_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    row = get_group(chat_id)
    if not row:
        return
    for mid in (row["last_primary_msg_id"], row["last_voice_msg_id"]):
        if mid:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=int(mid))
            except Exception:
                pass

async def schedule_delete(bot, chat_id: int, message_id: int, delay: int):
    try:
        await asyncio.sleep(delay)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def send_photo_with_retry(bot, **kwargs):
    last_error = None
    for attempt in range(2):
        try:
            return await bot.send_photo(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_photo", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

async def send_voice_with_retry(bot, **kwargs):
    last_error = None
    for attempt in range(2):
        try:
            return await bot.send_voice(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_voice", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

async def send_text_with_retry(bot, **kwargs):
    last_error = None
    for attempt in range(2):
        try:
            return await bot.send_message(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_message", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

def build_milestone_card_bytes(group_title: str, count: int) -> BytesIO:
    width, height = 1280, 720
    img = Image.new("RGB", (width, height), (26, 22, 45))
    draw = ImageDraw.Draw(img)
    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(26 * (1 - blend) + 104 * blend)
        g = int(22 * (1 - blend) + 55 * blend)
        b = int(45 * (1 - blend) + 186 * blend)
        draw.line((0, y, width, y), fill=(r, g, b))
    draw.rounded_rectangle((90, 90, 1190, 630), radius=48, fill=(12, 16, 31))
    draw.rounded_rectangle((120, 120, 1140, 600), radius=36, outline=(255, 214, 122), width=6)
    title_font = pick_font(70, True)
    big_font = pick_font(126, True)
    sub_font = pick_font(38, False)
    draw.text((160, 160), "MILESTONE", fill=(255, 244, 214), font=title_font)
    draw.text((160, 290), str(count), fill=(255, 214, 122), font=big_font)
    draw.text((160, 465), f"MEMBERS • {ascii_name(group_title or 'GROUP').upper()}", fill=(220, 232, 255), font=sub_font)
    draw.text((160, 540), BOT_NAME.upper(), fill=(176, 255, 223), font=sub_font)
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "milestone.png"
    bio.seek(0)
    return bio


def build_combined_welcome_card_bytes(group_title: str, lang: str, names_text: str, style: str = "auto", footer: str = "") -> BytesIO:
    width, height = 1280, 720
    phase = phase_now()
    c1, c2, glow, accent, resolved_style = theme_palette(style, phase)
    img = Image.new("RGB", (width, height), c1)
    draw = ImageDraw.Draw(img)
    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(c1[0] * (1 - blend) + c2[0] * blend)
        g = int(c1[1] * (1 - blend) + c2[1] * blend)
        b = int(c1[2] * (1 - blend) + c2[2] * blend)
        draw.line((0, y, width, y), fill=(r, g, b))
    draw.rounded_rectangle((90, 90, 1190, 630), radius=48, fill=(12, 16, 31))
    draw.rounded_rectangle((120, 120, 1140, 600), radius=36, outline=accent, width=6)
    title_font = pick_font(64, True)
    name_font = pick_font(42, True)
    sub_font = pick_font(30, False)
    footer_font = pick_font(24, True)
    draw.text((160, 155), "WELCOME CREW", fill=glow, font=title_font)
    draw.text((160, 280), ascii_name(group_title or "GROUP").upper(), fill=(255, 226, 170), font=name_font)
    wrapped = names_text[:120]
    draw.text((160, 380), wrapped, fill=(222, 233, 255), font=sub_font)
    draw.text((160, 530), (footer or f"Powered by {BOT_NAME}")[:60], fill=(214, 229, 255), font=footer_font)
    draw.rounded_rectangle((905, 140, 1110, 184), radius=20, fill=(255,255,255))
    draw.text((930, 150), resolved_style.upper()[:12], fill=(38,52,87), font=footer_font)
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome_burst.png"
    bio.seek(0)
    return bio

async def maybe_send_milestone(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, lang: str):
    try:
        row = get_group(chat_id)
        if not row:
            return
        member_count = await context.bot.get_chat_member_count(chat_id)
        milestone = next_milestone(member_count, int(row["last_milestone_sent"] or 0))
        if not milestone:
            return
        card = build_milestone_card_bytes(title or "GROUP", milestone)
        caption = t(lang, "milestone", count=milestone)
        msg = await send_photo_with_retry(context.bot, chat_id=chat_id, photo=card, caption=caption)
        set_group_value(chat_id, "last_milestone_sent", milestone)
        asyncio.create_task(schedule_delete(context.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER + 30))
    except Exception:
        logger.exception("Milestone send failed in chat %s", chat_id)

async def maybe_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, user):
    ensure_group(chat_id, title or "")
    group = get_group(chat_id)
    if not group or int(group["enabled"]) != 1 or user.is_bot:
        return
    if is_recent_duplicate(chat_id, user.id):
        return
    if time.time() - get_last_join_time(chat_id, user.id) < REJOIN_IGNORE_SECONDS:
        return

    lang = get_group_lang(chat_id)
    first_name = clean_name(user.first_name)
    mention_name = user.mention_html(first_name)
    save_join_time(chat_id, user.id)

    burst_mode = is_join_burst(chat_id)
    if burst_mode:
        try:
            compact = t(lang, "burst_compact", name=mention_name, group=(title or ("our group" if lang == "en" else "আমাদের গ্রুপ")))
            msg = await send_text_with_retry(context.bot, chat_id=chat_id, text=compact, parse_mode=ParseMode.HTML)
            set_group_value(chat_id, "last_primary_msg_id", msg.message_id)
            increment_group_counter(chat_id, "total_welcome_sent")
            set_group_value(chat_id, "last_welcome_at", int(time.time()))
            asyncio.create_task(schedule_delete(context.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER))
            await maybe_send_milestone(context, chat_id, title or "", lang)
        except Exception:
            logger.exception("Compact burst welcome failed in %s", chat_id)
        return

    text_welcome, voice_text = welcome_texts(lang, mention_name, first_name, title or "", group["custom_welcome"])
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        style = current_welcome_style(chat_id)
        footer = current_footer_text(chat_id)
        member_count = None
        try:
            member_count = await context.bot.get_chat_member_count(chat_id)
        except Exception:
            member_count = None
        profile_bytes = await fetch_profile_photo_bytes(context.bot, user.id)
        cover = build_cover_bytes(first_name, title or "GROUP", lang, style=style, footer=footer, profile_bytes=profile_bytes, member_count=member_count)
        try:
            primary = await send_photo_with_retry(context.bot, chat_id=chat_id, photo=cover, caption=text_welcome, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Photo welcome failed in chat %s, switching to text-only", chat_id)
            primary = await send_text_with_retry(context.bot, chat_id=chat_id, text=re.sub(r"<[^>]+>", "", text_welcome))

        if int(group["voice_enabled"]) == 1 and primary and getattr(primary, 'photo', None):
            try:
                voice_name = selected_voice_name(lang, chat_id)
                await make_voice_file(voice_text, voice_name, voice_path)
                voice_msg = await send_voice_with_retry(context.bot, chat_id=chat_id, voice=voice_path.read_bytes(), caption=t(lang, "welcome_voice_caption"))
            except Exception:
                logger.exception("Voice welcome failed in chat %s; keeping banner+text only", chat_id)
                voice_msg = None

        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        set_group_value(chat_id, "last_welcome_at", int(time.time()))
        increment_group_counter(chat_id, "total_welcome_sent")

        if primary:
            asyncio.create_task(schedule_delete(context.bot, chat_id, primary.message_id, WELCOME_DELETE_AFTER))
        if voice_msg:
            asyncio.create_task(schedule_delete(context.bot, chat_id, voice_msg.message_id, WELCOME_DELETE_AFTER))

        await maybe_send_milestone(context, chat_id, title or "", lang)
    except Exception:
        logger.exception("Welcome failed in chat %s for user %s", chat_id, user.id)
    finally:
        if voice_path.exists():
            try:
                voice_path.unlink()
            except Exception:
                pass

async def track_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in {"group", "supergroup"}:
        ensure_group(chat.id, chat.title or "")

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    if update.effective_chat and update.effective_chat.type in {"group", "supergroup"}:
        await update.effective_message.reply_text(t(get_group_lang(update.effective_chat.id), "start_group"))
    else:
        await update.effective_message.reply_text(t("bn", "start_private"))

async def on_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group", "supergroup"} else "bn"
    await update.effective_message.reply_text(t(lang, "support"))

async def on_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group", "supergroup"} else "bn"
    await update.effective_message.reply_text(t(lang, "ping", tz=TIMEZONE_NAME, time=local_now().strftime("%I:%M %p")))

async def on_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else 0
    await update.effective_message.reply_text(t("en", "myid", user_id=uid))

async def on_ai_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    allowed = False
    if chat and chat.type in {"group", "supergroup"} and user:
        member = await context.bot.get_chat_member(chat.id, user.id)
        allowed = member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    elif user:
        allowed = is_super_admin(user.id)
    if not allowed:
        await update.effective_message.reply_text("Only group admins or bot owners can use this command.")
        return
    await update.effective_message.reply_text("Checking Groq status...")
    ok, result = await asyncio.to_thread(groq_live_check)
    checked = LAST_GROQ_STATUS["last_checked_at"] or "Never"
    configured = "YES" if GROQ_API_KEY else "NO"
    enabled = "YES" if AI_HOURLY_ENABLED else "NO"
    lang = get_group_lang(chat.id) if chat and chat.type in {"group", "supergroup"} else "en"
    await update.effective_message.reply_text(
        t(
            lang,
            "aistatus",
            configured=configured,
            enabled=enabled,
            checked=checked,
            result=("OK" if ok else "FAILED") + f" | {result}",
            model=GROQ_MODEL,
        )
    )

async def on_setvoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    current = "bd" if current_voice_choice(chat.id) == "bd" else "in"
    if not context.args:
        await update.effective_message.reply_text(t(lang, "setvoice_usage", current=current))
        return
    value = context.args[0].strip().lower()
    if value not in {"bd", "in"}:
        await update.effective_message.reply_text(t(lang, "setvoice_usage", current=current))
        return
    set_group_value(chat.id, "voice_choice", value)
    label = "Bangladesh female" if value == "bd" else "India Bengali female"
    await update.effective_message.reply_text(t(lang, "setvoice_set", value=label))

async def on_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    row = get_group(chat.id)
    await update.effective_message.reply_text(
        t(
            lang,
            "analytics",
            welcomes=int(row["total_welcome_sent"] or 0),
            hourly=int(row["total_hourly_sent"] or 0),
            ai=format_ts(int(row["last_ai_success_at"] or 0)),
            fallback=format_ts(int(row["last_fallback_used_at"] or 0)),
            welcome=format_ts(int(row["last_welcome_at"] or 0)),
            voice=("Bangladesh female" if current_voice_choice(chat.id) == "bd" else "India Bengali female"),
        )
    )


async def require_owner_private(update: Update) -> bool:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private" or not is_super_admin(user.id):
        await update.effective_message.reply_text("Only bot owners can use this command in private chat.")
        return False
    return True

async def on_welcomestyle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    if not context.args:
        await update.effective_message.reply_text(
            f"Current: {current_welcome_style(chat.id)}\n\nUse:\n/welcomestyle list\n/welcomestyle random\n/welcomestyle gold"
        )
        return
    value = context.args[0].strip().lower()
    if value == "list":
        await update.effective_message.reply_text("Available themes:\n" + list_theme_names_text())
        return
    if value not in {"auto", "random"} and value not in THEME_NAMES:
        await update.effective_message.reply_text("Invalid theme.\nUse /welcomestyle list")
        return
    set_group_value(chat.id, "welcome_style", value)
    await update.effective_message.reply_text(f"Welcome style set to: {value}")

async def on_setfooter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    raw = update.effective_message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.effective_message.reply_text("Usage:\n/setfooter Powered by Maya")
        return
    footer = parts[1].strip()[:60]
    set_group_value(chat.id, "footer_text", footer)
    await update.effective_message.reply_text(f"Footer set to:\n{footer}")

async def on_groupcount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    known = count_known_groups()
    enabled = len(get_all_enabled_groups())
    await update.effective_message.reply_text(f"Known groups: {known}\nEnabled groups: {enabled}")

async def on_activegroups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    rows = get_active_groups(20)
    if not rows:
        await update.effective_message.reply_text("No active groups found.")
        return
    lines = ["Recent active groups:"]
    for r in rows:
        lines.append(f"- {r['title'] or 'Untitled'} | {r['chat_id']} | {format_ts(int(r['updated_at'] or 0))}")
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_failedgroups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    rows = get_recent_failed_groups(15)
    if not rows:
        await update.effective_message.reply_text("No failed groups recorded.")
        return
    lines = ["Recent failed groups:"]
    for r in rows:
        lines.append(f"- {r['title'] or 'Untitled'} | {r['chat_id']} | fails={r['fail_count']} | last={format_ts(int(r['last_time'] or 0))}")
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_lastaierrors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    rows = get_recent_ai_errors(10)
    if not rows:
        await update.effective_message.reply_text("No recent AI errors.")
        return
    lines = ["Recent AI errors:"]
    for r in rows:
        lines.append(f"- {format_ts(int(r['created_at'] or 0))} | {r['error']}")
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_broadcastphoto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.photo:
        await msg.reply_text("Reply to a photo with /broadcastphoto optional caption")
        return
    caption = " ".join(context.args).strip()
    file = await context.bot.get_file(msg.reply_to_message.photo[-1].file_id)
    data = bytes(await file.download_as_bytearray())
    groups = get_all_enabled_groups()
    ok_count = fail_count = 0
    status = await msg.reply_text(f"Broadcasting photo to {len(groups)} groups...")
    for gid in groups:
        try:
            bio = BytesIO(data)
            bio.name = "broadcast.jpg"
            await context.bot.send_photo(chat_id=gid, photo=bio, caption=caption or None)
            ok_count += 1
        except Exception as e:
            record_failure("broadcast", gid, "", f"broadcastphoto: {e}")
            fail_count += 1
    await status.edit_text(f"Photo broadcast finished.\nSuccess: {ok_count}\nFailed: {fail_count}")

async def on_broadcastvoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update):
        return
    msg = update.effective_message
    reply = msg.reply_to_message
    if not reply or not (reply.voice or reply.audio):
        await msg.reply_text("Reply to a voice/audio with /broadcastvoice optional caption")
        return
    source = reply.voice or reply.audio
    caption = " ".join(context.args).strip()
    file = await context.bot.get_file(source.file_id)
    data = bytes(await file.download_as_bytearray())
    groups = get_all_enabled_groups()
    ok_count = fail_count = 0
    status = await msg.reply_text(f"Broadcasting voice to {len(groups)} groups...")
    for gid in groups:
        try:
            bio = BytesIO(data)
            bio.name = "broadcast.ogg"
            await context.bot.send_voice(chat_id=gid, voice=bio, caption=caption or None)
            ok_count += 1
        except Exception as e:
            record_failure("broadcast", gid, "", f"broadcastvoice: {e}")
            fail_count += 1
    await status.edit_text(f"Voice broadcast finished.\nSuccess: {ok_count}\nFailed: {fail_count}")

async def on_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang, "lang_usage"))
        return
    new_lang = context.args[0].strip().lower()
    if new_lang not in {"bn", "en"}:
        await update.effective_message.reply_text(t(lang, "lang_usage"))
        return
    set_group_value(chat.id, "language", new_lang)
    await update.effective_message.reply_text(t(new_lang, "lang_set_en" if new_lang == "en" else "lang_set_bn"))

async def on_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        current = "ON" if int(group["voice_enabled"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "voice_usage", current=current))
        return
    value = context.args[0].strip().lower()
    if value not in {"on", "off"}:
        current = "ON" if int(group["voice_enabled"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "voice_usage", current=current))
        return
    set_group_value(chat.id, "voice_enabled", 1 if value == "on" else 0)
    await update.effective_message.reply_text(t(lang, "voice_set", value=value.upper()))

async def on_delete_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        current = "ON" if int(group["delete_service"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "deleteservice_usage", current=current))
        return
    value = context.args[0].strip().lower()
    if value not in {"on", "off"}:
        current = "ON" if int(group["delete_service"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "deleteservice_usage", current=current))
        return
    set_group_value(chat.id, "delete_service", 1 if value == "on" else 0)
    await update.effective_message.reply_text(t(lang, "deleteservice_set", value=value.upper()))

async def on_hourly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        current = "ON" if int(group["hourly_enabled"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "hourly_usage", current=current))
        return
    value = context.args[0].strip().lower()
    if value == "now":
        phase = phase_now()
        pool, source = await asyncio.to_thread(get_batch_pool, lang, phase)
        used_ai = source == "ai"
        msg = pick_hourly_message(chat.id, lang, phase, pool)
        await update.effective_message.reply_text(msg)
        set_group_value(chat.id, "last_hourly_at", int(time.time()))
        increment_group_counter(chat.id, "total_hourly_sent")
        if used_ai:
            set_group_value(chat.id, "last_ai_success_at", int(time.time()))
        else:
            set_group_value(chat.id, "last_fallback_used_at", int(time.time()))
        await update.effective_message.reply_text(t(lang, "hourly_now"))
        return
    if value not in {"on", "off"}:
        current = "ON" if int(group["hourly_enabled"]) == 1 else "OFF"
        await update.effective_message.reply_text(t(lang, "hourly_usage", current=current))
        return
    set_group_value(chat.id, "hourly_enabled", 1 if value == "on" else 0)
    if value == "on":
        set_group_value(chat.id, "last_hourly_at", 0)
    await update.effective_message.reply_text(t(lang, "hourly_set", value=value.upper()))

async def on_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    raw = update.effective_message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.effective_message.reply_text("Usage:\n/setwelcome your text\n\nAvailable placeholders:\n{name} = user mention\n{group} = group title\n{phase} = morning/day/evening/night")
        return
    set_group_value(chat.id, "custom_welcome", parts[1].strip()[:600])
    await update.effective_message.reply_text(t(lang, "welcome_saved"))

async def on_resetwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    set_group_value(chat.id, "custom_welcome", None)
    await update.effective_message.reply_text(t(lang, "welcome_reset"))

async def on_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    group = get_group(chat.id)
    lang = get_group_lang(chat.id)
    await update.effective_message.reply_text(
        t(
            lang,
            "status",
            lang_name="Bangla" if lang == "bn" else "English",
            voice="ON" if int(group["voice_enabled"]) == 1 else "OFF",
            delete_service="ON" if int(group["delete_service"]) == 1 else "OFF",
            hourly="ON" if int(group["hourly_enabled"]) == 1 else "OFF",
            tz=TIMEZONE_NAME,
            phase=phase_now(),
        )
    )

async def on_testwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    chat = update.effective_chat
    user = update.effective_user
    if chat and user and chat.type in {"group", "supergroup"}:
        await maybe_welcome(context, chat.id, chat.title or "", user)

async def on_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_super_admin(update.effective_user.id):
        await update.effective_message.reply_text(t("en", "broadcast_owner_only"))
        return
    raw = update.effective_message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.effective_message.reply_text(t("en", "broadcast_usage"))
        return
    groups = get_all_enabled_groups()
    if not groups:
        await update.effective_message.reply_text(t("en", "broadcast_none"))
        return
    status = await update.effective_message.reply_text(t("en", "broadcast_start", count=len(groups)))
    ok_count = 0
    fail_count = 0
    for gid in groups:
        try:
            await context.bot.send_message(chat_id=gid, text=parts[1].strip())
            ok_count += 1
        except Exception:
            fail_count += 1
    await status.edit_text(t("en", "broadcast_done", ok=ok_count, fail=fail_count))

async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in {"group", "supergroup"} or not update.effective_message:
        return
    ensure_group(chat.id, chat.title or "")
    group = get_group(chat.id)
    if int(group["delete_service"]) == 1:
        try:
            await update.effective_message.delete()
        except Exception:
            pass

    members = [m for m in (update.effective_message.new_chat_members or []) if not m.is_bot]
    if not members:
        return

    if len(members) >= 5:
        try:
            lang = get_group_lang(chat.id)
            names_text = build_combined_names(members)
            card = build_combined_welcome_card_bytes(
                chat.title or "GROUP",
                lang,
                names_text,
                style=current_welcome_style(chat.id),
                footer=current_footer_text(chat.id),
            )
            caption = build_burst_text(lang, chat.title or "", members)
            msg = await send_photo_with_retry(context.bot, chat_id=chat.id, photo=card, caption=caption)
            set_group_value(chat.id, "last_primary_msg_id", msg.message_id)
            increment_group_counter(chat.id, "total_welcome_sent", amount=len(members))
            set_group_value(chat.id, "last_welcome_at", int(time.time()))
            asyncio.create_task(schedule_delete(context.bot, chat.id, msg.message_id, WELCOME_DELETE_AFTER))
            for member in members:
                save_join_time(chat.id, member.id)
            await maybe_send_milestone(context, chat.id, chat.title or "", lang)
            return
        except Exception:
            logger.exception("Combined welcome failed in %s", chat.id)

    for member in members:
        await maybe_welcome(context, chat.id, chat.title or "", member)
        break

async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu:
        return
    chat = cmu.chat
    if chat.type not in {"group", "supergroup"}:
        return
    ensure_group(chat.id, chat.title or "")
    new_status = cmu.new_chat_member.status
    old_status = cmu.old_chat_member.status
    if cmu.new_chat_member.user.is_bot:
        return
    if old_status in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED} and new_status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
        await maybe_welcome(context, chat.id, chat.title or "", cmu.new_chat_member.user)

def hourly_loop():
    logger.info("Hourly loop started")
    while True:
        try:
            due_rows = get_enabled_groups_for_hourly()
            if due_rows:
                phase = phase_now()
                pools = {}
                pool_source = {}
                langs = {get_group_lang(int(r["chat_id"])) for r in due_rows}
                for lang in langs:
                    texts, source = get_batch_pool(lang, phase)
                    pools[lang] = texts
                    pool_source[lang] = source

                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang = get_group_lang(chat_id)
                    msg = pick_hourly_message(chat_id, lang, phase, pools[lang])
                    if send_message_http(chat_id, msg):
                        set_group_value(chat_id, "last_hourly_at", int(time.time()))
                        increment_group_counter(chat_id, "total_hourly_sent")
                        if pool_source.get(lang) == "ai":
                            set_group_value(chat_id, "last_ai_success_at", int(time.time()))
                        else:
                            set_group_value(chat_id, "last_fallback_used_at", int(time.time()))
                        logger.info("Hourly sent to %s", chat_id)
                    else:
                        logger.warning("Hourly failed to %s", chat_id)
        except Exception:
            logger.exception("hourly_loop failed")
        time.sleep(60)

async def post_init(application: Application):
    delete_webhook()
    commands = [
        BotCommand("start", "Show bot info"),
        BotCommand("ping", "Bot alive check"),
        BotCommand("support", "Support group"),
        BotCommand("myid", "Show your user id"),
        BotCommand("aistatus", "Check Groq AI status"),
        BotCommand("analytics", "Show group analytics"),
        BotCommand("setvoice", "Set Bengali female voice"),
        BotCommand("welcomestyle", "Set welcome banner theme"),
        BotCommand("setfooter", "Set welcome footer text"),
        BotCommand("lang", "Change group language"),
        BotCommand("groupcount", "Owner: count groups"),
        BotCommand("activegroups", "Owner: recent active groups"),
        BotCommand("failedgroups", "Owner: recent failed groups"),
        BotCommand("lastaierrors", "Owner: recent AI errors"),
        BotCommand("broadcastphoto", "Owner: broadcast replied photo"),
        BotCommand("broadcastvoice", "Owner: broadcast replied voice"),
        BotCommand("voice", "Toggle welcome voice"),
        BotCommand("deleteservice", "Toggle service delete"),
        BotCommand("hourly", "Toggle hourly texts"),
        BotCommand("setwelcome", "Custom welcome text"),
        BotCommand("resetwelcome", "Reset custom welcome"),
        BotCommand("status", "Show group status"),
        BotCommand("testwelcome", "Send test welcome"),
        BotCommand("broadcast", "Owner broadcast"),
    ]
    await application.bot.set_my_commands(commands)

def build_app() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", on_start))
    application.add_handler(CommandHandler("support", on_support))
    application.add_handler(CommandHandler("ping", on_ping))
    application.add_handler(CommandHandler("myid", on_myid))
    application.add_handler(CommandHandler("aistatus", on_ai_status))
    application.add_handler(CommandHandler("analytics", on_analytics))
    application.add_handler(CommandHandler("setvoice", on_setvoice))
    application.add_handler(CommandHandler("welcomestyle", on_welcomestyle))
    application.add_handler(CommandHandler("setfooter", on_setfooter))
    application.add_handler(CommandHandler("lang", on_lang))
    application.add_handler(CommandHandler("groupcount", on_groupcount))
    application.add_handler(CommandHandler("activegroups", on_activegroups))
    application.add_handler(CommandHandler("failedgroups", on_failedgroups))
    application.add_handler(CommandHandler("lastaierrors", on_lastaierrors))
    application.add_handler(CommandHandler("broadcastphoto", on_broadcastphoto))
    application.add_handler(CommandHandler("broadcastvoice", on_broadcastvoice))
    application.add_handler(CommandHandler("voice", on_voice))
    application.add_handler(CommandHandler("deleteservice", on_delete_service))
    application.add_handler(CommandHandler("hourly", on_hourly))
    application.add_handler(CommandHandler("setwelcome", on_setwelcome))
    application.add_handler(CommandHandler("resetwelcome", on_resetwelcome))
    application.add_handler(CommandHandler("status", on_status))
    application.add_handler(CommandHandler("testwelcome", on_testwelcome))
    application.add_handler(CommandHandler("broadcast", on_broadcast))
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_chat_members))
    application.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, track_group))
    return application

def main():
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=hourly_loop, daemon=True).start()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    logger.info("Flask started on port %s", PORT)
    logger.info("Starting %s", BOT_NAME)
    app = build_app()
    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
        close_loop=False,
    )

if __name__ == "__main__":
    main()
