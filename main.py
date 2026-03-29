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
from telegram import BotCommand, Update, Message
from telegram.constants import ChatMemberStatus, ParseMode, ChatAction
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
WELCOME_QUEUE_MIN_SECONDS = int(os.environ.get("WELCOME_QUEUE_MIN_SECONDS", "20"))
WELCOME_QUEUE_MAX_SECONDS = int(os.environ.get("WELCOME_QUEUE_MAX_SECONDS", "30"))
KEYWORD_REPLY_ENABLED_DEFAULT = os.environ.get("KEYWORD_REPLY_ENABLED_DEFAULT", "true").strip().lower() == "true"
KEYWORD_COOLDOWN_SECONDS = int(os.environ.get("KEYWORD_COOLDOWN_SECONDS", "900"))
KEYWORD_USER_COOLDOWN_SECONDS = int(os.environ.get("KEYWORD_USER_COOLDOWN_SECONDS", "600"))
KEYWORD_REPLY_CHANCE = float(os.environ.get("KEYWORD_REPLY_CHANCE", "0.55"))
HUMAN_DELAY_ENABLED = os.environ.get("HUMAN_DELAY_ENABLED", "true").strip().lower() == "true"
HOURLY_DELETE_AFTER_DEFAULT = int(os.environ.get("HOURLY_DELETE_AFTER_DEFAULT", "0"))
FESTIVAL_MODE_DEFAULT = os.environ.get("FESTIVAL_MODE_DEFAULT", "true").strip().lower() == "true"
EID_FITR_DATE = os.environ.get("EID_FITR_DATE", "").strip()
EID_ADHA_DATE = os.environ.get("EID_ADHA_DATE", "").strip()
COUNTDOWN_NOTIFY_WINDOW_DAYS = int(os.environ.get("COUNTDOWN_NOTIFY_WINDOW_DAYS", "7"))

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

AI_BATCH_CACHE: dict[tuple[str, str, str, str], dict] = {}
HOURLY_MOODS = ["peaceful", "motivating", "classy", "cozy", "soft", "energetic"]
pending_join_members: dict[int, dict[int, object]] = defaultdict(dict)
pending_join_titles: dict[int, str] = {}
pending_join_tasks: dict[int, asyncio.Task] = {}
keyword_last_chat_at: dict[int, float] = {}
keyword_last_user_at: dict[tuple[int, int], float] = {}
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS countdowns (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                target_ts INTEGER NOT NULL,
                event_type TEXT NOT NULL DEFAULT 'event',
                last_sent_day TEXT NOT NULL DEFAULT '',
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
            "hourly_delete_after": "INTEGER NOT NULL DEFAULT 0",
            "festival_mode": "INTEGER NOT NULL DEFAULT 1",
            "keyword_replies_enabled": "INTEGER NOT NULL DEFAULT 1",
            "mood_index": "INTEGER NOT NULL DEFAULT 0",
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
        "hourly_delete_after",
        "festival_mode",
        "keyword_replies_enabled",
        "mood_index",
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


def current_hourly_delete_after(chat_id: int) -> int:
    row = get_group(chat_id)
    return int(row["hourly_delete_after"] or 0) if row else 0

def current_festival_mode(chat_id: int) -> bool:
    row = get_group(chat_id)
    return bool(int(row["festival_mode"] or 1)) if row else FESTIVAL_MODE_DEFAULT

def current_keyword_mode(chat_id: int) -> bool:
    row = get_group(chat_id)
    return bool(int(row["keyword_replies_enabled"] or 1)) if row else KEYWORD_REPLY_ENABLED_DEFAULT

def current_mood_index(chat_id: int) -> int:
    row = get_group(chat_id)
    return int(row["mood_index"] or 0) if row else 0

def next_hourly_mood(chat_id: int) -> str:
    idx = current_mood_index(chat_id)
    mood = HOURLY_MOODS[idx % len(HOURLY_MOODS)]
    set_group_value(chat_id, "mood_index", (idx + 1) % len(HOURLY_MOODS))
    return mood

def peek_hourly_mood(chat_id: int) -> str:
    idx = current_mood_index(chat_id)
    return HOURLY_MOODS[idx % len(HOURLY_MOODS)]

def schedule_http_delete(chat_id: int, message_id: int, delay: int):
    def _worker():
        try:
            time.sleep(max(1, delay))
            tg_post("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
        except Exception:
            logger.exception("HTTP delete failed for %s/%s", chat_id, message_id)
    threading.Thread(target=_worker, daemon=True).start()

def current_festival():
    now = local_now()
    today = now.strftime("%Y-%m-%d")
    md = now.strftime("%m-%d")
    if EID_FITR_DATE and today == EID_FITR_DATE:
        return {"key": "eid_fitr", "name_bn": "ঈদ মোবারক", "name_en": "Eid Mubarak", "theme": "gold"}
    if EID_ADHA_DATE and today == EID_ADHA_DATE:
        return {"key": "eid_adha", "name_bn": "ঈদ মোবারক", "name_en": "Eid Mubarak", "theme": "emerald"}
    static = {
        "01-01": {"key": "new_year", "name_bn": "নতুন বছর", "name_en": "New Year", "theme": "crystal"},
        "03-26": {"key": "independence", "name_bn": "স্বাধীনতা দিবস", "name_en": "Independence Day", "theme": "royal-blue"},
        "04-14": {"key": "pohela_boishakh", "name_bn": "পহেলা বৈশাখ", "name_en": "Pohela Boishakh", "theme": "flame"},
        "12-16": {"key": "victory", "name_bn": "বিজয় দিবস", "name_en": "Victory Day", "theme": "emerald"},
    }
    return static.get(md)

def effective_style_footer(chat_id: int, style: str, footer: str):
    festival = current_festival() if current_festival_mode(chat_id) else None
    resolved_style = style
    resolved_footer = footer
    if festival:
        resolved_style = festival.get("theme") or style
        fest_name = festival["name_bn"] if get_group_lang(chat_id) == "bn" else festival["name_en"]
        if not resolved_footer:
            resolved_footer = f"{fest_name} | Powered by {BOT_NAME}"
    return resolved_style, resolved_footer, festival

def get_countdown(chat_id: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM countdowns WHERE chat_id = ?", (chat_id,)).fetchone()

def set_countdown(chat_id: int, title: str, target_ts: int, event_type: str):
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO countdowns (chat_id, title, target_ts, event_type, last_sent_day, created_at)
            VALUES (?, ?, ?, ?, '', ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                target_ts = excluded.target_ts,
                event_type = excluded.event_type,
                created_at = excluded.created_at
            """,
            (chat_id, title[:80], int(target_ts), event_type[:24], int(time.time())),
        )
        conn.commit()

def clear_countdown(chat_id: int):
    with db_connect() as conn:
        conn.execute("DELETE FROM countdowns WHERE chat_id = ?", (chat_id,))
        conn.commit()

def update_countdown_last_sent_day(chat_id: int, day_key: str):
    with db_connect() as conn:
        conn.execute("UPDATE countdowns SET last_sent_day = ? WHERE chat_id = ?", (day_key, chat_id))
        conn.commit()

async def human_delay(kind: str = "reply"):
    if not HUMAN_DELAY_ENABLED:
        return
    if kind == "reply":
        await asyncio.sleep(random.choice([1.5, 3.0, 5.0]))
    else:
        await asyncio.sleep(random.uniform(0.4, 1.2))

def parse_duration_to_seconds(value: str) -> int:
    v = value.strip().lower()
    if v in {"off", "0", "0m", "0h"}:
        return 0
    if v.endswith("m") and v[:-1].isdigit():
        return int(v[:-1]) * 60
    if v.endswith("h") and v[:-1].isdigit():
        return int(v[:-1]) * 3600
    if v.isdigit():
        return int(v)
    raise ValueError("Invalid duration")

def parse_countdown_input(raw: str):
    text = raw.strip()
    if "|" not in text:
        raise ValueError("Use format: /setcountdown YYYY-MM-DD HH:MM | Event title")
    left, right = [x.strip() for x in text.split("|", 1)]
    title = right[:80]
    dt = datetime.strptime(left, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(TIMEZONE_NAME))
    return int(dt.timestamp()), title

def keyword_reply_match(text: str):
    lowered = (text or "").lower().strip()
    checks = [
        ("salam", ["assalamu alaikum", "assalamualaikum", "আসসালামু আলাইকুম", "আসসালামু আলাইকুম"]),
        ("hello", ["hello everyone", "hello", "hi everyone", "hey everyone", "হ্যালো সবাই", "হাই সবাই"]),
        ("night", ["good night", "gn", "শুভ রাত্রি", "গুড নাইট"]),
    ]
    for key, patterns in checks:
        if any(p.lower() in lowered for p in patterns):
            return key
    return None

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
    if 17 <= h < 20:
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

BN_MOOD_MIDDLES = {
    "peaceful": ["মনটা আজ একটু শান্ত আর নরম থাকুক।", "শান্তির একটু ছোঁয়া থাকুক সবার ভেতর।", "আজকের সময়টা হোক মোলায়েম আর স্বস্তির।"],
    "motivating": ["আজও ভালো কিছুর জন্য এগিয়ে যাও।", "ছোট্ট করে হলেও এগিয়ে থাকো।", "মনোবলটা ধরে রাখো, ভালো কিছু অপেক্ষায় আছে।"],
    "classy": ["ভদ্রতা আর সৌন্দর্য একসাথেই থাকুক।", "নরম, পরিপাটি আর সুন্দর energy থাকুক চারদিকে।", "আজকের vibe হোক classy আর refined।"],
    "cozy": ["স্বস্তির ছোট্ট একটা কোণ খুঁজে নাও আজ।", "মনটাকে একটু আরাম দাও।", "আরামদায়ক, উষ্ণ একটা অনুভূতি থাকুক।"],
    "soft": ["কথা আর মন—দুটোই থাকুক কোমল।", "আজ একটু নরম থেকো নিজের প্রতিও।", "হালকা, শান্ত, মিষ্টি একটা সময় কাটুক।"],
    "energetic": ["আজকের সময়টা হোক প্রাণবন্ত।", "ভালো vibe নিয়ে এগিয়ে যাও সবাই।", "চারদিকে থাকুক চনমনে একটা অনুভূতি।"],
}
EN_MOOD_MIDDLES = {
    "peaceful": ["May your mind feel a little calmer today.", "A softer and quieter vibe for everyone here.", "Let this hour feel gentle and peaceful."],
    "motivating": ["Keep moving forward with quiet confidence.", "A little progress still matters today.", "Hold your energy steady and keep going."],
    "classy": ["May the vibe stay elegant and refined.", "A graceful little note for this lovely group.", "Let the mood stay polished and warm."],
    "cozy": ["Hope this hour feels warm and comforting.", "Take a small cozy pause for yourself.", "Wishing everyone a softer, warmer moment."],
    "soft": ["Keep your words and heart gentle today.", "May this moment feel light and tender.", "A soft little reminder to breathe and smile."],
    "energetic": ["Hope this hour feels bright and alive.", "Sending a lively and positive mood to everyone.", "Keep the energy fresh and uplifting."],
}

KEYWORD_REPLIES = {
    "bn": {
        "salam": ["ওয়ালাইকুমুস সালাম 🌷 সবাই ভালো থাকুন।", "ওয়ালাইকুমুস সালাম ✨ সবার জন্য শুভেচ্ছা।", "ওয়ালাইকুমুস সালাম 🌸 শান্তি থাকুক সবার মাঝে।"],
        "hello": ["হ্যালো সবাই 🌼 সুন্দর সময় কাটুক।", "সবাইকে মিষ্টি শুভেচ্ছা ✨", "হাই সবাই 🌷 group-এ ভালো vibe থাকুক।"],
        "night": ["শুভ রাত্রি 🌙 শান্তিতে থাকুন সবাই।", "মিষ্টি এক রাত কাটুক সবার 💙", "রাতটা হোক শান্ত আর স্বস্তির 🌌"],
    },
    "en": {
        "salam": ["Wa alaikum assalam 🌷 warm wishes to everyone.", "Wa alaikum assalam ✨ peace to everyone here.", "Wa alaikum assalam 🌸 wishing the group calm and warmth."],
        "hello": ["Hello everyone 🌼 hope you're all doing well.", "Hi everyone ✨ warm little wishes to the group.", "Hey everyone 🌷 hope the vibe stays lovely here."],
        "night": ["Good night 🌙 wishing everyone a peaceful rest.", "Have a calm and gentle night 💙", "Wishing the group a soft night ahead 🌌"],
    },
}

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
                    text = normalize_hourly_text(f"{a} {b} {c}".strip())
                    if is_valid_hourly_text(text, lang, phase):
                        result.append(text)
    else:
        for a in BN_PHASE_OPENERS[phase]:
            for b in BN_MIDDLES:
                for c in BN_ENDINGS:
                    text = normalize_hourly_text(f"{a} {b} {c}".strip())
                    if is_valid_hourly_text(text, lang, phase):
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



PHASE_BLOCKLIST = {
    "bn": {
        "morning": ("রাত", "রাত্রি", "শুভ রাত্রি", "শুভ সন্ধ্যা"),
        "day": ("শুভ সকাল", "সকালের", "ভোর", "রাত", "রাত্রি", "শুভ সন্ধ্যা"),
        "evening": ("শুভ সকাল", "সকালের", "ভোর", "শুভ রাত্রি", "রাতের"),
        "night": ("শুভ সকাল", "সকালের", "ভোর", "দুপুর", "বিকাল", "শুভ সন্ধ্যা"),
    },
    "en": {
        "morning": ("good night", "night", "evening"),
        "day": ("good morning", "morning", "good night", "night", "evening"),
        "evening": ("good morning", "morning", "good night", "night"),
        "night": ("good morning", "morning", "afternoon", "daytime", "good evening", "evening"),
    },
}

WEAK_GENERIC_PHRASES = {
    "bn": {
        "উজ্জ্বল থাকুন",
        "সুখী থাকুন",
        "শুভ সকাল",
        "শুভ সন্ধ্যা",
        "শুভ রাত্রি",
        "ভালো থাকুন",
        "আশা সবুজ থাকুক",
        "সুরেলা দিন কাটুক",
    },
    "en": {
        "stay bright",
        "stay happy",
        "good morning",
        "good evening",
        "good night",
        "stay well",
        "be happy",
    },
}

def normalize_hourly_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip(" -•*\t\r\n")
    if text and text[-1] not in ".!?।":
        text += "।" if re.search(r"[ঀ-৾]", text) else "."
    return text

def is_valid_hourly_text(line: str, lang: str, phase: str) -> bool:
    raw = line.strip()
    if not raw:
        return False
    line_lower = raw.lower()
    if len(raw) < 18 or len(raw) > AI_MAX_TEXT_LEN:
        return False
    if raw in WEAK_GENERIC_PHRASES.get(lang, set()):
        return False
    bad = ["18+", "sex", "sexy", "dating", "kiss", "adult", "nude", "xxx", "porn"]
    if any(b in line_lower for b in bad):
        return False
    blocked = PHASE_BLOCKLIST.get(lang, {}).get(phase, ())
    if any(token.lower() in line_lower for token in blocked):
        return False
    if re.fullmatch(r"[\W_]*(শুভ সকাল|শুভ সন্ধ্যা|শুভ রাত্রি|good morning|good evening|good night)[\W_]*", line_lower):
        return False
    return True

def sanitize_ai_lines(text: str, lang: str, phase: str) -> list[str]:
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        line = re.sub(r"^[\-\*\d\.\)\s]+", "", line)
        line = normalize_hourly_text(line)
        if not is_valid_hourly_text(line, lang, phase):
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

    phase_label = {
        "bn": {
            "morning": "সকাল",
            "day": "দিন বা দুপুর",
            "evening": "সন্ধ্যা",
            "night": "রাত",
        },
        "en": {
            "morning": "morning",
            "day": "daytime or afternoon",
            "evening": "evening",
            "night": "night",
        },
    }

    prompt = (
        f"Write {AI_BATCH_SIZE} short premium Telegram group hourly messages in "
        f"{'Bengali' if lang == 'bn' else 'English'}.\n"
        f"Current time phase: {phase_label['bn' if lang == 'bn' else 'en'][phase]}.\n"
        f"Rules:\n"
        f"- warm, elegant, premium, tasteful, group-safe\n"
        f"- non-sexual, non-romantic, non-political, non-religious\n"
        f"- no flirting\n"
        f"- no hashtags\n"
        f"- each line must feel complete and natural\n"
        f"- do NOT mention the wrong time phase\n"
        f"- do NOT say good night in morning/day/evening\n"
        f"- do NOT say good morning in day/evening/night\n"
        f"- keep each between 18 and {AI_MAX_TEXT_LEN} characters\n"
        f"- each line must be different\n"
        f"- avoid robotic short phrases\n"
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
                    {
                        "role": "system",
                        "content": (
                            "You write tasteful, premium, natural Telegram group texts. "
                            "Never mismatch time-of-day greetings. Avoid robotic one-liners."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.85,
                "max_tokens": 420,
            },
            timeout=GROQ_TIMEOUT_SECONDS,
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        lines = sanitize_ai_lines(content, lang, phase)
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
    cleaned_pool = [
        normalize_hourly_text(x)
        for x in pool
        if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)
    ]
    if not cleaned_pool:
        cleaned_pool = [
            normalize_hourly_text(x)
            for x in build_fallback_messages(lang, phase)
            if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)
        ]
    recent = recent_hourly_by_chat[chat_id]
    choices = [x for x in cleaned_pool if x not in recent]
    if not choices:
        choices = cleaned_pool[:]
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

async def copy_message_with_retry(bot, **kwargs):
    last_error = None
    for attempt in range(2):
        try:
            return await bot.copy_message(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("copy_message", kwargs.get("chat_id"), "", str(last_error))
    raise last_error


def guess_broadcast_action(msg) -> str:
    if not msg:
        return ChatAction.TYPING
    if getattr(msg, "photo", None):
        return ChatAction.UPLOAD_PHOTO
    if getattr(msg, "video", None):
        return ChatAction.UPLOAD_VIDEO
    if getattr(msg, "voice", None) or getattr(msg, "audio", None):
        return ChatAction.UPLOAD_VOICE
    if getattr(msg, "document", None):
        return ChatAction.UPLOAD_DOCUMENT
    return ChatAction.TYPING

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
    await on_broadcast(update, context)

async def on_broadcastvoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await on_broadcast(update, context)

async def on_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    msg = update.effective_message
    reply = msg.reply_to_message if msg else None
    raw = msg.text or ""
    parts = raw.split(" ", 1)
    arg_text = parts[1].strip() if len(parts) > 1 and parts[1].strip() else ""

    if not reply and not arg_text:
        await msg.reply_text(
            "Usage:\n"
            "/broadcast your message\n\n"
            "Or reply to any text/photo/video/voice/document and send:\n"
            "/broadcast"
        )
        return

    groups = get_all_enabled_groups()
    if not groups:
        await msg.reply_text(t("en", "broadcast_none"))
        return

    ok_count = 0
    fail_count = 0

    if reply:
        status = await msg.reply_text(f"Broadcasting replied content to {len(groups)} groups...")
        action = guess_broadcast_action(reply)
        for gid in groups:
            try:
                await bot_humanize(context.bot, gid, action=action, kind="reply")
                await copy_message_with_retry(
                    context.bot,
                    chat_id=gid,
                    from_chat_id=reply.chat_id,
                    message_id=reply.message_id,
                )
                ok_count += 1
            except Exception as e:
                record_failure("broadcast", gid, "", f"broadcast_copy: {e}")
                fail_count += 1
        await status.edit_text(
            f"Broadcast finished.\nMode: copy\nSuccess: {ok_count}\nFailed: {fail_count}"
        )
        return

    status = await msg.reply_text(t("en", "broadcast_start", count=len(groups)))
    for gid in groups:
        try:
            await bot_humanize(context.bot, gid, action=ChatAction.TYPING, kind="reply")
            await send_text_with_retry(context.bot, chat_id=gid, text=arg_text)
            ok_count += 1
        except Exception as e:
            record_failure("broadcast", gid, "", f"broadcast_text: {e}")
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



def send_message_http_full(chat_id: int, text: str) -> tuple[bool, int | None]:
    data = tg_post("sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
    ok = bool(data.get("ok"))
    mid = data.get("result", {}).get("message_id") if ok else None
    if not ok:
        record_failure("send_message", chat_id, "", str(data)[:400])
    return ok, mid

def build_countdown_card_bytes(group_title: str, event_title: str, days_left: int, hours_left: int, lang: str) -> BytesIO:
    width, height = 1280, 720
    phase = phase_now()
    c1, c2, accent, glow = theme_palette("halo", phase)
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
    title_font = pick_font(60, True)
    name_font = pick_font(42, True)
    sub_font = pick_font(28, False)
    big_font = pick_font(104, True)
    footer_font = pick_font(24, True)
    draw.text((160, 150), "COUNTDOWN", fill=glow, font=title_font)
    draw.text((160, 255), ascii_name(group_title or "GROUP").upper(), fill=(255, 226, 170), font=name_font)
    draw.text((160, 330), (event_title or "EVENT")[:44], fill=(222, 233, 255), font=sub_font)
    draw.text((160, 410), f"{days_left}D  {hours_left}H", fill=(255, 248, 190), font=big_font)
    footer_text = "Special event reminder" if lang == "en" else "বিশেষ ইভেন্টের কাউন্টডাউন"
    draw.text((160, 550), footer_text, fill=(214, 229, 255), font=footer_font)
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "countdown.png"
    bio.seek(0)
    return bio

def festival_hourly_prefix(lang: str):
    fest = current_festival()
    if not fest:
        return ""
    return fest["name_bn"] if lang == "bn" else fest["name_en"]

def build_fallback_messages(lang: str, phase: str, mood: str = "soft", festival_key: str = "") -> list[str]:
    key = (lang, phase, mood, festival_key)
    if key in FALLBACK_CACHE:
        return FALLBACK_CACHE[key]
    result = []
    if lang == "en":
        mood_bank = EN_MOOD_MIDDLES.get(mood, EN_MOOD_MIDDLES["soft"])
        for a in EN_PHASE_OPENERS[phase]:
            for b in EN_MIDDLES + mood_bank:
                for c in EN_ENDINGS:
                    text = normalize_hourly_text(f"{a} {b} {c}".strip())
                    if festival_key and len(text) < AI_MAX_TEXT_LEN - 24:
                        text = normalize_hourly_text(f"{festival_hourly_prefix(lang)} vibes — {text}")
                    if is_valid_hourly_text(text, lang, phase):
                        result.append(text)
    else:
        mood_bank = BN_MOOD_MIDDLES.get(mood, BN_MOOD_MIDDLES["soft"])
        for a in BN_PHASE_OPENERS[phase]:
            for b in BN_MIDDLES + mood_bank:
                for c in BN_ENDINGS:
                    text = normalize_hourly_text(f"{a} {b} {c}".strip())
                    if festival_key and len(text) < AI_MAX_TEXT_LEN - 22:
                        text = normalize_hourly_text(f"{festival_hourly_prefix(lang)} এর শুভ vibes। {text}")
                    if is_valid_hourly_text(text, lang, phase):
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

def sanitize_ai_lines(text: str, lang: str, phase: str) -> list[str]:
    lines = []
    for raw in text.splitlines():
        line = raw.strip()
        line = re.sub(r"^[\-\*\d\.\)\s]+", "", line)
        line = normalize_hourly_text(line)
        if not is_valid_hourly_text(line, lang, phase):
            continue
        lines.append(line)
    uniq = []
    seen = set()
    for x in lines:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq

def groq_generate_batch(lang: str, phase: str, mood: str = "soft", festival_key: str = "") -> list[str]:
    if not AI_HOURLY_ENABLED or not GROQ_API_KEY:
        _update_groq_status(False, "Groq disabled or API key missing")
        return []
    phase_label = {
        "bn": {"morning": "সকাল", "day": "দিন বা দুপুর", "evening": "সন্ধ্যা", "night": "রাত"},
        "en": {"morning": "morning", "day": "daytime or afternoon", "evening": "evening", "night": "night"},
    }
    festival_note = ""
    if festival_key:
        festival_note = f"- lightly reflect a festive mood for {festival_hourly_prefix(lang)}\n"
    prompt = (
        f"Write {AI_BATCH_SIZE} short premium Telegram group hourly messages in "
        f"{'Bengali' if lang == 'bn' else 'English'}.\n"
        f"Current time phase: {phase_label['bn' if lang == 'bn' else 'en'][phase]}.\n"
        f"Current mood wheel: {mood}.\n"
        f"Rules:\n"
        f"- warm, elegant, premium, tasteful, group-safe\n"
        f"- non-sexual, non-romantic, non-political, non-religious\n"
        f"- no flirting\n"
        f"- no hashtags\n"
        f"- each line must feel complete and natural\n"
        f"- do NOT mention the wrong time phase\n"
        f"{festival_note}"
        f"- keep each between 18 and {AI_MAX_TEXT_LEN} characters\n"
        f"- each line must be different\n"
        f"- avoid robotic short phrases\n"
        f"Return only the messages, one per line."
    )
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "You write tasteful, premium, natural Telegram group texts. Never mismatch time-of-day greetings. Avoid robotic one-liners."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.9,
                "max_tokens": 420,
            },
            timeout=GROQ_TIMEOUT_SECONDS,
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        lines = sanitize_ai_lines(content, lang, phase)
        if lines:
            _update_groq_status(True, f"OK | {len(lines)} lines | {mood}")
            logger.info("Groq hourly success | lang=%s phase=%s mood=%s count=%s", lang, phase, mood, len(lines))
            return lines
        _update_groq_status(False, "Groq returned empty/filtered text")
        record_failure("ai", None, "", "Groq returned empty/filtered text")
        return []
    except Exception as e:
        _update_groq_status(False, f"Failed: {e}")
        record_failure("ai", None, "", str(e))
        logger.exception("Groq hourly failed | lang=%s phase=%s mood=%s", lang, phase, mood)
        return []

def get_batch_pool(lang: str, phase: str, mood: str = "soft", festival_key: str = ""):
    key = (lang, phase, mood, festival_key)
    cached = AI_BATCH_CACHE.get(key)
    now_ts = time.time()
    if cached and now_ts - cached["created_at"] < 900 and cached.get("texts"):
        return cached["texts"], cached["source"]
    ai_lines = groq_generate_batch(lang, phase, mood=mood, festival_key=festival_key)
    if ai_lines:
        source = "ai"
        texts = ai_lines
    else:
        source = "fallback"
        texts = build_fallback_messages(lang, phase, mood=mood, festival_key=festival_key)
    AI_BATCH_CACHE[key] = {"texts": texts, "source": source, "created_at": now_ts}
    for line in texts[:min(len(texts), 12)]:
        try:
            save_generated_text(lang, phase, source, line)
        except Exception:
            pass
    return texts, source

def pick_hourly_message(chat_id: int, lang: str, phase: str, pool: list[str]) -> str:
    cleaned_pool = [normalize_hourly_text(x) for x in pool if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)]
    if not cleaned_pool:
        cleaned_pool = [normalize_hourly_text(x) for x in build_fallback_messages(lang, phase, mood=peek_hourly_mood(chat_id), festival_key=(current_festival() or {}).get("key", "")) if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)]
    recent = recent_hourly_by_chat[chat_id]
    choices = [x for x in cleaned_pool if x not in recent]
    if not choices:
        choices = cleaned_pool[:] or build_fallback_messages(lang, phase, mood=peek_hourly_mood(chat_id), festival_key=(current_festival() or {}).get("key", ""))
    text = random.choice(choices)
    recent.append(text)
    return text

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

    text_welcome, voice_text = welcome_texts(lang, mention_name, first_name, title or "", group["custom_welcome"])
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        style = current_welcome_style(chat_id)
        footer = current_footer_text(chat_id)
        style, footer, festival = effective_style_footer(chat_id, style, footer)
        if festival and len(text_welcome) < 900:
            fest_name = festival["name_bn"] if lang == "bn" else festival["name_en"]
            text_welcome = f"{text_welcome}\n\n✨ {fest_name}"
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

        if int(group["voice_enabled"]) == 1 and primary:
            try:
                voice_name = selected_voice_name(lang, chat_id)
                await make_voice_file(voice_text, voice_name, voice_path)
                voice_msg = await send_voice_with_retry(context.bot, chat_id=chat_id, voice=voice_path.read_bytes(), caption=t(lang, "welcome_voice_caption"))
            except Exception:
                logger.exception("Voice welcome failed in chat %s; keeping banner/text only", chat_id)

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

async def flush_join_queue(application: Application, chat_id: int):
    task = pending_join_tasks.pop(chat_id, None)
    title = pending_join_titles.pop(chat_id, "")
    members = list(pending_join_members.pop(chat_id, {}).values())
    if not members:
        return
    ctx = type("QueueContext", (), {"bot": application.bot})()
    lang = get_group_lang(chat_id)
    if len(members) >= 2:
        try:
            style = current_welcome_style(chat_id)
            footer = current_footer_text(chat_id)
            style, footer, festival = effective_style_footer(chat_id, style, footer)
            names_text = build_combined_names(members)
            card = build_combined_welcome_card_bytes(title or "GROUP", lang, names_text, style=style, footer=footer)
            caption = build_burst_text(lang, title or "", members)
            if festival:
                fest_name = festival["name_bn"] if lang == "bn" else festival["name_en"]
                caption = f"{caption}\n\n✨ {fest_name}"
            msg = await send_photo_with_retry(application.bot, chat_id=chat_id, photo=card, caption=caption)
            set_group_value(chat_id, "last_primary_msg_id", msg.message_id)
            set_group_value(chat_id, "last_voice_msg_id", None)
            increment_group_counter(chat_id, "total_welcome_sent", amount=len(members))
            set_group_value(chat_id, "last_welcome_at", int(time.time()))
            asyncio.create_task(schedule_delete(application.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER))
            await maybe_send_milestone(ctx, chat_id, title or "", lang)
            return
        except Exception:
            logger.exception("Queued combined welcome failed in %s", chat_id)
    for member in members[:1]:
        await maybe_welcome(ctx, chat_id, title or "", member)

async def queue_join_welcome(application: Application, chat_id: int, title: str, user):
    if user.is_bot:
        return
    if is_recent_duplicate(chat_id, user.id):
        return
    pending_join_members[chat_id][user.id] = user
    pending_join_titles[chat_id] = title or ""
    if chat_id not in pending_join_tasks or pending_join_tasks[chat_id].done():
        async def _runner():
            await asyncio.sleep(random.randint(WELCOME_QUEUE_MIN_SECONDS, WELCOME_QUEUE_MAX_SECONDS))
            await flush_join_queue(application, chat_id)
        pending_join_tasks[chat_id] = asyncio.create_task(_runner())

async def on_setcountdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    raw = (update.effective_message.text or "").split(" ", 1)
    if len(raw) < 2:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Usage:\n/setcountdown YYYY-MM-DD HH:MM | Event title")
        return
    try:
        target_ts, title = parse_countdown_input(raw[1])
        set_countdown(update.effective_chat.id, title, target_ts, "event")
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Countdown saved successfully.")
    except Exception as e:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(str(e))

async def on_showcountdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in {"group", "supergroup"}:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Use /countdown in group.")
        return
    row = get_countdown(chat.id)
    if not row:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("No countdown set for this group.")
        return
    now_ts = int(time.time())
    diff = max(0, int(row["target_ts"]) - now_ts)
    days_left = diff // 86400
    hours_left = (diff % 86400) // 3600
    lang = get_group_lang(chat.id)
    card = build_countdown_card_bytes(chat.title or "GROUP", row["title"], days_left, hours_left, lang)
    await human_delay_and_action(context, update)
    await send_photo_with_retry(context.bot, chat_id=chat.id, photo=card, caption=f"{row['title']}\n{days_left} days {hours_left} hours left")

async def on_clearcountdown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    clear_countdown(update.effective_chat.id)
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text("Countdown cleared.")

async def on_hourlyclean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    if not context.args:
        current = current_hourly_delete_after(chat.id)
        label = "OFF" if current <= 0 else f"{current//60}m" if current < 3600 else f"{current//3600}h"
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(f"Usage:\n/hourlyclean off\n/hourlyclean 30m\n/hourlyclean 1h\n\nCurrent: {label}")
        return
    try:
        seconds = parse_duration_to_seconds(context.args[0])
        set_group_value(chat.id, "hourly_delete_after", seconds)
        label = "OFF" if seconds <= 0 else f"{seconds//60}m" if seconds < 3600 else f"{seconds//3600}h"
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(f"Hourly auto-clean set to {label}.")
    except Exception:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Use /hourlyclean off, /hourlyclean 30m or /hourlyclean 1h")

async def on_keyword_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user
    if not chat or chat.type not in {"group", "supergroup"} or not msg or not user or user.is_bot:
        return
    ensure_group(chat.id, chat.title or "")
    if not current_keyword_mode(chat.id):
        return
    matched = keyword_reply_match(msg.text or "")
    if not matched:
        return
    now_ts = time.time()
    if now_ts - keyword_last_chat_at.get(chat.id, 0) < KEYWORD_COOLDOWN_SECONDS:
        return
    if now_ts - keyword_last_user_at.get((chat.id, user.id), 0) < KEYWORD_USER_COOLDOWN_SECONDS:
        return
    if random.random() > KEYWORD_REPLY_CHANCE:
        return
    keyword_last_chat_at[chat.id] = now_ts
    keyword_last_user_at[(chat.id, user.id)] = now_ts
    lang = get_group_lang(chat.id)
    replies = KEYWORD_REPLIES["en" if lang == "en" else "bn"][matched]
    await human_delay_and_action(context, update)
    try:
        await msg.reply_text(random.choice(replies))
    except Exception:
        logger.exception("Keyword reply failed in %s", chat.id)

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
    for member in members:
        chat_join_history[chat.id].append(time.time())
        await queue_join_welcome(context.application, chat.id, chat.title or "", member)

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
        chat_join_history[chat.id].append(time.time())
        await queue_join_welcome(context.application, chat.id, chat.title or "", cmu.new_chat_member.user)

async def on_hourly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        current = "ON" if int(group["hourly_enabled"]) == 1 else "OFF"
        mood = peek_hourly_mood(chat.id)
        clean_after = current_hourly_delete_after(chat.id)
        clean_label = "OFF" if clean_after <= 0 else f"{clean_after//60}m" if clean_after < 3600 else f"{clean_after//3600}h"
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(f"{t(lang, 'hourly_usage', current=current)}\nMood wheel: {mood}\nAuto-clean: {clean_label}")
        return
    value = context.args[0].strip().lower()
    if value == "now":
        phase = phase_now()
        mood = next_hourly_mood(chat.id)
        festival_key = (current_festival() or {}).get("key", "")
        pool, source = await asyncio.to_thread(get_batch_pool, lang, phase, mood, festival_key)
        used_ai = source == "ai"
        msg = pick_hourly_message(chat.id, lang, phase, pool)
        await human_delay_and_action(context, update)
        sent = await send_text_with_retry(context.bot, chat_id=chat.id, text=msg)
        clean_after = current_hourly_delete_after(chat.id)
        if clean_after > 0:
            asyncio.create_task(schedule_delete(context.bot, chat.id, sent.message_id, clean_after))
        set_group_value(chat.id, "last_hourly_at", int(time.time()))
        increment_group_counter(chat.id, "total_hourly_sent")
        if used_ai:
            set_group_value(chat.id, "last_ai_success_at", int(time.time()))
        else:
            set_group_value(chat.id, "last_fallback_used_at", int(time.time()))
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(f"{t(lang, 'hourly_now')}\nMood: {mood}")
        return
    if value not in {"on", "off"}:
        current = "ON" if int(group["hourly_enabled"]) == 1 else "OFF"
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(t(lang, "hourly_usage", current=current))
        return
    set_group_value(chat.id, "hourly_enabled", 1 if value == "on" else 0)
    if value == "on":
        set_group_value(chat.id, "last_hourly_at", 0)
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(t(lang, "hourly_set", value=value.upper()))

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    await human_delay_and_action(context, update)
    if update.effective_chat and update.effective_chat.type in {"group", "supergroup"}:
        await update.effective_message.reply_text(t(get_group_lang(update.effective_chat.id), "start_group"))
    else:
        await update.effective_message.reply_text(t("bn", "start_private"))

async def on_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group", "supergroup"} else "bn"
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(t(lang, "support"))

async def on_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group", "supergroup"} else "bn"
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(t(lang, "ping", tz=TIMEZONE_NAME, time=local_now().strftime("%I:%M %p")))

async def on_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else 0
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(t("en", "myid", user_id=uid))

async def maybe_send_countdown_reminder(chat_id: int, title: str):
    row = get_countdown(chat_id)
    if not row:
        return
    now = local_now()
    today_key = now.strftime("%Y-%m-%d")
    if row["last_sent_day"] == today_key:
        return
    diff = int(row["target_ts"]) - int(now.timestamp())
    if diff <= 0 or diff > COUNTDOWN_NOTIFY_WINDOW_DAYS * 86400:
        return
    days_left = diff // 86400
    hours_left = (diff % 86400) // 3600
    lang = get_group_lang(chat_id)
    card = build_countdown_card_bytes(title or "GROUP", row["title"], days_left, hours_left, lang)
    try:
        # send countdown as photo via Bot API HTTP not trivial; use text fallback in loop
        text = f"⏳ {row['title']}\n{days_left} days {hours_left} hours left"
        ok, mid = send_message_http_full(chat_id, text)
        if ok:
            update_countdown_last_sent_day(chat_id, today_key)
    except Exception:
        logger.exception("Countdown reminder failed for %s", chat_id)

def hourly_loop():
    logger.info("Hourly loop started")
    while True:
        try:
            due_rows = get_enabled_groups_for_hourly()
            if due_rows:
                phase = phase_now()
                prepared = {}
                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang = get_group_lang(chat_id)
                    mood = next_hourly_mood(chat_id)
                    festival_key = (current_festival() or {}).get("key", "") if current_festival_mode(chat_id) else ""
                    prepared[chat_id] = (lang, mood, festival_key)
                pools = {}
                pool_source = {}
                unique_keys = {(lang, mood, festival_key) for lang, mood, festival_key in prepared.values()}
                for lang, mood, festival_key in unique_keys:
                    texts, source = get_batch_pool(lang, phase, mood, festival_key)
                    pools[(lang, mood, festival_key)] = texts
                    pool_source[(lang, mood, festival_key)] = source

                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang, mood, festival_key = prepared[chat_id]
                    msg = pick_hourly_message(chat_id, lang, phase, pools[(lang, mood, festival_key)])
                    ok, mid = send_message_http_full(chat_id, msg)
                    if ok:
                        set_group_value(chat_id, "last_hourly_at", int(time.time()))
                        increment_group_counter(chat_id, "total_hourly_sent")
                        if pool_source.get((lang, mood, festival_key)) == "ai":
                            set_group_value(chat_id, "last_ai_success_at", int(time.time()))
                        else:
                            set_group_value(chat_id, "last_fallback_used_at", int(time.time()))
                        clean_after = current_hourly_delete_after(chat_id)
                        if clean_after > 0 and mid:
                            schedule_http_delete(chat_id, mid, clean_after)
                        try:
                            maybe_send_countdown_reminder(chat_id, row["title"] or "")
                        except Exception:
                            pass
                        logger.info("Hourly sent to %s | mood=%s", chat_id, mood)
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
        BotCommand("hourlyclean", "Auto-delete hourly messages"),
        BotCommand("setcountdown", "Set special event countdown"),
        BotCommand("countdown", "Show current countdown card"),
        BotCommand("clearcountdown", "Clear group countdown"),
        BotCommand("voice", "Toggle welcome voice"),
        BotCommand("deleteservice", "Toggle service delete"),
        BotCommand("hourly", "Toggle hourly texts"),
        BotCommand("setwelcome", "Custom welcome text"),
        BotCommand("resetwelcome", "Reset custom welcome"),
        BotCommand("status", "Show group status"),
        BotCommand("testwelcome", "Send test welcome"),
        BotCommand("broadcast", "Owner: broadcast text or replied media"),
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
    application.add_handler(CommandHandler("broadcastphoto", on_broadcast))
    application.add_handler(CommandHandler("broadcastvoice", on_broadcast))
    application.add_handler(CommandHandler("hourlyclean", on_hourlyclean))
    application.add_handler(CommandHandler("setcountdown", on_setcountdown))
    application.add_handler(CommandHandler("countdown", on_showcountdown))
    application.add_handler(CommandHandler("clearcountdown", on_clearcountdown))
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
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_keyword_message))
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, track_group))
    return application



# ===== Premium v6 overrides: festival APIs, multi Groq keys, weekly scheduler, exam reminders, link-safe keyword replies, typing actions =====

GROQ_API_KEYS_RAW = os.environ.get("GROQ_API_KEYS", "").strip()
GROQ_API_KEYS = [k.strip() for k in GROQ_API_KEYS_RAW.split(",") if k.strip()]
if not GROQ_API_KEYS and GROQ_API_KEY:
    GROQ_API_KEYS = [GROQ_API_KEY]
LAST_GROQ_STATUS["configured"] = bool(GROQ_API_KEYS)
LAST_GROQ_STATUS["key_count"] = len(GROQ_API_KEYS)

NAGER_COUNTRY_CODE = (os.environ.get("NAGER_COUNTRY_CODE", "BD").strip() or "BD").upper()
ALADHAN_COUNTRY = os.environ.get("ALADHAN_COUNTRY", "Bangladesh").strip() or "Bangladesh"
ALADHAN_CITY = os.environ.get("ALADHAN_CITY", "Dhaka").strip() or "Dhaka"

FRIDAY_SPECIAL_HOUR = int(os.environ.get("FRIDAY_SPECIAL_HOUR", "20"))
MONDAY_SPECIAL_HOUR = int(os.environ.get("MONDAY_SPECIAL_HOUR", "9"))
SPECIAL_EVENT_DELETE_AFTER = int(os.environ.get("SPECIAL_EVENT_DELETE_AFTER", "0"))

NAGER_YEAR_CACHE: dict[int, list] = {}
ALADHAN_DAY_CACHE: dict[str, dict] = {}
DAILY_EVENT_MARK_CACHE: dict[tuple[int, str, str], float] = {}
GROQ_KEY_POINTER = 0

URLISH_RE = re.compile(r"(https?://|www\.|t\.me/|\+[\w\-]{8,})", re.I)

_old_init_db = init_db

def init_db():
    _old_init_db()
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_events (
                chat_id INTEGER NOT NULL,
                event_kind TEXT NOT NULL,
                title TEXT NOT NULL,
                target_ts INTEGER NOT NULL,
                last_sent_day TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, event_kind)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_event_marks (
                chat_id INTEGER NOT NULL,
                event_key TEXT NOT NULL,
                day_key TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, event_key, day_key)
            )
            """
        )
        conn.commit()

def set_scheduled_event(chat_id: int, event_kind: str, title: str, target_ts: int):
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO scheduled_events (chat_id, event_kind, title, target_ts, last_sent_day, created_at)
            VALUES (?, ?, ?, ?, '', ?)
            ON CONFLICT(chat_id, event_kind) DO UPDATE SET
                title = excluded.title,
                target_ts = excluded.target_ts,
                last_sent_day = '',
                created_at = excluded.created_at
            """,
            (chat_id, event_kind, title[:90], target_ts, int(time.time())),
        )
        conn.commit()

def get_scheduled_event(chat_id: int, event_kind: str):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM scheduled_events WHERE chat_id = ? AND event_kind = ?",
            (chat_id, event_kind),
        ).fetchone()

def clear_scheduled_event(chat_id: int, event_kind: str):
    with db_connect() as conn:
        conn.execute(
            "DELETE FROM scheduled_events WHERE chat_id = ? AND event_kind = ?",
            (chat_id, event_kind),
        )
        conn.commit()

def mark_daily_event_sent(chat_id: int, event_key: str, day_key: str):
    with db_connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO daily_event_marks (chat_id, event_key, day_key, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (chat_id, event_key, day_key, int(time.time())),
        )
        conn.commit()
    DAILY_EVENT_MARK_CACHE[(chat_id, event_key, day_key)] = time.time()

def was_daily_event_sent(chat_id: int, event_key: str, day_key: str) -> bool:
    if (chat_id, event_key, day_key) in DAILY_EVENT_MARK_CACHE:
        return True
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM daily_event_marks
            WHERE chat_id = ? AND event_key = ? AND day_key = ?
            """,
            (chat_id, event_key, day_key),
        ).fetchone()
        return bool(row)

def get_all_enabled_group_rows():
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE enabled = 1").fetchall()

def cleanup_daily_marks():
    cutoff = int(time.time()) - 86400 * 60
    with db_connect() as conn:
        conn.execute("DELETE FROM daily_event_marks WHERE created_at < ?", (cutoff,))
        conn.commit()

def groq_candidate_keys() -> list[str]:
    global GROQ_KEY_POINTER
    if not GROQ_API_KEYS:
        return []
    start = GROQ_KEY_POINTER % len(GROQ_API_KEYS)
    ordered = GROQ_API_KEYS[start:] + GROQ_API_KEYS[:start]
    GROQ_KEY_POINTER = (GROQ_KEY_POINTER + 1) % max(1, len(GROQ_API_KEYS))
    return ordered

def _groq_chat_request(payload: dict):
    last_error = None
    for idx, key in enumerate(groq_candidate_keys(), start=1):
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=GROQ_TIMEOUT_SECONDS,
            )
            data = resp.json()
            if isinstance(data, dict) and data.get("choices"):
                LAST_GROQ_STATUS["last_key_index"] = idx
                return data
            last_error = data
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(str(last_error)[:500] if last_error is not None else "No Groq key succeeded")

def groq_live_check() -> tuple[bool, str]:
    if not GROQ_API_KEYS:
        _update_groq_status(False, "No Groq API key configured")
        return False, "No key configured"
    try:
        data = _groq_chat_request(
            {
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": "Reply with just OK"}],
                "max_tokens": 8,
                "temperature": 0,
            }
        )
        content = (data["choices"][0]["message"]["content"] or "").strip()
        result = content[:80] if content else "Empty"
        _update_groq_status(True, f"Live OK via key #{LAST_GROQ_STATUS.get('last_key_index', 1)}")
        return True, result
    except Exception as e:
        _update_groq_status(False, f"Live check failed: {e}")
        record_failure("ai", None, "", f"Live check failed: {e}")
        return False, str(e)[:120]

def fetch_nager_holidays(year: int) -> list:
    cached = NAGER_YEAR_CACHE.get(year)
    if cached is not None:
        return cached
    try:
        resp = requests.get(
            f"https://date.nager.at/api/v3/PublicHolidays/{year}/{NAGER_COUNTRY_CODE}",
            timeout=15,
        )
        data = resp.json()
        if isinstance(data, list):
            NAGER_YEAR_CACHE[year] = data
            return data
    except Exception:
        logger.exception("Nager.Date fetch failed for year %s", year)
    NAGER_YEAR_CACHE[year] = []
    return []

def fetch_aladhan_today() -> dict:
    today = local_now().strftime("%d-%m-%Y")
    if today in ALADHAN_DAY_CACHE:
        return ALADHAN_DAY_CACHE[today]
    # Prefer official API server; gToH is a common Gregorian-to-Hijri conversion endpoint.
    endpoints = [
        ("https://api.aladhan.com/v1/gToH", {"date": today}),
        ("https://api.aladhan.com/v1/gToHCalendar", {"month": local_now().strftime("%m"), "year": local_now().strftime("%Y"), "adjustment": 0}),
    ]
    result = {}
    for url, params in endpoints:
        try:
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
            if url.endswith("/gToH") and isinstance(data, dict) and data.get("data"):
                result = data["data"]
                break
            if url.endswith("/gToHCalendar") and isinstance(data, dict) and data.get("data"):
                day = int(local_now().strftime("%d"))
                calendar = data["data"]
                if 1 <= day <= len(calendar):
                    result = calendar[day - 1]
                    break
        except Exception:
            continue
    ALADHAN_DAY_CACHE[today] = result or {}
    return ALADHAN_DAY_CACHE[today]

def _map_nager_today() -> Optional[dict]:
    now = local_now()
    today = now.strftime("%Y-%m-%d")
    holidays = fetch_nager_holidays(now.year)
    for item in holidays:
        if item.get("date") != today:
            continue
        name = (item.get("name") or "").lower()
        local_name = (item.get("localName") or "").lower()
        combined = f"{name} {local_name}"
        if "new year" in combined:
            return {"key": "new_year", "name_bn": "নতুন বছর", "name_en": "New Year", "theme": "crystal"}
        if "independence" in combined:
            return {"key": "independence", "name_bn": "স্বাধীনতা দিবস", "name_en": "Independence Day", "theme": "royal-blue"}
        if "victory" in combined:
            return {"key": "victory", "name_bn": "বিজয় দিবস", "name_en": "Victory Day", "theme": "emerald"}
        if "bengali" in combined or "boishakh" in combined or "pohela" in combined or "pahela" in combined:
            return {"key": "pohela_boishakh", "name_bn": "পহেলা বৈশাখ", "name_en": "Pohela Boishakh", "theme": "flame"}
    return None

def _map_hijri_today() -> Optional[dict]:
    data = fetch_aladhan_today()
    hijri = {}
    if isinstance(data, dict):
        hijri = data.get("hijri") or data.get("data", {}).get("hijri") or {}
    month = str((hijri.get("month") or {}).get("number") or "")
    day = str(hijri.get("day") or "")
    if month == "10" and day == "1":
        return {"key": "eid_fitr", "name_bn": "ঈদ মোবারক", "name_en": "Eid Mubarak", "theme": "gold"}
    if month == "12" and day == "10":
        return {"key": "eid_adha", "name_bn": "ঈদ মোবারক", "name_en": "Eid Mubarak", "theme": "emerald"}
    return None

def current_festival():
    static = {
        "01-01": {"key": "new_year", "name_bn": "নতুন বছর", "name_en": "New Year", "theme": "crystal"},
        "03-26": {"key": "independence", "name_bn": "স্বাধীনতা দিবস", "name_en": "Independence Day", "theme": "royal-blue"},
        "04-14": {"key": "pohela_boishakh", "name_bn": "পহেলা বৈশাখ", "name_en": "Pohela Boishakh", "theme": "flame"},
        "12-16": {"key": "victory", "name_bn": "বিজয় দিবস", "name_en": "Victory Day", "theme": "emerald"},
    }
    # APIs first, then fallback to static dates.
    by_nager = _map_nager_today()
    if by_nager:
        return by_nager
    by_hijri = _map_hijri_today()
    if by_hijri:
        return by_hijri
    return static.get(local_now().strftime("%m-%d"))

async def human_delay_and_action(context: ContextTypes.DEFAULT_TYPE, update: Update, action: str = "typing"):
    chat = update.effective_chat if update else None
    if chat:
        try:
            await context.bot.send_chat_action(chat_id=chat.id, action=action)
        except Exception:
            pass
    await human_delay()

async def bot_humanize(bot, chat_id: int, action: str = "typing", kind: str = "reply"):
    try:
        await bot.send_chat_action(chat_id=chat_id, action=action)
    except Exception:
        pass
    if not HUMAN_DELAY_ENABLED:
        return
    if kind == "reply":
        await asyncio.sleep(random.choice([1.5, 3.0, 5.0]))
    elif kind == "photo":
        await asyncio.sleep(random.uniform(1.0, 2.2))
    elif kind == "voice":
        await asyncio.sleep(random.uniform(1.1, 2.4))
    else:
        await asyncio.sleep(random.uniform(0.5, 1.2))

def http_humanize(chat_id: int, action: str = "typing", kind: str = "auto"):
    try:
        tg_post("sendChatAction", {"chat_id": chat_id, "action": action})
    except Exception:
        pass
    if HUMAN_DELAY_ENABLED:
        if kind == "auto":
            time.sleep(random.uniform(0.4, 1.0))
        else:
            time.sleep(random.choice([1.5, 3.0]))

async def send_photo_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.UPLOAD_PHOTO, "photo")
            return await bot.send_photo(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_photo", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

async def send_voice_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.UPLOAD_VOICE, "voice")
            return await bot.send_voice(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_voice", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

async def send_text_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.TYPING, "reply")
            return await bot.send_message(**kwargs)
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_message", kwargs.get("chat_id"), "", str(last_error))
    raise last_error

def send_message_http_full(chat_id: int, text: str) -> tuple[bool, int | None]:
    http_humanize(chat_id, "typing", "auto")
    data = tg_post("sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
    ok = bool(data.get("ok"))
    mid = data.get("result", {}).get("message_id") if ok else None
    if not ok:
        record_failure("send_message", chat_id, "", str(data)[:400])
    return ok, mid

def shorten_name(name: str) -> str:
    name = clean_name(name)
    return name.split()[0][:18] if name else "বন্ধু"

def sweet_name(name: str, lang: str) -> str:
    short = shorten_name(name)
    if lang == "en":
        return short
    if short.endswith("া") or short.endswith("ি"):
        return short
    return short

def voice_name_variant(full_name: str, lang: str) -> str:
    options = [
        full_name,
        shorten_name(full_name),
        sweet_name(full_name, lang),
    ]
    clean = [x for x in options if x]
    return random.choice(clean)

def personalize_voice_text(voice_text: str, first_name: str, lang: str) -> str:
    variant = voice_name_variant(first_name, lang)
    if lang == "en":
        prefixes = [
            f"Hello {variant}. ",
            f"Hi {variant}. ",
            f"{variant}, ",
        ]
    else:
        prefixes = [
            f"হ্যালো {variant}। ",
            f"{variant}, ",
            f"শোনো {variant}। ",
        ]
    prefix = random.choice(prefixes)
    if voice_text.lower().startswith(("hello", "hi", "হ্যালো", first_name.lower())):
        return voice_text
    return f"{prefix}{voice_text}"

def is_linkish_message(msg: Message) -> bool:
    text = (msg.text or msg.caption or "").strip()
    if not text:
        return False
    if URLISH_RE.search(text):
        return True
    entities = list(msg.entities or []) + list(msg.caption_entities or [])
    for ent in entities:
        try:
            ent_type = str(ent.type).lower()
            if "url" in ent_type or "text_link" in ent_type:
                return True
        except Exception:
            continue
    if getattr(msg, "forward_origin", None) or getattr(msg, "forward_date", None):
        return True
    return False

def keyword_reply_match(text: str):
    lowered = re.sub(r"\s+", " ", (text or "").strip().lower())
    if not lowered or URLISH_RE.search(lowered):
        return None
    cleaned = re.sub(r"[^\w\s\u0980-\u09ff]", " ", lowered)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if len(cleaned) > 60:
        return None
    checks = [
        ("salam", [r"\bassalamu alaikum\b", r"\bassalamualaikum\b", r"^আসসালামু আলাইকুম$", r"^আসসালামু আলাইকুম$"]),
        ("hello", [r"\bhello everyone\b", r"\bhi everyone\b", r"\bhey everyone\b", r"^হ্যালো সবাই$", r"^হাই সবাই$"]),
        ("night", [r"\bgood night\b", r"^gn$", r"^শুভ রাত্রি$", r"^গুড নাইট$"]),
    ]
    for key, patterns in checks:
        for p in patterns:
            if re.search(p, cleaned, re.I):
                return key
    return None

async def on_keyword_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user
    if not chat or chat.type not in {"group", "supergroup"} or not msg or not user or user.is_bot:
        return
    ensure_group(chat.id, chat.title or "")
    if not current_keyword_mode(chat.id):
        return
    if is_linkish_message(msg):
        return
    matched = keyword_reply_match(msg.text or "")
    if not matched:
        return
    now_ts = time.time()
    if now_ts - keyword_last_chat_at.get(chat.id, 0) < KEYWORD_COOLDOWN_SECONDS:
        return
    if now_ts - keyword_last_user_at.get((chat.id, user.id), 0) < KEYWORD_USER_COOLDOWN_SECONDS:
        return
    if random.random() > KEYWORD_REPLY_CHANCE:
        return
    keyword_last_chat_at[chat.id] = now_ts
    keyword_last_user_at[(chat.id, user.id)] = now_ts
    lang = get_group_lang(chat.id)
    replies = KEYWORD_REPLIES["en" if lang == "en" else "bn"][matched]
    try:
        await bot_humanize(context.bot, chat.id, ChatAction.TYPING, "reply")
        await msg.reply_text(random.choice(replies))
    except Exception:
        logger.exception("Keyword reply failed in %s", chat.id)

def _special_lines(lang: str, key: str) -> list[str]:
    bank = {
        "bn": {
            "monday": [
                "🌟 নতুন সপ্তাহটা সুন্দরভাবে শুরু হোক। মনোযোগ, শান্তি আর সাফল্য থাকুক সবার সাথে।",
                "💼 সোমবার মানেই নতুন শুরু। আজকের দিনটা হোক গুছানো আর ফলপ্রসূ।",
                "✨ সপ্তাহের শুরুতে এই group-এর সবার জন্য রইল উজ্জ্বল শুভেচ্ছা।",
            ],
            "friday": [
                "🕌 জুমার দিনটা হোক শান্ত, সুন্দর আর বরকতময়। এই group-এর সবার জন্য শুভেচ্ছা।",
                "🌙 শুক্রবারের কোমল শুভেচ্ছা। আজকের দিনটা হোক প্রশান্তি ভরা।",
                "💙 জুমার দিনের মিষ্টি শুভেচ্ছা রইল। ভালো থাকুন সবাই।",
            ],
            "exam": [
                "📘 আজ {title}। শান্ত থাকুন, মনোযোগ ধরে রাখুন, আর নিজের সেরাটা দিন।",
                "📝 আজ {title}। আত্মবিশ্বাস রাখুন — ইনশাআল্লাহ ভালো হবে।",
                "🎯 {title} আজ। মনটা শান্ত রেখে সুন্দরভাবে এগিয়ে যান।",
            ],
        },
        "en": {
            "monday": [
                "🌟 A fresh week begins today. Wishing everyone focus, calm, and a strong start.",
                "💼 Monday is a new beginning. Hope the day feels organized and productive.",
                "✨ Warm wishes to this group for a bright and graceful week ahead.",
            ],
            "friday": [
                "🌙 Soft Friday wishes to everyone. Hope the day feels peaceful and kind.",
                "💙 Wishing this group a calm, gentle, and beautiful Friday.",
                "✨ May your Friday carry a little more peace and comfort.",
            ],
            "exam": [
                "📘 {title} is today. Stay calm, focused, and do your best.",
                "📝 It's {title} today. Trust yourself and move forward with confidence.",
                "🎯 {title} is today. Keep your mind steady and your heart calm.",
            ],
        },
    }
    return bank["en" if lang == "en" else "bn"][key]

def maybe_weekly_special_text(lang: str) -> tuple[str, str] | None:
    now = local_now()
    if now.weekday() == 0 and now.hour >= MONDAY_SPECIAL_HOUR:
        return "weekly_monday", random.choice(_special_lines(lang, "monday"))
    if now.weekday() == 4 and now.hour >= FRIDAY_SPECIAL_HOUR:
        return "weekly_friday", random.choice(_special_lines(lang, "friday"))
    return None

async def maybe_send_scheduled_specials(chat_row):
    chat_id = int(chat_row["chat_id"])
    title = chat_row["title"] or "GROUP"
    lang = get_group_lang(chat_id)
    today_key = local_now().strftime("%Y-%m-%d")

    weekly = maybe_weekly_special_text(lang)
    if weekly:
        event_key, text = weekly
        if not was_daily_event_sent(chat_id, event_key, today_key):
            ok, mid = send_message_http_full(chat_id, text)
            if ok:
                mark_daily_event_sent(chat_id, event_key, today_key)
                if SPECIAL_EVENT_DELETE_AFTER > 0 and mid:
                    schedule_http_delete(chat_id, mid, SPECIAL_EVENT_DELETE_AFTER)

    exam_row = get_scheduled_event(chat_id, "exam")
    if exam_row:
        target = int(exam_row["target_ts"])
        now_ts = int(local_now().timestamp())
        event_day = datetime.fromtimestamp(target, ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d")
        if event_day == today_key and exam_row["last_sent_day"] != today_key and now_ts >= target:
            text = random.choice(_special_lines(lang, "exam")).format(title=exam_row["title"])
            try:
                ok, mid = send_message_http_full(chat_id, text)
                if ok and SPECIAL_EVENT_DELETE_AFTER > 0 and mid:
                    schedule_http_delete(chat_id, mid, SPECIAL_EVENT_DELETE_AFTER)
            except Exception:
                pass
            with db_connect() as conn:
                conn.execute(
                    "UPDATE scheduled_events SET last_sent_day = ? WHERE chat_id = ? AND event_kind = 'exam'",
                    (today_key, chat_id),
                )
                conn.commit()

def hourly_loop():
    logger.info("Hourly loop started")
    while True:
        try:
            due_rows = get_enabled_groups_for_hourly()
            if due_rows:
                phase = phase_now()
                prepared = {}
                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang = get_group_lang(chat_id)
                    mood = next_hourly_mood(chat_id)
                    festival_key = (current_festival() or {}).get("key", "") if current_festival_mode(chat_id) else ""
                    prepared[chat_id] = (lang, mood, festival_key)
                pools = {}
                pool_source = {}
                unique_keys = {(lang, mood, festival_key) for lang, mood, festival_key in prepared.values()}
                for lang, mood, festival_key in unique_keys:
                    texts, source = get_batch_pool(lang, phase, mood, festival_key)
                    pools[(lang, mood, festival_key)] = texts
                    pool_source[(lang, mood, festival_key)] = source

                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang, mood, festival_key = prepared[chat_id]
                    msg = pick_hourly_message(chat_id, lang, phase, pools[(lang, mood, festival_key)])
                    ok, mid = send_message_http_full(chat_id, msg)
                    if ok:
                        set_group_value(chat_id, "last_hourly_at", int(time.time()))
                        increment_group_counter(chat_id, "total_hourly_sent")
                        if pool_source.get((lang, mood, festival_key)) == "ai":
                            set_group_value(chat_id, "last_ai_success_at", int(time.time()))
                        else:
                            set_group_value(chat_id, "last_fallback_used_at", int(time.time()))
                        clean_after = current_hourly_delete_after(chat_id)
                        if clean_after > 0 and mid:
                            schedule_http_delete(chat_id, mid, clean_after)
                        try:
                            maybe_send_countdown_reminder(chat_id, row["title"] or "")
                        except Exception:
                            pass
                        logger.info("Hourly sent to %s | mood=%s", chat_id, mood)
                    else:
                        logger.warning("Hourly failed to %s", chat_id)

            # Independent weekly special + exam reminder loop for all enabled groups.
            for row in get_all_enabled_group_rows():
                try:
                    asyncio.run(maybe_send_scheduled_specials(row))
                except RuntimeError:
                    # If an event loop is already present for some reason, fall back to direct call path.
                    try:
                        loop = asyncio.new_event_loop()
                        loop.run_until_complete(maybe_send_scheduled_specials(row))
                        loop.close()
                    except Exception:
                        logger.exception("Special scheduler failed in %s", row["chat_id"])
                except Exception:
                    logger.exception("Special scheduler failed in %s", row["chat_id"])

            cleanup_daily_marks()
        except Exception:
            logger.exception("hourly_loop failed")
        time.sleep(60)

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
    voice_text = personalize_voice_text(voice_text, first_name, lang)
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        style = current_welcome_style(chat_id)
        footer = current_footer_text(chat_id)
        style, footer, festival = effective_style_footer(chat_id, style, footer)
        if festival and len(text_welcome) < 900:
            fest_name = festival["name_bn"] if lang == "bn" else festival["name_en"]
            text_welcome = f"{text_welcome}\n\n✨ {fest_name}"
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

        if int(group["voice_enabled"]) == 1 and primary:
            try:
                voice_name = selected_voice_name(lang, chat_id)
                await make_voice_file(voice_text, voice_name, voice_path)
                voice_msg = await send_voice_with_retry(context.bot, chat_id=chat_id, voice=voice_path.read_bytes(), caption=t(lang, "welcome_voice_caption"))
            except Exception:
                logger.exception("Voice welcome failed in chat %s; keeping banner/text only", chat_id)

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
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Only group admins or bot owners can use this command.")
        return
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text("Checking Groq status...")
    ok, result = await asyncio.to_thread(groq_live_check)
    checked = LAST_GROQ_STATUS["last_checked_at"] or "Never"
    configured = "YES" if GROQ_API_KEYS else "NO"
    enabled = "YES" if AI_HOURLY_ENABLED else "NO"
    lang = get_group_lang(chat.id) if chat and chat.type in {"group", "supergroup"} else "en"
    key_count = len(GROQ_API_KEYS)
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(
        f"{t(lang, 'aistatus', configured=configured, enabled=enabled, checked=checked, result=('OK' if ok else 'FAILED') + f' | {result}', model=GROQ_MODEL)}\nKeys configured: {key_count}"
    )

async def on_setexamday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    raw = (update.effective_message.text or "").split(" ", 1)
    if len(raw) < 2:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Usage:\n/setexamday YYYY-MM-DD HH:MM | Exam title")
        return
    try:
        target_ts, title = parse_countdown_input(raw[1])
        set_scheduled_event(update.effective_chat.id, "exam", title, target_ts)
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("Exam day reminder saved successfully.")
    except Exception as e:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text(str(e))

async def on_examday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    row = get_scheduled_event(update.effective_chat.id, "exam")
    if not row:
        await human_delay_and_action(context, update)
        await update.effective_message.reply_text("No exam day reminder set.")
        return
    dt = datetime.fromtimestamp(int(row["target_ts"]), ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d %I:%M %p")
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(f"Exam reminder:\n{row['title']}\n{dt}")

async def on_clearexamday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    clear_scheduled_event(update.effective_chat.id, "exam")
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text("Exam day reminder cleared.")

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
        BotCommand("hourlyclean", "Auto-delete hourly messages"),
        BotCommand("setcountdown", "Set special event countdown"),
        BotCommand("countdown", "Show current countdown card"),
        BotCommand("clearcountdown", "Clear group countdown"),
        BotCommand("setexamday", "Set exam day reminder"),
        BotCommand("examday", "Show exam day reminder"),
        BotCommand("clearexamday", "Clear exam day reminder"),
        BotCommand("voice", "Toggle welcome voice"),
        BotCommand("deleteservice", "Toggle service delete"),
        BotCommand("hourly", "Toggle hourly texts"),
        BotCommand("setwelcome", "Custom welcome text"),
        BotCommand("resetwelcome", "Reset custom welcome"),
        BotCommand("status", "Show group status"),
        BotCommand("testwelcome", "Send test welcome"),
        BotCommand("broadcast", "Owner: broadcast text or replied media"),
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
    application.add_handler(CommandHandler("broadcastphoto", on_broadcast))
    application.add_handler(CommandHandler("broadcastvoice", on_broadcast))
    application.add_handler(CommandHandler("hourlyclean", on_hourlyclean))
    application.add_handler(CommandHandler("setcountdown", on_setcountdown))
    application.add_handler(CommandHandler("countdown", on_showcountdown))
    application.add_handler(CommandHandler("clearcountdown", on_clearcountdown))
    application.add_handler(CommandHandler("setexamday", on_setexamday))
    application.add_handler(CommandHandler("examday", on_examday))
    application.add_handler(CommandHandler("clearexamday", on_clearexamday))
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
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_keyword_message))
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, track_group))
    return application
# ===== end Premium v6 overrides =====



# ===== Premium v7 behavior pack overrides =====

def ensure_behavior_db():
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_text_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                text_norm TEXT NOT NULL,
                signature TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(groups)").fetchall()}
        migrations = {
            "last_presence_at": "INTEGER NOT NULL DEFAULT 0",
            "message_taste": "TEXT NOT NULL DEFAULT 'auto'",
            "variant_cursor": "INTEGER NOT NULL DEFAULT 0",
        }
        for col, ddl in migrations.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE groups ADD COLUMN {col} {ddl}")
        conn.commit()


def normalize_history_text(text: str) -> str:
    s = normalize_hourly_text(text or "")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s[:260]


def structure_signature(text: str) -> str:
    raw = normalize_history_text(text)
    if not raw:
        return "empty"
    words = raw.split()
    length_bucket = "s" if len(words) <= 6 else "m" if len(words) <= 13 else "l"
    sentence_bucket = str(max(1, len([x for x in re.split(r"[\.!?।]+", raw) if x.strip()])))
    starts = [
        ("greet", r"^(শুভ|hello|hi|good|warm|soft|gentle|calm|peaceful)\b"),
        ("wish", r"^(আশা|wishing|hope|may|আজ|today|এই)\b"),
        ("group", r"^(এই group|this group|সবাইকে|everyone)\b"),
    ]
    starter = "other"
    for name, pat in starts:
        if re.search(pat, raw, re.I):
            starter = name
            break
    emoji = "e1" if re.match(r"^[^\w\s]", text or "") else "e0"
    punct = "q" if "?" in raw else "x" if "!" in raw else "d"
    return f"{starter}|{length_bucket}|{sentence_bucket}|{emoji}|{punct}"


def was_recent_duplicate_text(chat_id: int, kind: str, text: str, lookback_days: int = 3) -> bool:
    since = int(time.time()) - (lookback_days * 86400)
    text_norm = normalize_history_text(text)
    sig = structure_signature(text)
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT 1 FROM sent_text_history
            WHERE chat_id = ? AND kind = ? AND created_at >= ?
              AND (text_norm = ? OR signature = ?)
            LIMIT 1
            """,
            (chat_id, kind, since, text_norm, sig),
        ).fetchone()
        return bool(row)


def record_sent_history(chat_id: int, kind: str, text: str):
    text_norm = normalize_history_text(text)
    if not text_norm:
        return
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO sent_text_history (chat_id, kind, text_norm, signature, created_at) VALUES (?, ?, ?, ?, ?)",
            (chat_id, kind[:24], text_norm, structure_signature(text), int(time.time())),
        )
        conn.commit()


def mark_presence(chat_id: int):
    try:
        set_group_value(chat_id, "last_presence_at", int(time.time()))
    except Exception:
        pass


def get_presence_gap(chat_id: int) -> int:
    row = get_group(chat_id)
    if not row:
        return 999999
    last_ts = int(row["last_presence_at"] or 0)
    if not last_ts:
        return 999999
    return max(0, int(time.time()) - last_ts)


def presence_tier(chat_id: int) -> str:
    gap = get_presence_gap(chat_id)
    if gap >= 4 * 3600:
        return "rich"
    if gap >= 3600:
        return "warm"
    if gap >= 900:
        return "normal"
    return "short"


group_taste_memory: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=40))


def detect_text_taste(text: str, title: str = "") -> str:
    sample = f"{title or ''} {(text or '')}".lower()
    if URLISH_RE.search(sample) or "http" in sample or "t.me" in sample or "link" in sample:
        return "minimal"
    classy_hints = ["official", "academy", "study", "crypto", "news", "family", "community", "team"]
    soft_hints = ["💗", "💕", "🌸", "✨", "dear", "sweet", "cute", "gentle", "soft", "calm"]
    if any(x in sample for x in classy_hints):
        return "classy"
    if any(x in sample for x in soft_hints):
        return "soft"
    return "balanced"


def current_message_taste(chat_id: int, title: str = "") -> str:
    row = get_group(chat_id)
    if row and (row["message_taste"] or "").strip().lower() not in {"", "auto"}:
        val = row["message_taste"].strip().lower()
        if val in {"minimal", "classy", "soft", "balanced"}:
            return val
    votes = list(group_taste_memory[chat_id])
    title_vote = detect_text_taste("", title)
    votes.append(title_vote)
    if not votes:
        return "balanced"
    counts = {k: votes.count(k) for k in {"minimal", "classy", "soft", "balanced"}}
    winner = max(counts.items(), key=lambda kv: kv[1])[0]
    return winner


async def track_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in {"group", "supergroup"}:
        ensure_group(chat.id, chat.title or "")
        msg = update.effective_message
        if msg and not getattr(msg, "from_user", None) or (msg and getattr(msg.from_user, "is_bot", False)):
            return
        if msg:
            body = (msg.text or msg.caption or "")[:300]
            taste = detect_text_taste(body, chat.title or "")
            group_taste_memory[chat.id].append(taste)


PHASE_LOCKED_MOODS = {
    "morning": ["motivating", "soft", "peaceful"],
    "day": ["motivating", "classy", "energetic", "soft"],
    "evening": ["cozy", "classy", "peaceful", "soft"],
    "night": ["peaceful", "cozy", "soft", "classy"],
}


def current_mood_index(chat_id: int) -> int:
    row = get_group(chat_id)
    return int(row["mood_index"] or 0) if row else 0


def next_hourly_mood(chat_id: int) -> str:
    phase = phase_now()
    allowed = PHASE_LOCKED_MOODS.get(phase, HOURLY_MOODS)
    idx = current_mood_index(chat_id)
    mood = allowed[idx % len(allowed)]
    set_group_value(chat_id, "mood_index", (idx + 1) % max(1, len(allowed)))
    return mood


def peek_hourly_mood(chat_id: int) -> str:
    phase = phase_now()
    allowed = PHASE_LOCKED_MOODS.get(phase, HOURLY_MOODS)
    idx = current_mood_index(chat_id)
    return allowed[idx % len(allowed)]


def filter_pool_by_taste(chat_id: int, pool: list[str]) -> list[str]:
    row = get_group(chat_id)
    title = row["title"] if row else ""
    taste = current_message_taste(chat_id, title or "")
    out = []
    for text in pool:
        emoji_count = sum(1 for ch in text if ord(ch) > 10000)
        length = len(text)
        if taste == "minimal":
            if length > 95 or emoji_count > 2:
                continue
        elif taste == "classy":
            if emoji_count > 3:
                continue
        elif taste == "soft":
            if length < 24:
                continue
        out.append(text)
    return out or pool


def variantize_message_text(chat_id: int, lang: str, text: str, kind: str = "hourly") -> list[str]:
    taste = current_message_taste(chat_id, (get_group(chat_id)["title"] if get_group(chat_id) else ""))
    tier = presence_tier(chat_id)
    base = normalize_hourly_text(text)
    variants = [base]
    if lang == "en":
        if tier in {"rich", "warm"}:
            variants.append(f"{base} Wishing everyone a beautiful moment ahead.")
            variants.append(f"Just a little note for this group — {base}")
        if taste == "classy":
            variants.append(f"{base} May the mood stay elegant and steady.")
        elif taste == "soft":
            variants.append(f"{base} Hope the heart feels a little softer today.")
        elif taste == "minimal":
            variants.append(base.replace(" everyone", ""))
    else:
        if tier in {"rich", "warm"}:
            variants.append(f"{base} এই group-এর সবার জন্য রইল কোমল শুভেচ্ছা।")
            variants.append(f"আজকের জন্য শুধু এটুকুই — {base}")
        if taste == "classy":
            variants.append(f"{base} আজকের সময়টা হোক স্থির, সুন্দর আর মার্জিত।")
        elif taste == "soft":
            variants.append(f"{base} মনটা আজ একটু নরম আর হালকা থাকুক।")
        elif taste == "minimal":
            variants.append(base.replace("সবাইকে", ""))
    cleaned = []
    seen = set()
    for v in variants:
        v = normalize_hourly_text(v)
        if not v or len(v) > AI_MAX_TEXT_LEN:
            continue
        if v not in seen:
            seen.add(v)
            cleaned.append(v)
    return cleaned or [base]


def pick_hourly_message(chat_id: int, lang: str, phase: str, pool: list[str]) -> str:
    candidates = [normalize_hourly_text(x) for x in pool if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)]
    if not candidates:
        candidates = [normalize_hourly_text(x) for x in build_fallback_messages(lang, phase, mood=peek_hourly_mood(chat_id), festival_key=(current_festival() or {}).get("key", "")) if is_valid_hourly_text(normalize_hourly_text(x), lang, phase)]
    candidates = filter_pool_by_taste(chat_id, candidates)
    expanded = []
    for c in candidates:
        expanded.extend(variantize_message_text(chat_id, lang, c, kind="hourly"))
    final_pool = []
    for cand in expanded:
        if not was_recent_duplicate_text(chat_id, "hourly", cand, lookback_days=3):
            final_pool.append(cand)
    if not final_pool:
        final_pool = expanded or candidates or build_fallback_messages(lang, phase, mood=peek_hourly_mood(chat_id), festival_key=(current_festival() or {}).get("key", ""))
    recent = recent_hourly_by_chat[chat_id]
    choices = [x for x in final_pool if x not in recent and structure_signature(x) not in {structure_signature(y) for y in recent}]
    if not choices:
        choices = final_pool
    text = random.choice(choices)
    recent.append(text)
    record_sent_history(chat_id, "hourly", text)
    return text


async def human_delay_and_action(context: ContextTypes.DEFAULT_TYPE, update: Update, action: str = "typing"):
    chat = update.effective_chat if update else None
    if chat:
        await bot_humanize(context.bot, chat.id, action=action, kind="reply")


async def bot_humanize(bot, chat_id: int, action: str = "typing", kind: str = "reply"):
    gap = get_presence_gap(chat_id)
    try:
        await bot.send_chat_action(chat_id=chat_id, action=action)
    except Exception:
        pass
    if not HUMAN_DELAY_ENABLED:
        return
    if gap >= 4 * 3600:
        delay = random.uniform(2.4, 4.2) if kind == "reply" else random.uniform(1.8, 3.1)
    elif gap >= 1800:
        delay = random.uniform(1.6, 2.8) if kind == "reply" else random.uniform(1.2, 2.1)
    elif gap >= 600:
        delay = random.uniform(1.0, 1.9) if kind == "reply" else random.uniform(0.9, 1.5)
    else:
        delay = random.uniform(0.6, 1.2) if kind == "reply" else random.uniform(0.5, 1.0)
    if delay > 2.2:
        await asyncio.sleep(delay / 2)
        try:
            await bot.send_chat_action(chat_id=chat_id, action=action)
        except Exception:
            pass
        await asyncio.sleep(delay / 2)
    else:
        await asyncio.sleep(delay)


async def send_photo_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.UPLOAD_PHOTO, "photo")
            photo = kwargs.get("photo")
            if attempt > 0 and hasattr(photo, "seek"):
                photo.seek(0)
            result = await bot.send_photo(**kwargs)
            if chat_id:
                mark_presence(chat_id)
                caption = kwargs.get("caption") or ""
                if caption:
                    record_sent_history(chat_id, "photo_caption", re.sub(r"<[^>]+>", "", caption))
            return result
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_photo", kwargs.get("chat_id"), "", str(last_error))
    raise last_error


async def send_voice_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.UPLOAD_VOICE, "voice")
            result = await bot.send_voice(**kwargs)
            if chat_id:
                mark_presence(chat_id)
            return result
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
                kwargs["caption"] = kwargs.get("caption") or ""
    record_failure("send_voice", kwargs.get("chat_id"), "", str(last_error))
    raise last_error


async def send_text_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id:
                await bot_humanize(bot, chat_id, ChatAction.TYPING, "reply")
            send_kwargs = dict(kwargs)
            if attempt > 0:
                send_kwargs.pop("parse_mode", None)
                send_kwargs.pop("disable_web_page_preview", None)
            result = await bot.send_message(**send_kwargs)
            if chat_id:
                mark_presence(chat_id)
                txt = send_kwargs.get("text") or ""
                if txt:
                    record_sent_history(chat_id, "text", re.sub(r"<[^>]+>", "", txt))
            return result
        except Exception as e:
            last_error = e
            if attempt == 0:
                await asyncio.sleep(1)
    record_failure("send_message", kwargs.get("chat_id"), "", str(last_error))
    raise last_error


def send_message_http_full(chat_id: int, text: str) -> tuple[bool, int | None]:
    http_humanize(chat_id, "typing", "auto")
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    data = tg_post("sendMessage", payload)
    ok = bool(data.get("ok"))
    mid = data.get("result", {}).get("message_id") if ok else None
    if ok:
        mark_presence(chat_id)
        record_sent_history(chat_id, "http_text", text)
        return ok, mid
    # silent fallback retry without preview flags
    data2 = tg_post("sendMessage", {"chat_id": chat_id, "text": text})
    ok2 = bool(data2.get("ok"))
    mid2 = data2.get("result", {}).get("message_id") if ok2 else None
    if ok2:
        mark_presence(chat_id)
        record_sent_history(chat_id, "http_text", text)
        return ok2, mid2
    record_failure("send_message", chat_id, "", str(data2 or data)[:400])
    return False, None


def build_text_styles(lang: str, mention_name: str, safe_group: str, phase: str) -> list[str]:
    if lang == "en":
        bank = {
            "morning": [
                f"🌼 Good morning {mention_name}. Welcome to {safe_group}.",
                f"✨ A bright morning welcome to {mention_name} in {safe_group}.",
                f"☀️ {mention_name}, morning feels warmer with you in {safe_group}.",
                f"💛 Warm morning wishes, {mention_name}. Welcome to {safe_group}.",
            ],
            "day": [
                f"🌸 Welcome {mention_name}. Glad to have you in {safe_group}.",
                f"✨ {mention_name}, a graceful daytime welcome to {safe_group}.",
                f"💫 A warm hello to {mention_name} in {safe_group}.",
                f"🌷 {mention_name}, happy to see you in {safe_group} today.",
            ],
            "evening": [
                f"🌙 Good evening {mention_name}. Welcome to {safe_group}.",
                f"✨ {mention_name}, an elegant evening welcome to {safe_group}.",
                f"🌆 {mention_name}, evening feels gentler with you in {safe_group}.",
                f"💜 Soft evening wishes and welcome, {mention_name}.",
            ],
            "night": [
                f"🌌 Good night {mention_name}. Welcome to {safe_group}.",
                f"💙 {mention_name}, a calm night welcome to {safe_group}.",
                f"⭐ {mention_name}, peaceful night wishes and welcome.",
                f"✨ A quiet and warm night welcome to {mention_name} in {safe_group}.",
            ],
        }
    else:
        bank = {
            "morning": [
                f"🌼 শুভ সকাল {mention_name}। {safe_group} এ তোমাকে স্বাগতম।",
                f"✨ সকালের কোমল শুভেচ্ছা, {mention_name}। {safe_group} এ স্বাগতম।",
                f"☀️ {mention_name}, সকালটা আরও সুন্দর হলো তোমাকে পেয়ে।",
                f"💛 মিষ্টি সকালের শুভেচ্ছা, {mention_name}। {safe_group} এ স্বাগতম।",
            ],
            "day": [
                f"🌸 স্বাগতম {mention_name}। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",
                f"✨ {mention_name}, দিনের নরম শুভেচ্ছা। {safe_group} এ স্বাগতম।",
                f"💫 {mention_name}, তোমাকে পেয়ে {safe_group} আরও উজ্জ্বল লাগছে।",
                f"🌷 আন্তরিক শুভেচ্ছা, {mention_name}। {safe_group} এ স্বাগতম।",
            ],
            "evening": [
                f"🌙 শুভ সন্ধ্যা {mention_name}। {safe_group} এ তোমাকে স্বাগতম।",
                f"✨ সন্ধ্যার নরম আলোয় তোমাকে স্বাগতম, {mention_name}।",
                f"🌆 {mention_name}, সন্ধ্যাটায় তোমাকে পেয়ে ভালো লাগছে।",
                f"💜 মৃদু সন্ধ্যার শুভেচ্ছা রইল, {mention_name}।",
            ],
            "night": [
                f"🌌 শুভ রাত্রি {mention_name}। {safe_group} এ তোমাকে স্বাগতম।",
                f"💙 রাতের শান্ত শুভেচ্ছা রইল, {mention_name}।",
                f"⭐ {mention_name}, তোমাকে পেয়ে রাতটা আরও কোমল লাগছে।",
                f"✨ নিঃশব্দ উষ্ণ শুভেচ্ছা, {mention_name}। {safe_group} এ স্বাগতম।",
            ],
        }
    return bank[phase]


def welcome_texts(lang: str, mention_name: str, first_name: str, group_title: str, custom_text: Optional[str]) -> tuple[str, str]:
    phase = phase_now()
    safe_group = group_title or ("our group" if lang == "en" else "আমাদের গ্রুপ")
    if custom_text:
        text = custom_text.replace("{name}", mention_name).replace("{group}", safe_group).replace("{phase}", phase)
        voice = f"Hello {first_name}, welcome to {safe_group}." if lang == "en" else f"{first_name}, তোমাকে {safe_group} এ স্বাগতম।"
        return text, voice
    pool = build_text_styles(lang, mention_name, safe_group, phase)
    taste = current_message_taste(0, safe_group) if not group_title else None
    # chat_id is not passed here; the selector below will be refined by maybe_welcome after building text.
    text = random.choice(pool)
    voice = f"Hello {first_name}, welcome to {safe_group}." if lang == "en" else f"{first_name}, তোমাকে {safe_group} এ স্বাগতম।"
    return text, voice


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
            variants = variantize_message_text(chat_id, lang, compact, kind="welcome")
            compact = next((v for v in variants if not was_recent_duplicate_text(chat_id, "welcome", v, 2)), variants[0])
            msg = await send_text_with_retry(context.bot, chat_id=chat_id, text=compact, parse_mode=ParseMode.HTML)
            record_sent_history(chat_id, "welcome", compact)
            set_group_value(chat_id, "last_primary_msg_id", msg.message_id)
            increment_group_counter(chat_id, "total_welcome_sent")
            set_group_value(chat_id, "last_welcome_at", int(time.time()))
            asyncio.create_task(schedule_delete(context.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER))
            await maybe_send_milestone(context, chat_id, title or "", lang)
        except Exception:
            logger.exception("Compact burst welcome failed in %s", chat_id)
        return

    text_welcome, voice_text = welcome_texts(lang, mention_name, first_name, title or "", group["custom_welcome"])
    variants = variantize_message_text(chat_id, lang, text_welcome, kind="welcome")
    text_welcome = next((v for v in variants if not was_recent_duplicate_text(chat_id, "welcome", v, 2)), variants[0])
    voice_text = personalize_voice_text(voice_text, first_name, lang)
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        style = current_welcome_style(chat_id)
        footer = current_footer_text(chat_id)
        style, footer, festival = effective_style_footer(chat_id, style, footer)
        if festival and len(text_welcome) < 900:
            fest_name = festival["name_bn"] if lang == "bn" else festival["name_en"]
            text_welcome = f"{text_welcome}\n\n✨ {fest_name}"
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

        if int(group["voice_enabled"]) == 1 and primary:
            try:
                voice_name = selected_voice_name(lang, chat_id)
                await make_voice_file(voice_text, voice_name, voice_path)
                voice_msg = await send_voice_with_retry(context.bot, chat_id=chat_id, voice=voice_path.read_bytes(), caption=t(lang, "welcome_voice_caption"))
            except Exception:
                logger.exception("Voice welcome failed in chat %s; keeping banner/text only", chat_id)

        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        set_group_value(chat_id, "last_welcome_at", int(time.time()))
        increment_group_counter(chat_id, "total_welcome_sent")
        record_sent_history(chat_id, "welcome", re.sub(r"<[^>]+>", "", text_welcome))
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


def keyword_reply_variants(lang: str, matched: str, chat_id: int) -> list[str]:
    base = KEYWORD_REPLIES["en" if lang == "en" else "bn"][matched]
    out = []
    for item in base:
        out.extend(variantize_message_text(chat_id, lang, item, kind="keyword"))
    seen = []
    used = set()
    for x in out:
        if x not in used:
            seen.append(x)
            used.add(x)
    return seen or base


async def on_keyword_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user
    if not chat or chat.type not in {"group", "supergroup"} or not msg or not user or user.is_bot:
        return
    ensure_group(chat.id, chat.title or "")
    if not current_keyword_mode(chat.id):
        return
    if is_linkish_message(msg):
        return
    matched = keyword_reply_match(msg.text or "")
    if not matched:
        return
    now_ts = time.time()
    if now_ts - keyword_last_chat_at.get(chat.id, 0) < KEYWORD_COOLDOWN_SECONDS:
        return
    if now_ts - keyword_last_user_at.get((chat.id, user.id), 0) < KEYWORD_USER_COOLDOWN_SECONDS:
        return
    if random.random() > KEYWORD_REPLY_CHANCE:
        return
    lang = get_group_lang(chat.id)
    options = [x for x in keyword_reply_variants(lang, matched, chat.id) if not was_recent_duplicate_text(chat.id, "keyword", x, 2)]
    if not options:
        options = keyword_reply_variants(lang, matched, chat.id)
    reply_text = random.choice(options)
    keyword_last_chat_at[chat.id] = now_ts
    keyword_last_user_at[(chat.id, user.id)] = now_ts
    try:
        await bot_humanize(context.bot, chat.id, ChatAction.TYPING, "reply")
        sent = await msg.reply_text(reply_text)
        mark_presence(chat.id)
        record_sent_history(chat.id, "keyword", reply_text)
    except Exception:
        logger.exception("Keyword reply failed in %s", chat.id)

# ===== end Premium v7 behavior pack overrides =====


def main():
    init_db()
    ensure_behavior_db()
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
