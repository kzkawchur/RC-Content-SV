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
from PIL import Image, ImageDraw, ImageFont
from telegram import BotCommand, InputFile, Update
from telegram.constants import ChatMemberStatus, ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackContext,
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
logger = logging.getLogger("MayaPremiumBot")

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
BURST_WINDOW_SECONDS = int(os.environ.get("BURST_WINDOW_SECONDS", "5"))
BURST_THRESHOLD = int(os.environ.get("BURST_THRESHOLD", "5"))
CLEANUP_INTERVAL_SECONDS = int(os.environ.get("CLEANUP_INTERVAL_SECONDS", "900"))
TMP_FILE_TTL_SECONDS = int(os.environ.get("TMP_FILE_TTL_SECONDS", "1800"))

AI_HOURLY_ENABLED = os.environ.get("AI_HOURLY_ENABLED", "true").strip().lower() == "true"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
GROQ_TIMEOUT_SECONDS = int(os.environ.get("GROQ_TIMEOUT_SECONDS", "20"))
AI_BATCH_SIZE = int(os.environ.get("AI_BATCH_SIZE", "8"))
AI_MAX_TEXT_LEN = int(os.environ.get("AI_MAX_TEXT_LEN", "140"))
AI_CACHE_TTL_SECONDS = int(os.environ.get("AI_CACHE_TTL_SECONDS", "21600"))

SUPER_ADMINS = {
    int(x.strip()) for x in os.environ.get("SUPER_ADMINS", "").split(",") if x.strip().isdigit()
}

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

flask_app = Flask(__name__)
recent_hourly_by_chat: dict[int, deque[str]] = defaultdict(lambda: deque(maxlen=12))
recent_welcome_keys: dict[str, float] = {}
burst_queues: dict[int, list] = defaultdict(list)
burst_tasks: dict[int, asyncio.Task] = {}

THEME_NAMES = [
    "gold","neon","soft-pink","royal-blue","night-glow","sunrise","moonlight","emerald","crystal",
    "lavender","rose-gold","aqua","midnight","pearl","ruby","sapphire","violet","coral","mint",
    "amber","silver","plum","ocean","sky","blossom","forest","ice","cherry","velvet","carbon",
    "sunset","galaxy","aurora","champagne","candy","blueberry","lime","copper","sand","lotus",
    "smoke","indigo","magenta","teal","bronze","pastel","electric","frost","dusk","dawn"
]

THEME_PALETTES = {
    "gold": ((255, 220, 120), (186, 132, 36)),
    "neon": ((0, 255, 170), (100, 0, 255)),
    "soft-pink": ((255, 200, 222), (255, 126, 185)),
    "royal-blue": ((116, 165, 255), (33, 78, 194)),
    "night-glow": ((18, 27, 51), (59, 130, 246)),
    "sunrise": ((255, 210, 120), (255, 130, 102)),
    "moonlight": ((153, 177, 255), (65, 85, 160)),
    "emerald": ((124, 255, 196), (9, 122, 85)),
    "crystal": ((216, 241, 255), (118, 178, 255)),
    "lavender": ((214, 193, 255), (140, 98, 255)),
    "rose-gold": ((255, 207, 179), (209, 131, 112)),
    "aqua": ((152, 249, 255), (38, 163, 181)),
    "midnight": ((20, 24, 46), (44, 63, 122)),
    "pearl": ((249, 248, 244), (187, 188, 204)),
    "ruby": ((255, 150, 170), (176, 18, 66)),
    "sapphire": ((124, 170, 255), (17, 70, 170)),
    "violet": ((204, 141, 255), (106, 51, 173)),
    "coral": ((255, 198, 170), (239, 103, 77)),
    "mint": ((199, 255, 226), (64, 173, 136)),
    "amber": ((255, 209, 102), (204, 119, 34)),
    "silver": ((230, 234, 240), (145, 155, 170)),
    "plum": ((222, 170, 228), (121, 55, 138)),
    "ocean": ((113, 206, 255), (0, 106, 175)),
    "sky": ((186, 230, 255), (84, 162, 255)),
    "blossom": ((255, 219, 232), (255, 132, 170)),
    "forest": ((111, 197, 123), (31, 92, 48)),
    "ice": ((230, 247, 255), (144, 203, 255)),
    "cherry": ((255, 176, 192), (199, 36, 94)),
    "velvet": ((95, 44, 96), (26, 14, 38)),
    "carbon": ((78, 84, 96), (33, 36, 43)),
    "sunset": ((255, 182, 111), (255, 94, 98)),
    "galaxy": ((30, 27, 75), (96, 165, 250)),
    "aurora": ((34, 211, 238), (168, 85, 247)),
    "champagne": ((255, 239, 190), (214, 175, 108)),
    "candy": ((255, 175, 204), (189, 147, 249)),
    "blueberry": ((139, 164, 255), (82, 52, 145)),
    "lime": ((221, 255, 128), (107, 142, 35)),
    "copper": ((236, 164, 123), (150, 79, 45)),
    "sand": ((242, 215, 167), (197, 162, 106)),
    "lotus": ((255, 196, 214), (181, 116, 152)),
    "smoke": ((182, 194, 205), (93, 108, 122)),
    "indigo": ((129, 140, 248), (55, 48, 163)),
    "magenta": ((255, 102, 196), (168, 0, 119)),
    "teal": ((102, 242, 212), (13, 148, 136)),
    "bronze": ((205, 138, 82), (118, 70, 33)),
    "pastel": ((255, 225, 240), (208, 236, 255)),
    "electric": ((0, 229, 255), (124, 58, 237)),
    "frost": ((233, 245, 255), (139, 182, 255)),
    "dusk": ((123, 97, 255), (255, 126, 95)),
    "dawn": ((255, 224, 178), (255, 171, 145)),
}

FONT_CACHE = {}

def pick_font(size: int, bold: bool = False):
    key = (size, bold)
    if key in FONT_CACHE:
        return FONT_CACHE[key]
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            font = ImageFont.truetype(path, size)
            FONT_CACHE[key] = font
            return font
        except Exception:
            continue
    font = ImageFont.load_default()
    FONT_CACHE[key] = font
    return font

@flask_app.get("/")
def home():
    return f"{BOT_NAME} Premium Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "bot": BOT_NAME})

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

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
                welcome_style TEXT NOT NULL DEFAULT 'royal-blue',
                footer_text TEXT NOT NULL DEFAULT 'Powered by Maya',
                last_primary_msg_id INTEGER,
                last_voice_msg_id INTEGER,
                last_hourly_at INTEGER NOT NULL DEFAULT 0,
                failed_sends INTEGER NOT NULL DEFAULT 0,
                last_failed_reason TEXT,
                last_failed_at INTEGER,
                last_ai_ok INTEGER,
                last_ai_error TEXT,
                last_ai_error_at INTEGER,
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
            CREATE TABLE IF NOT EXISTS ai_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lang TEXT NOT NULL,
                phase TEXT NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                used_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                error_text TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()

def ensure_group(chat_id: int, title: str):
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO groups (chat_id, title, updated_at, last_hourly_at)
            VALUES (?, ?, ?, 0)
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

def set_group_value(chat_id: int, field: str, value):
    allowed = {
        "title","enabled","language","custom_welcome","voice_enabled","delete_service",
        "hourly_enabled","welcome_style","footer_text","last_primary_msg_id","last_voice_msg_id",
        "last_hourly_at","failed_sends","last_failed_reason","last_failed_at","last_ai_ok",
        "last_ai_error","last_ai_error_at","updated_at"
    }
    if field not in allowed:
        raise ValueError("invalid field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()

def get_group_lang(chat_id: int) -> str:
    row = get_group(chat_id)
    lang = (row["language"] if row else "bn") or "bn"
    lang = lang.strip().lower()
    return lang if lang in {"bn","en"} else "bn"

def get_last_join_time(chat_id: int, user_id: int) -> int:
    with db_connect() as conn:
        row = conn.execute("SELECT joined_at FROM join_memory WHERE chat_id=? AND user_id=?", (chat_id, user_id)).fetchone()
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

def get_enabled_groups_for_hourly():
    now_ts = int(time.time())
    with db_connect() as conn:
        return conn.execute(
            """
            SELECT * FROM groups
            WHERE enabled = 1 AND hourly_enabled = 1 AND (? - last_hourly_at) >= ?
            ORDER BY updated_at DESC
            """,
            (now_ts, HOURLY_INTERVAL_SECONDS),
        ).fetchall()

def get_all_enabled_groups():
    with db_connect() as conn:
        return [int(r["chat_id"]) for r in conn.execute("SELECT chat_id FROM groups WHERE enabled = 1").fetchall()]

def get_failed_groups(limit: int = 20):
    with db_connect() as conn:
        return conn.execute(
            "SELECT chat_id, title, failed_sends, last_failed_reason, last_failed_at FROM groups WHERE failed_sends > 0 ORDER BY failed_sends DESC, updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

def get_recent_ai_errors(limit: int = 10):
    with db_connect() as conn:
        return conn.execute(
            "SELECT provider, error_text, created_at FROM ai_errors ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

def add_ai_error(provider: str, error_text: str):
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO ai_errors (provider, error_text, created_at) VALUES (?, ?, ?)",
            (provider, error_text[:500], now_ts),
        )
        conn.commit()

def save_ai_cache(lang: str, phase: str, texts: list[str], source: str):
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.executemany(
            "INSERT INTO ai_cache (lang, phase, text, source, created_at, used_count) VALUES (?, ?, ?, ?, ?, 0)",
            [(lang, phase, text, source, now_ts) for text in texts],
        )
        conn.commit()

def get_cached_ai_texts(lang: str, phase: str, limit: int = 24):
    min_ts = int(time.time()) - AI_CACHE_TTL_SECONDS
    with db_connect() as conn:
        return [
            r["text"]
            for r in conn.execute(
                """
                SELECT text FROM ai_cache
                WHERE lang=? AND phase=? AND created_at >= ?
                ORDER BY used_count ASC, id DESC
                LIMIT ?
                """,
                (lang, phase, min_ts, limit),
            ).fetchall()
        ]

def bump_ai_cache_usage(text: str):
    with db_connect() as conn:
        conn.execute("UPDATE ai_cache SET used_count = used_count + 1 WHERE text = ?", (text,))
        conn.commit()

def cleanup_db():
    cutoff_cache = int(time.time()) - AI_CACHE_TTL_SECONDS * 2
    cutoff_err = int(time.time()) - 7 * 24 * 3600
    with db_connect() as conn:
        conn.execute("DELETE FROM ai_cache WHERE created_at < ?", (cutoff_cache,))
        conn.execute("DELETE FROM ai_errors WHERE created_at < ?", (cutoff_err,))
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
    return s[:22] if s else "FRIEND"

def is_super_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in SUPER_ADMINS)

TEXTS = {
    "bn": {
        "start_private": [
            "আমি {bot} 🌸\n\nCommands:\n/ping\n/myid\n/support\n/groupcount\n/activegroups\n/failedgroups\n/lastaierrors\n/aistatus\n/broadcast <text>\n\nGroup-এ আমাকে add করলেই আমি auto কাজ শুরু করব।",
            "{bot} ready 🌷\n\nআমি group-এ welcome, premium banner, hourly AI text আর voice handle করি।",
        ],
        "start_group": [
            "{bot} ready for this group 🌸\nআমি premium welcome, voice, owner tools আর hourly text handle করব।",
            "{bot} এই group-এ ready আছে 🌷\nআমি সুন্দর welcome banner, hourly text আর smart anti-spam নিয়ে কাজ করব।",
        ],
        "only_group_admin": ["Only group admins can use this command.", "এই command শুধু group admin ব্যবহার করতে পারবে।"],
        "only_super_admin": ["Only owner/admin can use this command.", "এই command শুধু owner/admin ব্যবহার করতে পারবে।"],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set_bn": ["ঠিক আছে, এখন থেকে আমি বাংলায় কথা বলব।", "Language changed to বাংলা."],
        "lang_set_en": ["Okay, I will speak in English now.", "Language changed to English."],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "ঠিক আছে, voice welcome এখন {value}।"],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "hourly_usage": ["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],
        "hourly_set": ["Hourly text: {value}", "Hourly premium text mode: {value}"],
        "hourly_now": ["এখনই একটা premium hourly message পাঠালাম।", "ঠিক আছে, এখনই একটা সুন্দর message দিলাম।"],
        "style_usage": ["Usage:\n/welcomestyle <theme>\n\nUse /welcomestyle list to see all themes."],
        "style_list": ["Available themes:\n{themes}"],
        "style_set": ["Welcome theme set to: {value}", "ঠিক আছে, welcome banner theme এখন {value}।"],
        "footer_usage": ["Usage:\n/setfooter your footer text"],
        "footer_set": ["Footer updated.", "Banner footer text update করা হয়েছে।"],
        "welcome_saved": ["Custom welcome text saved successfully.", "Custom welcome text save হয়ে গেছে।"],
        "welcome_reset": ["Custom welcome reset done.", "Custom welcome reset করা হয়েছে।"],
        "status": ["Bot: {bot}\nLanguage: {lang_name}\nVoice: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTheme: {theme}\nFooter: {footer}\nTimezone: {tz}\nPhase: {phase}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "combined_voice_caption": ["🎤 {bot} group welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
        "aistatus_group": ["AI Hourly: {enabled}\nGroq key present: {present}\nLast AI OK: {last_ok}\nLast AI error: {last_error}"],
        "groupcount": ["Total enabled groups: {count}"],
        "activegroups": ["Active groups:\n{rows}"],
        "failedgroups": ["Failed groups:\n{rows}"],
        "lastaierrors": ["Last AI errors:\n{rows}"],
        "broadcastphoto_usage": ["Reply to a photo with /broadcastphoto [caption] in private chat."],
        "broadcastvoice_usage": ["Reply to a voice/audio with /broadcastvoice [caption] in private chat."],
        "media_broadcast_start": ["Media broadcast started to {count} groups..."],
        "media_broadcast_done": ["Media broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "hourly_texts": {
            "morning": ["🌼 শুভ সকাল সবাইকে। আশা করি আজকের দিনটা সুন্দর কাটবে।"],
            "day": ["🌸 সবার দিনটা সুন্দর কাটুক—এই কামনা রইল।"],
            "evening": ["🌙 শুভ সন্ধ্যা সবাইকে। সন্ধ্যাটা হোক শান্ত আর মিষ্টি।"],
            "night": ["🌌 শুভ রাত্রি সবাইকে। রাতটা হোক শান্ত আর আরামদায়ক।"],
        },
    },
    "en": {
        "start_private": [
            "I am {bot} 🌸\n\nCommands:\n/ping\n/myid\n/support\n/groupcount\n/activegroups\n/failedgroups\n/lastaierrors\n/aistatus\n/broadcast <text>\n\nOnce I am added to a group, I start working automatically.",
            "{bot} is ready 🌷\n\nI handle premium welcomes, hourly AI text, owner tools and voice.",
        ],
        "start_group": [
            "{bot} is ready for this group 🌸\nI will handle premium welcome banners, voice and hourly texts here.",
            "{bot} is now ready in this group 🌷\nI can send beautiful welcome banners and premium hourly texts.",
        ],
        "only_group_admin": ["Only group admins can use this command."],
        "only_super_admin": ["Only owner/admin can use this command."],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set_bn": ["Language changed to Bangla."],
        "lang_set_en": ["Language changed to English.", "Okay, I will speak in English now."],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "Voice welcome is now {value}."],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "hourly_usage": ["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],
        "hourly_set": ["Hourly text: {value}", "Hourly premium text mode: {value}"],
        "hourly_now": ["I just sent a premium hourly message.", "Okay, I sent a beautiful message right now."],
        "style_usage": ["Usage:\n/welcomestyle <theme>\n\nUse /welcomestyle list to see all themes."],
        "style_list": ["Available themes:\n{themes}"],
        "style_set": ["Welcome theme set to: {value}", "The welcome banner theme is now {value}."],
        "footer_usage": ["Usage:\n/setfooter your footer text"],
        "footer_set": ["Footer updated."],
        "welcome_saved": ["Custom welcome text saved successfully.", "Your custom welcome text has been saved."],
        "welcome_reset": ["Custom welcome has been reset."],
        "status": ["Bot: {bot}\nLanguage: {lang_name}\nVoice: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTheme: {theme}\nFooter: {footer}\nTimezone: {tz}\nPhase: {phase}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "combined_voice_caption": ["🎤 {bot} group welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
        "aistatus_group": ["AI Hourly: {enabled}\nGroq key present: {present}\nLast AI OK: {last_ok}\nLast AI error: {last_error}"],
        "groupcount": ["Total enabled groups: {count}"],
        "activegroups": ["Active groups:\n{rows}"],
        "failedgroups": ["Failed groups:\n{rows}"],
        "lastaierrors": ["Last AI errors:\n{rows}"],
        "broadcastphoto_usage": ["Reply to a photo with /broadcastphoto [caption] in private chat."],
        "broadcastvoice_usage": ["Reply to a voice/audio with /broadcastvoice [caption] in private chat."],
        "media_broadcast_start": ["Media broadcast started to {count} groups..."],
        "media_broadcast_done": ["Media broadcast finished.\nSuccess: {ok}\nFailed: {fail}"],
        "hourly_texts": {
            "morning": ["🌼 Good morning everyone. Hope your day begins beautifully."],
            "day": ["🌸 Hope everyone is having a beautiful day."],
            "evening": ["🌙 Good evening everyone. Hope your evening feels calm and lovely."],
            "night": ["🌌 Good night everyone. Wishing you all a calm and restful night."],
        },
    },
}

BN_PHASE_OPENERS = {
    "morning": ["🌼 শুভ সকাল সবাইকে।","☀️ সকালের সুন্দর শুভেচ্ছা রইল।","✨ নতুন সকাল মানেই নতুন আলো।","💛 সকালটা হোক কোমল আর সুন্দর।","🌸 মিষ্টি এক সকালের শুভেচ্ছা।","🍃 আজকের সকালটা শান্ত হোক।","🕊️ ভালো একটি সকাল সবার জন্য।","🌤️ আলো ভরা সকাল তোমাদের জন্য।","🌞 শুভ সকাল, সবার মন ভালো থাকুক।","🌷 নতুন দিনের নরম শুভেচ্ছা।"],
    "day": ["🌷 দিনের শুভেচ্ছা সবাইকে।","💫 দিনটা যেন সুন্দর কাটে।","🌸 একটু হাসো, একটু ভালো থাকো।","🍀 আজকের দিনটা হোক দারুণ।","🌞 উষ্ণ দিনের শুভেচ্ছা রইল।","✨ নরম এক দিনের শুভেচ্ছা।","🌺 সবার জন্য সুন্দর দিনের বার্তা।","💐 ভালো থাকুক এই group-এর সবাই।","🌼 দিনের মাঝেও একটু শান্তি থাকুক।","🕊️ একটু হালকা, একটু উজ্জ্বল থাকো।"],
    "evening": ["🌙 শুভ সন্ধ্যা সবাইকে।","✨ সন্ধ্যার নরম শুভেচ্ছা রইল।","🌆 আজকের সন্ধ্যাটা হোক মিষ্টি।","💜 শান্ত এক সন্ধ্যার শুভেচ্ছা।","🕯️ সন্ধ্যার আলোয় ভালোবাসা রইল।","🌃 নরম সন্ধ্যার শুভেচ্ছা সবাইকে।","🍂 সন্ধ্যাটা হোক আরামদায়ক।","💫 ক্লান্তি ভুলে একটু ভালো থাকো।","🌸 মায়াময় সন্ধ্যার শুভেচ্ছা।","🌷 সন্ধ্যায় একটু শান্তি খুঁজে নাও।"],
    "night": ["🌌 শুভ রাত্রি সবাইকে।","⭐ রাতের শান্ত শুভেচ্ছা রইল।","💙 আজকের রাতটা হোক শান্ত।","🌙 মিষ্টি এক রাতের শুভেচ্ছা।","🕊️ নীরব রাতের কোমল শুভেচ্ছা।","✨ রাতের শেষে ভালো থেকো সবাই।","🌠 আরামদায়ক একটি রাত কামনা করি।","💫 সবার জন্য শান্ত রাতের বার্তা।","🌸 রাতটা হোক নরম আর নির্ভার।","🍃 নীরবতায়ও থাকুক উষ্ণতা।"],
}
BN_MIDDLES = [
    "এই group-এর সবার জন্য অনেক শুভকামনা।","একটু হাসো, একটু স্বস্তিতে থাকো।","নিজের মনটাকে আজ একটু হালকা রাখো।","আশা করি সময়টা তোমাদের ভালো কাটছে।","সবাই যেন সুন্দর আর নিরাপদে থাকো।",
    "দিনের ভিড়ে মনটাও যেন সুন্দর থাকে।","মনে রাখো, শান্ত থাকাও একধরনের শক্তি।","আজও ভালো কিছুর অপেক্ষা থাকুক।","সুন্দর কথা, সুন্দর মন—দুটোই জরুরি।","ভালো vibes ছড়িয়ে দাও চারদিকে।",
    "মন খারাপ হলে একটু থেমে শ্বাস নাও।","ভালো কিছু ছোট ছোট জায়গাতেও পাওয়া যায়।","একটু কোমল থাকলেও মানুষ শক্তিশালী হতে পারে।","এই group-এর উষ্ণতা যেন এমনই থাকে।","যেখানে সুন্দর মন, সেখানেই শান্তি।",
]
BN_ENDINGS = ["🌷 ভালো থাকো সবাই।","💫 সুন্দর থাকো সবাই।","🌼 হাসিখুশি থাকো সবাই।","💙 শান্তিতে থাকো সবাই।","✨ হৃদয়টা নরম আর সুন্দর থাকুক।","🕊️ মনটা হোক হালকা আর শান্ত।","🌸 তোমাদের সবার জন্য রইল শুভেচ্ছা।","🌿 দিনশেষে যেন শান্তি মেলে।"]

EN_PHASE_OPENERS = {
    "morning": ["🌼 Good morning everyone.","☀️ A gentle morning hello to all of you.","✨ Wishing this group a soft and beautiful morning.","💛 Hope your morning feels light and peaceful.","🌸 Sending warm morning wishes to everyone.","🍃 May this morning begin softly for you all.","🕊️ A calm and lovely morning to this group.","🌤️ Bright morning wishes to everyone here.","🌞 A warm start to the day for everyone.","🌷 Wishing this group a lovely morning."],
    "day": ["🌷 Hope everyone is having a good day.","💫 Sending warm daytime vibes to this group.","🌸 A little beautiful message for your day.","🍀 Wishing everyone a smooth and lovely day.","🌞 Daytime wishes to all of you.","✨ Hope today feels a little softer and brighter.","🌺 Sending kindness across the group today.","💐 A warm little note for everyone here.","🌼 Wishing this group a graceful day.","🕊️ May the day stay gentle and kind."],
    "evening": ["🌙 Good evening everyone.","✨ Sending peaceful evening wishes to this group.","🌆 Hope your evening feels calm and gentle.","💜 A soft evening hello to all of you.","🕯️ Wishing everyone a lovely evening.","🌃 Evening warmth to this beautiful group.","🍂 Hope the evening brings a little peace.","💫 A gentle evening message for everyone here.","🌸 Evening softness to this group.","🌷 Wishing calm and beauty this evening."],
    "night": ["🌌 Good night everyone.","⭐ Sending calm night wishes to all of you.","💙 Hope your night feels peaceful and restful.","🌙 A soft night message for this group.","🕊️ Wishing everyone a gentle and quiet night.","✨ End the day with a little peace.","🌠 Warm night wishes to everyone here.","💫 A peaceful close to the day for all of you.","🌸 Wishing comfort and quiet tonight.","🍃 Let the night feel a little lighter."],
}
EN_MIDDLES = [
    "Wishing this group a little more peace and softness.","Hope your heart feels a little lighter today.","Take a small moment to breathe and smile.","May your day carry a little extra kindness.","Sending good energy to everyone here.",
    "Hope things feel a bit easier and brighter.","A small warm message can change a day.","Keep your heart gentle and your mind steady.","You all deserve a peaceful moment today.","May this group stay kind, calm, and warm.",
    "Even a simple message can make a space softer.","May your thoughts feel a little more balanced.","A bit of peace can go a long way today.","Let this group stay warm and welcoming.","Wishing everyone a softer moment right now.",
]
EN_ENDINGS = ["🌷 Stay well, everyone.","💫 Stay beautiful, everyone.","🌼 Wishing you comfort and peace.","💙 Take care, everyone.","✨ Keep your vibe soft and bright.","🕊️ May your mind feel calm.","🌸 Warm wishes to all of you.","🌿 Hope the day treats you gently."]

def t(lang: str, key: str, **kwargs) -> str:
    lang = lang if lang in TEXTS else "bn"
    arr = TEXTS[lang].get(key) or TEXTS["bn"].get(key) or [key]
    return random.choice(arr).format(bot=BOT_NAME, support=support_text(), **kwargs)

def build_fallback_messages(lang: str, phase: str) -> list[str]:
    result = []
    if lang == "en":
        for a in EN_PHASE_OPENERS[phase]:
            for b in EN_MIDDLES:
                for c in EN_ENDINGS:
                    x = f"{a} {b} {c}".strip()
                    if len(x) <= AI_MAX_TEXT_LEN:
                        result.append(x)
    else:
        for a in BN_PHASE_OPENERS[phase]:
            for b in BN_MIDDLES:
                for c in BN_ENDINGS:
                    x = f"{a} {b} {c}".strip()
                    if len(x) <= AI_MAX_TEXT_LEN:
                        result.append(x)
    seen = set()
    uniq = []
    for item in result:
        if item not in seen:
            seen.add(item)
            uniq.append(item)
    return uniq

def sanitize_ai_lines(text: str) -> list[str]:
    lines = []
    for raw in text.splitlines():
        x = re.sub(r"^[\-\*\d\.\)\s]+", "", raw.strip())
        x = re.sub(r"\s+", " ", x).strip()
        if not x or len(x) > AI_MAX_TEXT_LEN:
            continue
        lowered = x.lower()
        if any(bad in lowered for bad in ["18+","porn","sex","nude","dating","kiss","adult","xxx"]):
            continue
        lines.append(x)
    out = []
    seen = set()
    for x in lines:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def groq_generate_batch(lang: str, phase: str) -> list[str]:
    if not AI_HOURLY_ENABLED or not GROQ_API_KEY:
        return []
    prompt = (
        f"Write {AI_BATCH_SIZE} short, premium Telegram group hourly messages in "
        f"{'Bengali' if lang == 'bn' else 'English'}.\n"
        f"Rules:\n"
        f"- elegant, warm, classy, group-safe\n"
        f"- no flirting, no adult tone, no politics, no religion\n"
        f"- each message must be different\n"
        f"- suitable for {phase}\n"
        f"- each message under {AI_MAX_TEXT_LEN} characters\n"
        f"Return only the messages, one per line."
    )
    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "You write short premium Telegram group texts."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.95,
                "max_tokens": 350,
            },
            timeout=GROQ_TIMEOUT_SECONDS,
        )
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        lines = sanitize_ai_lines(content)
        if lines:
            save_ai_cache(lang, phase, lines, "groq")
        return lines
    except Exception as e:
        add_ai_error("groq", str(e))
        logger.exception("Groq batch failed")
        return []

def get_hourly_pool(lang: str, phase: str) -> list[str]:
    cached = get_cached_ai_texts(lang, phase)
    if len(cached) >= max(6, AI_BATCH_SIZE // 2):
        return cached
    ai = groq_generate_batch(lang, phase)
    if ai:
        cached = get_cached_ai_texts(lang, phase)
        return cached or ai
    fallback = build_fallback_messages(lang, phase)
    return fallback

def pick_hourly_message(chat_id: int, lang: str, phase: str, pool: list[str]) -> str:
    recent = recent_hourly_by_chat[chat_id]
    choices = [x for x in pool if x not in recent]
    if not choices:
        choices = pool[:]
    if not choices:
        choices = TEXTS[lang]["hourly_texts"][phase]
    msg = random.choice(choices)
    recent.append(msg)
    bump_ai_cache_usage(msg)
    return msg

async def retry_async(func, *args, retries: int = 2, delay: float = 1.2, **kwargs):
    last_exc = None
    for i in range(retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if i < retries:
                await asyncio.sleep(delay)
    raise last_exc

def record_send_failure(chat_id: int, reason: str):
    row = get_group(chat_id)
    ensure_group(chat_id, row["title"] if row else "")
    count = int(row["failed_sends"]) + 1 if row else 1
    set_group_value(chat_id, "failed_sends", count)
    set_group_value(chat_id, "last_failed_reason", reason[:300])
    set_group_value(chat_id, "last_failed_at", int(time.time()))

def record_ai_status(chat_id: int, ok: bool, err: Optional[str] = None):
    set_group_value(chat_id, "last_ai_ok", int(time.time()) if ok else None)
    if err:
        set_group_value(chat_id, "last_ai_error", err[:300])
        set_group_value(chat_id, "last_ai_error_at", int(time.time()))

async def fetch_profile_photo(bot, user_id: int) -> Optional[bytes]:
    try:
        photos = await bot.get_user_profile_photos(user_id=user_id, limit=1)
        if not photos or photos.total_count == 0:
            return None
        file_id = photos.photos[0][-1].file_id
        f = await bot.get_file(file_id)
        data = await f.download_as_bytearray()
        return bytes(data)
    except Exception:
        return None

def add_circle_avatar(base: Image.Image, avatar_bytes: Optional[bytes], box: tuple[int, int, int, int]):
    draw = ImageDraw.Draw(base)
    x1, y1, x2, y2 = box
    if avatar_bytes:
        try:
            avatar = Image.open(BytesIO(avatar_bytes)).convert("RGB")
            avatar = avatar.resize((x2 - x1, y2 - y1))
            mask = Image.new("L", (x2 - x1, y2 - y1), 0)
            md = ImageDraw.Draw(mask)
            md.ellipse((0, 0, x2 - x1, y2 - y1), fill=255)
            base.paste(avatar, (x1, y1), mask)
            return
        except Exception:
            pass
    draw.ellipse(box, fill=(255, 255, 255))

def build_single_banner(first_name: str, group_title: str, theme: str, footer: str, avatar_bytes: Optional[bytes]) -> BytesIO:
    width, height = 1280, 720
    c1, c2 = THEME_PALETTES.get(theme, THEME_PALETTES["royal-blue"])
    img = Image.new("RGB", (width, height), c1)
    draw = ImageDraw.Draw(img)
    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(c1[0] * (1 - blend) + c2[0] * blend)
        g = int(c1[1] * (1 - blend) + c2[1] * blend)
        b = int(c1[2] * (1 - blend) + c2[2] * blend)
        draw.line((0, y, width, y), fill=(r, g, b))
    draw.rounded_rectangle((80, 80, 1200, 640), radius=46, fill=(14, 18, 32))
    draw.rounded_rectangle((105, 105, 1125, 615), radius=38, outline=(255, 255, 255), width=2)
    add_circle_avatar(img, avatar_bytes, (110, 195, 350, 435))
    title_font = pick_font(64, True)
    name_font = pick_font(90, True)
    sub_font = pick_font(34, False)
    foot_font = pick_font(28, False)
    draw.text((400, 150), "WELCOME", fill=(255, 255, 255), font=title_font)
    draw.text((400, 260), ascii_name(first_name).upper(), fill=(255, 224, 153), font=name_font)
    draw.text((400, 390), f"TO {ascii_name(group_title or 'GROUP').upper()}", fill=(214, 228, 255), font=sub_font)
    draw.text((400, 545), footer[:48], fill=(210, 240, 255), font=foot_font)
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

def build_combined_banner(names: list[str], group_title: str, theme: str, footer: str, avatars: list[Optional[bytes]]) -> BytesIO:
    width, height = 1280, 720
    c1, c2 = THEME_PALETTES.get(theme, THEME_PALETTES["royal-blue"])
    img = Image.new("RGB", (width, height), c1)
    draw = ImageDraw.Draw(img)
    for y in range(height):
        blend = y / max(1, height - 1)
        r = int(c1[0] * (1 - blend) + c2[0] * blend)
        g = int(c1[1] * (1 - blend) + c2[1] * blend)
        b = int(c1[2] * (1 - blend) + c2[2] * blend)
        draw.line((0, y, width, y), fill=(r, g, b))
    draw.rounded_rectangle((80, 80, 1200, 640), radius=46, fill=(14, 18, 32))
    positions = [(120,190,260,330),(285,190,425,330),(120,355,260,495),(285,355,425,495)]
    for i, box in enumerate(positions):
        add_circle_avatar(img, avatars[i] if i < len(avatars) else None, box)
    title_font = pick_font(62, True)
    sub_font = pick_font(34, False)
    names_font = pick_font(36, True)
    foot_font = pick_font(28, False)
    draw.text((500, 150), "WELCOME TO THE GROUP", fill=(255,255,255), font=title_font)
    text_names = ", ".join(ascii_name(x) for x in names[:8])
    draw.multiline_text((500, 280), text_names.upper(), fill=(255,224,153), font=names_font, spacing=8)
    draw.text((500, 460), f"TO {ascii_name(group_title or 'GROUP').upper()}", fill=(214,228,255), font=sub_font)
    draw.text((500, 545), footer[:48], fill=(210,240,255), font=foot_font)
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome_group.png"
    bio.seek(0)
    return bio

async def make_voice_file(text: str, lang: str, path: Path):
    voice = VOICE_NAME_EN if lang == "en" else VOICE_NAME_BN
    communicate = edge_tts.Communicate(text=text, voice=voice, rate=VOICE_RATE, pitch=VOICE_PITCH, volume=VOICE_VOLUME)
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
            "morning": [f"🌼 Good morning {mention_name}!\nWelcome to {safe_group}.", f"✨ {mention_name}, warm morning wishes and welcome to {safe_group}.", f"☀️ Hello {mention_name}!\nA bright morning welcome to {safe_group}."],
            "day": [f"🌸 Welcome {mention_name}!\nWe are happy to have you in {safe_group}.", f"💫 Hello {mention_name}!\nA warm welcome to {safe_group}.", f"🌷 {mention_name}, glad to see you in {safe_group}. Welcome!"],
            "evening": [f"🌙 Good evening {mention_name}!\nWelcome to {safe_group}.", f"✨ {mention_name}, lovely evening wishes and welcome to {safe_group}.", f"🌆 Hello {mention_name}!\nEvening smiles and a warm welcome to {safe_group}."],
            "night": [f"🌌 Good night {mention_name}!\nWelcome to {safe_group}.", f"💙 {mention_name}, peaceful night wishes and welcome to {safe_group}.", f"⭐ Hello {mention_name}!\nA calm night welcome to {safe_group}."],
        }
        voice_bank = {
            "morning": [f"{first_name}, good morning. A warm welcome to {safe_group}.", f"Hello {first_name}, welcome to {safe_group}. We are glad to have you here."],
            "day": [f"{first_name}, welcome to {safe_group}. We are really happy to have you here.", f"Hello {first_name}, a warm welcome to {safe_group}."],
            "evening": [f"{first_name}, good evening. Welcome to {safe_group}. Hope you enjoy your time here.", f"Hello {first_name}, evening wishes and welcome to {safe_group}."],
            "night": [f"{first_name}, good night. A warm welcome to {safe_group}.", f"Hello {first_name}, welcome to {safe_group}. Glad to have you here."],
        }
    else:
        bank = {
            "morning": [f"🌼 শুভ সকাল {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"✨ {mention_name}, সকালের মিষ্টি শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"☀️ হ্যালো {mention_name}!\nএকটা সুন্দর সকালের স্বাগতম রইল {safe_group} এ।"],
            "day": [f"🌸 স্বাগতম {mention_name}!\n{safe_group} এ তোমাকে পেয়ে খুব ভালো লাগছে।", f"💫 হ্যালো {mention_name}!\n{safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"🌷 {mention_name}, তোমাকে পেয়ে {safe_group} আরও সুন্দর লাগছে। স্বাগতম।"],
            "evening": [f"🌙 শুভ সন্ধ্যা {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"✨ {mention_name}, সন্ধ্যার সুন্দর শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"🌆 হ্যালো {mention_name}!\nসন্ধ্যার নরম আলোয় তোমাকে {safe_group} এ স্বাগতম।"],
            "night": [f"🌌 শুভ রাত্রি {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"💙 {mention_name}, রাতের শান্ত শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"⭐ হ্যালো {mention_name}!\nরাতের শান্ত শুভেচ্ছার সাথে তোমাকে স্বাগতম {safe_group} এ।"],
        }
        voice_bank = {
            "morning": [f"{first_name}, শুভ সকাল। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"হ্যালো {first_name}, সকালের সুন্দর শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
            "day": [f"{first_name}, তোমাকে {safe_group} এ আন্তরিক স্বাগতম। তোমাকে পেয়ে ভালো লাগছে।", f"হ্যালো {first_name}, {safe_group} এ তোমাকে পেয়ে সত্যিই ভালো লাগছে। স্বাগতম।"],
            "evening": [f"{first_name}, শুভ সন্ধ্যা। {safe_group} এ তোমাকে স্বাগতম। আশা করি এখানে ভালো সময় কাটাবে।", f"হ্যালো {first_name}, সন্ধ্যার মিষ্টি শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
            "night": [f"{first_name}, শুভ রাত্রি। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"হ্যালো {first_name}, রাতের শান্ত শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
        }
    return random.choice(bank[phase]), random.choice(voice_bank[phase])

async def maybe_delete_previous(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    row = get_group(chat_id)
    if not row:
        return
    for mid in (row["last_primary_msg_id"], row["last_voice_msg_id"]):
        if mid:
            try:
                await context.bot.delete_message(chat_id, int(mid))
            except Exception:
                pass

async def schedule_delete(bot, chat_id: int, message_id: int, delay: int):
    try:
        await asyncio.sleep(delay)
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass

async def send_single_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, user):
    group = get_group(chat_id)
    if not group:
        return
    lang = get_group_lang(chat_id)
    first_name = clean_name(user.first_name)
    mention_name = user.mention_html(first_name)
    text_welcome, voice_text = welcome_texts(lang, mention_name, first_name, title or "", group["custom_welcome"])
    avatar = await fetch_profile_photo(context.bot, user.id)
    theme = group["welcome_style"] or "royal-blue"
    footer = group["footer_text"] or f"Powered by {BOT_NAME}"
    banner = build_single_banner(first_name, title or "GROUP", theme, footer, avatar)
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    primary = None
    voice_msg = None
    try:
        primary = await retry_async(context.bot.send_photo, chat_id=chat_id, photo=banner, caption=text_welcome, parse_mode=ParseMode.HTML)
        if int(group["voice_enabled"]) == 1:
            await make_voice_file(voice_text, lang, voice_path)
            with open(voice_path, "rb") as fh:
                voice_msg = await retry_async(context.bot.send_voice, chat_id=chat_id, voice=fh, caption=t(lang, "welcome_voice_caption"))
        save_join_time(chat_id, user.id)
        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        if primary:
            asyncio.create_task(schedule_delete(context.bot, chat_id, primary.message_id, WELCOME_DELETE_AFTER))
        if voice_msg:
            asyncio.create_task(schedule_delete(context.bot, chat_id, voice_msg.message_id, WELCOME_DELETE_AFTER))
    except Exception as e:
        record_send_failure(chat_id, str(e))
    finally:
        if voice_path.exists():
            try:
                voice_path.unlink()
            except Exception:
                pass

async def send_combined_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, users: list):
    group = get_group(chat_id)
    if not group or not users:
        return
    lang = get_group_lang(chat_id)
    names = [clean_name(u.first_name) for u in users[:8]]
    mention_names = ", ".join(u.mention_html(clean_name(u.first_name)) for u in users[:8])
    if lang == "en":
        caption = f"✨ Warm welcome to {mention_names}\nWe are happy to have you all in {title or 'our group'}."
        voice_text = f"Warm welcome to all new members. We are happy to have you in {title or 'our group'}."
    else:
        caption = f"✨ {mention_names}\nতোমাদের সবাইকে {title or 'আমাদের গ্রুপ'} এ আন্তরিক স্বাগতম।"
        voice_text = f"নতুন সবাইকে আন্তরিক স্বাগতম। তোমাদের সবাইকে পেয়ে ভালো লাগছে।"
    avatars = []
    for u in users[:4]:
        avatars.append(await fetch_profile_photo(context.bot, u.id))
    banner = build_combined_banner(names, title or "GROUP", group["welcome_style"] or "royal-blue", group["footer_text"] or f"Powered by {BOT_NAME}", avatars)
    voice_path = TMP_DIR / f"combined_{chat_id}_{int(time.time())}.mp3"
    primary = None
    voice_msg = None
    try:
        primary = await retry_async(context.bot.send_photo, chat_id=chat_id, photo=banner, caption=caption, parse_mode=ParseMode.HTML)
        if int(group["voice_enabled"]) == 1:
            await make_voice_file(voice_text, lang, voice_path)
            with open(voice_path, "rb") as fh:
                voice_msg = await retry_async(context.bot.send_voice, chat_id=chat_id, voice=fh, caption=t(lang, "combined_voice_caption"))
        for u in users:
            save_join_time(chat_id, u.id)
        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        if primary:
            asyncio.create_task(schedule_delete(context.bot, chat_id, primary.message_id, WELCOME_DELETE_AFTER))
        if voice_msg:
            asyncio.create_task(schedule_delete(context.bot, chat_id, voice_msg.message_id, WELCOME_DELETE_AFTER))
    except Exception as e:
        record_send_failure(chat_id, str(e))
    finally:
        if voice_path.exists():
            try:
                voice_path.unlink()
            except Exception:
                pass

async def process_burst_queue(chat_id: int, title: str, context: ContextTypes.DEFAULT_TYPE):
    await asyncio.sleep(BURST_WINDOW_SECONDS)
    users = burst_queues.pop(chat_id, [])
    burst_tasks.pop(chat_id, None)
    if not users:
        return
    dedup = {}
    for u in users:
        dedup[u.id] = u
    users = list(dedup.values())
    await maybe_delete_previous(context, chat_id)
    if len(users) >= BURST_THRESHOLD:
        await send_combined_welcome(context, chat_id, title, users)
    else:
        for u in users:
            await send_single_welcome(context, chat_id, title, u)

async def enqueue_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, user):
    group = get_group(chat_id)
    if not group or int(group["enabled"]) != 1 or user.is_bot:
        return
    key = f"{chat_id}:{user.id}"
    now = time.time()
    if key in recent_welcome_keys and now - recent_welcome_keys[key] < 8:
        return
    recent_welcome_keys[key] = now
    if now - get_last_join_time(chat_id, user.id) < REJOIN_IGNORE_SECONDS:
        return
    burst_queues[chat_id].append(user)
    if chat_id not in burst_tasks or burst_tasks[chat_id].done():
        burst_tasks[chat_id] = asyncio.create_task(process_burst_queue(chat_id, title, context))

async def track_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in {"group", "supergroup"}:
        ensure_group(chat.id, chat.title or "")

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

async def require_owner_private(update: Update) -> bool:
    if not update.effective_user or not is_super_admin(update.effective_user.id):
        await update.effective_message.reply_text(t("en", "only_super_admin"))
        return False
    if not update.effective_chat or update.effective_chat.type != "private":
        await update.effective_message.reply_text("Use this command in private chat.")
        return False
    return True

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    if update.effective_chat and update.effective_chat.type in {"group", "supergroup"}:
        await update.effective_message.reply_text(t(get_group_lang(update.effective_chat.id), "start_group"))
    else:
        await update.effective_message.reply_text(t("bn", "start_private"))

async def on_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group","supergroup"} else "bn"
    await update.effective_message.reply_text(t(lang, "support"))

async def on_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group","supergroup"} else "bn"
    await update.effective_message.reply_text(t(lang, "ping", tz=TIMEZONE_NAME, time=local_now().strftime("%I:%M %p")))

async def on_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(t("en", "myid", user_id=update.effective_user.id if update.effective_user else 0))

async def on_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    current = get_group_lang(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(current, "lang_usage"))
        return
    new = context.args[0].lower().strip()
    if new not in {"bn","en"}:
        await update.effective_message.reply_text(t(current, "lang_usage"))
        return
    set_group_value(chat.id, "language", new)
    await update.effective_message.reply_text(t(new, "lang_set_en" if new == "en" else "lang_set_bn"))

async def on_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang, "voice_usage", current="ON" if int(group["voice_enabled"]) else "OFF"))
        return
    v = context.args[0].lower()
    if v not in {"on","off"}:
        await update.effective_message.reply_text(t(lang, "voice_usage", current="ON" if int(group["voice_enabled"]) else "OFF"))
        return
    set_group_value(chat.id, "voice_enabled", 1 if v == "on" else 0)
    await update.effective_message.reply_text(t(lang, "voice_set", value=v.upper()))

async def on_delete_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang, "deleteservice_usage", current="ON" if int(group["delete_service"]) else "OFF"))
        return
    v = context.args[0].lower()
    if v not in {"on","off"}:
        await update.effective_message.reply_text(t(lang, "deleteservice_usage", current="ON" if int(group["delete_service"]) else "OFF"))
        return
    set_group_value(chat.id, "delete_service", 1 if v == "on" else 0)
    await update.effective_message.reply_text(t(lang, "deleteservice_set", value=v.upper()))

async def on_hourly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    group = get_group(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang, "hourly_usage", current="ON" if int(group["hourly_enabled"]) else "OFF"))
        return
    v = context.args[0].lower()
    if v == "now":
        phase = phase_now()
        msg = pick_hourly_message(chat.id, lang, phase, get_hourly_pool(lang, phase))
        await update.effective_message.reply_text(msg)
        set_group_value(chat.id, "last_hourly_at", int(time.time()))
        await update.effective_message.reply_text(t(lang, "hourly_now"))
        return
    if v not in {"on","off"}:
        await update.effective_message.reply_text(t(lang, "hourly_usage", current="ON" if int(group["hourly_enabled"]) else "OFF"))
        return
    set_group_value(chat.id, "hourly_enabled", 1 if v == "on" else 0)
    if v == "on":
        set_group_value(chat.id, "last_hourly_at", 0)
    await update.effective_message.reply_text(t(lang, "hourly_set", value=v.upper()))

async def on_welcomestyle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang, "style_usage"))
        return
    arg = context.args[0].lower()
    if arg == "list":
        themes = ", ".join(THEME_NAMES)
        await update.effective_message.reply_text(t(lang, "style_list", themes=themes))
        return
    if arg not in THEME_NAMES:
        await update.effective_message.reply_text(t(lang, "style_usage"))
        return
    set_group_value(chat.id, "welcome_style", arg)
    await update.effective_message.reply_text(t(lang, "style_set", value=arg))

async def on_setfooter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    raw = update.effective_message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.effective_message.reply_text(t(lang, "footer_usage"))
        return
    set_group_value(chat.id, "footer_text", parts[1].strip()[:48])
    await update.effective_message.reply_text(t(lang, "footer_set"))

async def on_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    raw = update.effective_message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.effective_message.reply_text("Usage:\n/setwelcome your text\n\nPlaceholders:\n{name}\n{group}\n{phase}")
        return
    set_group_value(chat.id, "custom_welcome", parts[1].strip()[:600])
    await update.effective_message.reply_text(t(lang, "welcome_saved"))

async def on_resetwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    set_group_value(chat.id, "custom_welcome", None)
    await update.effective_message.reply_text(t(lang, "welcome_reset"))

async def on_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    group = get_group(chat.id)
    lang = get_group_lang(chat.id)
    await update.effective_message.reply_text(
        t(
            lang, "status",
            lang_name="Bangla" if lang == "bn" else "English",
            voice="ON" if int(group["voice_enabled"]) else "OFF",
            delete_service="ON" if int(group["delete_service"]) else "OFF",
            hourly="ON" if int(group["hourly_enabled"]) else "OFF",
            theme=group["welcome_style"],
            footer=group["footer_text"],
            tz=TIMEZONE_NAME,
            phase=phase_now(),
        )
    )

async def on_testwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    chat = update.effective_chat
    if not chat or chat.type not in {"group","supergroup"} or not update.effective_user:
        return
    await maybe_delete_previous(context, chat.id)
    await send_single_welcome(context, chat.id, chat.title or "", update.effective_user)

async def on_aistatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat and chat.type in {"group","supergroup"}:
        if not await require_group_admin(update, context):
            return
        row = get_group(chat.id)
        last_ok = datetime.fromtimestamp(row["last_ai_ok"], ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d %I:%M %p") if row and row["last_ai_ok"] else "Never"
        last_err = row["last_ai_error"] if row and row["last_ai_error"] else "None"
        await update.effective_message.reply_text(t(get_group_lang(chat.id), "aistatus_group", enabled="ON" if AI_HOURLY_ENABLED else "OFF", present="YES" if bool(GROQ_API_KEY) else "NO", last_ok=last_ok, last_error=last_err))
        return
    if not user or not is_super_admin(user.id):
        await update.effective_message.reply_text(t("en", "only_super_admin"))
        return
    errs = get_recent_ai_errors(3)
    preview = " | ".join(e["error_text"][:40] for e in errs) if errs else "None"
    await update.effective_message.reply_text(t("en", "aistatus_group", enabled="ON" if AI_HOURLY_ENABLED else "OFF", present="YES" if bool(GROQ_API_KEY) else "NO", last_ok="N/A", last_error=preview))

async def on_groupcount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    await update.effective_message.reply_text(t("en", "groupcount", count=len(get_all_enabled_groups())))

async def on_activegroups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    rows = []
    with db_connect() as conn:
        data = conn.execute("SELECT chat_id, title FROM groups WHERE enabled=1 ORDER BY updated_at DESC LIMIT 50").fetchall()
        for r in data:
            rows.append(f"- {r['title'] or 'Untitled'} ({r['chat_id']})")
    await update.effective_message.reply_text(t("en", "activegroups", rows="\n".join(rows) if rows else "None"))

async def on_failedgroups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    rows = []
    for r in get_failed_groups():
        when = datetime.fromtimestamp(r["last_failed_at"], ZoneInfo(TIMEZONE_NAME)).strftime("%m-%d %I:%M %p") if r["last_failed_at"] else "?"
        rows.append(f"- {r['title'] or 'Untitled'} ({r['chat_id']}) | fails={r['failed_sends']} | {when} | {r['last_failed_reason'] or 'unknown'}")
    await update.effective_message.reply_text(t("en", "failedgroups", rows="\n".join(rows) if rows else "None"))

async def on_lastaierrors(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    rows = []
    for r in get_recent_ai_errors():
        when = datetime.fromtimestamp(r["created_at"], ZoneInfo(TIMEZONE_NAME)).strftime("%m-%d %I:%M %p")
        rows.append(f"- {when} | {r['provider']} | {r['error_text'][:120]}")
    await update.effective_message.reply_text(t("en", "lastaierrors", rows="\n".join(rows) if rows else "None"))

async def on_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
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
    ok = 0
    fail = 0
    for gid in groups:
        try:
            await retry_async(context.bot.send_message, chat_id=gid, text=parts[1].strip())
            ok += 1
        except Exception as e:
            record_send_failure(gid, str(e))
            fail += 1
    await status.edit_text(t("en", "broadcast_done", ok=ok, fail=fail))

async def on_broadcastphoto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    reply = update.effective_message.reply_to_message
    if not reply or not reply.photo:
        await update.effective_message.reply_text(t("en", "broadcastphoto_usage"))
        return
    caption = " ".join(context.args).strip()
    photo = reply.photo[-1].file_id
    groups = get_all_enabled_groups()
    status = await update.effective_message.reply_text(t("en", "media_broadcast_start", count=len(groups)))
    ok = fail = 0
    for gid in groups:
        try:
            await retry_async(context.bot.send_photo, chat_id=gid, photo=photo, caption=caption or None)
            ok += 1
        except Exception as e:
            record_send_failure(gid, str(e))
            fail += 1
    await status.edit_text(t("en", "media_broadcast_done", ok=ok, fail=fail))

async def on_broadcastvoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_owner_private(update): return
    reply = update.effective_message.reply_to_message
    file_id = None
    if reply:
        if reply.voice:
            file_id = reply.voice.file_id
        elif reply.audio:
            file_id = reply.audio.file_id
    if not file_id:
        await update.effective_message.reply_text(t("en", "broadcastvoice_usage"))
        return
    caption = " ".join(context.args).strip()
    groups = get_all_enabled_groups()
    status = await update.effective_message.reply_text(t("en", "media_broadcast_start", count=len(groups)))
    ok = fail = 0
    for gid in groups:
        try:
            await retry_async(context.bot.send_voice, chat_id=gid, voice=file_id, caption=caption or None)
            ok += 1
        except Exception as e:
            record_send_failure(gid, str(e))
            fail += 1
    await status.edit_text(t("en", "media_broadcast_done", ok=ok, fail=fail))

async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in {"group","supergroup"}:
        return
    ensure_group(chat.id, chat.title or "")
    group = get_group(chat.id)
    if group and int(group["delete_service"]) == 1 and update.effective_message:
        try:
            await update.effective_message.delete()
        except Exception:
            pass
    for member in update.effective_message.new_chat_members or []:
        if member.is_bot:
            continue
        await enqueue_welcome(context, chat.id, chat.title or "", member)

async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmu = update.chat_member
    if not cmu:
        return
    chat = cmu.chat
    if chat.type not in {"group","supergroup"}:
        return
    ensure_group(chat.id, chat.title or "")
    user = cmu.new_chat_member.user
    if user.is_bot:
        return
    if cmu.old_chat_member.status in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED} and cmu.new_chat_member.status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
        await enqueue_welcome(context, chat.id, chat.title or "", user)

def cleanup_thread():
    while True:
        try:
            now = time.time()
            for p in TMP_DIR.glob("*"):
                try:
                    if p.is_file() and now - p.stat().st_mtime > TMP_FILE_TTL_SECONDS:
                        p.unlink()
                except Exception:
                    pass
            cleanup_db()
        except Exception:
            logger.exception("cleanup thread failed")
        time.sleep(CLEANUP_INTERVAL_SECONDS)

def hourly_thread():
    while True:
        try:
            due = get_enabled_groups_for_hourly()
            if due:
                phase = phase_now()
                pools = {}
                langs = {get_group_lang(int(r["chat_id"])) for r in due}
                for lang in langs:
                    pools[lang] = get_hourly_pool(lang, phase)
                for row in due:
                    gid = int(row["chat_id"])
                    lang = get_group_lang(gid)
                    text = pick_hourly_message(gid, lang, phase, pools[lang])
                    payload = {"chat_id": gid, "text": text, "disable_web_page_preview": True}
                    data = requests.post(f"{API_BASE}/sendMessage", json=payload, timeout=30).json()
                    if data.get("ok"):
                        set_group_value(gid, "last_hourly_at", int(time.time()))
                        set_group_value(gid, "last_ai_ok", int(time.time()))
                    else:
                        reason = str(data)[:300]
                        record_send_failure(gid, reason)
                        set_group_value(gid, "last_ai_error", reason)
                        set_group_value(gid, "last_ai_error_at", int(time.time()))
        except Exception as e:
            add_ai_error("hourly-loop", str(e))
            logger.exception("hourly thread failed")
        time.sleep(60)

async def post_init(application: Application):
    requests.post(f"{API_BASE}/deleteWebhook", json={"drop_pending_updates": False}, timeout=30)
    commands = [
        BotCommand("start", "Show bot info"),
        BotCommand("ping", "Bot alive check"),
        BotCommand("support", "Support group"),
        BotCommand("myid", "Show your user id"),
        BotCommand("lang", "Change group language"),
        BotCommand("voice", "Toggle welcome voice"),
        BotCommand("deleteservice", "Toggle service delete"),
        BotCommand("hourly", "Toggle hourly texts"),
        BotCommand("welcomestyle", "Change banner theme"),
        BotCommand("setfooter", "Set banner footer"),
        BotCommand("setwelcome", "Custom welcome text"),
        BotCommand("resetwelcome", "Reset custom welcome"),
        BotCommand("status", "Show group status"),
        BotCommand("testwelcome", "Send test welcome"),
        BotCommand("aistatus", "Check AI status"),
        BotCommand("groupcount", "Owner groups count"),
        BotCommand("activegroups", "Owner active groups"),
        BotCommand("failedgroups", "Owner failed groups"),
        BotCommand("lastaierrors", "Owner AI errors"),
        BotCommand("broadcast", "Owner broadcast text"),
        BotCommand("broadcastphoto", "Owner broadcast photo"),
        BotCommand("broadcastvoice", "Owner broadcast voice"),
    ]
    await application.bot.set_my_commands(commands)

def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CommandHandler("support", on_support))
    app.add_handler(CommandHandler("ping", on_ping))
    app.add_handler(CommandHandler("myid", on_myid))
    app.add_handler(CommandHandler("lang", on_lang))
    app.add_handler(CommandHandler("voice", on_voice))
    app.add_handler(CommandHandler("deleteservice", on_delete_service))
    app.add_handler(CommandHandler("hourly", on_hourly))
    app.add_handler(CommandHandler("welcomestyle", on_welcomestyle))
    app.add_handler(CommandHandler("setfooter", on_setfooter))
    app.add_handler(CommandHandler("setwelcome", on_setwelcome))
    app.add_handler(CommandHandler("resetwelcome", on_resetwelcome))
    app.add_handler(CommandHandler("status", on_status))
    app.add_handler(CommandHandler("testwelcome", on_testwelcome))
    app.add_handler(CommandHandler("aistatus", on_aistatus))
    app.add_handler(CommandHandler("groupcount", on_groupcount))
    app.add_handler(CommandHandler("activegroups", on_activegroups))
    app.add_handler(CommandHandler("failedgroups", on_failedgroups))
    app.add_handler(CommandHandler("lastaierrors", on_lastaierrors))
    app.add_handler(CommandHandler("broadcast", on_broadcast))
    app.add_handler(CommandHandler("broadcastphoto", on_broadcastphoto))
    app.add_handler(CommandHandler("broadcastvoice", on_broadcastvoice))
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_chat_members))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS & ~filters.COMMAND, track_group))
    return app

def main():
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=hourly_thread, daemon=True).start()
    threading.Thread(target=cleanup_thread, daemon=True).start()
    logger.info("Starting %s", BOT_NAME)
    application = build_app()
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
        close_loop=False,
    )

if __name__ == "__main__":
    main()
