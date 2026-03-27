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
from PIL import Image, ImageDraw, ImageFilter, ImageFont
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
    return bool(data.get("ok"))

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
        return []
    except Exception as e:
        _update_groq_status(False, f"Failed: {e}")
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
        return False, msg
    except Exception as e:
        _update_groq_status(False, str(e))
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

def build_cover_bytes(first_name: str, group_title: str, lang: str) -> BytesIO:
    width, height = 1280, 720
    phase = phase_now()
    palettes = {
        "morning": ((255, 234, 167), (255, 145, 110), (255, 255, 255)),
        "day": ((125, 235, 255), (84, 105, 255), (255, 255, 255)),
        "evening": ((189, 116, 255), (255, 98, 174), (255, 241, 255)),
        "night": ((16, 24, 40), (39, 102, 248), (220, 238, 255)),
    }
    c1, c2, glow = palettes[phase]
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
    overlay = overlay.filter(ImageFilter.GaussianBlur(8))
    img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(img)
    shadow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle((90, 95, 1188, 628), radius=48, fill=(0, 0, 0, 95))
    shadow = shadow.filter(ImageFilter.GaussianBlur(18))
    img = Image.alpha_composite(img.convert("RGBA"), shadow).convert("RGB")
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((100, 100, 1180, 620), radius=44, fill=(11, 17, 36))
    draw.rounded_rectangle((130, 125, 156, 595), radius=12, fill=(255, 218, 122))
    title_font = pick_font(64, True)
    name_font = pick_font(92, True)
    sub_font = pick_font(36, False)
    mini_font = pick_font(28, True)
    phase_font = pick_font(24, True)
    group_text = ascii_name(group_title or ("OUR GROUP" if lang == "en" else "GROUP")).upper()
    name_text = ascii_name(first_name).upper()
    draw.text((182, 152), "WELCOME", fill=glow, font=title_font)
    draw.text((182, 252), name_text, fill=(255, 226, 170), font=name_font)
    draw.text((182, 392), f"TO {group_text}", fill=(222, 233, 255), font=sub_font)
    draw.text((182, 480), BOT_NAME.upper(), fill=(176, 255, 223), font=mini_font)
    badge_w, badge_h = 190, 46
    badge_x, badge_y = 955, 145
    draw.rounded_rectangle((badge_x, badge_y, badge_x + badge_w, badge_y + badge_h), radius=22, fill=(255, 255, 255))
    draw.text((badge_x + 26, badge_y + 10), phase.upper(), fill=(38, 52, 87), font=phase_font)
    draw.rounded_rectangle((182, 540, 430, 552), radius=6, fill=(255, 255, 255))
    draw.rounded_rectangle((182, 570, 334, 582), radius=6, fill=(196, 226, 255))
    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

async def make_voice_file(text: str, lang: str, path: Path):
    voice = VOICE_NAME_EN if lang == "en" else VOICE_NAME_BN
    communicate = edge_tts.Communicate(
        text=text,
        voice=voice,
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
            msg = await context.bot.send_message(chat_id=chat_id, text=compact, parse_mode=ParseMode.HTML)
            set_group_value(chat_id, "last_primary_msg_id", msg.message_id)
            asyncio.create_task(schedule_delete(context.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER))
        except Exception:
            logger.exception("Compact burst welcome failed in %s", chat_id)
        return

    text_welcome, voice_text = welcome_texts(lang, mention_name, first_name, title or "", group["custom_welcome"])
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        cover = build_cover_bytes(first_name, title or "GROUP", lang)
        primary = await context.bot.send_photo(chat_id=chat_id, photo=cover, caption=text_welcome, parse_mode=ParseMode.HTML)
        if int(group["voice_enabled"]) == 1:
            await make_voice_file(voice_text, lang, voice_path)
            voice_msg = await context.bot.send_voice(chat_id=chat_id, voice=voice_path.read_bytes(), caption=t(lang, "welcome_voice_caption"))
        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        if primary:
            asyncio.create_task(schedule_delete(context.bot, chat_id, primary.message_id, WELCOME_DELETE_AFTER))
        if voice_msg:
            asyncio.create_task(schedule_delete(context.bot, chat_id, voice_msg.message_id, WELCOME_DELETE_AFTER))
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
        ai_pool = await asyncio.to_thread(groq_generate_batch, lang, phase)
        pool = ai_pool or build_fallback_messages(lang, phase)
        msg = pick_hourly_message(chat.id, lang, phase, pool)
        await update.effective_message.reply_text(msg)
        set_group_value(chat.id, "last_hourly_at", int(time.time()))
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
    for member in update.effective_message.new_chat_members or []:
        if member.is_bot:
            continue
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
                langs = {get_group_lang(int(r["chat_id"])) for r in due_rows}
                for lang in langs:
                    ai_lines = groq_generate_batch(lang, phase)
                    pools[lang] = ai_lines or build_fallback_messages(lang, phase)
                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang = get_group_lang(chat_id)
                    msg = pick_hourly_message(chat_id, lang, phase, pools[lang])
                    if send_message_http(chat_id, msg):
                        set_group_value(chat_id, "last_hourly_at", int(time.time()))
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
        BotCommand("lang", "Change group language"),
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
    application.add_handler(CommandHandler("lang", on_lang))
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
