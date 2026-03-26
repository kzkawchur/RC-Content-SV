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
from pyrogram.types import Message, ChatMemberUpdated
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("MayaWelcomeBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT", 8080))

DB_PATH = os.environ.get("DB_PATH", "maya_welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/maya_welcome_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

VOICE_NAME_BN = os.environ.get("VOICE_NAME_BN", "bn-BD-NabanitaNeural")
VOICE_NAME_EN = os.environ.get("VOICE_NAME_EN", "en-US-JennyNeural")
VOICE_RATE = os.environ.get("VOICE_RATE", "-2%")
VOICE_PITCH = os.environ.get("VOICE_PITCH", "+0Hz")
VOICE_VOLUME = os.environ.get("VOICE_VOLUME", "+0%")

TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME", "Asia/Dhaka")
WELCOME_DELETE_AFTER = int(os.environ.get("WELCOME_DELETE_AFTER", "90"))
JOIN_COOLDOWN_SECONDS = int(os.environ.get("JOIN_COOLDOWN_SECONDS", "15"))
REJOIN_IGNORE_SECONDS = int(os.environ.get("REJOIN_IGNORE_SECONDS", "300"))
SECRET_CODE_TTL_SECONDS = int(os.environ.get("SECRET_CODE_TTL_SECONDS", "600"))
WELCOME_DEDUP_SECONDS = int(os.environ.get("WELCOME_DEDUP_SECONDS", "8"))

SUPER_ADMINS = {
    int(x.strip())
    for x in os.environ.get("SUPER_ADMINS", "").split(",")
    if x.strip().isdigit()
}

BOT_NAME = os.environ.get("BOT_NAME", "Maya")
SUPPORT_GROUP_NAME = os.environ.get("SUPPORT_GROUP_NAME", "Support Group")
SUPPORT_GROUP_URL = os.environ.get("SUPPORT_GROUP_URL", "").strip()

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return f"{BOT_NAME} Welcome Bot is running"

@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "bot": BOT_NAME})

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    with db_connect() as conn:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                activated INTEGER NOT NULL DEFAULT 0,
                activated_by INTEGER,
                language TEXT NOT NULL DEFAULT 'bn',
                custom_welcome TEXT,
                voice_enabled INTEGER NOT NULL DEFAULT 1,
                delete_service INTEGER NOT NULL DEFAULT 1,
                last_primary_msg_id INTEGER,
                last_voice_msg_id INTEGER,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS activation_codes (
                code TEXT PRIMARY KEY,
                issued_by INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                used INTEGER NOT NULL DEFAULT 0,
                used_by INTEGER,
                used_in_chat INTEGER,
                created_at INTEGER NOT NULL
            )
            '''
        )
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS join_memory (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                joined_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, user_id)
            )
            '''
        )
        conn.commit()

def ensure_group(chat_id: int, title: str) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            '''
            INSERT INTO groups (chat_id, title, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                updated_at = excluded.updated_at
            ''',
            (chat_id, title or "", now_ts),
        )
        conn.commit()

def get_group(chat_id: int) -> Optional[sqlite3.Row]:
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE chat_id = ?", (chat_id,)).fetchone()

def get_group_lang(chat_id: int) -> str:
    row = get_group(chat_id)
    if not row:
        return "bn"
    lang = (row["language"] or "bn").strip().lower()
    return lang if lang in {"bn", "en"} else "bn"

def activate_group(chat_id: int, title: str, activated_by: int) -> None:
    now_ts = int(time.time())
    with db_connect() as conn:
        conn.execute(
            '''
            INSERT INTO groups (chat_id, title, activated, activated_by, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                activated = 1,
                activated_by = excluded.activated_by,
                updated_at = excluded.updated_at
            ''',
            (chat_id, title or "", activated_by, now_ts),
        )
        conn.commit()

def set_group_value(chat_id: int, field: str, value) -> None:
    allowed = {
        "custom_welcome",
        "voice_enabled",
        "delete_service",
        "last_primary_msg_id",
        "last_voice_msg_id",
        "updated_at",
        "title",
        "language",
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()

def get_activated_groups() -> list[int]:
    with db_connect() as conn:
        rows = conn.execute("SELECT chat_id FROM groups WHERE activated = 1").fetchall()
        return [int(r["chat_id"]) for r in rows]

def generate_secret_code(length: int = 6) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))

def create_activation_code(issued_by: int) -> tuple[str, int]:
    expires_at = int(time.time()) + SECRET_CODE_TTL_SECONDS
    now_ts = int(time.time())
    for _ in range(20):
        code = generate_secret_code()
        try:
            with db_connect() as conn:
                conn.execute(
                    '''
                    INSERT INTO activation_codes
                    (code, issued_by, expires_at, used, used_by, used_in_chat, created_at)
                    VALUES (?, ?, ?, 0, NULL, NULL, ?)
                    ''',
                    (code, issued_by, expires_at, now_ts),
                )
                conn.commit()
                return code, expires_at
        except sqlite3.IntegrityError:
            continue
    raise RuntimeError("Could not generate unique code")

def consume_activation_code(code: str, used_by: int, used_in_chat: int) -> bool:
    now_ts = int(time.time())
    with db_connect() as conn:
        row = conn.execute(
            '''
            SELECT code, expires_at, used
            FROM activation_codes
            WHERE code = ?
            ''',
            (code,),
        ).fetchone()
        if not row:
            return False
        if int(row["used"]) == 1:
            return False
        if int(row["expires_at"]) < now_ts:
            return False
        conn.execute(
            '''
            UPDATE activation_codes
            SET used = 1, used_by = ?, used_in_chat = ?
            WHERE code = ?
            ''',
            (used_by, used_in_chat, code),
        )
        conn.commit()
        return True

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
            '''
            INSERT INTO join_memory (chat_id, user_id, joined_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET joined_at = excluded.joined_at
            ''',
            (chat_id, user_id, now_ts),
        )
        conn.commit()

app = Client(
    "maya-welcome-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

chat_last_welcome_ts: dict[int, float] = {}
recent_welcome_keys: dict[str, float] = {}

MESSAGES = {
    "bn": {
        "start_private": [
            "আমি {bot} 🌸\n\nPrivate commands:\n/getcode - bot admin code নেবে\n/ping - bot alive check\n/myid - তোমার user id\n/broadcast <text> - owner broadcast\n/support - support info\n\nGroup setup:\n1. আমাকে group-এ add করো\n2. Delete Messages permission দাও\n3. bot admin personal chat-এ /getcode দেবে\n4. group-এ যে কেউ /activate CODE দেবে\n5. admin চাইলে /lang bn বা /lang en দেবে",
            "{bot} ready 🌷\n\nCommands:\n/getcode\n/ping\n/myid\n/broadcast <text>\n/support\n\nSetup:\n1. Add me to the group\n2. Give me Delete Messages permission\n3. Bot admin gets code in private chat\n4. Anyone in the group can activate with /activate CODE",
        ],
        "start_group": [
            "{bot} is here.\nএই group-এ use করতে:\n1. Bot admin personal chat-এ /getcode নেবে\n2. Group-এ যে কেউ /activate CODE দেবে",
            "{bot} ready for this group.\nকোড লাগবে? Bot admin personal chat-এ /getcode নেবে, তারপর এখানে /activate CODE দাও।",
        ],
        "owner_only_code": [
            "Only bot owner/admin can get activation code.",
            "এই command শুধু bot admin ব্যবহার করতে পারবে।",
        ],
        "support_hint": [
            "কোড দরকার হলে support-এ যোগাযোগ করো: {support}",
            "Activation code লাগলে support group-এ যোগাযোগ করো: {support}",
        ],
        "code_msg": [
            "🔐 Secret activation code: `{code}`\n\nএই code group-এ যে কেউ use করতে পারবে.\nGroup-এ লিখবে:\n`/activate {code}`\n\nCode {mins} মিনিট valid থাকবে.",
            "🔐 তোমার activation code: `{code}`\n\nTarget group-এ যে কেউ এই code use করতে পারবে:\n`/activate {code}`\n\nValidity: {mins} মিনিট.",
        ],
        "activate_group_only": ["Use /activate শুধু group-এ."],
        "activate_usage": ["Usage:\n/activate YOURCODE"],
        "invalid_code": [
            "Invalid, expired, or already used code.",
            "কোড ভুল, expired, বা already used.",
        ],
        "activated": [
            "✅ {bot} এখন এই group-এ active.\nআমি join/leave message delete করব, সুন্দর welcome দেব, আর spam কম রাখব।",
            "✅ {bot} activated successfully.\nএখন আমি join/leave service message delete করব, সুন্দর cover welcome পাঠাব, sweet voice welcome দেব।",
        ],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "ঠিক আছে, voice welcome এখন {value}।"],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set": ["Language changed to বাংলা.", "ঠিক আছে, এখন থেকে আমি বাংলায় কথা বলব।"],
        "lang_set_en": ["Language changed to English.", "Okay, I will speak in English now."],
        "only_group_admin": ["Only group admins can use this command.", "এই command শুধু group admin ব্যবহার করতে পারবে।"],
        "not_activated": ["এই group এখনো activated না. আগে /activate CODE দাও."],
        "welcome_saved": ["Custom welcome text saved successfully.", "Custom welcome text save হয়ে গেছে।"],
        "welcome_reset": ["Custom welcome reset done.", "Custom welcome reset করা হয়েছে।"],
        "status": ["Bot name: {bot}\nActivated: {activated}\nLanguage: {lang_name}\nVoice welcome: {voice}\nDelete service message: {delete_service}\nTimezone: {tz}\nPhase now: {phase}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No activated groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\n\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
    },
    "en": {
        "start_private": [
            "I am {bot} 🌸\n\nPrivate commands:\n/getcode - bot admin gets activation code\n/ping - bot alive check\n/myid - show your user id\n/broadcast <text> - owner broadcast\n/support - support info\n\nGroup setup:\n1. Add me to the group\n2. Give me Delete Messages permission\n3. A bot admin uses /getcode in private chat\n4. Anyone in the group can use /activate CODE\n5. A group admin can use /lang bn or /lang en",
            "{bot} is ready 🌷\n\nPrivate commands:\n/getcode\n/ping\n/myid\n/broadcast <text>\n/support\n\nSetup:\nAdd me to the group, grant Delete Messages, get a code in private chat, then activate with /activate CODE in the group.",
        ],
        "start_group": [
            "{bot} is here.\nTo use me in this group:\n1. A bot admin gets a code in private chat with /getcode\n2. Anyone in the group can use /activate CODE",
            "{bot} is ready for this group.\nNeed setup? A bot admin gets a code in private chat, then anyone here can use /activate CODE.",
        ],
        "owner_only_code": ["Only bot owner/admin can get activation code."],
        "support_hint": ["Need an activation code? Contact support: {support}"],
        "code_msg": [
            "🔐 Secret activation code: `{code}`\n\nAnyone in the target group can use it.\nIn group, send:\n`/activate {code}`\n\nThe code will stay valid for {mins} minutes.",
            "🔐 Your activation code is `{code}`\n\nAny member of the target group can activate with:\n`/activate {code}`\n\nValid for {mins} minutes.",
        ],
        "activate_group_only": ["Use /activate only in a group."],
        "activate_usage": ["Usage:\n/activate YOURCODE"],
        "invalid_code": ["Invalid, expired, or already used code."],
        "activated": [
            "✅ {bot} is now active in this group.\nI will delete join/leave messages, send a nicer welcome cover, and keep spam low.",
            "✅ {bot} activated successfully.\nI will now handle join/leave cleanup and welcome messages here.",
        ],
        "voice_usage": ["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],
        "voice_set": ["Voice welcome: {value}", "Voice welcome is now {value}."],
        "deleteservice_usage": ["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],
        "deleteservice_set": ["Delete service message: {value}", "Service message delete mode: {value}"],
        "lang_usage": ["Usage:\n/lang bn\n/lang en"],
        "lang_set": ["Language changed to Bangla."],
        "lang_set_en": ["Language changed to English.", "Okay, I will speak in English now."],
        "only_group_admin": ["Only group admins can use this command."],
        "not_activated": ["This group is not activated yet. Use /activate CODE first."],
        "welcome_saved": ["Custom welcome text saved successfully.", "Your custom welcome text has been saved."],
        "welcome_reset": ["Custom welcome has been reset."],
        "status": ["Bot name: {bot}\nActivated: {activated}\nLanguage: {lang_name}\nVoice welcome: {voice}\nDelete service message: {delete_service}\nTimezone: {tz}\nCurrent phase: {phase}"],
        "broadcast_owner_only": ["Broadcast is owner-only."],
        "broadcast_usage": ["Usage:\n/broadcast your message"],
        "broadcast_none": ["No activated groups found."],
        "broadcast_start": ["Broadcast started to {count} groups..."],
        "broadcast_done": ["Broadcast finished.\n\nSuccess: {ok}\nFailed: {fail}"],
        "test_voice_caption": ["🎤 {bot} test voice"],
        "welcome_voice_caption": ["🎤 {bot} welcome voice"],
        "ping": ["pong | {tz} | {time}"],
        "myid": ["Your user ID: {user_id}"],
        "support": ["Support: {support}"],
    },
}

def support_text() -> str:
    if SUPPORT_GROUP_URL and SUPPORT_GROUP_NAME:
        return f"{SUPPORT_GROUP_NAME} | {SUPPORT_GROUP_URL}"
    if SUPPORT_GROUP_URL:
        return SUPPORT_GROUP_URL
    return SUPPORT_GROUP_NAME

def msg_text(lang: str, key: str, **kwargs) -> str:
    base_lang = lang if lang in MESSAGES else "bn"
    variants = MESSAGES[base_lang].get(key) or MESSAGES["bn"].get(key) or [key]
    return random.choice(variants).format(bot=BOT_NAME, support=support_text(), **kwargs)

def chat_type_name(chat) -> str:
    if not chat:
        return ""
    return getattr(chat.type, "value", str(chat.type)).lower()

def member_status_name(status) -> str:
    return getattr(status, "value", str(status)).lower()

def clean_name(name: str) -> str:
    if not name:
        return "বন্ধু"
    return name.replace("\n", " ").strip()[:40]

def ascii_name(name: str) -> str:
    s = (name or "").encode("ascii", "ignore").decode().strip()
    return s[:22] if s else "FRIEND"

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

def voice_name_for_lang(lang: str) -> str:
    return VOICE_NAME_EN if lang == "en" else VOICE_NAME_BN

def build_welcome_copy(lang: str, first_name: str, mention_name: str, group_title: str, custom_text: Optional[str]) -> tuple[str, str]:
    phase = get_day_phase()
    safe_group = group_title or ("our group" if lang == "en" else "আমাদের গ্রুপ")
    if custom_text:
        text_welcome = custom_text.replace("{name}", mention_name).replace("{group}", safe_group).replace("{phase}", phase)
        voice_text = f"Hello {first_name}, welcome to {safe_group}." if lang == "en" else f"{first_name}, তোমাকে {safe_group} এ স্বাগতম।"
        return text_welcome, voice_text

    if lang == "en":
        templates = {
            "morning": [f"🌼 Good morning {mention_name}!\nWelcome to {safe_group}.", f"✨ {mention_name}, warm morning wishes and welcome to {safe_group}.", f"☀️ Hello {mention_name}!\nA bright morning welcome to {safe_group}."],
            "day": [f"🌸 Welcome {mention_name}!\nWe are happy to have you in {safe_group}.", f"💫 Hello {mention_name}!\nA warm welcome to {safe_group}.", f"🌷 {mention_name}, glad to see you in {safe_group}. Welcome!"],
            "evening": [f"🌙 Good evening {mention_name}!\nWelcome to {safe_group}.", f"✨ {mention_name}, lovely evening wishes and welcome to {safe_group}.", f"🌆 Hello {mention_name}!\nEvening smiles and a warm welcome to {safe_group}."],
            "night": [f"🌌 Good night {mention_name}!\nWelcome to {safe_group}.", f"💙 {mention_name}, peaceful night wishes and welcome to {safe_group}.", f"⭐ Hello {mention_name}!\nA calm night welcome to {safe_group}."],
        }
        voices = {
            "morning": [f"{first_name}, good morning. A warm welcome to {safe_group}.", f"Hello {first_name}, welcome to {safe_group}. We are glad to have you here."],
            "day": [f"{first_name}, welcome to {safe_group}. We are really happy to have you here.", f"Hello {first_name}, a warm welcome to {safe_group}."],
            "evening": [f"{first_name}, good evening. Welcome to {safe_group}. Hope you enjoy your time here.", f"Hello {first_name}, evening wishes and welcome to {safe_group}."],
            "night": [f"{first_name}, good night. A warm welcome to {safe_group}.", f"Hello {first_name}, welcome to {safe_group}. Glad to have you here."],
        }
    else:
        templates = {
            "morning": [f"🌼 শুভ সকাল {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"✨ {mention_name}, সকালের মিষ্টি শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"☀️ হ্যালো {mention_name}!\nএকটা সুন্দর সকালের স্বাগতম রইল {safe_group} এ।"],
            "day": [f"🌸 স্বাগতম {mention_name}!\n{safe_group} এ তোমাকে পেয়ে খুব ভালো লাগছে।", f"💫 হ্যালো {mention_name}!\n{safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"🌷 {mention_name}, তোমাকে পেয়ে {safe_group} আরও সুন্দর লাগছে। স্বাগতম।"],
            "evening": [f"🌙 শুভ সন্ধ্যা {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"✨ {mention_name}, সন্ধ্যার সুন্দর শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"🌆 হ্যালো {mention_name}!\nসন্ধ্যার নরম আলোয় তোমাকে {safe_group} এ স্বাগতম।"],
            "night": [f"🌌 শুভ রাত্রি {mention_name}!\n{safe_group} এ তোমাকে স্বাগতম।", f"💙 {mention_name}, রাতের শান্ত শুভেচ্ছা। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।", f"⭐ হ্যালো {mention_name}!\nরাতের শান্ত শুভেচ্ছার সাথে তোমাকে স্বাগতম {safe_group} এ।"],
        }
        voices = {
            "morning": [f"{first_name}, শুভ সকাল। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"হ্যালো {first_name}, সকালের সুন্দর শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
            "day": [f"{first_name}, তোমাকে {safe_group} এ আন্তরিক স্বাগতম। তোমাকে পেয়ে ভালো লাগছে।", f"হ্যালো {first_name}, {safe_group} এ তোমাকে পেয়ে সত্যিই ভালো লাগছে। স্বাগতম।"],
            "evening": [f"{first_name}, শুভ সন্ধ্যা। {safe_group} এ তোমাকে স্বাগতম। আশা করি এখানে ভালো সময় কাটাবে।", f"হ্যালো {first_name}, সন্ধ্যার মিষ্টি শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
            "night": [f"{first_name}, শুভ রাত্রি। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।", f"হ্যালো {first_name}, রাতের শান্ত শুভেচ্ছা। তোমাকে পেয়ে ভালো লাগছে।"],
        }

    return random.choice(templates[phase]), random.choice(voices[phase])

def is_super_admin(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id in SUPER_ADMINS)

async def is_group_admin(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return any(x in member_status_name(member.status) for x in ("administrator", "owner", "creator"))
    except Exception:
        logger.exception("Failed to check admin status for user %s in chat %s", user_id, chat_id)
        return False

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

def recent_key(chat_id: int, user_id: int, kind: str) -> str:
    return f"{kind}:{chat_id}:{user_id}"

def is_recent_duplicate(chat_id: int, user_id: int, kind: str) -> bool:
    key = recent_key(chat_id, user_id, kind)
    now_ts = time.time()
    prev = recent_welcome_keys.get(key, 0)
    recent_welcome_keys[key] = now_ts
    return now_ts - prev < WELCOME_DEDUP_SECONDS

async def delete_previous_welcome(client: Client, chat_id: int) -> None:
    group = get_group(chat_id)
    if not group:
        return
    for mid in (group["last_primary_msg_id"], group["last_voice_msg_id"]):
        if mid:
            try:
                await client.delete_messages(chat_id, int(mid))
            except Exception:
                pass

async def schedule_delete_message(client: Client, chat_id: int, message_id: int, delay: int) -> None:
    try:
        await asyncio.sleep(delay)
        await client.delete_messages(chat_id, message_id)
    except Exception:
        pass

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

async def make_voice_file(text: str, lang: str, output_path: Path) -> None:
    communicate = edge_tts.Communicate(text=text, voice=voice_name_for_lang(lang), rate=VOICE_RATE, pitch=VOICE_PITCH, volume=VOICE_VOLUME)
    await communicate.save(str(output_path))

def build_cover_bytes(first_name: str, group_title: str, lang: str) -> BytesIO:
    width, height = 1280, 720
    phase = get_day_phase()
    palette = {
        "morning": ((255, 226, 150), (255, 143, 116)),
        "day": ((119, 215, 255), (82, 103, 255)),
        "evening": ((175, 110, 255), (255, 99, 171)),
        "night": ((17, 24, 39), (37, 99, 235)),
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
    draw.ellipse((70, 60, 250, 240), fill=(255, 255, 255))
    draw.ellipse((1030, 90, 1210, 270), fill=(255, 255, 255))
    draw.ellipse((930, 480, 1160, 710), fill=(255, 255, 255))
    draw.rounded_rectangle((95, 95, 1185, 625), radius=42, fill=(13, 18, 35))
    draw.rounded_rectangle((120, 120, 145, 600), radius=10, fill=(255, 214, 120))

    title_font = pick_font(62, bold=True)
    name_font = pick_font(88, bold=True)
    sub_font = pick_font(34, bold=False)
    mini_font = pick_font(28, bold=True)

    name_text = ascii_name(first_name).upper()
    group_text = ascii_name(group_title or ("OUR GROUP" if lang == "en" else "GROUP")).upper()

    draw.text((175, 155), "WELCOME", fill=(255, 255, 255), font=title_font)
    draw.text((175, 255), name_text, fill=(255, 224, 153), font=name_font)
    draw.text((175, 385), f"TO {group_text}", fill=(214, 228, 255), font=sub_font)
    draw.text((175, 470), BOT_NAME.upper(), fill=(173, 255, 223), font=mini_font)
    draw.rounded_rectangle((175, 535, 410, 548), radius=6, fill=(255, 255, 255))
    draw.rounded_rectangle((175, 565, 330, 578), radius=6, fill=(190, 225, 255))

    bio = BytesIO()
    img.save(bio, format="PNG")
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

async def perform_welcome(client: Client, chat_id: int, chat_title: str, user_obj) -> None:
    ensure_group(chat_id, chat_title or "")
    group = get_group(chat_id)
    if not group or int(group["activated"]) != 1:
        return
    lang = get_group_lang(chat_id)
    user_id = user_obj.id
    first_name = clean_name(user_obj.first_name)
    mention_name = user_obj.mention(first_name)

    if is_recent_duplicate(chat_id, user_id, "join"):
        return
    if should_skip_for_spam(chat_id, user_id):
        logger.info("Skipped welcome due to anti-spam rules | chat_id=%s user_id=%s", chat_id, user_id)
        return

    await delete_previous_welcome(client, chat_id)
    text_welcome, voice_text = build_welcome_copy(lang, first_name, mention_name, chat_title or ("our group" if lang == "en" else "আমাদের গ্রুপ"), group["custom_welcome"])
    primary_message = None
    voice_message = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user_id}_{int(time.time())}.mp3"

    try:
        cover = build_cover_bytes(first_name, chat_title or "GROUP", lang)
        primary_message = await client.send_photo(chat_id=chat_id, photo=cover, caption=text_welcome)
        if int(group["voice_enabled"]) == 1:
            await make_voice_file(voice_text, lang, voice_path)
            voice_message = await client.send_voice(chat_id=chat_id, voice=str(voice_path), caption=msg_text(lang, "welcome_voice_caption"))
        mark_welcomed(chat_id, user_id)
        set_group_value(chat_id, "last_primary_msg_id", primary_message.id if primary_message else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_message.id if voice_message else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        if primary_message:
            asyncio.create_task(schedule_delete_message(client, chat_id, primary_message.id, WELCOME_DELETE_AFTER))
        if voice_message:
            asyncio.create_task(schedule_delete_message(client, chat_id, voice_message.id, WELCOME_DELETE_AFTER))
    except Exception:
        logger.exception("Failed welcome flow in chat %s for user %s", chat_id, user_id)
    finally:
        try:
            if voice_path.exists():
                voice_path.unlink()
        except Exception:
            pass

@app.on_message(filters.service)
async def service_handler(client: Client, message: Message):
    chat = message.chat
    if not chat or chat_type_name(chat) not in ("group", "supergroup"):
        return
    ensure_group(chat.id, chat.title or "")
    group = get_group(chat.id)
    if not group or int(group["activated"]) != 1:
        return

    if int(group["delete_service"]) == 1:
        try:
            await client.delete_messages(chat.id, message.id)
        except Exception:
            logger.exception("Failed deleting service message in chat %s", chat.id)

    if message.new_chat_members:
        me = await client.get_me()
        target_member = None
        for member in message.new_chat_members:
            if member.is_bot and member.id == me.id:
                continue
            if not member.is_bot:
                target_member = member
        if target_member is not None:
            await perform_welcome(client, chat.id, chat.title or "", target_member)

@app.on_chat_member_updated()
async def chat_member_updated_handler(client: Client, update: ChatMemberUpdated):
    chat = update.chat
    if not chat or chat_type_name(chat) not in ("group", "supergroup"):
        return
    ensure_group(chat.id, chat.title or "")
    group = get_group(chat.id)
    if not group or int(group["activated"]) != 1:
        return

    old_status = member_status_name(update.old_chat_member.status) if update.old_chat_member else ""
    new_status = member_status_name(update.new_chat_member.status) if update.new_chat_member else ""
    user_obj = update.new_chat_member.user if update.new_chat_member else None
    if not user_obj or user_obj.is_bot:
        return
    if old_status in {"left", "kicked", "banned"} and new_status in {"member", "administrator", "owner", "creator"}:
        await perform_welcome(client, chat.id, chat.title or "", user_obj)

@app.on_message(filters.command("start"))
async def start_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) in ("group", "supergroup"):
        ensure_group(message.chat.id, message.chat.title or "")
        await message.reply_text(msg_text(get_group_lang(message.chat.id), "start_group"))
    else:
        await message.reply_text(msg_text("bn", "start_private"))

@app.on_message(filters.command("support"))
async def support_cmd(_, message: Message):
    lang = get_group_lang(message.chat.id) if chat_type_name(message.chat) in ("group", "supergroup") else "bn"
    await message.reply_text(msg_text(lang, "support"))

@app.on_message(filters.command("ping"))
async def ping_cmd(_, message: Message):
    lang = get_group_lang(message.chat.id) if chat_type_name(message.chat) in ("group", "supergroup") else "bn"
    await message.reply_text(msg_text(lang, "ping", tz=TIMEZONE_NAME, time=get_local_time().strftime("%I:%M %p")))

@app.on_message(filters.command("myid"))
async def myid_cmd(_, message: Message):
    await message.reply_text(msg_text("en", "myid", user_id=message.from_user.id if message.from_user else 0))

@app.on_message(filters.command("getcode") & filters.private)
async def getcode_cmd(_, message: Message):
    if not message.from_user or not is_super_admin(message.from_user.id):
        text = msg_text("en", "owner_only_code")
        if SUPPORT_GROUP_URL or SUPPORT_GROUP_NAME:
            text += "\n\n" + msg_text("bn", "support_hint")
        await message.reply_text(text)
        return
    code, _ = create_activation_code(message.from_user.id)
    await message.reply_text(msg_text("bn", "code_msg", code=code, mins=max(1, SECRET_CODE_TTL_SECONDS // 60)))

@app.on_message(filters.command("activate"))
async def activate_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text(msg_text("bn", "activate_group_only"))
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if len(message.command) < 2:
        await message.reply_text(msg_text(lang, "activate_usage"))
        return
    code = message.command[1].strip().upper()
    if not consume_activation_code(code, message.from_user.id if message.from_user else 0, message.chat.id):
        await message.reply_text(msg_text(lang, "invalid_code"))
        return
    activate_group(message.chat.id, message.chat.title or "", message.from_user.id if message.from_user else 0)
    await message.reply_text(msg_text(lang, "activated"))

@app.on_message(filters.command("lang"))
async def lang_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text(msg_text("en", "lang_usage"))
        return
    ensure_group(message.chat.id, message.chat.title or "")
    current_lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(current_lang, "only_group_admin"))
        return
    if len(message.command) < 2:
        await message.reply_text(msg_text(current_lang, "lang_usage"))
        return
    new_lang = message.command[1].strip().lower()
    if new_lang not in {"bn", "en"}:
        await message.reply_text(msg_text(current_lang, "lang_usage"))
        return
    set_group_value(message.chat.id, "language", new_lang)
    await message.reply_text(msg_text(new_lang, "lang_set_en" if new_lang == "en" else "lang_set"))

@app.on_message(filters.command("voice"))
async def voice_toggle_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text("Use /voice in group.")
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(lang, "only_group_admin"))
        return
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text(msg_text(lang, "not_activated"))
        return
    if len(message.command) < 2:
        current = "ON" if int(group["voice_enabled"]) == 1 else "OFF"
        await message.reply_text(msg_text(lang, "voice_usage", current=current))
        return
    value = message.command[1].strip().lower()
    if value not in ("on", "off"):
        current = "ON" if int(group["voice_enabled"]) == 1 else "OFF"
        await message.reply_text(msg_text(lang, "voice_usage", current=current))
        return
    set_group_value(message.chat.id, "voice_enabled", 1 if value == "on" else 0)
    await message.reply_text(msg_text(lang, "voice_set", value=value.upper()))

@app.on_message(filters.command("deleteservice"))
async def deleteservice_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text("Use /deleteservice in group.")
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(lang, "only_group_admin"))
        return
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text(msg_text(lang, "not_activated"))
        return
    if len(message.command) < 2:
        current = "ON" if int(group["delete_service"]) == 1 else "OFF"
        await message.reply_text(msg_text(lang, "deleteservice_usage", current=current))
        return
    value = message.command[1].strip().lower()
    if value not in ("on", "off"):
        current = "ON" if int(group["delete_service"]) == 1 else "OFF"
        await message.reply_text(msg_text(lang, "deleteservice_usage", current=current))
        return
    set_group_value(message.chat.id, "delete_service", 1 if value == "on" else 0)
    await message.reply_text(msg_text(lang, "deleteservice_set", value=value.upper()))

@app.on_message(filters.command("setwelcome"))
async def setwelcome_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text("Use /setwelcome in group.")
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(lang, "only_group_admin"))
        return
    group = get_group(message.chat.id)
    if not group or int(group["activated"]) != 1:
        await message.reply_text(msg_text(lang, "not_activated"))
        return
    parts = (message.text or "").split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text("Usage:\n/setwelcome your text\n\nAvailable placeholders:\n{name} = user mention\n{group} = group title\n{phase} = morning/day/evening/night")
        return
    set_group_value(message.chat.id, "custom_welcome", parts[1].strip()[:600])
    await message.reply_text(msg_text(lang, "welcome_saved"))

@app.on_message(filters.command("resetwelcome"))
async def resetwelcome_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text("Use /resetwelcome in group.")
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(lang, "only_group_admin"))
        return
    set_group_value(message.chat.id, "custom_welcome", None)
    await message.reply_text(msg_text(lang, "welcome_reset"))

@app.on_message(filters.command("status"))
async def status_cmd(client: Client, message: Message):
    if chat_type_name(message.chat) not in ("group", "supergroup"):
        await message.reply_text("Use /status in group.")
        return
    ensure_group(message.chat.id, message.chat.title or "")
    lang = get_group_lang(message.chat.id)
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        await message.reply_text(msg_text(lang, "only_group_admin"))
        return
    group = get_group(message.chat.id)
    if not group:
        await message.reply_text("No group config found.")
        return
    await message.reply_text(msg_text(lang, "status", activated="YES" if int(group["activated"]) == 1 else "NO", lang_name="Bangla" if get_group_lang(message.chat.id) == "bn" else "English", voice="ON" if int(group["voice_enabled"]) == 1 else "OFF", delete_service="ON" if int(group["delete_service"]) == 1 else "OFF", tz=TIMEZONE_NAME, phase=get_day_phase()))

@app.on_message(filters.command("testwelcome"))
async def testwelcome_cmd(client: Client, message: Message):
    lang = get_group_lang(message.chat.id) if chat_type_name(message.chat) in ("group", "supergroup") else "bn"
    first_name = clean_name(message.from_user.first_name if message.from_user else ("Friend" if lang == "en" else "বন্ধু"))
    mention_name = message.from_user.mention(first_name) if message.from_user else first_name
    text_welcome, voice_text = build_welcome_copy(lang, first_name, mention_name, message.chat.title if message.chat and message.chat.title else ("our group" if lang == "en" else "আমাদের গ্রুপ"), None)

    cover = build_cover_bytes(first_name, message.chat.title if message.chat else "GROUP", lang)
    primary_message = await client.send_photo(chat_id=message.chat.id, photo=cover, caption=text_welcome)
    voice_path = TMP_DIR / f"test_{message.chat.id}_{int(time.time())}.mp3"
    voice_message = None
    try:
        await make_voice_file(voice_text, lang, voice_path)
        voice_message = await client.send_voice(chat_id=message.chat.id, voice=str(voice_path), caption=msg_text(lang, "test_voice_caption"))
    except Exception:
        logger.exception("Failed sending test welcome voice")
    finally:
        try:
            if voice_path.exists():
                voice_path.unlink()
        except Exception:
            pass
    asyncio.create_task(schedule_delete_message(client, message.chat.id, primary_message.id, WELCOME_DELETE_AFTER))
    if voice_message:
        asyncio.create_task(schedule_delete_message(client, message.chat.id, voice_message.id, WELCOME_DELETE_AFTER))

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client: Client, message: Message):
    if not message.from_user or not is_super_admin(message.from_user.id):
        await message.reply_text(msg_text("en", "broadcast_owner_only"))
        return
    parts = (message.text or "").split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await message.reply_text(msg_text("en", "broadcast_usage"))
        return
    group_ids = get_activated_groups()
    if not group_ids:
        await message.reply_text(msg_text("en", "broadcast_none"))
        return
    status = await message.reply_text(msg_text("en", "broadcast_start", count=len(group_ids)))
    ok_count = 0
    fail_count = 0
    for gid in group_ids:
        try:
            await client.send_message(gid, parts[1].strip())
            ok_count += 1
        except Exception:
            fail_count += 1
            logger.exception("Broadcast failed to group %s", gid)
    await status.edit_text(msg_text("en", "broadcast_done", ok=ok_count, fail=fail_count))

def main():
    init_db()
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info("Flask started on port %s", PORT)
    logger.info("Starting %s", BOT_NAME)
    app.run()

if __name__ == "__main__":
    main()
