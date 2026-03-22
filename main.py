import os
import re
import sys
import time
import json
import hmac
import math
import sqlite3
import logging
import asyncio
import threading
import hashlib
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple, List

from flask import Flask, jsonify
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ChatPermissions,
    Message
)
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import FloodWait, RPCError

# =========================================================
# 1) Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("grouphelp_style_bot")

recent_logs: List[str] = []


class RecentLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            recent_logs.append(msg)
            if len(recent_logs) > 200:
                recent_logs.pop(0)
        except Exception:
            pass


_recent_handler = RecentLogHandler()
_recent_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logging.getLogger().addHandler(_recent_handler)

# =========================================================
# 2) Config
# =========================================================
def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on"}


@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str

    port: int
    db_path: str

    owner_id: int
    admins: Set[int]

    maintenance_mode: bool
    secret_key: str


def parse_admins(raw: str) -> Set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


def load_config() -> Config:
    required = ["API_ID", "API_HASH", "BOT_TOKEN"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    owner_id = int(os.environ.get("OWNER_ID", "0") or 0)
    admins = parse_admins(os.environ.get("ADMIN_IDS", ""))
    if owner_id:
        admins.add(owner_id)

    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],
        port=int(os.environ.get("PORT", "10000")),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),
        owner_id=owner_id,
        admins=admins,
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        secret_key=os.environ.get("SECRET_KEY", "change-this-secret"),
    )


CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

state = {
    "maintenance_mode": CFG.maintenance_mode,
    "started_at": time.time(),
    "success_actions": 0,
    "failed_actions": 0,
}

@app.route("/")
def home():
    return "✅ Moderation bot is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "maintenance": state["maintenance_mode"],
        "success_actions": state["success_actions"],
        "failed_actions": state["failed_actions"],
    }), 200


def run_web_server():
    app.run(host="0.0.0.0", port=CFG.port)

# =========================================================
# 4) Event loop
# =========================================================
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# =========================================================
# 5) Bot client
# =========================================================
bot = Client(
    "grouphelp_style_bot",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token
)

# =========================================================
# 6) Runtime
# =========================================================
flood_tracker: Dict[Tuple[int, int], List[float]] = {}
panel_sessions: Dict[Tuple[int, int], Dict] = {}

# =========================================================
# 7) Text
# =========================================================
TEXTS = {
    "en": {
        "welcome_private": "⚡ **Welcome, {name}!**\n\nI can protect and manage Telegram groups.\nUse the buttons below.",
        "maintenance": "🛠️ Bot is under maintenance. Please try again later.",
        "blocked": "🚫 You are blocked from using this bot.",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 Admin only.",
        "settings": "**Settings**\n\n🌐 Language: `{lang}`",
        "join_required": "🛑 You must join the required channel first.",
        "not_group_admin": "🚫 You must be a group admin to manage settings.",
        "group_choose": "Select a group from below, or add me to a group first.",
        "support_text": "Need help? Contact the bot owner or support group.",
        "info_text": "This bot provides group protection, welcome system, warns, locks, flood control, notes, custom commands and admin panels.",
        "language_text": "Choose your preferred language.",
        "panel_closed": "Panel closed.",
    },
    "bn": {
        "welcome_private": "⚡ **স্বাগতম, {name}!**\n\nআমি Telegram group protect ও manage করতে পারি। নিচের বাটন ব্যবহার করো।",
        "maintenance": "🛠️ বট maintenance-এ আছে। পরে আবার চেষ্টা করো।",
        "blocked": "🚫 তুমি এই বট ব্যবহার করতে পারবে না।",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 শুধু admin ব্যবহার করতে পারবে।",
        "settings": "**Settings**\n\n🌐 Language: `{lang}`",
        "join_required": "🛑 আগে required channel-এ join করতে হবে।",
        "not_group_admin": "🚫 settings চালাতে হলে group admin হতে হবে।",
        "group_choose": "নিচ থেকে group বেছে নাও, অথবা আগে আমাকে group-এ add করো।",
        "support_text": "সাহায্য দরকার হলে owner/support এর সাথে যোগাযোগ করো।",
        "info_text": "এই bot group protection, welcome, warns, locks, flood control, notes, custom commands এবং admin panel দেয়।",
        "language_text": "ভাষা বেছে নাও।",
        "panel_closed": "Panel বন্ধ করা হয়েছে।",
    }
}

# =========================================================
# 8) DB
# =========================================================
def db_connect():
    return sqlite3.connect(CFG.db_path)


def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            username TEXT,
            first_name TEXT,
            is_blocked INTEGER NOT NULL DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            language TEXT NOT NULL DEFAULT 'en'
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_settings (
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            welcome_enabled INTEGER NOT NULL DEFAULT 1,
            welcome_text TEXT NOT NULL DEFAULT '👋 Welcome, {mention}!',
            goodbye_enabled INTEGER NOT NULL DEFAULT 0,
            goodbye_text TEXT NOT NULL DEFAULT '👋 Goodbye, {name}.',
            rules_text TEXT NOT NULL DEFAULT '📜 No rules set yet.',
            banned_words TEXT NOT NULL DEFAULT '',
            flood_limit INTEGER NOT NULL DEFAULT 6,
            flood_window_sec INTEGER NOT NULL DEFAULT 10,
            warn_limit INTEGER NOT NULL DEFAULT 3,
            warn_action TEXT NOT NULL DEFAULT 'mute',
            warn_mute_minutes INTEGER NOT NULL DEFAULT 60,
            link_lock INTEGER NOT NULL DEFAULT 0,
            media_lock INTEGER NOT NULL DEFAULT 0,
            forwarding_block INTEGER NOT NULL DEFAULT 0,
            quote_block INTEGER NOT NULL DEFAULT 0,
            telegram_link_block INTEGER NOT NULL DEFAULT 0,
            total_link_block INTEGER NOT NULL DEFAULT 0,
            night_mode INTEGER NOT NULL DEFAULT 0,
            night_mode_action TEXT NOT NULL DEFAULT 'silence',
            night_start INTEGER NOT NULL DEFAULT 0,
            night_end INTEGER NOT NULL DEFAULT 7,
            admin_tag_founder INTEGER NOT NULL DEFAULT 0,
            admin_tag_admins INTEGER NOT NULL DEFAULT 0,
            approval_mode INTEGER NOT NULL DEFAULT 0,
            service_delete INTEGER NOT NULL DEFAULT 0,
            command_delete INTEGER NOT NULL DEFAULT 0,
            edit_checks INTEGER NOT NULL DEFAULT 0,
            scheduled_delete INTEGER NOT NULL DEFAULT 0,
            block_cancellation INTEGER NOT NULL DEFAULT 0,
            delete_all_messages INTEGER NOT NULL DEFAULT 0,
            self_destruct INTEGER NOT NULL DEFAULT 0,
            captcha_mode INTEGER NOT NULL DEFAULT 0,
            media_action TEXT NOT NULL DEFAULT 'delete',
            log_channel_id INTEGER NOT NULL DEFAULT 0,
            force_sub_channel TEXT NOT NULL DEFAULT '',
            language TEXT NOT NULL DEFAULT 'en'
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_warns (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS custom_commands (
            chat_id INTEGER NOT NULL,
            cmd TEXT NOT NULL,
            response_text TEXT NOT NULL,
            PRIMARY KEY(chat_id, cmd)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_notes (
            chat_id INTEGER NOT NULL,
            note_name TEXT NOT NULL,
            note_text TEXT NOT NULL,
            PRIMARY KEY(chat_id, note_name)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_registry (
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            added_at INTEGER NOT NULL DEFAULT 0
        )
        """)

        conn.commit()


def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users(user_id, first_seen, last_seen, username, first_name)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_seen=excluded.last_seen,
                username=excluded.username,
                first_name=excluded.first_name
        """, (user_id, now, now, username or "", first_name or ""))
        conn.commit()


def is_blocked_user(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_blocked FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return bool(row[0]) if row else False


def set_blocked_user(user_id: int, blocked: bool):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users(user_id, first_seen, last_seen, username, first_name, is_blocked)
            VALUES(?, ?, ?, '', '', ?)
            ON CONFLICT(user_id) DO UPDATE SET is_blocked=excluded.is_blocked, last_seen=excluded.last_seen
        """, (user_id, now, now, 1 if blocked else 0))
        conn.commit()


def get_user_language(user_id: int) -> str:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT language FROM user_settings WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row[0] if row and row[0] in {"en", "bn"} else "en"


def set_user_language(user_id: int, language: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_settings(user_id, language)
            VALUES(?, ?)
            ON CONFLICT(user_id) DO UPDATE SET language=excluded.language
        """, (user_id, language))
        conn.commit()


def register_group(chat_id: int, title: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO group_registry(chat_id, title, added_at)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title
        """, (chat_id, title or "", int(time.time())))
        cur.execute("INSERT OR IGNORE INTO group_settings(chat_id, title) VALUES(?, ?)", (chat_id, title or ""))
        cur.execute("UPDATE group_settings SET title = ? WHERE chat_id = ?", (title or "", chat_id))
        conn.commit()


def list_registered_groups(limit: int = 100) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id, title FROM group_registry ORDER BY title ASC LIMIT ?", (limit,))
        return cur.fetchall()


def ensure_group_row(chat_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO group_settings(chat_id) VALUES(?)", (chat_id,))
        conn.commit()


def get_group_settings(chat_id: int) -> dict:
    ensure_group_row(chat_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT title, welcome_enabled, welcome_text, goodbye_enabled, goodbye_text, rules_text, banned_words,
                   flood_limit, flood_window_sec, warn_limit, warn_action, warn_mute_minutes,
                   link_lock, media_lock, forwarding_block, quote_block, telegram_link_block, total_link_block,
                   night_mode, night_mode_action, night_start, night_end,
                   admin_tag_founder, admin_tag_admins, approval_mode,
                   service_delete, command_delete, edit_checks, scheduled_delete,
                   block_cancellation, delete_all_messages, self_destruct, captcha_mode,
                   media_action, log_channel_id, force_sub_channel, language
            FROM group_settings WHERE chat_id = ?
        """, (chat_id,))
        row = cur.fetchone()
        return {
            "title": row[0],
            "welcome_enabled": bool(row[1]),
            "welcome_text": row[2],
            "goodbye_enabled": bool(row[3]),
            "goodbye_text": row[4],
            "rules_text": row[5],
            "banned_words": [w.strip().lower() for w in row[6].split(",") if w.strip()],
            "flood_limit": int(row[7]),
            "flood_window_sec": int(row[8]),
            "warn_limit": int(row[9]),
            "warn_action": row[10],
            "warn_mute_minutes": int(row[11]),
            "link_lock": bool(row[12]),
            "media_lock": bool(row[13]),
            "forwarding_block": bool(row[14]),
            "quote_block": bool(row[15]),
            "telegram_link_block": bool(row[16]),
            "total_link_block": bool(row[17]),
            "night_mode": bool(row[18]),
            "night_mode_action": row[19],
            "night_start": int(row[20]),
            "night_end": int(row[21]),
            "admin_tag_founder": bool(row[22]),
            "admin_tag_admins": bool(row[23]),
            "approval_mode": bool(row[24]),
            "service_delete": bool(row[25]),
            "command_delete": bool(row[26]),
            "edit_checks": bool(row[27]),
            "scheduled_delete": bool(row[28]),
            "block_cancellation": bool(row[29]),
            "delete_all_messages": bool(row[30]),
            "self_destruct": bool(row[31]),
            "captcha_mode": bool(row[32]),
            "media_action": row[33],
            "log_channel_id": int(row[34]),
            "force_sub_channel": row[35].strip(),
            "language": row[36] if row[36] in {"en", "bn"} else "en",
        }


def update_group_setting(chat_id: int, field: str, value):
    allowed = {
        "title", "welcome_enabled", "welcome_text", "goodbye_enabled", "goodbye_text",
        "rules_text", "banned_words", "flood_limit", "flood_window_sec", "warn_limit",
        "warn_action", "warn_mute_minutes", "link_lock", "media_lock", "forwarding_block",
        "quote_block", "telegram_link_block", "total_link_block", "night_mode",
        "night_mode_action", "night_start", "night_end", "admin_tag_founder",
        "admin_tag_admins", "approval_mode", "service_delete", "command_delete",
        "edit_checks", "scheduled_delete", "block_cancellation", "delete_all_messages",
        "self_destruct", "captcha_mode", "media_action", "log_channel_id",
        "force_sub_channel", "language"
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    ensure_group_row(chat_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE group_settings SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()


def get_warns(chat_id: int, user_id: int) -> int:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT warns FROM group_warns WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        row = cur.fetchone()
        return row[0] if row else 0


def set_warns(chat_id: int, user_id: int, warns: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO group_warns(chat_id, user_id, warns)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET warns=excluded.warns
        """, (chat_id, user_id, warns))
        conn.commit()


def get_warned_list(chat_id: int, limit: int = 20) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, warns
            FROM group_warns
            WHERE chat_id = ? AND warns > 0
            ORDER BY warns DESC, user_id ASC
            LIMIT ?
        """, (chat_id, limit))
        return cur.fetchall()


def save_custom_command(chat_id: int, cmd: str, response_text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO custom_commands(chat_id, cmd, response_text)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, cmd) DO UPDATE SET response_text=excluded.response_text
        """, (chat_id, cmd.lower(), response_text))
        conn.commit()


def get_custom_command(chat_id: int, cmd: str) -> Optional[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT response_text FROM custom_commands WHERE chat_id = ? AND cmd = ?", (chat_id, cmd.lower()))
        row = cur.fetchone()
        return row[0] if row else None


def save_note(chat_id: int, note_name: str, note_text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO group_notes(chat_id, note_name, note_text)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, note_name) DO UPDATE SET note_text=excluded.note_text
        """, (chat_id, note_name.lower(), note_text))
        conn.commit()


def get_note(chat_id: int, note_name: str) -> Optional[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT note_text FROM group_notes WHERE chat_id = ? AND note_name = ?", (chat_id, note_name.lower()))
        row = cur.fetchone()
        return row[0] if row else None


def del_note(chat_id: int, note_name: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM group_notes WHERE chat_id = ? AND note_name = ?", (chat_id, note_name.lower()))
        conn.commit()


def list_notes(chat_id: int) -> List[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT note_name FROM group_notes WHERE chat_id = ? ORDER BY note_name ASC", (chat_id,))
        return [r[0] for r in cur.fetchall()]

# =========================================================
# 9) Helpers
# =========================================================
def is_root_admin(user_id: int) -> bool:
    return user_id in CFG.admins


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_language(user_id)
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


def is_group_chat(message: Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}


async def is_group_admin(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
    except Exception:
        return False


async def is_group_owner(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status == ChatMemberStatus.OWNER
    except Exception:
        return False


def safe_bool_icon(v: bool) -> str:
    return "✅" if v else "❌"


def text_contains_link(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"(https?://|t\.me/|www\.)", text, re.I))


def text_contains_telegram_link(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"(t\.me/|telegram\.me/)", text, re.I))


def text_contains_banned_word(text: str, banned_words: List[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for word in banned_words:
        if re.search(rf"\b{re.escape(word)}\b", low):
            return word
    return None


def in_night_mode_window(start_hour: int, end_hour: int) -> bool:
    now_h = time.localtime().tm_hour
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= now_h < end_hour
    return now_h >= start_hour or now_h < end_hour


async def log_to_channel(client: Client, chat_id: int, text: str):
    try:
        settings = get_group_settings(chat_id)
        log_channel_id = settings["log_channel_id"]
        if log_channel_id:
            await client.send_message(log_channel_id, text)
    except Exception as e:
        logger.warning(f"log_to_channel failed: {e}")


async def check_forcesub_membership(client: Client, channel_ref: str, user_id: int) -> bool:
    if not channel_ref:
        return True
    try:
        member = await client.get_chat_member(channel_ref, user_id)
        return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    except Exception:
        return False


def sign_payload(payload: str) -> str:
    sig = hmac.new(CFG.secret_key.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]
    return f"{payload}|{sig}"


def verify_payload(data: str) -> Optional[str]:
    if "|" not in data:
        return None
    payload, sig = data.rsplit("|", 1)
    expected = hmac.new(CFG.secret_key.encode(), payload.encode(), hashlib.sha256).hexdigest()[:12]
    return payload if hmac.compare_digest(sig, expected) else None


def make_cb(uid: int, page: str, chat_id: int = 0, extra: str = "") -> str:
    payload = f"{uid}:{page}:{chat_id}:{extra}"
    return sign_payload(payload)


def parse_cb(data: str) -> Optional[dict]:
    payload = verify_payload(data)
    if not payload:
        return None
    parts = payload.split(":", 3)
    if len(parts) != 4:
        return None
    try:
        return {
            "uid": int(parts[0]),
            "page": parts[1],
            "chat_id": int(parts[2]),
            "extra": parts[3],
        }
    except Exception:
        return None


def build_panel_text(user_id: int, page: str, chat_id: int = 0) -> str:
    if page == "home":
        return (
            "🤖 **Moderation Bot Panel**\n\n"
            "Choose an option below."
        )

    if page == "support":
        return t(user_id, "support_text")

    if page == "info":
        return t(user_id, "info_text")

    if page == "language":
        return t(user_id, "language_text")

    if chat_id:
        settings = get_group_settings(chat_id)
        title = settings["title"] or f"Chat {chat_id}"

        if page == "group_home":
            return (
                f"⚙️ **{title}**\n\n"
                f"Manage group settings from the menus below."
            )

        if page == "regulation":
            return (
                f"🛡️ **Regulation**\n\n"
                f"Rules set: `{settings['rules_text'][:60] + ('...' if len(settings['rules_text']) > 60 else '')}`\n"
                f"Approval mode: `{settings['approval_mode']}`"
            )

        if page == "welcome":
            return (
                f"👋 **Welcome**\n\n"
                f"Enabled: `{settings['welcome_enabled']}`\n"
                f"Text:\n{settings['welcome_text']}"
            )

        if page == "goodbye":
            return (
                f"👋 **Goodbye**\n\n"
                f"Enabled: `{settings['goodbye_enabled']}`\n"
                f"Text:\n{settings['goodbye_text']}"
            )

        if page == "captcha":
            return (
                f"🧩 **Captcha**\n\n"
                f"Mode: `{settings['captcha_mode']}`\n"
                f"Basic placeholder mode available."
            )

        if page == "admin":
            return (
                f"👮 **Admin**\n\n"
                f"Tag Founder: `{settings['admin_tag_founder']}`\n"
                f"Tag Admins: `{settings['admin_tag_admins']}`"
            )

        if page == "media":
            return (
                f"🖼 **Media**\n\n"
                f"Media Lock: `{settings['media_lock']}`\n"
                f"Action: `{settings['media_action']}`"
            )

        if page == "warns":
            return (
                f"⚠️ **Warns**\n\n"
                f"Limit: `{settings['warn_limit']}`\n"
                f"Action: `{settings['warn_action']}`\n"
                f"Mute Duration: `{settings['warn_mute_minutes']}` minutes"
            )

        if page == "antispam":
            return (
                f"🚫 **Anti-Spam**\n\n"
                f"Telegram links block: `{settings['telegram_link_block']}`\n"
                f"Forwarding block: `{settings['forwarding_block']}`\n"
                f"Quote block: `{settings['quote_block']}`\n"
                f"Total links block: `{settings['total_link_block']}`"
            )

        if page == "antiflood":
            return (
                f"🌊 **Anti-Flood**\n\n"
                f"Limit: `{settings['flood_limit']}` messages\n"
                f"Window: `{settings['flood_window_sec']}` seconds"
            )

        if page == "blocks":
            return (
                f"⛔ **Blocks / Blacklist**\n\n"
                f"Banned words count: `{len(settings['banned_words'])}`"
            )

        if page == "night":
            return (
                f"🌙 **Night**\n\n"
                f"Enabled: `{settings['night_mode']}`\n"
                f"Action: `{settings['night_mode_action']}`\n"
                f"Hours: `{settings['night_start']}:00 - {settings['night_end']}:00`"
            )

        if page == "links":
            return (
                f"🔗 **Links**\n\n"
                f"Link Lock: `{settings['link_lock']}`\n"
                f"Telegram Links Block: `{settings['telegram_link_block']}`\n"
                f"Total Links Block: `{settings['total_link_block']}`"
            )

        if page == "deleting":
            return (
                f"🗑 **Deleting Messages**\n\n"
                f"Commands: `{settings['command_delete']}`\n"
                f"Service Messages: `{settings['service_delete']}`\n"
                f"Edit Checks: `{settings['edit_checks']}`\n"
                f"Scheduled deletion: `{settings['scheduled_delete']}`\n"
                f"Block cancellation: `{settings['block_cancellation']}`\n"
                f"Delete all messages: `{settings['delete_all_messages']}`\n"
                f"Self-destruction: `{settings['self_destruct']}`"
            )

        if page == "lang_group":
            return (
                f"🌐 **Group Language**\n\n"
                f"Current: `{settings['language']}`"
            )

        if page == "other":
            return (
                f"⚙️ **Other**\n\n"
                f"Force Sub: `{settings['force_sub_channel'] or 'off'}`\n"
                f"Log Channel: `{settings['log_channel_id']}`"
            )

    return "Panel"


def build_settings_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data=make_cb(user_id, "setlang", 0, "en")),
            InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data=make_cb(user_id, "setlang", 0, "bn")),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]
    ])


def build_main_panel(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add me to a Group", url="https://t.me/your_bot_username?startgroup=true")],
        [InlineKeyboardButton("⚙️ Manage group settings", callback_data=make_cb(user_id, "choose_group"))],
        [InlineKeyboardButton("👥 Group", callback_data=make_cb(user_id, "choose_group")),
         InlineKeyboardButton("📢 Channel", callback_data=make_cb(user_id, "info"))],
        [InlineKeyboardButton("🆘 Support", callback_data=make_cb(user_id, "support")),
         InlineKeyboardButton("ℹ️ Information", callback_data=make_cb(user_id, "info"))],
        [InlineKeyboardButton("🌐 Languages", callback_data=make_cb(user_id, "language"))]
    ])


def build_group_list_panel(user_id: int) -> InlineKeyboardMarkup:
    rows = []
    groups = list_registered_groups(50)
    for chat_id, title in groups[:20]:
        rows.append([InlineKeyboardButton(title[:50] or str(chat_id), callback_data=make_cb(user_id, "group_home", chat_id))])
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))])
    return InlineKeyboardMarkup(rows)


def build_group_home_panel(user_id: int, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛡 Regulation", callback_data=make_cb(user_id, "regulation", chat_id)),
         InlineKeyboardButton("👋 Welcome", callback_data=make_cb(user_id, "welcome", chat_id))],
        [InlineKeyboardButton("👋 Goodbye", callback_data=make_cb(user_id, "goodbye", chat_id)),
         InlineKeyboardButton("🧩 Captcha", callback_data=make_cb(user_id, "captcha", chat_id))],
        [InlineKeyboardButton("👮 Admin", callback_data=make_cb(user_id, "admin", chat_id)),
         InlineKeyboardButton("🖼 Media", callback_data=make_cb(user_id, "media", chat_id))],
        [InlineKeyboardButton("⚠️ Warns", callback_data=make_cb(user_id, "warns", chat_id)),
         InlineKeyboardButton("🚫 Anti-Spam", callback_data=make_cb(user_id, "antispam", chat_id))],
        [InlineKeyboardButton("🌊 Anti-Flood", callback_data=make_cb(user_id, "antiflood", chat_id)),
         InlineKeyboardButton("⛔ Blocks", callback_data=make_cb(user_id, "blocks", chat_id))],
        [InlineKeyboardButton("🌙 Night", callback_data=make_cb(user_id, "night", chat_id)),
         InlineKeyboardButton("🔗 Link", callback_data=make_cb(user_id, "links", chat_id))],
        [InlineKeyboardButton("🗑 Deleting", callback_data=make_cb(user_id, "deleting", chat_id)),
         InlineKeyboardButton("🌐 Lang", callback_data=make_cb(user_id, "lang_group", chat_id))],
        [InlineKeyboardButton("⚙️ Other", callback_data=make_cb(user_id, "other", chat_id))],
        [InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "choose_group"))]
    ])


def build_toggle_panel(user_id: int, page: str, chat_id: int, rows: List[List[Tuple[str, str]]], back_page: str = "group_home") -> InlineKeyboardMarkup:
    built = []
    for row in rows:
        b_row = []
        for label, extra in row:
            b_row.append(InlineKeyboardButton(label, callback_data=make_cb(user_id, page, chat_id, extra)))
        built.append(b_row)
    built.append([InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, back_page, chat_id))])
    return InlineKeyboardMarkup(built)


async def render_page(client: Client, cq: CallbackQuery, user_id: int, page: str, chat_id: int = 0):
    text = build_panel_text(user_id, page, chat_id)

    if page == "home":
        kb = build_main_panel(user_id)
    elif page == "choose_group":
        kb = build_group_list_panel(user_id)
        text = t(user_id, "group_choose")
    elif page in {"support", "info"}:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]])
    elif page == "language":
        kb = build_settings_panel(user_id)
    elif page == "group_home":
        kb = build_group_home_panel(user_id, chat_id)
    elif page == "regulation":
        kb = build_toggle_panel(user_id, "regulation_act", chat_id, [
            [("📝 Customize message", "customize_rules"), ("✅ Approval", "toggle_approval")],
            [("📜 View rules", "view_rules")]
        ])
    elif page == "welcome":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "welcome_act", chat_id, [
            [(f"{safe_bool_icon(settings['welcome_enabled'])} Toggle", "toggle_welcome")],
            [("📝 Set Text", "set_welcome_text"), ("👀 Preview", "preview_welcome")]
        ])
    elif page == "goodbye":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "goodbye_act", chat_id, [
            [(f"{safe_bool_icon(settings['goodbye_enabled'])} Toggle", "toggle_goodbye")],
            [("📝 Set Text", "set_goodbye_text"), ("👀 Preview", "preview_goodbye")]
        ])
    elif page == "captcha":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "captcha_act", chat_id, [
            [(f"{safe_bool_icon(settings['captcha_mode'])} Toggle", "toggle_captcha")]
        ])
    elif page == "admin":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "admin_act", chat_id, [
            [(f"{safe_bool_icon(settings['admin_tag_founder'])} Tag Founder", "toggle_founder")],
            [(f"{safe_bool_icon(settings['admin_tag_admins'])} Tag Admins", "toggle_admins")]
        ])
    elif page == "media":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "media_act", chat_id, [
            [(f"{safe_bool_icon(settings['media_lock'])} Media Lock", "toggle_media_lock")],
            [("🗑 Action: Delete", "set_action_delete"), ("🔇 Action: Mute", "set_action_mute")]
        ])
    elif page == "warns":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "warns_act", chat_id, [
            [("🚫 Off", "warn_off"), ("👢 Kick", "warn_kick"), ("🔇 Mute", "warn_mute"), ("⛔ Ban", "warn_ban")],
            [("📋 Warned List", "warned_list"), ("➕ Limit +1", "limit_plus"), ("➖ Limit -1", "limit_minus")],
            [("⏱ Mute +10m", "mute_plus"), ("⏱ Mute -10m", "mute_minus")]
        ])
    elif page == "antispam":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "antispam_act", chat_id, [
            [(f"{safe_bool_icon(settings['telegram_link_block'])} Telegram links", "toggle_tg_links")],
            [(f"{safe_bool_icon(settings['forwarding_block'])} Forwarding", "toggle_forwarding")],
            [(f"{safe_bool_icon(settings['quote_block'])} Quote", "toggle_quote")],
            [(f"{safe_bool_icon(settings['total_link_block'])} Total links block", "toggle_total_links")]
        ])
    elif page == "antiflood":
        kb = build_toggle_panel(user_id, "antiflood_act", chat_id, [
            [("➕ Limit", "limit_plus"), ("➖ Limit", "limit_minus")],
            [("➕ Window", "window_plus"), ("➖ Window", "window_minus")]
        ])
    elif page == "blocks":
        kb = build_toggle_panel(user_id, "blocks_act", chat_id, [
            [("➕ Add banned word", "add_bw"), ("➖ Remove banned word", "remove_bw")],
            [("📋 Show blacklist", "show_bw")]
        ])
    elif page == "night":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "night_act", chat_id, [
            [(f"{safe_bool_icon(settings['night_mode'])} Toggle", "toggle_night")],
            [("🗑 Delete medias", "night_delete"), ("🔇 Global Silence", "night_silence")],
            [("➕ Start", "start_plus"), ("➖ Start", "start_minus"), ("➕ End", "end_plus"), ("➖ End", "end_minus")]
        ])
    elif page == "links":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "links_act", chat_id, [
            [(f"{safe_bool_icon(settings['link_lock'])} Link lock", "toggle_link_lock")],
            [(f"{safe_bool_icon(settings['telegram_link_block'])} Telegram link block", "toggle_tg_links")],
            [(f"{safe_bool_icon(settings['total_link_block'])} Total link block", "toggle_total_links")]
        ])
    elif page == "deleting":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "deleting_act", chat_id, [
            [(f"{safe_bool_icon(settings['command_delete'])} Commands", "toggle_command_delete")],
            [(f"{safe_bool_icon(settings['service_delete'])} Service Messages", "toggle_service_delete")],
            [(f"{safe_bool_icon(settings['edit_checks'])} Edit Checks", "toggle_edit_checks")],
            [(f"{safe_bool_icon(settings['scheduled_delete'])} Scheduled Deletion", "toggle_scheduled_delete")],
            [(f"{safe_bool_icon(settings['block_cancellation'])} Block cancellation", "toggle_block_cancellation")],
            [(f"{safe_bool_icon(settings['delete_all_messages'])} Delete all messages", "toggle_delete_all")],
            [(f"{safe_bool_icon(settings['self_destruct'])} Self-destruction", "toggle_self_destruct")]
        ])
    elif page == "lang_group":
        settings = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "lang_group_act", chat_id, [
            [(f"{'✅ ' if settings['language']=='en' else ''}English", "set_en"),
             (f"{'✅ ' if settings['language']=='bn' else ''}বাংলা", "set_bn")]
        ])
    elif page == "other":
        kb = build_toggle_panel(user_id, "other_act", chat_id, [
            [("📢 Set ForceSub", "set_forcesub"), ("❌ Disable ForceSub", "disable_forcesub")],
            [("📝 Set Log Channel", "set_log"), ("📄 Notes", "show_notes")]
        ])
    else:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]])

    await cq.message.edit_text(text, reply_markup=kb)

# =========================================================
# 10) Private commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)

    if is_blocked_user(user.id):
        return await message.reply_text(t(user.id, "blocked"))
    if state["maintenance_mode"] and not is_root_admin(user.id):
        return await message.reply_text(t(user.id, "maintenance"))

    text = t(user.id, "welcome_private", name=user.first_name or "User")
    await message.reply_text(text, reply_markup=build_main_panel(user.id))


@bot.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    help_text = (
        "**Commands**\n\n"
        "/start - Open main panel\n"
        "/settings - User settings\n"
        "/lang en|bn - Change personal language\n"
        "/rules - Show group rules\n"
        "/warn /mute /ban - Reply moderation commands\n"
        "/save /getnote /notes - Notes\n"
        "/setcmd - Custom commands\n"
        "/forcesub - Force subscription\n"
        "/setrules /setwelcome /setgoodbye - Customize texts\n"
    )
    await message.reply_text(help_text)


@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client, message: Message):
    lang = get_user_language(message.from_user.id)
    await message.reply_text(
        t(message.from_user.id, "settings", lang=lang),
        reply_markup=build_settings_panel(message.from_user.id)
    )


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /lang en অথবা /lang bn")
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only en or bn")
    set_user_language(message.from_user.id, lang)
    await message.reply_text(t(message.from_user.id, "lang_set_bn" if lang == "bn" else "lang_set_en"))

# =========================================================
# 11) Group events
# =========================================================
@bot.on_message(filters.new_chat_members)
async def welcome_new_members(client, message: Message):
    if not is_group_chat(message):
        return

    register_group(message.chat.id, message.chat.title or "")
    settings = get_group_settings(message.chat.id)

    for user in message.new_chat_members:
        if settings["welcome_enabled"]:
            mention = user.mention
            text = settings["welcome_text"].replace("{mention}", mention).replace("{name}", user.first_name or "User")
            try:
                await message.reply_text(text)
            except Exception:
                pass

        await log_to_channel(client, message.chat.id, f"👤 New member joined\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}")


@bot.on_message(filters.left_chat_member)
async def goodbye_left_member(client, message: Message):
    if not is_group_chat(message):
        return
    register_group(message.chat.id, message.chat.title or "")
    settings = get_group_settings(message.chat.id)

    user = message.left_chat_member
    if not user:
        return

    if settings["goodbye_enabled"]:
        text = settings["goodbye_text"].replace("{name}", user.first_name or "User")
        try:
            await message.reply_text(text)
        except Exception:
            pass

    await log_to_channel(client, message.chat.id, f"👋 Member left\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}")


@bot.on_message(filters.group)
async def registry_touch(client, message: Message):
    register_group(message.chat.id, message.chat.title or "")
    if message.from_user:
        upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

# =========================================================
# 12) Public group commands
# =========================================================
@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    await message.reply_text(settings["rules_text"])


@bot.on_message(filters.command("notes") & filters.group)
async def notes_cmd(client, message: Message):
    items = list_notes(message.chat.id)
    await message.reply_text("📝 Notes:\n" + ("\n".join(f"- `{x}`" for x in items) if items else "No notes."))


@bot.on_message(filters.command("getnote") & filters.group)
async def getnote_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /getnote note_name")
    note = get_note(message.chat.id, parts[1].strip())
    if not note:
        return await message.reply_text("Note not found.")
    await message.reply_text(note)

# =========================================================
# 13) Admin commands
# =========================================================
async def require_group_admin(client: Client, message: Message) -> bool:
    if not message.from_user:
        return False
    return await is_group_admin(client, message.chat.id, message.from_user.id)


async def extract_target_user(client: Client, message: Message):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user

    parts = message.text.split()
    if len(parts) < 2:
        return None

    raw = parts[1].strip().lstrip("@")
    try:
        if raw.isdigit():
            return await client.get_users(int(raw))
        return await client.get_users(raw)
    except Exception:
        return None


@bot.on_message(filters.command("panel") & filters.private)
async def panel_cmd(client, message: Message):
    await message.reply_text("🤖 **Moderation Bot Panel**", reply_markup=build_main_panel(message.from_user.id))


@bot.on_message(filters.command("setrules") & filters.group)
async def setrules_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setrules your rules here")
    update_group_setting(message.chat.id, "rules_text", parts[1].strip())
    await message.reply_text("✅ Rules updated.")


@bot.on_message(filters.command("setwelcome") & filters.group)
async def setwelcome_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setwelcome text")
    update_group_setting(message.chat.id, "welcome_text", parts[1].strip())
    await message.reply_text("✅ Welcome text updated.")


@bot.on_message(filters.command("setgoodbye") & filters.group)
async def setgoodbye_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setgoodbye text")
    update_group_setting(message.chat.id, "goodbye_text", parts[1].strip())
    await message.reply_text("✅ Goodbye text updated.")


@bot.on_message(filters.command("welcome") & filters.group)
async def toggle_welcome_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1] not in {"on", "off"}:
        return await message.reply_text("Usage: /welcome on|off")
    update_group_setting(message.chat.id, "welcome_enabled", 1 if parts[1] == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("goodbye") & filters.group)
async def toggle_goodbye_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1] not in {"on", "off"}:
        return await message.reply_text("Usage: /goodbye on|off")
    update_group_setting(message.chat.id, "goodbye_enabled", 1 if parts[1] == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("setlog") & filters.group)
async def setlog_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setlog -1001234567890")
    try:
        log_chat_id = int(parts[1].strip())
        update_group_setting(message.chat.id, "log_channel_id", log_chat_id)
        await message.reply_text("✅ Log channel saved.")
    except Exception:
        await message.reply_text("Invalid channel id.")


@bot.on_message(filters.command("forcesub") & filters.group)
async def forcesub_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /forcesub @channel অথবা /forcesub off")
    value = parts[1].strip()
    if value.lower() == "off":
        update_group_setting(message.chat.id, "force_sub_channel", "")
        return await message.reply_text("✅ ForceSub disabled.")
    update_group_setting(message.chat.id, "force_sub_channel", value)
    await message.reply_text(f"✅ ForceSub set to `{value}`")


@bot.on_message(filters.command("save") & filters.group)
async def save_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /save note_name note text")
    save_note(message.chat.id, parts[1].strip(), parts[2].strip())
    await message.reply_text("✅ Note saved.")


@bot.on_message(filters.command("delnote") & filters.group)
async def delnote_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /delnote note_name")
    del_note(message.chat.id, parts[1].strip())
    await message.reply_text("✅ Note deleted.")


@bot.on_message(filters.command("setcmd") & filters.group)
async def setcmd_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /setcmd hello Hello everyone!")
    save_custom_command(message.chat.id, parts[1].lstrip("/").lower(), parts[2])
    await message.reply_text("✅ Custom command saved.")


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")

    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give user id.")

    settings = get_group_settings(message.chat.id)
    warns = get_warns(message.chat.id, target.id) + 1
    set_warns(message.chat.id, target.id, warns)

    await apply_warn_action(client, message.chat.id, target.id, target.first_name or "User", settings, warns)
    await message.reply_text(f"⚠️ Warned: {target.mention} (`{warns}/{settings['warn_limit']}`)")


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")

    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give user id.")

    warns = max(get_warns(message.chat.id, target.id) - 1, 0)
    set_warns(message.chat.id, target.id, warns)
    await message.reply_text(f"✅ Current warns: `{warns}`")


@bot.on_message(filters.command("mute") & filters.group)
async def mute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to user.")
    try:
        await client.restrict_chat_member(message.chat.id, target.id, ChatPermissions(can_send_messages=False))
        await message.reply_text(f"🔇 Muted {target.mention}")
    except Exception as e:
        await message.reply_text(f"Mute failed: {e}")


@bot.on_message(filters.command("unmute") & filters.group)
async def unmute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to user.")
    try:
        await client.restrict_chat_member(
            message.chat.id,
            target.id,
            ChatPermissions(
                can_send_messages=True,
                can_send_polls=True,
                can_invite_users=True
            )
        )
        await message.reply_text(f"🔊 Unmuted {target.mention}")
    except Exception as e:
        await message.reply_text(f"Unmute failed: {e}")


@bot.on_message(filters.command("ban") & filters.group)
async def ban_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to user.")
    try:
        await client.ban_chat_member(message.chat.id, target.id)
        await message.reply_text(f"⛔ Banned {target.mention}")
    except Exception as e:
        await message.reply_text(f"Ban failed: {e}")


@bot.on_message(filters.command("unban") & filters.group)
async def unban_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /unban user_id")
    try:
        uid = int(parts[1])
        await client.unban_chat_member(message.chat.id, uid)
        await message.reply_text(f"✅ Unbanned `{uid}`")
    except Exception as e:
        await message.reply_text(f"Unban failed: {e}")


@bot.on_message(filters.command("banword") & filters.group)
async def banword_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /banword word")
    settings = get_group_settings(message.chat.id)
    words = set(settings["banned_words"])
    words.add(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Added.")


@bot.on_message(filters.command("unbanword") & filters.group)
async def unbanword_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /unbanword word")
    settings = get_group_settings(message.chat.id)
    words = set(settings["banned_words"])
    words.discard(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Removed.")


@bot.on_message(filters.command("banwords") & filters.group)
async def banwords_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    words = settings["banned_words"]
    await message.reply_text("🚫 Blacklist:\n" + (", ".join(words) if words else "Empty"))


@bot.on_message(filters.command("gsettings") & filters.group)
async def gsettings_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    txt = (
        f"⚙️ **Group Settings**\n\n"
        f"Welcome: `{settings['welcome_enabled']}`\n"
        f"Goodbye: `{settings['goodbye_enabled']}`\n"
        f"Warn Limit: `{settings['warn_limit']}`\n"
        f"Warn Action: `{settings['warn_action']}`\n"
        f"Flood: `{settings['flood_limit']}` / `{settings['flood_window_sec']}` sec\n"
        f"Link Lock: `{settings['link_lock']}`\n"
        f"Media Lock: `{settings['media_lock']}`\n"
        f"Night: `{settings['night_mode']}`\n"
        f"ForceSub: `{settings['force_sub_channel'] or 'off'}`\n"
        f"Log: `{settings['log_channel_id']}`"
    )
    await message.reply_text(txt)

# =========================================================
# 14) Custom command handler
# =========================================================
BUILTIN_COMMANDS = {
    "start", "help", "settings", "lang", "panel", "rules", "notes", "getnote",
    "setrules", "setwelcome", "setgoodbye", "welcome", "goodbye", "setlog",
    "forcesub", "save", "delnote", "setcmd", "warn", "unwarn", "mute",
    "unmute", "ban", "unban", "banword", "unbanword", "banwords", "gsettings"
}

@bot.on_message(filters.group & filters.text, group=20)
async def custom_command_handler(client, message: Message):
    if not message.text or not message.text.startswith("/"):
        return
    cmd = message.text.split()[0].lstrip("/").split("@")[0].lower()
    if cmd in BUILTIN_COMMANDS:
        return
    response = get_custom_command(message.chat.id, cmd)
    if response:
        await message.reply_text(response)

# =========================================================
# 15) Auto moderation
# =========================================================
async def apply_warn_action(client: Client, chat_id: int, user_id: int, name: str, settings: dict, warns: int):
    if warns < settings["warn_limit"]:
        return

    try:
        action = settings["warn_action"]
        if action == "off":
            return

        if action == "kick":
            await client.ban_chat_member(chat_id, user_id)
            await client.unban_chat_member(chat_id, user_id)

        elif action == "mute":
            await client.restrict_chat_member(chat_id, user_id, ChatPermissions(can_send_messages=False))

        elif action == "ban":
            await client.ban_chat_member(chat_id, user_id)

        set_warns(chat_id, user_id, 0)
        await log_to_channel(client, chat_id, f"⚠️ Warn action applied\nUser: {user_id}\nAction: {action}")
    except Exception as e:
        logger.warning(f"warn action failed: {e}")


@bot.on_message(filters.group & ~filters.service, group=10)
async def group_protection_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    register_group(message.chat.id, message.chat.title or "")
    settings = get_group_settings(message.chat.id)

    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    # force sub
    if settings["force_sub_channel"]:
        ok = await check_forcesub_membership(client, settings["force_sub_channel"], message.from_user.id)
        if not ok:
            try:
                await message.delete()
            except Exception:
                pass

            btn = None
            join_target = settings["force_sub_channel"]
            if join_target.startswith("@"):
                btn = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Join Required Channel", url=f"https://t.me/{join_target.lstrip('@')}")]
                ])
            try:
                await message.reply_text(
                    f"🛑 {message.from_user.mention}, আগে `{join_target}` channel-এ join করতে হবে।",
                    reply_markup=btn
                )
            except Exception:
                pass
            return

    text = message.text or message.caption or ""

    # quote / forward checks
    if settings["forwarding_block"] and (message.forward_date or message.forward_from or message.forward_sender_name):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if settings["quote_block"] and message.reply_to_message:
        try:
            await message.delete()
        except Exception:
            pass
        return

    # link controls
    if settings["telegram_link_block"] and text_contains_telegram_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if settings["total_link_block"] and text_contains_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if settings["link_lock"] and text_contains_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    # media lock
    if settings["media_lock"] and (message.media is not None):
        try:
            await message.delete()
            if settings["media_action"] == "mute":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
        except Exception:
            pass
        return

    # night mode
    if settings["night_mode"] and in_night_mode_window(settings["night_start"], settings["night_end"]):
        try:
            await message.delete()
            if settings["night_mode_action"] == "silence":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
        except Exception:
            pass
        return

    # banned words
    hit = text_contains_banned_word(text, settings["banned_words"])
    if hit:
        try:
            await message.delete()
        except Exception:
            pass

        warns = get_warns(message.chat.id, message.from_user.id) + 1
        set_warns(message.chat.id, message.from_user.id, warns)
        await apply_warn_action(client, message.chat.id, message.from_user.id, message.from_user.first_name or "User", settings, warns)
        await log_to_channel(client, message.chat.id, f"🚫 Banned word hit: {hit}\nUser: {message.from_user.id}\nWarns: {warns}")
        return

    # anti flood
    key = (message.chat.id, message.from_user.id)
    now = time.time()
    arr = flood_tracker.get(key, [])
    arr = [x for x in arr if now - x <= settings["flood_window_sec"]]
    arr.append(now)
    flood_tracker[key] = arr

    if len(arr) >= settings["flood_limit"]:
        try:
            await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
            await log_to_channel(client, message.chat.id, f"🌊 Flood mute\nUser: {message.from_user.id}\nCount: {len(arr)}")
        except Exception as e:
            logger.warning(f"flood action failed: {e}")

# =========================================================
# 16) Callback panel logic
# =========================================================
def store_panel_action(user_id: int, message_id: int, action: str, chat_id: int):
    panel_sessions[(user_id, message_id)] = {
        "action": action,
        "chat_id": chat_id,
        "created_at": time.time(),
    }


def get_panel_action(user_id: int, message_id: int) -> Optional[dict]:
    sess = panel_sessions.get((user_id, message_id))
    if not sess:
        return None
    if time.time() - sess["created_at"] > 600:
        panel_sessions.pop((user_id, message_id), None)
        return None
    return sess


@bot.on_callback_query()
async def callback_handler(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    parsed = parse_cb(cq.data)
    if not parsed:
        return await cq.answer("Invalid panel.", show_alert=True)

    if parsed["uid"] != user_id:
        return await cq.answer("This panel is not for you.", show_alert=True)

    page = parsed["page"]
    chat_id = parsed["chat_id"]
    extra = parsed["extra"]

    try:
        if page == "setlang":
            if extra not in {"en", "bn"}:
                return await cq.answer("Invalid language", show_alert=True)
            set_user_language(user_id, extra)
            return await render_page(client, cq, user_id, "language")

        if page in {"home", "choose_group", "support", "info", "language"}:
            return await render_page(client, cq, user_id, page)

        if chat_id:
            if not await is_group_admin(client, chat_id, user_id):
                return await cq.answer("You are not admin in this group.", show_alert=True)

        if page in {
            "group_home", "regulation", "welcome", "goodbye", "captcha",
            "admin", "media", "warns", "antispam", "antiflood",
            "blocks", "night", "links", "deleting", "lang_group", "other"
        }:
            return await render_page(client, cq, user_id, page, chat_id)

        # Action pages
        if page == "regulation_act":
            if extra == "view_rules":
                rules = get_group_settings(chat_id)["rules_text"]
                await cq.answer("Rules shown below.")
                return await cq.message.reply_text(rules)
            if extra == "customize_rules":
                store_panel_action(user_id, cq.message.id, "setrules", chat_id)
                await cq.answer("Send new rules in next message.", show_alert=True)
                return
            if extra == "toggle_approval":
                s = get_group_settings(chat_id)
                update_group_setting(chat_id, "approval_mode", 0 if s["approval_mode"] else 1)
                return await render_page(client, cq, user_id, "regulation", chat_id)

        if page == "welcome_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_welcome":
                update_group_setting(chat_id, "welcome_enabled", 0 if s["welcome_enabled"] else 1)
                return await render_page(client, cq, user_id, "welcome", chat_id)
            if extra == "set_welcome_text":
                store_panel_action(user_id, cq.message.id, "setwelcome", chat_id)
                return await cq.answer("Send new welcome text.", show_alert=True)
            if extra == "preview_welcome":
                txt = s["welcome_text"].replace("{mention}", cq.from_user.mention).replace("{name}", cq.from_user.first_name or "User")
                await cq.message.reply_text(txt)
                return await cq.answer("Preview sent.")

        if page == "goodbye_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_goodbye":
                update_group_setting(chat_id, "goodbye_enabled", 0 if s["goodbye_enabled"] else 1)
                return await render_page(client, cq, user_id, "goodbye", chat_id)
            if extra == "set_goodbye_text":
                store_panel_action(user_id, cq.message.id, "setgoodbye", chat_id)
                return await cq.answer("Send new goodbye text.", show_alert=True)
            if extra == "preview_goodbye":
                txt = s["goodbye_text"].replace("{name}", cq.from_user.first_name or "User")
                await cq.message.reply_text(txt)
                return await cq.answer("Preview sent.")

        if page == "captcha_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_captcha":
                update_group_setting(chat_id, "captcha_mode", 0 if s["captcha_mode"] else 1)
                return await render_page(client, cq, user_id, "captcha", chat_id)

        if page == "admin_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_founder":
                update_group_setting(chat_id, "admin_tag_founder", 0 if s["admin_tag_founder"] else 1)
            elif extra == "toggle_admins":
                update_group_setting(chat_id, "admin_tag_admins", 0 if s["admin_tag_admins"] else 1)
            return await render_page(client, cq, user_id, "admin", chat_id)

        if page == "media_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_media_lock":
                update_group_setting(chat_id, "media_lock", 0 if s["media_lock"] else 1)
            elif extra == "set_action_delete":
                update_group_setting(chat_id, "media_action", "delete")
            elif extra == "set_action_mute":
                update_group_setting(chat_id, "media_action", "mute")
            return await render_page(client, cq, user_id, "media", chat_id)

        if page == "warns_act":
            s = get_group_settings(chat_id)
            if extra == "warn_off":
                update_group_setting(chat_id, "warn_action", "off")
            elif extra == "warn_kick":
                update_group_setting(chat_id, "warn_action", "kick")
            elif extra == "warn_mute":
                update_group_setting(chat_id, "warn_action", "mute")
            elif extra == "warn_ban":
                update_group_setting(chat_id, "warn_action", "ban")
            elif extra == "limit_plus":
                update_group_setting(chat_id, "warn_limit", min(20, s["warn_limit"] + 1))
            elif extra == "limit_minus":
                update_group_setting(chat_id, "warn_limit", max(1, s["warn_limit"] - 1))
            elif extra == "mute_plus":
                update_group_setting(chat_id, "warn_mute_minutes", min(1440, s["warn_mute_minutes"] + 10))
            elif extra == "mute_minus":
                update_group_setting(chat_id, "warn_mute_minutes", max(10, s["warn_mute_minutes"] - 10))
            elif extra == "warned_list":
                rows = get_warned_list(chat_id)
                txt = "📋 Warned List\n\n" + ("\n".join([f"`{uid}` → {w}" for uid, w in rows]) if rows else "Empty.")
                await cq.message.reply_text(txt)
                await cq.answer("Warned list sent.")
                return
            return await render_page(client, cq, user_id, "warns", chat_id)

        if page == "antispam_act":
            s = get_group_settings(chat_id)
            mapping = {
                "toggle_tg_links": "telegram_link_block",
                "toggle_forwarding": "forwarding_block",
                "toggle_quote": "quote_block",
                "toggle_total_links": "total_link_block",
            }
            if extra in mapping:
                field = mapping[extra]
                update_group_setting(chat_id, field, 0 if s[field] else 1)
            return await render_page(client, cq, user_id, "antispam", chat_id)

        if page == "antiflood_act":
            s = get_group_settings(chat_id)
            if extra == "limit_plus":
                update_group_setting(chat_id, "flood_limit", min(50, s["flood_limit"] + 1))
            elif extra == "limit_minus":
                update_group_setting(chat_id, "flood_limit", max(2, s["flood_limit"] - 1))
            elif extra == "window_plus":
                update_group_setting(chat_id, "flood_window_sec", min(300, s["flood_window_sec"] + 2))
            elif extra == "window_minus":
                update_group_setting(chat_id, "flood_window_sec", max(2, s["flood_window_sec"] - 2))
            return await render_page(client, cq, user_id, "antiflood", chat_id)

        if page == "blocks_act":
            if extra == "show_bw":
                s = get_group_settings(chat_id)
                txt = "⛔ Blacklist\n\n" + (", ".join(s["banned_words"]) if s["banned_words"] else "Empty.")
                await cq.message.reply_text(txt)
                return await cq.answer("Blacklist sent.")
            if extra == "add_bw":
                store_panel_action(user_id, cq.message.id, "add_bw", chat_id)
                return await cq.answer("Send a word to add.", show_alert=True)
            if extra == "remove_bw":
                store_panel_action(user_id, cq.message.id, "remove_bw", chat_id)
                return await cq.answer("Send a word to remove.", show_alert=True)

        if page == "night_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_night":
                update_group_setting(chat_id, "night_mode", 0 if s["night_mode"] else 1)
            elif extra == "night_delete":
                update_group_setting(chat_id, "night_mode_action", "delete")
            elif extra == "night_silence":
                update_group_setting(chat_id, "night_mode_action", "silence")
            elif extra == "start_plus":
                update_group_setting(chat_id, "night_start", (s["night_start"] + 1) % 24)
            elif extra == "start_minus":
                update_group_setting(chat_id, "night_start", (s["night_start"] - 1) % 24)
            elif extra == "end_plus":
                update_group_setting(chat_id, "night_end", (s["night_end"] + 1) % 24)
            elif extra == "end_minus":
                update_group_setting(chat_id, "night_end", (s["night_end"] - 1) % 24)
            return await render_page(client, cq, user_id, "night", chat_id)

        if page == "links_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_link_lock":
                update_group_setting(chat_id, "link_lock", 0 if s["link_lock"] else 1)
            elif extra == "toggle_tg_links":
                update_group_setting(chat_id, "telegram_link_block", 0 if s["telegram_link_block"] else 1)
            elif extra == "toggle_total_links":
                update_group_setting(chat_id, "total_link_block", 0 if s["total_link_block"] else 1)
            return await render_page(client, cq, user_id, "links", chat_id)

        if page == "deleting_act":
            s = get_group_settings(chat_id)
            mapping = {
                "toggle_command_delete": "command_delete",
                "toggle_service_delete": "service_delete",
                "toggle_edit_checks": "edit_checks",
                "toggle_scheduled_delete": "scheduled_delete",
                "toggle_block_cancellation": "block_cancellation",
                "toggle_delete_all": "delete_all_messages",
                "toggle_self_destruct": "self_destruct",
            }
            if extra in mapping:
                field = mapping[extra]
                update_group_setting(chat_id, field, 0 if s[field] else 1)
            return await render_page(client, cq, user_id, "deleting", chat_id)

        if page == "lang_group_act":
            if extra == "set_en":
                update_group_setting(chat_id, "language", "en")
            elif extra == "set_bn":
                update_group_setting(chat_id, "language", "bn")
            return await render_page(client, cq, user_id, "lang_group", chat_id)

        if page == "other_act":
            s = get_group_settings(chat_id)
            if extra == "disable_forcesub":
                update_group_setting(chat_id, "force_sub_channel", "")
                return await render_page(client, cq, user_id, "other", chat_id)
            if extra == "set_forcesub":
                store_panel_action(user_id, cq.message.id, "set_forcesub", chat_id)
                return await cq.answer("Send @channel or -100 id.", show_alert=True)
            if extra == "set_log":
                store_panel_action(user_id, cq.message.id, "set_log", chat_id)
                return await cq.answer("Send log channel id.", show_alert=True)
            if extra == "show_notes":
                notes = list_notes(chat_id)
                txt = "📝 Notes\n\n" + ("\n".join(f"- `{n}`" for n in notes) if notes else "Empty.")
                await cq.message.reply_text(txt)
                return await cq.answer("Notes sent.")

        await cq.answer("Updated.")
    except Exception as e:
        logger.exception("callback error")
        state["failed_actions"] += 1
        await cq.answer(f"Error: {e}", show_alert=True)

# =========================================================
# 17) Pending panel text input
# =========================================================
@bot.on_message(filters.private & filters.text, group=30)
async def pending_panel_text_handler(client, message: Message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    if message.text.startswith("/"):
        return

    # Find any recent session by this user
    target = None
    for (uid, msg_id), sess in list(panel_sessions.items()):
        if uid == user_id and time.time() - sess["created_at"] <= 600:
            target = (msg_id, sess)
    if not target:
        return

    msg_id, sess = target
    action = sess["action"]
    chat_id = sess["chat_id"]

    try:
        if action == "setrules":
            update_group_setting(chat_id, "rules_text", message.text.strip())
            await message.reply_text("✅ Rules updated.")
        elif action == "setwelcome":
            update_group_setting(chat_id, "welcome_text", message.text.strip())
            await message.reply_text("✅ Welcome text updated.")
        elif action == "setgoodbye":
            update_group_setting(chat_id, "goodbye_text", message.text.strip())
            await message.reply_text("✅ Goodbye text updated.")
        elif action == "add_bw":
            s = get_group_settings(chat_id)
            words = set(s["banned_words"])
            words.add(message.text.strip().lower())
            update_group_setting(chat_id, "banned_words", ",".join(sorted(words)))
            await message.reply_text("✅ Word added.")
        elif action == "remove_bw":
            s = get_group_settings(chat_id)
            words = set(s["banned_words"])
            words.discard(message.text.strip().lower())
            update_group_setting(chat_id, "banned_words", ",".join(sorted(words)))
            await message.reply_text("✅ Word removed.")
        elif action == "set_forcesub":
            update_group_setting(chat_id, "force_sub_channel", message.text.strip())
            await message.reply_text("✅ ForceSub updated.")
        elif action == "set_log":
            try:
                log_chat_id = int(message.text.strip())
                update_group_setting(chat_id, "log_channel_id", log_chat_id)
                await message.reply_text("✅ Log channel updated.")
            except Exception:
                await message.reply_text("❌ Invalid log channel id.")
        else:
            return
        panel_sessions.pop((user_id, msg_id), None)
    except Exception as e:
        await message.reply_text(f"❌ Failed: {e}")

# =========================================================
# 18) Hard security
# =========================================================
# Important:
# - This bot never promotes admins.
# - No can_promote_members logic exists.
# - All callback actions are signed and scoped to a specific user.
# - Group actions always re-check live admin status.
# - Root admin commands are separate and optional.

@bot.on_message(filters.command("blockuser") & filters.private)
async def blockuser_cmd(client, message: Message):
    if not is_root_admin(message.from_user.id):
        return await message.reply_text("Root admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /blockuser user_id")
    set_blocked_user(int(parts[1]), True)
    await message.reply_text("✅ User blocked.")


@bot.on_message(filters.command("unblockuser") & filters.private)
async def unblockuser_cmd(client, message: Message):
    if not is_root_admin(message.from_user.id):
        return await message.reply_text("Root admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /unblockuser user_id")
    set_blocked_user(int(parts[1]), False)
    await message.reply_text("✅ User unblocked.")

# =========================================================
# 19) Startup
# =========================================================
async def startup_report():
    if not CFG.owner_id:
        return
    try:
        await bot.send_message(CFG.owner_id, "✅ **Bot Started Successfully**")
    except Exception:
        pass


async def main_runner():
    init_db()
    threading.Thread(target=run_web_server, daemon=True).start()

    await bot.start()
    logger.info("Bot started successfully")

    asyncio.create_task(startup_report())
    await idle()


if __name__ == "__main__":
    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        pass