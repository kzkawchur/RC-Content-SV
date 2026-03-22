import os
import re
import sys
import time
import sqlite3
import logging
import asyncio
import threading
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
    Message,
    ChatJoinRequest
)
from pyrogram.enums import ChatMemberStatus, ChatType


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
            if len(recent_logs) > 150:
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
    app_name: str
    support_url: str
    updates_url: str


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
        app_name=os.environ.get("APP_NAME", "Group Guard"),
        support_url=os.environ.get("SUPPORT_URL", "https://t.me/"),
        updates_url=os.environ.get("UPDATES_URL", "https://t.me/"),
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
    "total_actions": 0,
    "success_actions": 0,
    "failed_actions": 0,
}

runtime = {
    "active_chat_id": None,
    "active_user_id": None,
}


@app.route("/")
def home():
    return f"✅ {CFG.app_name} is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "maintenance": state["maintenance_mode"],
        "total_actions": state["total_actions"],
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
# 6) Runtime caches
# =========================================================
flood_tracker: Dict[Tuple[int, int], List[float]] = {}
message_signature_cache: Dict[Tuple[int, int], List[Tuple[float, str]]] = {}
approval_pending_cache: Dict[Tuple[int, int], float] = {}


# =========================================================
# 7) Texts
# =========================================================
TEXTS = {
    "en": {
        "welcome_private": (
            "⚡ **Welcome, {name}!**\n\n"
            "I can manage and protect Telegram groups.\n"
            "Use the buttons below or send `/help`."
        ),
        "maintenance": "🛠️ Bot is under maintenance. Please try again later.",
        "blocked": "🚫 You are blocked from using this bot.",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 Admin only.",
        "help": (
            "**Private Commands**\n"
            "/start\n"
            "/help\n"
            "/settings\n"
            "/lang en|bn\n"
            "/panel\n"
            "/admin\n\n"
            "**Group Member Commands**\n"
            "/rules\n"
            "/notes\n"
            "/getnote <name>\n"
            "/gsettings\n\n"
            "**Group Admin Commands**\n"
            "/setrules <text>\n"
            "/setwelcome <text>\n"
            "/setgoodbye <text>\n"
            "/welcome on|off\n"
            "/goodbye on|off\n"
            "/warn (reply)\n"
            "/unwarn (reply)\n"
            "/mute (reply)\n"
            "/unmute (reply)\n"
            "/ban (reply)\n"
            "/unban <user_id>\n"
            "/banword <word>\n"
            "/unbanword <word>\n"
            "/banwords\n"
            "/locklink on|off\n"
            "/lockmedia on|off\n"
            "/nightmode on <start> <end>\n"
            "/nightmode off\n"
            "/setwarnlimit <n>\n"
            "/setflood <count> <sec>\n"
            "/setspam <repeat> <sec>\n"
            "/approval on|off\n"
            "/forcesub <channel>\n"
            "/forcesub off\n"
            "/setlog <chat_id>\n"
            "/save <name> <text>\n"
            "/delnote <name>\n"
            "/setcmd <cmd> <text>\n"
        ),
    },
    "bn": {
        "welcome_private": (
            "⚡ **স্বাগতম, {name}!**\n\n"
            "আমি Telegram group manage ও protect করতে পারি।\n"
            "নিচের button ব্যবহার করো বা `/help` দাও।"
        ),
        "maintenance": "🛠️ বট maintenance-এ আছে। পরে আবার চেষ্টা করো।",
        "blocked": "🚫 তুমি এই বট ব্যবহার করতে পারবে না।",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 শুধু admin ব্যবহার করতে পারবে।",
        "help": (
            "**Private Commands**\n"
            "/start\n"
            "/help\n"
            "/settings\n"
            "/lang en|bn\n"
            "/panel\n"
            "/admin\n\n"
            "**Group Member Commands**\n"
            "/rules\n"
            "/notes\n"
            "/getnote <name>\n"
            "/gsettings\n\n"
            "**Group Admin Commands**\n"
            "/setrules <text>\n"
            "/setwelcome <text>\n"
            "/setgoodbye <text>\n"
            "/welcome on|off\n"
            "/goodbye on|off\n"
            "/warn (reply)\n"
            "/unwarn (reply)\n"
            "/mute (reply)\n"
            "/unmute (reply)\n"
            "/ban (reply)\n"
            "/unban <user_id>\n"
            "/banword <word>\n"
            "/unbanword <word>\n"
            "/banwords\n"
            "/locklink on|off\n"
            "/lockmedia on|off\n"
            "/nightmode on <start> <end>\n"
            "/nightmode off\n"
            "/setwarnlimit <n>\n"
            "/setflood <count> <sec>\n"
            "/setspam <repeat> <sec>\n"
            "/approval on|off\n"
            "/forcesub <channel>\n"
            "/forcesub off\n"
            "/setlog <chat_id>\n"
            "/save <name> <text>\n"
            "/delnote <name>\n"
            "/setcmd <cmd> <text>\n"
        ),
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
            first_name TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            banned_at INTEGER NOT NULL
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
            welcome_enabled INTEGER NOT NULL DEFAULT 1,
            welcome_text TEXT NOT NULL DEFAULT '👋 Welcome, {mention}!',
            goodbye_enabled INTEGER NOT NULL DEFAULT 0,
            goodbye_text TEXT NOT NULL DEFAULT '👋 Goodbye, {name}.',
            rules_text TEXT NOT NULL DEFAULT '📜 No rules set yet.',
            banned_words TEXT NOT NULL DEFAULT '',
            flood_limit INTEGER NOT NULL DEFAULT 6,
            flood_window_sec INTEGER NOT NULL DEFAULT 10,
            spam_repeat_limit INTEGER NOT NULL DEFAULT 4,
            spam_window_sec INTEGER NOT NULL DEFAULT 20,
            warn_limit INTEGER NOT NULL DEFAULT 3,
            link_lock INTEGER NOT NULL DEFAULT 0,
            media_lock INTEGER NOT NULL DEFAULT 0,
            night_mode INTEGER NOT NULL DEFAULT 0,
            night_start INTEGER NOT NULL DEFAULT 0,
            night_end INTEGER NOT NULL DEFAULT 7,
            approval_mode INTEGER NOT NULL DEFAULT 0,
            auto_delete_service_msg INTEGER NOT NULL DEFAULT 0,
            log_channel_id INTEGER NOT NULL DEFAULT 0,
            force_sub_channel TEXT NOT NULL DEFAULT ''
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
        CREATE TABLE IF NOT EXISTS approvals (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            approved_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
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


def safe_total_users() -> int:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]


def latest_users(limit: int = 10) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, first_name, last_seen
            FROM users ORDER BY last_seen DESC LIMIT ?
        """, (limit,))
        return cur.fetchall()


def is_banned(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None


def ban_user(user_id: int, reason: str = ""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bans(user_id, reason, banned_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                reason=excluded.reason,
                banned_at=excluded.banned_at
        """, (user_id, reason, int(time.time())))
        conn.commit()


def unban_user(user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
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
            SELECT welcome_enabled, welcome_text, goodbye_enabled, goodbye_text,
                   rules_text, banned_words, flood_limit, flood_window_sec,
                   spam_repeat_limit, spam_window_sec, warn_limit, link_lock,
                   media_lock, night_mode, night_start, night_end,
                   approval_mode, auto_delete_service_msg, log_channel_id,
                   force_sub_channel
            FROM group_settings WHERE chat_id = ?
        """, (chat_id,))
        row = cur.fetchone()
        return {
            "welcome_enabled": bool(row[0]),
            "welcome_text": row[1],
            "goodbye_enabled": bool(row[2]),
            "goodbye_text": row[3],
            "rules_text": row[4],
            "banned_words": [w.strip().lower() for w in row[5].split(",") if w.strip()],
            "flood_limit": int(row[6]),
            "flood_window_sec": int(row[7]),
            "spam_repeat_limit": int(row[8]),
            "spam_window_sec": int(row[9]),
            "warn_limit": int(row[10]),
            "link_lock": bool(row[11]),
            "media_lock": bool(row[12]),
            "night_mode": bool(row[13]),
            "night_start": int(row[14]),
            "night_end": int(row[15]),
            "approval_mode": bool(row[16]),
            "auto_delete_service_msg": bool(row[17]),
            "log_channel_id": int(row[18]),
            "force_sub_channel": row[19].strip(),
        }


def update_group_setting(chat_id: int, field: str, value):
    allowed = {
        "welcome_enabled", "welcome_text", "goodbye_enabled", "goodbye_text",
        "rules_text", "banned_words", "flood_limit", "flood_window_sec",
        "spam_repeat_limit", "spam_window_sec", "warn_limit", "link_lock",
        "media_lock", "night_mode", "night_start", "night_end",
        "approval_mode", "auto_delete_service_msg", "log_channel_id",
        "force_sub_channel"
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


def approve_user(chat_id: int, user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO approvals(chat_id, user_id, approved_at)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET approved_at=excluded.approved_at
        """, (chat_id, user_id, int(time.time())))
        conn.commit()


def unapprove_user(chat_id: int, user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM approvals WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        conn.commit()


def is_approved(chat_id: int, user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM approvals WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        return cur.fetchone() is not None


# =========================================================
# 9) Helpers
# =========================================================
def is_bot_admin_global(user_id: int) -> bool:
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


async def log_to_channel(client: Client, chat_id: int, text: str):
    try:
        settings = get_group_settings(chat_id)
        log_channel_id = settings["log_channel_id"]
        if log_channel_id:
            await client.send_message(log_channel_id, text)
    except Exception as e:
        logger.warning(f"log_to_channel failed: {e}")


def text_contains_link(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"(https?://|t\.me/|www\.)", text, re.I))


def text_contains_banned_word(text: str, banned_words: List[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for word in banned_words:
        if re.search(rf"\b{re.escape(word)}\b", low):
            return word
    return None


def message_signature(message: Message) -> str:
    text = (message.text or message.caption or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    if not text and message.media:
        text = f"__MEDIA__:{str(message.media)}"
    return text[:350]


def in_night_mode_window(start_hour: int, end_hour: int) -> bool:
    now_h = time.localtime().tm_hour
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= now_h < end_hour
    return now_h >= start_hour or now_h < end_hour


async def extract_target_user(client: Client, message: Message):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user

    parts = (message.text or "").split()
    if len(parts) < 2:
        return None

    raw = parts[1].strip().lstrip("@")
    try:
        if raw.isdigit():
            return await client.get_users(int(raw))
        return await client.get_users(raw)
    except Exception:
        return None


async def check_forcesub_membership(client: Client, channel_ref: str, user_id: int) -> bool:
    if not channel_ref:
        return True
    try:
        member = await client.get_chat_member(channel_ref, user_id)
        return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    except Exception:
        return False


def build_private_main_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add me to a Group", url="https://t.me/share/url?url=https://t.me/")],
        [InlineKeyboardButton("⚙️ Manage group settings", callback_data="ui_manage_help")],
        [InlineKeyboardButton("👥 Group", callback_data="ui_group_info"),
         InlineKeyboardButton("📢 Channel", url=CFG.updates_url)],
        [InlineKeyboardButton("🆘 Support", url=CFG.support_url),
         InlineKeyboardButton("ℹ️ Information", callback_data="ui_information")],
        [InlineKeyboardButton("🌐 Languages", callback_data="ui_languages")]
    ])


def build_settings_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data="setlang_en"),
         InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data="setlang_bn")],
        [InlineKeyboardButton("⬅️ Back", callback_data="ui_home")]
    ])


def build_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
         InlineKeyboardButton("👥 Users", callback_data="admin_users")],
        [InlineKeyboardButton("🛠 Maintenance", callback_data="admin_maint"),
         InlineKeyboardButton("📜 Logs", callback_data="admin_logs")],
        [InlineKeyboardButton("⬅️ Back", callback_data="ui_home")]
    ])


def build_group_panel(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Regulation", callback_data=f"gp:{chat_id}:regulation"),
         InlineKeyboardButton("Welcome", callback_data=f"gp:{chat_id}:welcome")],
        [InlineKeyboardButton("Goodbye", callback_data=f"gp:{chat_id}:goodbye"),
         InlineKeyboardButton("Admin", callback_data=f"gp:{chat_id}:admin")],
        [InlineKeyboardButton("Media", callback_data=f"gp:{chat_id}:media"),
         InlineKeyboardButton("Warns", callback_data=f"gp:{chat_id}:warns")],
        [InlineKeyboardButton("Anti-Spam", callback_data=f"gp:{chat_id}:spam"),
         InlineKeyboardButton("Anti-Flood", callback_data=f"gp:{chat_id}:flood")],
        [InlineKeyboardButton("Night", callback_data=f"gp:{chat_id}:night"),
         InlineKeyboardButton("Link", callback_data=f"gp:{chat_id}:link")],
        [InlineKeyboardButton("Approval mode", callback_data=f"gp:{chat_id}:approval"),
         InlineKeyboardButton("Force Sub", callback_data=f"gp:{chat_id}:forcesub")],
        [InlineKeyboardButton("Logs", callback_data=f"gp:{chat_id}:logs"),
         InlineKeyboardButton("Info", callback_data=f"gp:{chat_id}:info")],
    ])


def yes_no_row(chat_id: int, key_name: str) -> List[List[InlineKeyboardButton]]:
    return [[
        InlineKeyboardButton("ON", callback_data=f"set:{chat_id}:{key_name}:1"),
        InlineKeyboardButton("OFF", callback_data=f"set:{chat_id}:{key_name}:0")
    ]]


# =========================================================
# 10) Private commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)

    if is_banned(user.id):
        return await message.reply_text(t(user.id, "blocked"))
    if state["maintenance_mode"] and not is_bot_admin_global(user.id):
        return await message.reply_text(t(user.id, "maintenance"))

    await message.reply_text(
        t(user.id, "welcome_private", name=user.first_name or "User"),
        reply_markup=build_private_main_panel()
    )


@bot.on_message(filters.command("help") & (filters.private | filters.group))
async def help_cmd(client, message: Message):
    uid = message.from_user.id if message.from_user else 0
    await message.reply_text(t(uid, "help"))


@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client, message: Message):
    user_id = message.from_user.id
    lang = get_user_language(user_id)
    await message.reply_text(
        f"**Settings**\n\n🌐 Language: `{lang}`",
        reply_markup=build_settings_panel(user_id)
    )


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message: Message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/lang en` or `/lang bn`")
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only `en` or `bn`.")
    set_user_language(message.from_user.id, lang)
    await message.reply_text(t(message.from_user.id, f"lang_set_{lang}"))


@bot.on_message(filters.command("panel") & filters.private)
async def panel_cmd(client, message: Message):
    await message.reply_text(
        "⚙️ **Main Panel**",
        reply_markup=build_private_main_panel()
    )


@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message: Message):
    if not is_bot_admin_global(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))
    await message.reply_text("🧩 **Admin Panel**", reply_markup=build_admin_panel())


# =========================================================
# 11) Callback handler
# =========================================================
@bot.on_callback_query()
async def callback_handler(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data or ""

    if data == "ui_home":
        await callback_query.message.edit_text(
            "⚙️ **Main Panel**",
            reply_markup=build_private_main_panel()
        )
        return await callback_query.answer()

    if data == "ui_languages":
        lang = get_user_language(user_id)
        await callback_query.message.edit_text(
            f"**Settings**\n\n🌐 Language: `{lang}`",
            reply_markup=build_settings_panel(user_id)
        )
        return await callback_query.answer()

    if data == "ui_information":
        txt = (
            f"**{CFG.app_name}**\n\n"
            f"Protection bot for groups.\n"
            f"Use /help for commands.\n"
            f"Use /panel for buttons."
        )
        await callback_query.message.edit_text(txt, reply_markup=build_private_main_panel())
        return await callback_query.answer()

    if data == "ui_manage_help":
        txt = (
            "**How to manage a group**\n\n"
            "1. Add the bot to your group.\n"
            "2. Make it admin with delete/ban/restrict permissions.\n"
            "3. Use /panel in private and /gpanel <chat_id> if needed.\n"
            "4. Or use commands inside the group."
        )
        await callback_query.message.edit_text(txt, reply_markup=build_private_main_panel())
        return await callback_query.answer()

    if data == "ui_group_info":
        await callback_query.message.edit_text(
            "👥 Add me to a group, give me admin permissions, then configure rules, welcome, warnings, locks, and protections.",
            reply_markup=build_private_main_panel()
        )
        return await callback_query.answer()

    if data.startswith("setlang_"):
        lang = data.split("_", 1)[1]
        set_user_language(user_id, lang)
        await callback_query.message.edit_text(
            f"**Settings**\n\n🌐 Language: `{lang}`",
            reply_markup=build_settings_panel(user_id)
        )
        return await callback_query.answer("Language updated.")

    if data.startswith("gp:"):
        parts = data.split(":")
        if len(parts) != 3:
            return await callback_query.answer("Invalid", show_alert=True)

        chat_id = int(parts[1])
        section = parts[2]

        if not await is_group_admin(client, chat_id, user_id):
            return await callback_query.answer("Group admin only", show_alert=True)

        s = get_group_settings(chat_id)

        if section == "regulation":
            txt = (
                f"**Regulation**\n\n"
                f"Rules set: `{bool(s['rules_text'])}`\n"
                f"Link lock: `{s['link_lock']}`\n"
                f"Night mode: `{s['night_mode']}`\n"
                f"Approval: `{s['approval_mode']}`"
            )
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "welcome":
            txt = (
                f"**Welcome**\n\n"
                f"Enabled: `{s['welcome_enabled']}`\n"
                f"Text:\n{s['welcome_text']}"
            )
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "welcome_enabled") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "goodbye":
            txt = (
                f"**Goodbye**\n\n"
                f"Enabled: `{s['goodbye_enabled']}`\n"
                f"Text:\n{s['goodbye_text']}"
            )
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "goodbye_enabled") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "admin":
            txt = (
                f"**Admin / Security**\n\n"
                f"Warn limit: `{s['warn_limit']}`\n"
                f"Approval mode: `{s['approval_mode']}`\n"
                f"Force sub: `{s['force_sub_channel'] or 'off'}`\n\n"
                f"Promote-by-bot: `disabled`"
            )
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "media":
            txt = f"**Media**\n\nMedia lock: `{s['media_lock']}`"
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "media_lock") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "warns":
            txt = f"**Warns**\n\nWarn limit: `{s['warn_limit']}`"
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "spam":
            txt = (
                f"**Anti-Spam**\n\n"
                f"Repeat limit: `{s['spam_repeat_limit']}`\n"
                f"Window: `{s['spam_window_sec']}` sec"
            )
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "flood":
            txt = (
                f"**Anti-Flood**\n\n"
                f"Limit: `{s['flood_limit']}` messages\n"
                f"Window: `{s['flood_window_sec']}` sec"
            )
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "night":
            txt = (
                f"**Night Mode**\n\n"
                f"Enabled: `{s['night_mode']}`\n"
                f"From: `{s['night_start']}:00`\n"
                f"To: `{s['night_end']}:00`"
            )
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "night_mode") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "link":
            txt = f"**Links**\n\nLink lock: `{s['link_lock']}`"
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "link_lock") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "approval":
            txt = f"**Approval Mode**\n\nEnabled: `{s['approval_mode']}`"
            await callback_query.message.edit_text(
                txt,
                reply_markup=InlineKeyboardMarkup(yes_no_row(chat_id, "approval_mode") + [[InlineKeyboardButton("⬅️ Back", callback_data=f"gback:{chat_id}")]])
            )

        elif section == "forcesub":
            txt = f"**Force Sub**\n\nCurrent: `{s['force_sub_channel'] or 'off'}`"
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "logs":
            txt = f"**Logs**\n\nLog channel id: `{s['log_channel_id']}`"
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        elif section == "info":
            txt = (
                f"**Group Info**\n\n"
                f"Chat ID: `{chat_id}`\n"
                f"Welcome: `{s['welcome_enabled']}`\n"
                f"Goodbye: `{s['goodbye_enabled']}`\n"
                f"Link lock: `{s['link_lock']}`\n"
                f"Media lock: `{s['media_lock']}`\n"
                f"Approval: `{s['approval_mode']}`"
            )
            await callback_query.message.edit_text(txt, reply_markup=build_group_panel(chat_id))

        return await callback_query.answer()

    if data.startswith("gback:"):
        chat_id = int(data.split(":")[1])
        if not await is_group_admin(client, chat_id, user_id):
            return await callback_query.answer("Group admin only", show_alert=True)
        await callback_query.message.edit_text(
            f"⚙️ **Group Panel**\n\nChat ID: `{chat_id}`",
            reply_markup=build_group_panel(chat_id)
        )
        return await callback_query.answer()

    if data.startswith("set:"):
        parts = data.split(":")
        if len(parts) != 4:
            return await callback_query.answer("Invalid", show_alert=True)

        chat_id = int(parts[1])
        field = parts[2]
        value = int(parts[3])

        if not await is_group_admin(client, chat_id, user_id):
            return await callback_query.answer("Group admin only", show_alert=True)

        safe_fields = {"welcome_enabled", "goodbye_enabled", "media_lock", "link_lock", "night_mode", "approval_mode"}
        if field not in safe_fields:
            return await callback_query.answer("Blocked", show_alert=True)

        update_group_setting(chat_id, field, value)
        return await callback_query.answer("Updated.")

    if not is_bot_admin_global(user_id):
        return await callback_query.answer("Admin only", show_alert=True)

    if data == "admin_stats":
        txt = (
            f"📊 Users: {safe_total_users()}\n"
            f"✅ Success: {state['success_actions']}\n"
            f"❌ Failed: {state['failed_actions']}\n"
            f"🛠 Maintenance: {state['maintenance_mode']}"
        )
        await callback_query.message.edit_text(txt, reply_markup=build_admin_panel())

    elif data == "admin_users":
        rows = latest_users(10)
        if not rows:
            txt = "No users yet."
        else:
            txt = "👥 Latest users\n\n"
            for uid, username, first_name, last_seen in rows:
                txt += f"`{uid}` | @{username or '-'} | {first_name or '-'} | {last_seen}\n"
        await callback_query.message.edit_text(txt, reply_markup=build_admin_panel())

    elif data == "admin_maint":
        state["maintenance_mode"] = not state["maintenance_mode"]
        await callback_query.message.edit_text(
            f"🛠 Maintenance: `{state['maintenance_mode']}`",
            reply_markup=build_admin_panel()
        )

    elif data == "admin_logs":
        txt = "\n".join(recent_logs[-20:]) if recent_logs else "No logs."
        if len(txt) > 3900:
            txt = txt[-3900:]
        await callback_query.message.edit_text(f"📜 Recent Logs\n\n`{txt}`", reply_markup=build_admin_panel())

    await callback_query.answer()


# =========================================================
# 12) Group panel command
# =========================================================
@bot.on_message(filters.command("gpanel") & filters.private)
async def gpanel_cmd(client, message: Message):
    if not message.from_user:
        return
    parts = (message.text or "").split()
    if len(parts) < 2 or not re.fullmatch(r"-?\d+", parts[1]):
        return await message.reply_text("Usage: /gpanel -1001234567890")

    chat_id = int(parts[1])
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You are not an admin of that group.")

    await message.reply_text(
        f"⚙️ **Group Panel**\n\nChat ID: `{chat_id}`",
        reply_markup=build_group_panel(chat_id)
    )


# =========================================================
# 13) Welcome / goodbye
# =========================================================
@bot.on_message(filters.new_chat_members)
async def welcome_new_members(client, message: Message):
    if not is_group_chat(message):
        return

    settings = get_group_settings(message.chat.id)
    if not settings["welcome_enabled"]:
        return

    for user in message.new_chat_members:
        mention = user.mention
        text = settings["welcome_text"].replace("{mention}", mention).replace("{name}", user.first_name or "User")
        try:
            await message.reply_text(text)
            state["success_actions"] += 1
        except Exception:
            state["failed_actions"] += 1

        await log_to_channel(
            client,
            message.chat.id,
            f"👤 New member joined\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}"
        )


@bot.on_message(filters.left_chat_member)
async def goodbye_handler(client, message: Message):
    if not is_group_chat(message):
        return

    settings = get_group_settings(message.chat.id)
    if not settings["goodbye_enabled"]:
        return

    user = message.left_chat_member
    if not user:
        return

    text = settings["goodbye_text"].replace("{mention}", user.mention).replace("{name}", user.first_name or "User")
    try:
        await message.reply_text(text)
    except Exception:
        pass

    await log_to_channel(
        client,
        message.chat.id,
        f"👋 Member left\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}"
    )


# =========================================================
# 14) Public group commands
# =========================================================
@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    await message.reply_text(settings["rules_text"])


@bot.on_message(filters.command("gsettings") & filters.group)
async def gsettings_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    txt = (
        f"⚙️ **Group Settings**\n\n"
        f"Welcome: `{settings['welcome_enabled']}`\n"
        f"Goodbye: `{settings['goodbye_enabled']}`\n"
        f"Warn Limit: `{settings['warn_limit']}`\n"
        f"Flood: `{settings['flood_limit']}` / `{settings['flood_window_sec']}` sec\n"
        f"Spam: `{settings['spam_repeat_limit']}` / `{settings['spam_window_sec']}` sec\n"
        f"Link Lock: `{settings['link_lock']}`\n"
        f"Media Lock: `{settings['media_lock']}`\n"
        f"Night Mode: `{settings['night_mode']}` (`{settings['night_start']}`-`{settings['night_end']}`)\n"
        f"Approval: `{settings['approval_mode']}`\n"
        f"Force Sub: `{settings['force_sub_channel'] or 'off'}`\n"
        f"Log Channel: `{settings['log_channel_id']}`"
    )
    await message.reply_text(txt)


@bot.on_message(filters.command("notes") & filters.group)
async def notes_cmd(client, message: Message):
    items = list_notes(message.chat.id)
    await message.reply_text("📝 Notes:\n" + ("\n".join(f"- `{x}`" for x in items) if items else "No notes."))


@bot.on_message(filters.command("getnote") & filters.group)
async def getnote_cmd(client, message: Message):
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /getnote note_name")
    note = get_note(message.chat.id, parts[1].strip())
    if not note:
        return await message.reply_text("Note not found.")
    await message.reply_text(note)


# =========================================================
# 15) Admin group commands
# =========================================================
@bot.on_message(filters.command("setrules") & filters.group)
async def setrules_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setrules your rules here")
    update_group_setting(message.chat.id, "rules_text", parts[1].strip())
    await message.reply_text("✅ Rules updated.")


@bot.on_message(filters.command("setwelcome") & filters.group)
async def setwelcome_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setwelcome text\nUse {mention} or {name}")
    update_group_setting(message.chat.id, "welcome_text", parts[1].strip())
    await message.reply_text("✅ Welcome text updated.")


@bot.on_message(filters.command("setgoodbye") & filters.group)
async def setgoodbye_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setgoodbye text\nUse {mention} or {name}")
    update_group_setting(message.chat.id, "goodbye_text", parts[1].strip())
    await message.reply_text("✅ Goodbye text updated.")


@bot.on_message(filters.command("welcome") & filters.group)
async def welcome_toggle_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /welcome on|off")
    update_group_setting(message.chat.id, "welcome_enabled", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Welcome updated.")


@bot.on_message(filters.command("goodbye") & filters.group)
async def goodbye_toggle_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /goodbye on|off")
    update_group_setting(message.chat.id, "goodbye_enabled", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Goodbye updated.")


@bot.on_message(filters.command("setlog") & filters.group)
async def setlog_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2 or not re.fullmatch(r"-?\d+", parts[1].strip()):
        return await message.reply_text("Usage: /setlog -1001234567890")
    update_group_setting(message.chat.id, "log_channel_id", int(parts[1].strip()))
    await message.reply_text("✅ Log channel saved.")


@bot.on_message(filters.command("setwarnlimit") & filters.group)
async def setwarnlimit_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /setwarnlimit 3")
    value = max(1, min(int(parts[1]), 20))
    update_group_setting(message.chat.id, "warn_limit", value)
    await message.reply_text(f"✅ Warn limit set to `{value}`")


@bot.on_message(filters.command("setflood") & filters.group)
async def setflood_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await message.reply_text("Usage: /setflood 6 10")
    count = max(2, min(int(parts[1]), 50))
    seconds = max(2, min(int(parts[2]), 300))
    update_group_setting(message.chat.id, "flood_limit", count)
    update_group_setting(message.chat.id, "flood_window_sec", seconds)
    await message.reply_text(f"✅ Flood set to `{count}` messages in `{seconds}` sec")


@bot.on_message(filters.command("setspam") & filters.group)
async def setspam_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await message.reply_text("Usage: /setspam 4 20")
    repeat_limit = max(2, min(int(parts[1]), 20))
    seconds = max(2, min(int(parts[2]), 300))
    update_group_setting(message.chat.id, "spam_repeat_limit", repeat_limit)
    update_group_setting(message.chat.id, "spam_window_sec", seconds)
    await message.reply_text(f"✅ Spam repeat set to `{repeat_limit}` in `{seconds}` sec")


@bot.on_message(filters.command("approval") & filters.group)
async def approval_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /approval on|off")
    update_group_setting(message.chat.id, "approval_mode", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Approval mode updated.")


@bot.on_message(filters.command("forcesub") & filters.group)
async def forcesub_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")

    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage:\n/forcesub @channelusername\n/forcesub -1001234567890\n/forcesub off")

    value = parts[1].strip()
    if value.lower() == "off":
        update_group_setting(message.chat.id, "force_sub_channel", "")
        return await message.reply_text("✅ Force-sub disabled.")

    update_group_setting(message.chat.id, "force_sub_channel", value)
    await message.reply_text(f"✅ Force-sub enabled: `{value}`")


@bot.on_message(filters.command("banword") & filters.group)
async def banword_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /banword word")
    settings = get_group_settings(message.chat.id)
    words = set(settings["banned_words"])
    words.add(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Word added.")


@bot.on_message(filters.command("unbanword") & filters.group)
async def unbanword_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /unbanword word")
    settings = get_group_settings(message.chat.id)
    words = set(settings["banned_words"])
    words.discard(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Word removed.")


@bot.on_message(filters.command("banwords") & filters.group)
async def banwords_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    words = settings["banned_words"]
    await message.reply_text("🚫 Banned words:\n" + (", ".join(words) if words else "None"))


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")

    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")

    settings = get_group_settings(message.chat.id)
    warns = get_warns(message.chat.id, target.id) + 1
    set_warns(message.chat.id, target.id, warns)

    if warns >= settings["warn_limit"]:
        try:
            await client.ban_chat_member(message.chat.id, target.id)
            set_warns(message.chat.id, target.id, 0)
            await message.reply_text(f"⛔ {target.mention} banned after warn limit.")
            await log_to_channel(client, message.chat.id, f"⛔ Auto-ban\nUser: {target.id}")
        except Exception as e:
            await message.reply_text(f"Warn added, but ban failed: {e}")
    else:
        await message.reply_text(f"⚠️ {target.mention} warned: `{warns}/{settings['warn_limit']}`")


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    warns = max(get_warns(message.chat.id, target.id) - 1, 0)
    set_warns(message.chat.id, target.id, warns)
    await message.reply_text(f"✅ Warn removed. Current warns: `{warns}`")


@bot.on_message(filters.command("mute") & filters.group)
async def mute_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    try:
        await client.restrict_chat_member(message.chat.id, target.id, ChatPermissions(can_send_messages=False))
        await message.reply_text(f"🔇 {target.mention} muted.")
    except Exception as e:
        await message.reply_text(f"❌ Mute failed: {e}")


@bot.on_message(filters.command("unmute") & filters.group)
async def unmute_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
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
        await message.reply_text(f"🔊 {target.mention} unmuted.")
    except Exception as e:
        await message.reply_text(f"❌ Unmute failed: {e}")


@bot.on_message(filters.command("ban") & filters.group)
async def ban_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    try:
        await client.ban_chat_member(message.chat.id, target.id)
        await message.reply_text(f"⛔ {target.mention} banned.")
    except Exception as e:
        await message.reply_text(f"❌ Ban failed: {e}")


@bot.on_message(filters.command("unban") & filters.group)
async def unban_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /unban user_id")
    try:
        await client.unban_chat_member(message.chat.id, int(parts[1]))
        await message.reply_text(f"✅ `{parts[1]}` unbanned.")
    except Exception as e:
        await message.reply_text(f"❌ Unban failed: {e}")


@bot.on_message(filters.command("locklink") & filters.group)
async def locklink_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /locklink on|off")
    update_group_setting(message.chat.id, "link_lock", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Link lock updated.")


@bot.on_message(filters.command("lockmedia") & filters.group)
async def lockmedia_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /lockmedia on|off")
    update_group_setting(message.chat.id, "media_lock", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Media lock updated.")


@bot.on_message(filters.command("nightmode") & filters.group)
async def nightmode_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")

    parts = (message.text or "").split()
    if len(parts) < 2:
        return await message.reply_text("Usage:\n/nightmode on 0 7\n/nightmode off")

    mode = parts[1].lower()
    if mode == "off":
        update_group_setting(message.chat.id, "night_mode", 0)
        return await message.reply_text("🌙 Night mode disabled.")

    if mode == "on":
        start_h = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        end_h = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 7
        update_group_setting(message.chat.id, "night_mode", 1)
        update_group_setting(message.chat.id, "night_start", max(0, min(start_h, 23)))
        update_group_setting(message.chat.id, "night_end", max(0, min(end_h, 23)))
        return await message.reply_text(f"🌙 Night mode enabled: `{start_h}:00 - {end_h}:00`")

    await message.reply_text("Usage:\n/nightmode on 0 7\n/nightmode off")


@bot.on_message(filters.command("setcmd") & filters.group)
async def setcmd_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /setcmd hello Hello everyone!")
    cmd = parts[1].lstrip("/").lower()
    response = parts[2]
    save_custom_command(message.chat.id, cmd, response)
    await message.reply_text(f"✅ Saved /{cmd}")


@bot.on_message(filters.command("save") & filters.group)
async def save_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /save note_name note text")
    save_note(message.chat.id, parts[1].strip().lower(), parts[2].strip())
    await message.reply_text(f"✅ Note `{parts[1].strip().lower()}` saved.")


@bot.on_message(filters.command("delnote") & filters.group)
async def delnote_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    parts = (message.text or "").split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /delnote note_name")
    del_note(message.chat.id, parts[1].strip())
    await message.reply_text("✅ Note deleted.")


@bot.on_message(filters.command("approve") & filters.group)
async def approve_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    approve_user(message.chat.id, target.id)
    await message.reply_text(f"✅ {target.mention} approved.")


@bot.on_message(filters.command("unapprove") & filters.group)
async def unapprove_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    unapprove_user(message.chat.id, target.id)
    await message.reply_text(f"✅ {target.mention} approval removed.")


# =========================================================
# 16) Join request handler
# =========================================================
@bot.on_chat_join_request()
async def join_request_handler(client, join_request: ChatJoinRequest):
    chat_id = join_request.chat.id
    user_id = join_request.from_user.id
    settings = get_group_settings(chat_id)

    if settings["force_sub_channel"]:
        ok = await check_forcesub_membership(client, settings["force_sub_channel"], user_id)
        if not ok:
            try:
                await join_request.decline()
                await log_to_channel(client, chat_id, f"🚫 Join request declined by force-sub\nUser: {user_id}")
                return
            except Exception:
                return

    if settings["approval_mode"]:
        try:
            await join_request.decline()
            await log_to_channel(client, chat_id, f"⏳ Join request declined due to approval mode\nUser: {user_id}")
        except Exception:
            pass
    else:
        try:
            await join_request.approve()
            await log_to_channel(client, chat_id, f"✅ Join request approved\nUser: {user_id}")
        except Exception:
            pass


# =========================================================
# 17) Auto moderation
# =========================================================
@bot.on_message(filters.group & ~filters.service, group=10)
async def group_protection_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    state["total_actions"] += 1
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    settings = get_group_settings(message.chat.id)

    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    # force-sub
    if settings["force_sub_channel"]:
        ok = await check_forcesub_membership(client, settings["force_sub_channel"], message.from_user.id)
        if not ok:
            try:
                await message.delete()
            except Exception:
                pass

            join_target = settings["force_sub_channel"]
            btn = None
            if join_target.startswith("@"):
                btn = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Join Required Channel", url=f"https://t.me/{join_target.lstrip('@')}")]
                ])

            try:
                await message.reply_text(
                    f"🛑 {message.from_user.mention}, আগে `{join_target}` channel-এ join করতে হবে.",
                    reply_markup=btn
                )
            except Exception:
                pass

            await log_to_channel(client, message.chat.id, f"🚫 Force-sub blocked message\nUser: {message.from_user.id}")
            return

    # approval mode
    if settings["approval_mode"] and not is_approved(message.chat.id, message.from_user.id):
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.reply_text(f"⏳ {message.from_user.mention}, you are not approved yet.")
        except Exception:
            pass
        return

    text = message.text or message.caption or ""

    # link lock
    if settings["link_lock"] and text_contains_link(text):
        try:
            await message.delete()
            await log_to_channel(client, message.chat.id, f"🔗 Link deleted\nUser: {message.from_user.id}")
            state["success_actions"] += 1
        except Exception:
            state["failed_actions"] += 1
        return

    # media lock
    if settings["media_lock"] and (message.media is not None):
        try:
            await message.delete()
            await log_to_channel(client, message.chat.id, f"🖼 Media deleted\nUser: {message.from_user.id}")
            state["success_actions"] += 1
        except Exception:
            state["failed_actions"] += 1
        return

    # night mode
    if settings["night_mode"] and in_night_mode_window(settings["night_start"], settings["night_end"]):
        try:
            await message.delete()
            state["success_actions"] += 1
        except Exception:
            state["failed_actions"] += 1
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

        if warns >= settings["warn_limit"]:
            try:
                await client.ban_chat_member(message.chat.id, message.from_user.id)
                set_warns(message.chat.id, message.from_user.id, 0)
            except Exception:
                pass

        await log_to_channel(client, message.chat.id, f"🚫 Banned word: {hit}\nUser: {message.from_user.id}\nWarns: {warns}")
        return

    # anti-flood
    key = (message.chat.id, message.from_user.id)
    now = time.time()
    arr = flood_tracker.get(key, [])
    arr = [x for x in arr if now - x <= settings["flood_window_sec"]]
    arr.append(now)
    flood_tracker[key] = arr

    if len(arr) >= settings["flood_limit"]:
        try:
            await client.restrict_chat_member(
                message.chat.id,
                message.from_user.id,
                ChatPermissions(can_send_messages=False)
            )
            await message.reply_text(f"🚫 Flood detected. {message.from_user.mention} muted.")
            await log_to_channel(client, message.chat.id, f"🚫 Flood mute\nUser: {message.from_user.id}\nCount: {len(arr)}")
            state["success_actions"] += 1
        except Exception as e:
            logger.warning(f"flood action failed: {e}")
            state["failed_actions"] += 1
        return

    # anti-spam repeated message
    sig = message_signature(message)
    if sig:
        skey = (message.chat.id, message.from_user.id)
        items = message_signature_cache.get(skey, [])
        items = [(ts, s) for ts, s in items if now - ts <= settings["spam_window_sec"]]
        items.append((now, sig))
        message_signature_cache[skey] = items

        same_count = sum(1 for ts, s in items if s == sig)
        if same_count >= settings["spam_repeat_limit"]:
            try:
                await message.delete()
            except Exception:
                pass
            warns = get_warns(message.chat.id, message.from_user.id) + 1
            set_warns(message.chat.id, message.from_user.id, warns)
            try:
                await message.reply_text(f"🚫 Spam detected. Warning `{warns}/{settings['warn_limit']}`")
            except Exception:
                pass
            await log_to_channel(client, message.chat.id, f"🚫 Spam repeat\nUser: {message.from_user.id}\nCount: {same_count}")
            return


# =========================================================
# 18) Custom command handler
# =========================================================
BUILTIN_COMMANDS = {
    "start", "help", "settings", "lang", "panel", "admin", "gpanel",
    "rules", "notes", "getnote", "gsettings",
    "setrules", "setwelcome", "setgoodbye", "welcome", "goodbye",
    "warn", "unwarn", "mute", "unmute", "ban", "unban",
    "banword", "unbanword", "banwords",
    "locklink", "lockmedia", "nightmode",
    "setwarnlimit", "setflood", "setspam",
    "approval", "forcesub", "setlog",
    "save", "delnote", "setcmd",
    "approve", "unapprove"
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
# 19) Hard security policy notes
# =========================================================
# No promote / demote logic exists here.
# Bot never grants admin permissions to anyone.
# All dangerous actions require live group admin verification.


# =========================================================
# 20) Startup
# =========================================================
async def startup_report():
    if not CFG.owner_id:
        return
    try:
        await bot.send_message(
            CFG.owner_id,
            f"✅ **{CFG.app_name} Started**\n\n"
            f"Users DB ready.\n"
            f"Maintenance: `{state['maintenance_mode']}`"
        )
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