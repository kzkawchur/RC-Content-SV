import os
import re
import sys
import time
import sqlite3
import logging
import asyncio
import threading
import inspect
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple, List
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import FloodWait, RPCError
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ChatPermissions,
    Message,
)

# =========================================================
# Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("group_guard_bot")
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
# Config
# =========================================================
def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on"}


def parse_admins(raw: str) -> Set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part and re.fullmatch(r"\d+", part):
            ids.add(int(part))
    return ids


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
    timezone_str: str
    default_force_sub_channel: str
    max_warn_limit: int
    max_flood_limit: int
    max_flood_window_sec: int


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
        timezone_str=os.environ.get("TIMEZONE", "Asia/Dhaka"),
        default_force_sub_channel=os.environ.get("DEFAULT_FORCE_SUB_CHANNEL", "").strip(),
        max_warn_limit=int(os.environ.get("MAX_WARN_LIMIT", "20")),
        max_flood_limit=int(os.environ.get("MAX_FLOOD_LIMIT", "50")),
        max_flood_window_sec=int(os.environ.get("MAX_FLOOD_WINDOW_SEC", "300")),
    )


CFG = load_config()
TZ = ZoneInfo(CFG.timezone_str)

# =========================================================
# Flask health server
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
    return "✅ Group Guard Bot is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "maintenance": state["maintenance_mode"],
        "success_actions": state["success_actions"],
        "failed_actions": state["failed_actions"],
        "time": datetime.now(TZ).isoformat(),
    }), 200


def run_web_server():
    app.run(host="0.0.0.0", port=CFG.port)


# =========================================================
# Event loop / client
# =========================================================
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

bot = Client(
    "group_guard_bot",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token,
    parse_mode=ParseMode.MARKDOWN,
)


# =========================================================
# Runtime trackers
# =========================================================
flood_tracker: Dict[Tuple[int, int], List[float]] = {}
join_tracker: Dict[int, List[float]] = {}
raid_lock_until: Dict[int, float] = {}


# =========================================================
# Texts
# =========================================================
TEXTS = {
    "en": {
        "welcome_private": (
            "⚡ **Welcome, {name}!**\n\n"
            "I can help protect and manage Telegram groups with moderation, force-sub, notes, custom commands and security checks."
        ),
        "maintenance": "🛠️ Bot is under maintenance. Please try again later.",
        "blocked": "🚫 You are blocked from using this bot.",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 Admin only.",
        "settings": "**Settings**\n\n🌐 Language: `{lang}`",
        "help": (
            "**Private**\n"
            "/start, /panel, /help, /settings, /lang en|bn\n\n"
            "**Group public**\n"
            "/rules, /gsettings, /notes, /getnote <name>, /report (reply), /id\n\n"
            "**Group admin**\n"
            "/settings, /setrules, /setwelcome, /welcome on|off\n"
            "/setlog, /forcesub, /setwarnlimit, /setflood\n"
            "/locklink on|off, /lockmedia on|off, /nightmode on <start> <end>|off\n"
            "/warn, /unwarn, /mute, /tmute <minutes>, /unmute, /ban, /unban\n"
            "/banword, /unbanword, /banwords\n"
            "/setcmd <cmd> <text>, /delcmd <cmd>\n"
            "/save <note> <text>, /delnote <name>\n"
            "/purge <count>, /checkbot"
        ),
    },
    "bn": {
        "welcome_private": (
            "⚡ **স্বাগতম, {name}!**\n\n"
            "আমি Telegram group protect ও manage করতে পারি—moderation, force-sub, notes, custom commands আর security checks সহ।"
        ),
        "maintenance": "🛠️ বট maintenance-এ আছে। পরে আবার চেষ্টা করো।",
        "blocked": "🚫 তুমি এই বট ব্যবহার করতে পারবে না।",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "admin_only": "🚫 শুধু admin ব্যবহার করতে পারবে।",
        "settings": "**Settings**\n\n🌐 Language: `{lang}`",
        "help": (
            "**Private**\n"
            "/start, /panel, /help, /settings, /lang en|bn\n\n"
            "**Group public**\n"
            "/rules, /gsettings, /notes, /getnote <name>, /report (reply), /id\n\n"
            "**Group admin**\n"
            "/settings, /setrules, /setwelcome, /welcome on|off\n"
            "/setlog, /forcesub, /setwarnlimit, /setflood\n"
            "/locklink on|off, /lockmedia on|off, /nightmode on <start> <end>|off\n"
            "/warn, /unwarn, /mute, /tmute <minutes>, /unmute, /ban, /unban\n"
            "/banword, /unbanword, /banwords\n"
            "/setcmd <cmd> <text>, /delcmd <cmd>\n"
            "/save <note> <text>, /delnote <name>\n"
            "/purge <count>, /checkbot"
        ),
    },
}


# =========================================================
# DB
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
            rules_text TEXT NOT NULL DEFAULT '📜 No rules set yet.',
            banned_words TEXT NOT NULL DEFAULT '',
            flood_limit INTEGER NOT NULL DEFAULT 6,
            flood_window_sec INTEGER NOT NULL DEFAULT 10,
            warn_limit INTEGER NOT NULL DEFAULT 3,
            link_lock INTEGER NOT NULL DEFAULT 0,
            media_lock INTEGER NOT NULL DEFAULT 0,
            night_mode INTEGER NOT NULL DEFAULT 0,
            night_start INTEGER NOT NULL DEFAULT 0,
            night_end INTEGER NOT NULL DEFAULT 7,
            log_channel_id INTEGER NOT NULL DEFAULT 0,
            force_sub_channel TEXT NOT NULL DEFAULT '',
            reports_enabled INTEGER NOT NULL DEFAULT 1,
            raid_join_limit INTEGER NOT NULL DEFAULT 8,
            raid_window_sec INTEGER NOT NULL DEFAULT 30
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
        conn.commit()


def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users(user_id, first_seen, last_seen, username, first_name)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_seen=excluded.last_seen,
                username=excluded.username,
                first_name=excluded.first_name
            """,
            (user_id, now, now, username or "", first_name or ""),
        )
        conn.commit()


def safe_total_users() -> int:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return int(cur.fetchone()[0])


def latest_users(limit: int = 10) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, username, first_name, last_seen FROM users ORDER BY last_seen DESC LIMIT ?",
            (limit,),
        )
        return cur.fetchall()


def is_globally_banned(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None


def ban_global_user(user_id: int, reason: str = ""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bans(user_id, reason, banned_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, banned_at=excluded.banned_at
            """,
            (user_id, reason, int(time.time())),
        )
        conn.commit()


def unban_global_user(user_id: int):
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
        cur.execute(
            """
            INSERT INTO user_settings(user_id, language)
            VALUES(?, ?)
            ON CONFLICT(user_id) DO UPDATE SET language=excluded.language
            """,
            (user_id, language),
        )
        conn.commit()


def ensure_group_row(chat_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO group_settings(chat_id, force_sub_channel) VALUES(?, ?)",
            (chat_id, CFG.default_force_sub_channel),
        )
        conn.commit()


def get_group_settings(chat_id: int) -> dict:
    ensure_group_row(chat_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT welcome_enabled, welcome_text, rules_text, banned_words,
                   flood_limit, flood_window_sec, warn_limit, link_lock,
                   media_lock, night_mode, night_start, night_end,
                   log_channel_id, force_sub_channel, reports_enabled,
                   raid_join_limit, raid_window_sec
            FROM group_settings WHERE chat_id = ?
            """,
            (chat_id,),
        )
        row = cur.fetchone()
        return {
            "welcome_enabled": bool(row[0]),
            "welcome_text": row[1],
            "rules_text": row[2],
            "banned_words": [w.strip().lower() for w in row[3].split(",") if w.strip()],
            "flood_limit": int(row[4]),
            "flood_window_sec": int(row[5]),
            "warn_limit": int(row[6]),
            "link_lock": bool(row[7]),
            "media_lock": bool(row[8]),
            "night_mode": bool(row[9]),
            "night_start": int(row[10]),
            "night_end": int(row[11]),
            "log_channel_id": int(row[12]),
            "force_sub_channel": row[13].strip(),
            "reports_enabled": bool(row[14]),
            "raid_join_limit": int(row[15]),
            "raid_window_sec": int(row[16]),
        }


def update_group_setting(chat_id: int, field: str, value):
    allowed = {
        "welcome_enabled", "welcome_text", "rules_text", "banned_words",
        "flood_limit", "flood_window_sec", "warn_limit", "link_lock",
        "media_lock", "night_mode", "night_start", "night_end",
        "log_channel_id", "force_sub_channel", "reports_enabled",
        "raid_join_limit", "raid_window_sec"
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
        return int(row[0]) if row else 0


def set_warns(chat_id: int, user_id: int, warns: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO group_warns(chat_id, user_id, warns)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, user_id) DO UPDATE SET warns=excluded.warns
            """,
            (chat_id, user_id, warns),
        )
        conn.commit()


def save_custom_command(chat_id: int, cmd: str, response_text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO custom_commands(chat_id, cmd, response_text)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, cmd) DO UPDATE SET response_text=excluded.response_text
            """,
            (chat_id, cmd.lower(), response_text),
        )
        conn.commit()


def get_custom_command(chat_id: int, cmd: str) -> Optional[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT response_text FROM custom_commands WHERE chat_id = ? AND cmd = ?", (chat_id, cmd.lower()))
        row = cur.fetchone()
        return row[0] if row else None


def delete_custom_command(chat_id: int, cmd: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM custom_commands WHERE chat_id = ? AND cmd = ?", (chat_id, cmd.lower()))
        conn.commit()


def save_note(chat_id: int, note_name: str, note_text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO group_notes(chat_id, note_name, note_text)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id, note_name) DO UPDATE SET note_text=excluded.note_text
            """,
            (chat_id, note_name.lower(), note_text),
        )
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
# Helpers
# =========================================================
def is_owner(user_id: int) -> bool:
    return bool(CFG.owner_id and user_id == CFG.owner_id)


def is_global_admin(user_id: int) -> bool:
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


async def get_bot_member(client: Client, chat_id: int):
    me = await client.get_me()
    return await client.get_chat_member(chat_id, me.id)


async def user_can_manage_panel(client: Client, chat_id: int, user_id: int) -> bool:
    return await is_group_admin(client, chat_id, user_id) or is_global_admin(user_id)


def mention_user(user) -> str:
    try:
        return user.mention
    except Exception:
        return f"[{user.first_name}](tg://user?id={user.id})"


async def log_to_channel(client: Client, chat_id: int, text: str):
    try:
        settings = get_group_settings(chat_id)
        log_channel_id = settings["log_channel_id"]
        if log_channel_id:
            await client.send_message(log_channel_id, text)
    except Exception as e:
        logger.warning(f"log_to_channel failed: {e}")


def text_contains_link(text: str) -> bool:
    return bool(text and re.search(r"(https?://|t\.me/|telegram\.me/|www\.)", text, re.I))


def text_contains_banned_word(text: str, banned_words: List[str]) -> Optional[str]:
    if not text:
        return None
    low = text.lower()
    for word in banned_words:
        if re.search(rf"\b{re.escape(word)}\b", low):
            return word
    return None


def now_hour_local() -> int:
    return datetime.now(TZ).hour


def in_night_mode_window(start_hour: int, end_hour: int) -> bool:
    now_h = now_hour_local()
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= now_h < end_hour
    return now_h >= start_hour or now_h < end_hour


async def check_forcesub_membership(client: Client, channel_ref: str, user_id: int) -> bool:
    if not channel_ref:
        return True
    try:
        member = await client.get_chat_member(channel_ref, user_id)
        return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    except Exception:
        return False


def build_private_panel(user_id: int) -> InlineKeyboardMarkup:
    is_admin = is_global_admin(user_id)
    rows = [
        [InlineKeyboardButton("ℹ️ Help", callback_data="pv:help"), InlineKeyboardButton("🌐 Language", callback_data="pv:lang")],
        [InlineKeyboardButton("🛡️ Security", callback_data="pv:security"), InlineKeyboardButton("📘 Setup", callback_data="pv:setup")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton("🧩 Admin Panel", callback_data="pv:admin")])
    return InlineKeyboardMarkup(rows)


def build_language_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data="lang:en"),
            InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data="lang:bn"),
        ],
        [InlineKeyboardButton("⬅️ Back", callback_data="pv:home")],
    ])


def build_owner_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="ad:stats"), InlineKeyboardButton("👥 Users", callback_data="ad:users")],
        [InlineKeyboardButton("🛠 Maintenance", callback_data="ad:maint"), InlineKeyboardButton("📜 Logs", callback_data="ad:logs")],
        [InlineKeyboardButton("⬅️ Back", callback_data="pv:home")],
    ])


def build_group_settings_panel(chat_id: int) -> InlineKeyboardMarkup:
    s = get_group_settings(chat_id)
    rows = [
        [
            InlineKeyboardButton(f"Welcome {'✅' if s['welcome_enabled'] else '❌'}", callback_data=f"gs:{chat_id}:welcome"),
            InlineKeyboardButton(f"Reports {'✅' if s['reports_enabled'] else '❌'}", callback_data=f"gs:{chat_id}:reports"),
        ],
        [
            InlineKeyboardButton(f"Links {'🔒' if s['link_lock'] else '🔓'}", callback_data=f"gs:{chat_id}:links"),
            InlineKeyboardButton(f"Media {'🔒' if s['media_lock'] else '🔓'}", callback_data=f"gs:{chat_id}:media"),
        ],
        [
            InlineKeyboardButton(f"Night {'✅' if s['night_mode'] else '❌'}", callback_data=f"gs:{chat_id}:night"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"gs:{chat_id}:refresh"),
        ],
    ]
    if s["force_sub_channel"]:
        rows.append([InlineKeyboardButton("📢 Force-sub is ON", callback_data=f"gs:{chat_id}:noop")])
    else:
        rows.append([InlineKeyboardButton("📢 Force-sub is OFF", callback_data=f"gs:{chat_id}:noop")])
    return InlineKeyboardMarkup(rows)


def render_group_settings_text(chat_id: int) -> str:
    s = get_group_settings(chat_id)
    return (
        "⚙️ **Group Settings**\n\n"
        f"Welcome: `{s['welcome_enabled']}`\n"
        f"Reports: `{s['reports_enabled']}`\n"
        f"Warn Limit: `{s['warn_limit']}`\n"
        f"Flood: `{s['flood_limit']}` msgs / `{s['flood_window_sec']}` sec\n"
        f"Raid Guard: `{s['raid_join_limit']}` joins / `{s['raid_window_sec']}` sec\n"
        f"Link Lock: `{s['link_lock']}`\n"
        f"Media Lock: `{s['media_lock']}`\n"
        f"Night Mode: `{s['night_mode']}` (`{s['night_start']}`-`{s['night_end']}` {CFG.timezone_str})\n"
        f"Banned Words: `{len(s['banned_words'])}`\n"
        f"Log Channel: `{s['log_channel_id']}`\n"
        f"Force Sub: `{s['force_sub_channel'] or 'off'}`"
    )


def make_permissions(**kwargs) -> ChatPermissions:
    allowed = set(inspect.signature(ChatPermissions).parameters.keys())
    return ChatPermissions(**{k: v for k, v in kwargs.items() if k in allowed})


def full_unmute_permissions() -> ChatPermissions:
    return make_permissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_send_polls=True,
        can_change_info=False,
        can_invite_users=True,
        can_pin_messages=False,
        can_manage_topics=False,
    )


def mute_permissions() -> ChatPermissions:
    return make_permissions(can_send_messages=False)


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


async def safe_delete(message: Message):
    try:
        await message.delete()
    except Exception:
        pass


async def safe_reply(message: Message, text: str, **kwargs):
    try:
        return await message.reply_text(text, **kwargs)
    except Exception:
        return None


# =========================================================
# Private commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)
    if is_globally_banned(user.id):
        return await message.reply_text(t(user.id, "blocked"))
    if state["maintenance_mode"] and not is_global_admin(user.id):
        return await message.reply_text(t(user.id, "maintenance"))

    await message.reply_text(
        t(user.id, "welcome_private", name=user.first_name or "User"),
        reply_markup=build_private_panel(user.id),
    )


@bot.on_message(filters.command("panel") & filters.private)
async def panel_cmd(client, message: Message):
    uid = message.from_user.id
    await message.reply_text("🧭 **Panel**", reply_markup=build_private_panel(uid))


@bot.on_message(filters.command("help") & (filters.private | filters.group))
async def help_cmd(client, message: Message):
    uid = message.from_user.id if message.from_user else 0
    await message.reply_text(t(uid, "help"))


@bot.on_message(filters.command("settings") & filters.private)
async def settings_private_cmd(client, message: Message):
    uid = message.from_user.id
    await message.reply_text(t(uid, "settings", lang=get_user_language(uid)), reply_markup=build_language_panel(uid))


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /lang en or /lang bn")
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only en or bn.")
    set_user_language(message.from_user.id, lang)
    await message.reply_text(t(message.from_user.id, f"lang_set_{lang}"))


@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message: Message):
    if not is_global_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))
    await message.reply_text("🧩 **Admin Panel**", reply_markup=build_owner_panel())


# =========================================================
# Group commands
# =========================================================
@bot.on_message(filters.command("settings") & filters.group)
async def settings_group_cmd(client, message: Message):
    if not message.from_user or not await user_can_manage_panel(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    await message.reply_text(render_group_settings_text(message.chat.id), reply_markup=build_group_settings_panel(message.chat.id))


@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    settings = get_group_settings(message.chat.id)
    await message.reply_text(settings["rules_text"])


@bot.on_message(filters.command("gsettings") & filters.group)
async def gsettings_cmd(client, message: Message):
    await message.reply_text(render_group_settings_text(message.chat.id))


@bot.on_message(filters.command("notes") & filters.group)
async def notes_cmd(client, message: Message):
    items = list_notes(message.chat.id)
    text = "📝 Notes:\n" + ("\n".join(f"- `{x}`" for x in items) if items else "No notes.")
    await message.reply_text(text)


@bot.on_message(filters.command("getnote") & filters.group)
async def getnote_cmd(client, message: Message):
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /getnote note_name")
    note = get_note(message.chat.id, parts[1].strip())
    if not note:
        return await message.reply_text("Note not found.")
    await message.reply_text(note)


@bot.on_message(filters.command("id") & (filters.private | filters.group))
async def id_cmd(client, message: Message):
    lines = [f"Chat ID: `{message.chat.id}`"]
    if message.from_user:
        lines.append(f"Your ID: `{message.from_user.id}`")
    if message.reply_to_message and message.reply_to_message.from_user:
        lines.append(f"Replied User ID: `{message.reply_to_message.from_user.id}`")
    await message.reply_text("\n".join(lines))


@bot.on_message(filters.command("report") & filters.group)
async def report_cmd(client, message: Message):
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user's message with /report")
    settings = get_group_settings(message.chat.id)
    if not settings["reports_enabled"]:
        return await message.reply_text("Reports are disabled.")
    target = message.reply_to_message.from_user
    reporter = message.from_user
    text = (
        "🚨 **New Report**\n"
        f"Chat: {message.chat.title}\n"
        f"Reporter: {mention_user(reporter)} | `{reporter.id}`\n"
        f"Target: {mention_user(target)} | `{target.id}`\n"
        f"Message: `{message.reply_to_message.id}`"
    )
    await log_to_channel(client, message.chat.id, text)
    await message.reply_text("✅ Report sent to admins/log channel.")


# =========================================================
# Group admin text commands
# =========================================================
async def require_group_admin(client: Client, message: Message) -> bool:
    return bool(message.from_user and await user_can_manage_panel(client, message.chat.id, message.from_user.id))


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
        return await message.reply_text("Usage: /setwelcome text (use {mention} or {name})")
    update_group_setting(message.chat.id, "welcome_text", parts[1].strip())
    await message.reply_text("✅ Welcome text updated.")


@bot.on_message(filters.command("welcome") & filters.group)
async def welcome_toggle_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /welcome on|off")
    value = 1 if parts[1].lower() == "on" else 0
    update_group_setting(message.chat.id, "welcome_enabled", value)
    await message.reply_text(f"✅ Welcome {'enabled' if value else 'disabled'}.")


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
        await message.reply_text("❌ Invalid channel ID.")


@bot.on_message(filters.command("forcesub") & filters.group)
async def forcesub_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /forcesub @channelusername | -100... | off")
    value = parts[1].strip()
    if value.lower() == "off":
        update_group_setting(message.chat.id, "force_sub_channel", "")
        return await message.reply_text("✅ Force-sub disabled.")
    update_group_setting(message.chat.id, "force_sub_channel", value)
    await message.reply_text(f"✅ Force-sub enabled: `{value}`")


@bot.on_message(filters.command("setwarnlimit") & filters.group)
async def setwarnlimit_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /setwarnlimit 3")
    value = max(1, min(int(parts[1]), CFG.max_warn_limit))
    update_group_setting(message.chat.id, "warn_limit", value)
    await message.reply_text(f"✅ Warn limit set to `{value}`")


@bot.on_message(filters.command("setflood") & filters.group)
async def setflood_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await message.reply_text("Usage: /setflood 6 10")
    count = max(2, min(int(parts[1]), CFG.max_flood_limit))
    seconds = max(2, min(int(parts[2]), CFG.max_flood_window_sec))
    update_group_setting(message.chat.id, "flood_limit", count)
    update_group_setting(message.chat.id, "flood_window_sec", seconds)
    await message.reply_text(f"✅ Flood set to `{count}` messages in `{seconds}` sec")


@bot.on_message(filters.command("locklink") & filters.group)
async def locklink_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /locklink on|off")
    value = 1 if parts[1].lower() == "on" else 0
    update_group_setting(message.chat.id, "link_lock", value)
    await message.reply_text(f"✅ Link lock {'enabled' if value else 'disabled'}.")


@bot.on_message(filters.command("lockmedia") & filters.group)
async def lockmedia_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /lockmedia on|off")
    value = 1 if parts[1].lower() == "on" else 0
    update_group_setting(message.chat.id, "media_lock", value)
    await message.reply_text(f"✅ Media lock {'enabled' if value else 'disabled'}.")


@bot.on_message(filters.command("nightmode") & filters.group)
async def nightmode_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply_text("Usage: /nightmode on 0 7 | /nightmode off")
    mode = parts[1].lower()
    if mode == "off":
        update_group_setting(message.chat.id, "night_mode", 0)
        return await message.reply_text("🌙 Night mode disabled.")
    if mode == "on":
        start_h = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
        end_h = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 7
        start_h = max(0, min(start_h, 23))
        end_h = max(0, min(end_h, 23))
        update_group_setting(message.chat.id, "night_mode", 1)
        update_group_setting(message.chat.id, "night_start", start_h)
        update_group_setting(message.chat.id, "night_end", end_h)
        return await message.reply_text(f"🌙 Night mode enabled: `{start_h}:00 - {end_h}:00` ({CFG.timezone_str})")
    await message.reply_text("Usage: /nightmode on 0 7 | /nightmode off")


@bot.on_message(filters.command("banword") & filters.group)
async def banword_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /banword word")
    s = get_group_settings(message.chat.id)
    words = set(s["banned_words"])
    words.add(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Word added.")


@bot.on_message(filters.command("unbanword") & filters.group)
async def unbanword_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /unbanword word")
    s = get_group_settings(message.chat.id)
    words = set(s["banned_words"])
    words.discard(parts[1].strip().lower())
    update_group_setting(message.chat.id, "banned_words", ",".join(sorted(words)))
    await message.reply_text("✅ Word removed.")


@bot.on_message(filters.command("banwords") & filters.group)
async def banwords_cmd(client, message: Message):
    s = get_group_settings(message.chat.id)
    await message.reply_text("🚫 Banned words:\n" + (", ".join(s["banned_words"]) if s["banned_words"] else "None"))


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide username/user_id.")
    if await is_group_admin(client, message.chat.id, target.id):
        return await message.reply_text("I won't warn another admin.")
    s = get_group_settings(message.chat.id)
    warns = get_warns(message.chat.id, target.id) + 1
    set_warns(message.chat.id, target.id, warns)
    if warns >= s["warn_limit"]:
        try:
            await client.ban_chat_member(message.chat.id, target.id)
            set_warns(message.chat.id, target.id, 0)
            await message.reply_text(f"⛔ {mention_user(target)} banned after reaching warn limit.")
            await log_to_channel(client, message.chat.id, f"⛔ Auto-ban by warns\nUser: {target.id}\nWarn limit: {s['warn_limit']}")
            state["success_actions"] += 1
        except Exception as e:
            state["failed_actions"] += 1
            await message.reply_text(f"Warn added, but ban failed: {e}")
    else:
        await message.reply_text(f"⚠️ {mention_user(target)} warned. `{warns}/{s['warn_limit']}`")
        await log_to_channel(client, message.chat.id, f"⚠️ Warn issued\nUser: {target.id}\nWarns: {warns}/{s['warn_limit']}")
        state["success_actions"] += 1


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide username/user_id.")
    warns = max(get_warns(message.chat.id, target.id) - 1, 0)
    set_warns(message.chat.id, target.id, warns)
    await message.reply_text(f"✅ Current warns: `{warns}`")


@bot.on_message(filters.command("mute") & filters.group)
async def mute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide username/user_id.")
    if await is_group_admin(client, message.chat.id, target.id):
        return await message.reply_text("I won't mute another admin.")
    try:
        await client.restrict_chat_member(message.chat.id, target.id, mute_permissions())
        await message.reply_text(f"🔇 {mention_user(target)} muted.")
        state["success_actions"] += 1
    except Exception as e:
        state["failed_actions"] += 1
        await message.reply_text(f"❌ Mute failed: {e}")


@bot.on_message(filters.command("tmute") & filters.group)
async def tmute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user with /tmute <minutes>")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: reply + /tmute 10")
    minutes = max(1, min(int(parts[1]), 10080))
    target = message.reply_to_message.from_user
    if await is_group_admin(client, message.chat.id, target.id):
        return await message.reply_text("I won't mute another admin.")
    try:
        until_date = datetime.now(TZ).timestamp() + minutes * 60
        await client.restrict_chat_member(message.chat.id, target.id, mute_permissions(), until_date=int(until_date))
        await message.reply_text(f"🔇 {mention_user(target)} muted for `{minutes}` minute(s).")
        state["success_actions"] += 1
    except Exception as e:
        state["failed_actions"] += 1
        await message.reply_text(f"❌ Timed mute failed: {e}")


@bot.on_message(filters.command("unmute") & filters.group)
async def unmute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide username/user_id.")
    try:
        await client.restrict_chat_member(message.chat.id, target.id, full_unmute_permissions())
        await message.reply_text(f"🔊 {mention_user(target)} unmuted.")
        state["success_actions"] += 1
    except Exception as e:
        state["failed_actions"] += 1
        await message.reply_text(f"❌ Unmute failed: {e}")


@bot.on_message(filters.command("ban") & filters.group)
async def ban_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide username/user_id.")
    if await is_group_admin(client, message.chat.id, target.id):
        return await message.reply_text("I won't ban another admin.")
    try:
        await client.ban_chat_member(message.chat.id, target.id)
        await message.reply_text(f"⛔ {mention_user(target)} banned.")
        state["success_actions"] += 1
    except Exception as e:
        state["failed_actions"] += 1
        await message.reply_text(f"❌ Ban failed: {e}")


@bot.on_message(filters.command("unban") & filters.group)
async def unban_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /unban user_id")
    try:
        user_id = int(parts[1])
        await client.unban_chat_member(message.chat.id, user_id)
        await message.reply_text(f"✅ `{user_id}` unbanned.")
        state["success_actions"] += 1
    except Exception as e:
        state["failed_actions"] += 1
        await message.reply_text(f"❌ Unban failed: {e}")


@bot.on_message(filters.command("setcmd") & filters.group)
async def setcmd_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /setcmd hello Hello everyone!")
    cmd = parts[1].lstrip("/").lower()
    if not re.fullmatch(r"[a-z0-9_]{1,32}", cmd):
        return await message.reply_text("Invalid command name.")
    save_custom_command(message.chat.id, cmd, parts[2])
    await message.reply_text(f"✅ Saved /{cmd}")


@bot.on_message(filters.command("delcmd") & filters.group)
async def delcmd_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /delcmd hello")
    delete_custom_command(message.chat.id, parts[1].strip().lstrip("/").lower())
    await message.reply_text("✅ Custom command deleted.")


@bot.on_message(filters.command("save") & filters.group)
async def save_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /save note_name note text")
    name = parts[1].strip().lower()
    if not re.fullmatch(r"[a-z0-9_\-]{1,64}", name):
        return await message.reply_text("Invalid note name.")
    save_note(message.chat.id, name, parts[2].strip())
    await message.reply_text(f"✅ Note `{name}` saved.")


@bot.on_message(filters.command("delnote") & filters.group)
async def delnote_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /delnote note_name")
    del_note(message.chat.id, parts[1].strip())
    await message.reply_text("✅ Note deleted.")


@bot.on_message(filters.command("purge") & filters.group)
async def purge_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /purge 10")
    count = max(1, min(int(parts[1]), 100))
    deleted = 0
    async for msg in client.get_chat_history(message.chat.id, limit=count + 1):
        try:
            await msg.delete()
            deleted += 1
        except Exception:
            pass
    state["success_actions"] += 1
    await log_to_channel(client, message.chat.id, f"🧹 Purge used in {message.chat.title}. Deleted approx {deleted} messages.")


@bot.on_message(filters.command("checkbot") & filters.group)
async def checkbot_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    try:
        me = await get_bot_member(client, message.chat.id)
        status = me.status.value if hasattr(me.status, 'value') else str(me.status)
        lines = [f"Bot status: `{status}`"]
        priv = getattr(me, "privileges", None)
        if priv:
            attrs = [
                "can_delete_messages", "can_restrict_members", "can_invite_users",
                "can_pin_messages", "can_manage_chat", "can_manage_video_chats",
                "can_promote_members", "can_change_info", "can_manage_topics"
            ]
            for a in attrs:
                if hasattr(priv, a):
                    lines.append(f"{a}: `{getattr(priv, a)}`")
        lines.append("\nSecurity advice: keep `can_promote_members` OFF unless you explicitly need it.")
        await message.reply_text("\n".join(lines))
    except Exception as e:
        await message.reply_text(f"Failed to inspect bot rights: {e}")


# =========================================================
# Private owner-only global admin commands
# =========================================================
@bot.on_message(filters.command("gban") & filters.private)
async def gban_cmd(client, message: Message):
    if not is_global_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))
    parts = message.text.split(None, 2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /gban user_id [reason]")
    reason = parts[2] if len(parts) > 2 else ""
    ban_global_user(int(parts[1]), reason)
    await message.reply_text("✅ Globally banned.")


@bot.on_message(filters.command("ungban") & filters.private)
async def ungban_cmd(client, message: Message):
    if not is_global_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))
    parts = message.text.split(None, 1)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /ungban user_id")
    unban_global_user(int(parts[1]))
    await message.reply_text("✅ Global ban removed.")


# =========================================================
# Callbacks
# =========================================================
@bot.on_callback_query()
async def callback_handler(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data or ""

    # private panel
    if data == "pv:home":
        return await cq.message.edit_text("🧭 **Panel**", reply_markup=build_private_panel(user_id))
    if data == "pv:help":
        return await cq.message.edit_text(t(user_id, "help"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="pv:home")]]))
    if data == "pv:lang":
        return await cq.message.edit_text(t(user_id, "settings", lang=get_user_language(user_id)), reply_markup=build_language_panel(user_id))
    if data == "pv:setup":
        txt = (
            "**Quick setup**\n\n"
            "1. Add the bot to your group\n"
            "2. Give it delete/restrict/ban rights\n"
            "3. Run /settings in the group\n"
            "4. Configure /setrules, /setwelcome, /forcesub, /setlog\n"
            "5. Test /warn, /mute, /report"
        )
        return await cq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="pv:home")]]))
    if data == "pv:security":
        txt = (
            "**Security**\n\n"
            "• This bot has no promote-admin command.\n"
            "• All sensitive callbacks re-check sender permissions.\n"
            "• Admin features require actual Telegram admin status in the group.\n"
            "• Keep the bot's `can_promote_members` right OFF unless you really need it."
        )
        return await cq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="pv:home")]]))
    if data == "pv:admin":
        if not is_global_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        return await cq.message.edit_text("🧩 **Admin Panel**", reply_markup=build_owner_panel())

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        if lang not in {"en", "bn"}:
            return await cq.answer("Invalid language", show_alert=True)
        set_user_language(user_id, lang)
        await cq.message.edit_text(t(user_id, "settings", lang=lang), reply_markup=build_language_panel(user_id))
        return await cq.answer("Updated")

    if data.startswith("ad:"):
        if not is_global_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        action = data.split(":", 1)[1]
        if action == "stats":
            txt = (
                f"📊 Users: {safe_total_users()}\n"
                f"✅ Success: {state['success_actions']}\n"
                f"❌ Failed: {state['failed_actions']}\n"
                f"🛠 Maintenance: {state['maintenance_mode']}\n"
                f"🕒 Time: {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')} {CFG.timezone_str}"
            )
            await cq.message.edit_text(txt, reply_markup=build_owner_panel())
        elif action == "users":
            rows = latest_users(10)
            txt = "👥 Latest users\n\n" + ("\n".join(f"`{uid}` | @{un or '-'} | {fn or '-'} | {ls}" for uid, un, fn, ls in rows) if rows else "No users yet.")
            await cq.message.edit_text(txt, reply_markup=build_owner_panel())
        elif action == "maint":
            state["maintenance_mode"] = not state["maintenance_mode"]
            await cq.message.edit_text(f"🛠 Maintenance: `{state['maintenance_mode']}`", reply_markup=build_owner_panel())
        elif action == "logs":
            txt = "\n".join(recent_logs[-20:]) if recent_logs else "No logs."
            if len(txt) > 3900:
                txt = txt[-3900:]
            await cq.message.edit_text(f"📜 Recent Logs\n\n`{txt}`", reply_markup=build_owner_panel())
        return await cq.answer()

    if data.startswith("gs:"):
        try:
            _, chat_s, action = data.split(":", 2)
            chat_id = int(chat_s)
        except Exception:
            return await cq.answer("Bad callback", show_alert=True)
        if not await user_can_manage_panel(client, chat_id, user_id):
            return await cq.answer("Admin only", show_alert=True)

        if action == "welcome":
            s = get_group_settings(chat_id)
            update_group_setting(chat_id, "welcome_enabled", 0 if s["welcome_enabled"] else 1)
        elif action == "reports":
            s = get_group_settings(chat_id)
            update_group_setting(chat_id, "reports_enabled", 0 if s["reports_enabled"] else 1)
        elif action == "links":
            s = get_group_settings(chat_id)
            update_group_setting(chat_id, "link_lock", 0 if s["link_lock"] else 1)
        elif action == "media":
            s = get_group_settings(chat_id)
            update_group_setting(chat_id, "media_lock", 0 if s["media_lock"] else 1)
        elif action == "night":
            s = get_group_settings(chat_id)
            update_group_setting(chat_id, "night_mode", 0 if s["night_mode"] else 1)
        elif action in {"refresh", "noop"}:
            pass
        else:
            return await cq.answer("Unknown action", show_alert=True)

        await cq.message.edit_text(render_group_settings_text(chat_id), reply_markup=build_group_settings_panel(chat_id))
        return await cq.answer("Updated")


# =========================================================
# Welcome / raid watch
# =========================================================
@bot.on_message(filters.new_chat_members)
async def welcome_new_members(client, message: Message):
    if not is_group_chat(message):
        return
    settings = get_group_settings(message.chat.id)
    now = time.time()
    arr = [x for x in join_tracker.get(message.chat.id, []) if now - x <= settings["raid_window_sec"]]
    arr.extend([now] * len(message.new_chat_members))
    join_tracker[message.chat.id] = arr

    if len(arr) >= settings["raid_join_limit"]:
        raid_lock_until[message.chat.id] = now + 300
        await log_to_channel(client, message.chat.id, f"🚨 Raid guard activated in {message.chat.title} for 5 minutes.")
        await safe_reply(message, "🚨 Raid protection activated temporarily.")

    if settings["welcome_enabled"]:
        for user in message.new_chat_members:
            text = settings["welcome_text"].replace("{mention}", mention_user(user)).replace("{name}", user.first_name or "User")
            await safe_reply(message, text)
            await log_to_channel(client, message.chat.id, f"👤 New member joined\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}")


# =========================================================
# Moderation core
# =========================================================
@bot.on_message(filters.group & ~filters.service, group=10)
async def group_protection_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    if is_globally_banned(message.from_user.id):
        await safe_delete(message)
        return

    if state["maintenance_mode"] and not await is_group_admin(client, message.chat.id, message.from_user.id):
        await safe_delete(message)
        return

    settings = get_group_settings(message.chat.id)

    # Raid lockdown: temporary delete all non-admin messages
    if raid_lock_until.get(message.chat.id, 0) > time.time():
        if not await is_group_admin(client, message.chat.id, message.from_user.id):
            await safe_delete(message)
            return

    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    # Force-sub / force-task
    if settings["force_sub_channel"]:
        ok = await check_forcesub_membership(client, settings["force_sub_channel"], message.from_user.id)
        if not ok:
            await safe_delete(message)
            join_target = settings["force_sub_channel"]
            btn = None
            if join_target.startswith("@"):
                btn = InlineKeyboardMarkup([[InlineKeyboardButton("📢 Join Required Channel", url=f"https://t.me/{join_target.lstrip('@')}")]])
            await safe_reply(message, f"🛑 {mention_user(message.from_user)}, আগে `{join_target}` join করতে হবে.", reply_markup=btn)
            return

    text = message.text or message.caption or ""

    if settings["night_mode"] and in_night_mode_window(settings["night_start"], settings["night_end"]):
        await safe_delete(message)
        return

    if settings["link_lock"] and text_contains_link(text):
        await safe_delete(message)
        await log_to_channel(client, message.chat.id, f"🔗 Link deleted\nUser: {message.from_user.id}\nChat: {message.chat.title}")
        return

    if settings["media_lock"] and message.media is not None:
        await safe_delete(message)
        await log_to_channel(client, message.chat.id, f"🖼 Media deleted\nUser: {message.from_user.id}\nChat: {message.chat.title}")
        return

    hit = text_contains_banned_word(text, settings["banned_words"])
    if hit:
        await safe_delete(message)
        warns = get_warns(message.chat.id, message.from_user.id) + 1
        set_warns(message.chat.id, message.from_user.id, warns)
        await log_to_channel(client, message.chat.id, f"🚫 Banned word hit: {hit}\nUser: {message.from_user.id}\nWarns: {warns}")
        if warns >= settings["warn_limit"]:
            try:
                await client.ban_chat_member(message.chat.id, message.from_user.id)
                set_warns(message.chat.id, message.from_user.id, 0)
            except Exception:
                pass
        return

    key = (message.chat.id, message.from_user.id)
    now = time.time()
    arr = [x for x in flood_tracker.get(key, []) if now - x <= settings["flood_window_sec"]]
    arr.append(now)
    flood_tracker[key] = arr
    if len(arr) >= settings["flood_limit"]:
        try:
            await client.restrict_chat_member(message.chat.id, message.from_user.id, mute_permissions())
            await safe_reply(message, f"🚫 Flood detected. {mention_user(message.from_user)} muted.")
            await log_to_channel(client, message.chat.id, f"🚫 Flood mute\nUser: {message.from_user.id}\nCount: {len(arr)}")
            state["success_actions"] += 1
        except Exception as e:
            logger.warning(f"Flood action failed: {e}")
            state["failed_actions"] += 1


# =========================================================
# Custom command handler
# =========================================================
BUILTIN_COMMANDS = {
    "start", "panel", "help", "settings", "lang", "admin",
    "rules", "gsettings", "notes", "getnote", "report", "id",
    "setrules", "setwelcome", "welcome", "setlog", "forcesub",
    "setwarnlimit", "setflood", "locklink", "lockmedia", "nightmode",
    "banword", "unbanword", "banwords", "warn", "unwarn", "mute",
    "tmute", "unmute", "ban", "unban", "setcmd", "delcmd", "save",
    "delnote", "purge", "checkbot", "gban", "ungban"
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
# Startup / shutdown
# =========================================================
async def startup_report():
    if not CFG.owner_id:
        return
    try:
        await bot.send_message(CFG.owner_id, "✅ **Bot Started Successfully**\n\nGroup Guard Bot is online.")
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
