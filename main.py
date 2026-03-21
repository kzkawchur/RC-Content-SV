import os
import sys
import time
import sqlite3
import logging
import asyncio
import threading
import re
from dataclasses import dataclass
from contextlib import closing
from typing import Optional, Dict, Set, Tuple, List

from flask import Flask, jsonify
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    Message,
    ChatPermissions,
)
from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.errors import RPCError, FloodWait

# =========================================================
# 1) Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("group_guard_2")

recent_logs: List[str] = []


class RecentLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            recent_logs.append(msg)
            if len(recent_logs) > 100:
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


def parse_admins(raw: str) -> Set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids


@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str

    owner_id: int
    admins: Set[int]

    port: int
    db_path: str
    maintenance_mode: bool
    bot_name: str


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
        owner_id=owner_id,
        admins=admins,
        port=int(os.environ.get("PORT", "10000")),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        bot_name=os.environ.get("BOT_NAME", "Group Guard 2.0"),
    )


CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

state = {
    "maintenance_mode": CFG.maintenance_mode,
    "started_at": BOOT_TIME,
    "deleted_messages": 0,
    "muted_users": 0,
    "banned_users": 0,
}


@app.route("/")
def home():
    return f"✅ {CFG.bot_name} is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "bot_name": CFG.bot_name,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "maintenance": state["maintenance_mode"],
        "deleted_messages": state["deleted_messages"],
        "muted_users": state["muted_users"],
        "banned_users": state["banned_users"],
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
    "group_guard_2",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token
)

# =========================================================
# 6) Runtime
# =========================================================
flood_tracker: Dict[Tuple[int, int], List[float]] = {}

# =========================================================
# 7) Texts
# =========================================================
TEXTS = {
    "en": {
        "private_welcome": "⚡ **Welcome to Group Guard 2.0, {name}!**\n\nUse the buttons below to open panels.",
        "maintenance": "🛠️ Bot is under maintenance.",
        "blocked": "🚫 You are blocked from using this bot.",
        "admin_only": "🚫 Admin only.",
        "owner_only": "👑 Owner only.",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "settings_title": "**User Panel**\n\nChoose an option:",
        "help_text": (
            "**Group Guard 2.0**\n\n"
            "**Private Commands**\n"
            "/start\n/help\n/settings\n/lang en|bn\n/admin\n\n"
            "**Public Group Commands**\n"
            "/rules\n/gsettings\n/notes\n/getnote <name>\n\n"
            "**Admin Group Commands**\n"
            "/settings\n/setrules <text>\n/setwelcome <text>\n"
            "/welcome on|off\n"
            "/forcesub @channel_or_-100id\n/forcesub off\n"
            "/setlog -100channelid\n"
            "/setwarnlimit <n>\n/setflood <count> <seconds>\n"
            "/locklink on|off\n/lockmedia on|off\n/nightmode on <start> <end>\n/nightmode off\n"
            "/banword <word>\n/unbanword <word>\n/banwords\n"
            "/warn (reply)\n/unwarn (reply)\n"
            "/mute (reply)\n/unmute (reply)\n"
            "/ban (reply)\n/unban <user_id>\n"
            "/setcmd <cmd> <text>\n"
            "/save <name> <text>\n/delnote <name>\n"
        ),
    },
    "bn": {
        "private_welcome": "⚡ **স্বাগতম Group Guard 2.0-এ, {name}!**\n\nনিচের বাটন থেকে panel খুলো।",
        "maintenance": "🛠️ বট maintenance-এ আছে।",
        "blocked": "🚫 তুমি blocked.",
        "admin_only": "🚫 শুধু admin ব্যবহার করতে পারবে।",
        "owner_only": "👑 শুধু owner ব্যবহার করতে পারবে।",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "settings_title": "**User Panel**\n\nএকটি অপশন বেছে নাও:",
        "help_text": (
            "**Group Guard 2.0**\n\n"
            "**Private Commands**\n"
            "/start\n/help\n/settings\n/lang en|bn\n/admin\n\n"
            "**Public Group Commands**\n"
            "/rules\n/gsettings\n/notes\n/getnote <name>\n\n"
            "**Admin Group Commands**\n"
            "/settings\n/setrules <text>\n/setwelcome <text>\n"
            "/welcome on|off\n"
            "/forcesub @channel_or_-100id\n/forcesub off\n"
            "/setlog -100channelid\n"
            "/setwarnlimit <n>\n/setflood <count> <seconds>\n"
            "/locklink on|off\n/lockmedia on|off\n/nightmode on <start> <end>\n/nightmode off\n"
            "/banword <word>\n/unbanword <word>\n/banwords\n"
            "/warn (reply)\n/unwarn (reply)\n"
            "/mute (reply)\n/unmute (reply)\n"
            "/ban (reply)\n/unban <user_id>\n"
            "/setcmd <cmd> <text>\n"
            "/save <name> <text>\n/delnote <name>\n"
        ),
    }
}

# =========================================================
# 8) Database
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


def ban_bot_user(user_id: int, reason: str = ""):
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


def unban_bot_user(user_id: int):
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
            SELECT welcome_enabled, welcome_text, rules_text, banned_words,
                   flood_limit, flood_window_sec, warn_limit, link_lock,
                   media_lock, night_mode, night_start, night_end,
                   log_channel_id, force_sub_channel
            FROM group_settings WHERE chat_id = ?
        """, (chat_id,))
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
        }


def update_group_setting(chat_id: int, field: str, value):
    allowed = {
        "welcome_enabled", "welcome_text", "rules_text", "banned_words",
        "flood_limit", "flood_window_sec", "warn_limit", "link_lock",
        "media_lock", "night_mode", "night_start", "night_end",
        "log_channel_id", "force_sub_channel"
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

# =========================================================
# 9) Helpers
# =========================================================
def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_language(user_id)
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


def is_owner(user_id: int) -> bool:
    return user_id == CFG.owner_id


def is_bot_admin(user_id: int) -> bool:
    return user_id in CFG.admins


def is_group_chat(message: Message) -> bool:
    return message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}


async def is_group_admin(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
    except Exception:
        return False


async def bot_has_required_rights(client: Client, chat_id: int) -> bool:
    try:
        me = await client.get_me()
        member = await client.get_chat_member(chat_id, me.id)
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


async def check_forcesub_membership(client: Client, channel_ref: str, user_id: int) -> bool:
    if not channel_ref:
        return True
    try:
        member = await client.get_chat_member(channel_ref, user_id)
        return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    except Exception:
        return False


def build_private_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🌐 Language", callback_data="panel_language"),
            InlineKeyboardButton("ℹ️ Help", callback_data="panel_help")
        ],
        [
            InlineKeyboardButton("👤 User Panel", callback_data="panel_userinfo"),
            InlineKeyboardButton("🛡 Admin Panel", callback_data="panel_admin")
        ],
        [
            InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data="setlang_en"),
            InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data="setlang_bn")
        ]
    ])


def build_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
            InlineKeyboardButton("👥 Users", callback_data="admin_users")
        ],
        [
            InlineKeyboardButton("🛠 Maintenance", callback_data="admin_maint"),
            InlineKeyboardButton("📜 Logs", callback_data="admin_logs")
        ]
    ])


def build_group_settings_panel(chat_id: int) -> InlineKeyboardMarkup:
    s = get_group_settings(chat_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"Welcome {'✅' if s['welcome_enabled'] else '❌'}", callback_data=f"gs_welcome_{chat_id}"),
            InlineKeyboardButton(f"Link {'✅' if s['link_lock'] else '❌'}", callback_data=f"gs_link_{chat_id}")
        ],
        [
            InlineKeyboardButton(f"Media {'✅' if s['media_lock'] else '❌'}", callback_data=f"gs_media_{chat_id}"),
            InlineKeyboardButton(f"Night {'✅' if s['night_mode'] else '❌'}", callback_data=f"gs_night_{chat_id}")
        ]
    ])


def render_group_settings_text(chat_id: int) -> str:
    s = get_group_settings(chat_id)
    return (
        f"⚙️ **Group Settings**\n\n"
        f"Welcome: `{s['welcome_enabled']}`\n"
        f"Warn Limit: `{s['warn_limit']}`\n"
        f"Flood: `{s['flood_limit']}` msgs / `{s['flood_window_sec']}` sec\n"
        f"Link Lock: `{s['link_lock']}`\n"
        f"Media Lock: `{s['media_lock']}`\n"
        f"Night Mode: `{s['night_mode']}` (`{s['night_start']}`-`{s['night_end']}`)\n"
        f"Banned Words: `{len(s['banned_words'])}`\n"
        f"Log Channel: `{s['log_channel_id']}`\n"
        f"Force Sub: `{s['force_sub_channel'] or 'off'}`"
    )

# =========================================================
# 10) Private commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)

    if is_banned(user.id):
        return await message.reply_text(t(user.id, "blocked"))
    if state["maintenance_mode"] and not is_bot_admin(user.id):
        return await message.reply_text(t(user.id, "maintenance"))

    await message.reply_text(
        t(user.id, "private_welcome", name=user.first_name or "User"),
        reply_markup=build_private_panel(user.id)
    )


@bot.on_message(filters.command("help") & (filters.private | filters.group))
async def help_cmd(client, message: Message):
    uid = message.from_user.id if message.from_user else 0
    await message.reply_text(t(uid, "help_text"))


@bot.on_message(filters.command("settings") & filters.private)
async def private_settings_cmd(client, message: Message):
    await message.reply_text(
        t(message.from_user.id, "settings_title"),
        reply_markup=build_private_panel(message.from_user.id)
    )


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/lang en` or `/lang bn`")
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only `en` or `bn`.")
    set_user_language(message.from_user.id, lang)
    await message.reply_text(t(message.from_user.id, f"lang_set_{lang}"))


@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message: Message):
    if not is_bot_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))
    await message.reply_text("🧩 **Admin Panel**", reply_markup=build_admin_panel())

# =========================================================
# 11) Group settings command with panel
# =========================================================
@bot.on_message(filters.command("settings") & filters.group)
async def group_settings_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    if not await bot_has_required_rights(client, message.chat.id):
        return await message.reply_text("⚠️ First make the bot admin in this group.")
    await message.reply_text(
        render_group_settings_text(message.chat.id),
        reply_markup=build_group_settings_panel(message.chat.id)
    )

# =========================================================
# 12) Callback panels
# =========================================================
@bot.on_callback_query()
async def callback_handler(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data

    if data == "panel_language":
        await cq.message.edit_text(
            t(user_id, "settings_title"),
            reply_markup=build_private_panel(user_id)
        )
        return await cq.answer()

    if data == "panel_help":
        await cq.message.edit_text(t(user_id, "help_text"), reply_markup=build_private_panel(user_id))
        return await cq.answer()

    if data == "panel_userinfo":
        txt = (
            f"👤 **User Panel**\n\n"
            f"User ID: `{user_id}`\n"
            f"Language: `{get_user_language(user_id)}`\n"
            f"Bot Admin: `{is_bot_admin(user_id)}`\n"
            f"Owner: `{is_owner(user_id)}`"
        )
        await cq.message.edit_text(txt, reply_markup=build_private_panel(user_id))
        return await cq.answer()

    if data == "panel_admin":
        if not is_bot_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        await cq.message.edit_text("🧩 **Admin Panel**", reply_markup=build_admin_panel())
        return await cq.answer()

    if data.startswith("setlang_"):
        lang = data.split("_", 1)[1]
        set_user_language(user_id, lang)
        await cq.message.edit_text(
            t(user_id, "settings_title"),
            reply_markup=build_private_panel(user_id)
        )
        return await cq.answer("Updated")

    if data == "admin_stats":
        if not is_bot_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        txt = (
            f"📊 **Stats**\n\n"
            f"Users: `{safe_total_users()}`\n"
            f"Deleted: `{state['deleted_messages']}`\n"
            f"Muted: `{state['muted_users']}`\n"
            f"Banned: `{state['banned_users']}`\n"
            f"Maintenance: `{state['maintenance_mode']}`"
        )
        await cq.message.edit_text(txt, reply_markup=build_admin_panel())
        return await cq.answer()

    if data == "admin_users":
        if not is_bot_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        rows = latest_users(10)
        txt = "👥 **Latest Users**\n\n"
        if not rows:
            txt += "No users yet."
        else:
            for uid, username, first_name, last_seen in rows:
                txt += f"`{uid}` | @{username or '-'} | {first_name or '-'} | {last_seen}\n"
        await cq.message.edit_text(txt, reply_markup=build_admin_panel())
        return await cq.answer()

    if data == "admin_maint":
        if not is_owner(user_id):
            return await cq.answer("Owner only", show_alert=True)
        state["maintenance_mode"] = not state["maintenance_mode"]
        await cq.message.edit_text(
            f"🛠 Maintenance: `{state['maintenance_mode']}`",
            reply_markup=build_admin_panel()
        )
        return await cq.answer("Toggled")

    if data == "admin_logs":
        if not is_bot_admin(user_id):
            return await cq.answer("Admin only", show_alert=True)
        txt = "\n".join(recent_logs[-20:]) if recent_logs else "No logs."
        if len(txt) > 3900:
            txt = txt[-3900:]
        await cq.message.edit_text(f"📜 **Recent Logs**\n\n`{txt}`", reply_markup=build_admin_panel())
        return await cq.answer()

    if data.startswith("gs_"):
        parts = data.split("_")
        if len(parts) != 3:
            return await cq.answer("Invalid action", show_alert=True)

        _, key, chat_id_raw = parts
        try:
            chat_id = int(chat_id_raw)
        except ValueError:
            return await cq.answer("Invalid chat", show_alert=True)

        if not await is_group_admin(client, chat_id, user_id):
            return await cq.answer("Group admin only", show_alert=True)

        s = get_group_settings(chat_id)
        if key == "welcome":
            update_group_setting(chat_id, "welcome_enabled", 0 if s["welcome_enabled"] else 1)
        elif key == "link":
            update_group_setting(chat_id, "link_lock", 0 if s["link_lock"] else 1)
        elif key == "media":
            update_group_setting(chat_id, "media_lock", 0 if s["media_lock"] else 1)
        elif key == "night":
            update_group_setting(chat_id, "night_mode", 0 if s["night_mode"] else 1)
        else:
            return await cq.answer("Unknown toggle", show_alert=True)

        await cq.message.edit_text(
            render_group_settings_text(chat_id),
            reply_markup=build_group_settings_panel(chat_id)
        )
        return await cq.answer("Updated")

# =========================================================
# 13) Welcome
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
        except Exception:
            pass

        await log_to_channel(
            client,
            message.chat.id,
            f"👤 New member joined\nChat: {message.chat.title}\nUser: {user.id} | {user.first_name}"
        )

# =========================================================
# 14) Public group commands
# =========================================================
@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    await message.reply_text(get_group_settings(message.chat.id)["rules_text"])


@bot.on_message(filters.command("gsettings") & filters.group)
async def gsettings_cmd(client, message: Message):
    await message.reply_text(render_group_settings_text(message.chat.id))


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
# 15) Admin group commands
# =========================================================
async def require_group_admin(client: Client, message: Message) -> bool:
    return bool(message.from_user and await is_group_admin(client, message.chat.id, message.from_user.id))


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
    await message.reply_text("✅ Welcome message updated.")


@bot.on_message(filters.command("welcome") & filters.group)
async def toggle_welcome_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2 or parts[1].strip().lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /welcome on|off")
    update_group_setting(message.chat.id, "welcome_enabled", 1 if parts[1].strip().lower() == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("forcesub") & filters.group)
async def forcesub_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage:\n/forcesub @channel\n/forcesub -1001234567890\n/forcesub off")
    value = parts[1].strip()
    if value.lower() == "off":
        update_group_setting(message.chat.id, "force_sub_channel", "")
        return await message.reply_text("✅ Force-sub disabled.")
    update_group_setting(message.chat.id, "force_sub_channel", value)
    await message.reply_text(f"✅ Force-sub enabled: `{value}`")


@bot.on_message(filters.command("setlog") & filters.group)
async def setlog_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setlog -1001234567890")
    try:
        log_chat_id = int(parts[1].strip())
    except ValueError:
        return await message.reply_text("Invalid channel id.")
    update_group_setting(message.chat.id, "log_channel_id", log_chat_id)
    await message.reply_text("✅ Log channel saved.")


@bot.on_message(filters.command("setwarnlimit") & filters.group)
async def setwarnlimit_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply_text("Usage: /setwarnlimit 3")
    value = max(1, min(int(parts[1]), 20))
    update_group_setting(message.chat.id, "warn_limit", value)
    await message.reply_text(f"✅ Warn limit set to `{value}`")


@bot.on_message(filters.command("setflood") & filters.group)
async def setflood_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        return await message.reply_text("Usage: /setflood 6 10")
    count = max(2, min(int(parts[1]), 50))
    seconds = max(2, min(int(parts[2]), 300))
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
    update_group_setting(message.chat.id, "link_lock", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("lockmedia") & filters.group)
async def lockmedia_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /lockmedia on|off")
    update_group_setting(message.chat.id, "media_lock", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("nightmode") & filters.group)
async def nightmode_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
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
        update_group_setting(message.chat.id, "night_start", start_h)
        update_group_setting(message.chat.id, "night_end", end_h)
        return await message.reply_text(f"🌙 Night mode enabled: `{start_h}:00 - {end_h}:00`")
    await message.reply_text("Usage:\n/nightmode on 0 7\n/nightmode off")


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
    await message.reply_text("✅ Word added.")


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
    await message.reply_text("✅ Word removed.")


@bot.on_message(filters.command("banwords") & filters.group)
async def banwords_cmd(client, message: Message):
    words = get_group_settings(message.chat.id)["banned_words"]
    await message.reply_text("🚫 Banned words:\n" + (", ".join(words) if words else "None"))


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
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
            state["banned_users"] += 1
            await message.reply_text(f"⛔ {target.mention} banned after reaching warn limit.")
        except Exception as e:
            await message.reply_text(f"Warn added, but ban failed: {e}")
    else:
        await message.reply_text(f"⚠️ {target.mention} warned. `{warns}/{settings['warn_limit']}`")


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    warns = max(get_warns(message.chat.id, target.id) - 1, 0)
    set_warns(message.chat.id, target.id, warns)
    await message.reply_text(f"✅ Warn removed. Current warns: `{warns}`")


@bot.on_message(filters.command("mute") & filters.group)
async def mute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    try:
        await client.restrict_chat_member(message.chat.id, target.id, ChatPermissions(can_send_messages=False))
        state["muted_users"] += 1
        await message.reply_text(f"🔇 {target.mention} muted.")
    except Exception as e:
        await message.reply_text(f"❌ Mute failed: {e}")


@bot.on_message(filters.command("unmute") & filters.group)
async def unmute_cmd(client, message: Message):
    if not await require_group_admin(client, message):
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
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or give username/user_id.")
    try:
        await client.ban_chat_member(message.chat.id, target.id)
        state["banned_users"] += 1
        await message.reply_text(f"⛔ {target.mention} banned.")
    except Exception as e:
        await message.reply_text(f"❌ Ban failed: {e}")


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
        await message.reply_text(f"✅ `{uid}` unbanned.")
    except Exception as e:
        await message.reply_text(f"❌ Unban failed: {e}")


@bot.on_message(filters.command("setcmd") & filters.group)
async def setcmd_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /setcmd hello Hello everyone!")
    cmd = parts[1].lstrip("/").lower()
    response = parts[2]
    save_custom_command(message.chat.id, cmd, response)
    await message.reply_text(f"✅ Saved /{cmd}")


@bot.on_message(filters.command("save") & filters.group)
async def save_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 2)
    if len(parts) < 3:
        return await message.reply_text("Usage: /save note_name note_text")
    name = parts[1].strip().lower()
    note_text = parts[2].strip()
    save_note(message.chat.id, name, note_text)
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

# =========================================================
# 16) Auto moderation
# =========================================================
@bot.on_message(filters.group & ~filters.service, group=10)
async def group_protection_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    settings = get_group_settings(message.chat.id)

    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    text = message.text or message.caption or ""

    # Force-sub
    if settings["force_sub_channel"]:
        ok = await check_forcesub_membership(client, settings["force_sub_channel"], message.from_user.id)
        if not ok:
            try:
                await message.delete()
                state["deleted_messages"] += 1
            except Exception:
                pass

            join_target = settings["force_sub_channel"]
            markup = None
            if join_target.startswith("@"):
                markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Join Required Channel", url=f"https://t.me/{join_target.lstrip('@')}")]
                ])

            try:
                await message.reply_text(
                    f"🛑 {message.from_user.mention}, group-এ message দিতে হলে আগে `{join_target}`-এ join করতে হবে.",
                    reply_markup=markup
                )
            except Exception:
                pass
            return

    # Link lock
    if settings["link_lock"] and text_contains_link(text):
        try:
            await message.delete()
            state["deleted_messages"] += 1
            await log_to_channel(client, message.chat.id, f"🔗 Link deleted\nUser: {message.from_user.id}\nChat: {message.chat.title}")
        except Exception:
            pass
        return

    # Media lock
    if settings["media_lock"] and (message.media is not None):
        try:
            await message.delete()
            state["deleted_messages"] += 1
            await log_to_channel(client, message.chat.id, f"🖼 Media deleted\nUser: {message.from_user.id}\nChat: {message.chat.title}")
        except Exception:
            pass
        return

    # Night mode
    if settings["night_mode"] and in_night_mode_window(settings["night_start"], settings["night_end"]):
        try:
            await message.delete()
            state["deleted_messages"] += 1
        except Exception:
            pass
        return

    # Banned words
    hit = text_contains_banned_word(text, settings["banned_words"])
    if hit:
        try:
            await message.delete()
            state["deleted_messages"] += 1
        except Exception:
            pass

        warns = get_warns(message.chat.id, message.from_user.id) + 1
        set_warns(message.chat.id, message.from_user.id, warns)

        if warns >= settings["warn_limit"]:
            try:
                await client.ban_chat_member(message.chat.id, message.from_user.id)
                set_warns(message.chat.id, message.from_user.id, 0)
                state["banned_users"] += 1
            except Exception:
                pass

        await log_to_channel(client, message.chat.id, f"🚫 Banned word: {hit}\nUser: {message.from_user.id}\nWarns: {warns}")
        return

    # Flood
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
            state["muted_users"] += 1
            await message.reply_text(f"🚫 Flood detected. {message.from_user.mention} muted.")
            await log_to_channel(client, message.chat.id, f"🚫 Flood mute\nUser: {message.from_user.id}\nCount: {len(arr)}")
        except Exception as e:
            logger.warning(f"Flood action failed: {e}")

# =========================================================
# 17) Custom command handler
# =========================================================
BUILTIN_COMMANDS = {
    "start", "help", "settings", "lang", "admin",
    "rules", "gsettings", "notes", "getnote",
    "setrules", "setwelcome", "welcome",
    "forcesub", "setlog", "setwarnlimit", "setflood",
    "locklink", "lockmedia", "nightmode",
    "banword", "unbanword", "banwords",
    "warn", "unwarn", "mute", "unmute", "ban", "unban",
    "setcmd", "save", "delnote"
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
# 18) Startup
# =========================================================
async def startup_report():
    if not CFG.owner_id:
        return
    try:
        await bot.send_message(
            CFG.owner_id,
            f"✅ **{CFG.bot_name} Started**\n\nBot is online."
        )
    except Exception:
        pass


async def main_runner():
    init_db()
    threading.Thread(target=run_web_server, daemon=True).start()

    await bot.start()
    logger.info("%s started successfully", CFG.bot_name)

    asyncio.create_task(startup_report())
    await idle()


if __name__ == "__main__":
    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        pass
    except FloodWait as e:
        logger.error("FloodWait: %s", e.value)
    except RPCError as e:
        logger.exception("Telegram RPC error: %s", e)
    except Exception as e:
        logger.exception("Fatal error: %s", e)