import os
import re
import sys
import time
import hmac
import json
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

# =========================================================
# 1) Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("group_master_bot")

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
    auto_delete_seconds: int


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
        secret_key=os.environ.get("SECRET_KEY", "CHANGE_THIS_SECRET_NOW"),
        auto_delete_seconds=max(5, int(os.environ.get("AUTO_DELETE_SECONDS", "25")))
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
runtime = {
    "bot_username": "",
    "bot_id": 0,
}


@app.route("/")
def home():
    return "✅ Group moderation bot is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "maintenance": state["maintenance_mode"],
        "success_actions": state["success_actions"],
        "failed_actions": state["failed_actions"],
        "bot_username": runtime["bot_username"],
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
# 5) Bot
# =========================================================
bot = Client(
    "group_master_bot",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token,
)

# =========================================================
# 6) Runtime maps
# =========================================================
flood_tracker: Dict[Tuple[int, int], List[float]] = {}
panel_sessions: Dict[Tuple[int, int], Dict] = {}
pending_input_sessions: Dict[int, Dict] = {}

# =========================================================
# 7) DB
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
        CREATE TABLE IF NOT EXISTS group_registry (
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            added_at INTEGER NOT NULL DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS group_settings (
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',

            rules_text TEXT NOT NULL DEFAULT '📜 No rules set yet.',
            welcome_enabled INTEGER NOT NULL DEFAULT 1,
            welcome_text TEXT NOT NULL DEFAULT '👋 Welcome, {mention}!',
            goodbye_enabled INTEGER NOT NULL DEFAULT 0,
            goodbye_text TEXT NOT NULL DEFAULT '👋 Goodbye, {name}.',

            warn_limit INTEGER NOT NULL DEFAULT 3,
            warn_action TEXT NOT NULL DEFAULT 'mute',
            warn_mute_minutes INTEGER NOT NULL DEFAULT 60,

            flood_limit INTEGER NOT NULL DEFAULT 6,
            flood_window_sec INTEGER NOT NULL DEFAULT 10,

            link_lock INTEGER NOT NULL DEFAULT 0,
            media_lock INTEGER NOT NULL DEFAULT 0,
            media_action TEXT NOT NULL DEFAULT 'delete',

            telegram_link_block INTEGER NOT NULL DEFAULT 0,
            total_link_block INTEGER NOT NULL DEFAULT 0,
            forwarding_block INTEGER NOT NULL DEFAULT 0,
            quote_block INTEGER NOT NULL DEFAULT 0,

            approval_mode INTEGER NOT NULL DEFAULT 0,
            night_mode INTEGER NOT NULL DEFAULT 0,
            night_mode_action TEXT NOT NULL DEFAULT 'silence',
            night_start INTEGER NOT NULL DEFAULT 0,
            night_end INTEGER NOT NULL DEFAULT 7,

            command_delete INTEGER NOT NULL DEFAULT 0,
            service_delete INTEGER NOT NULL DEFAULT 0,
            edit_checks INTEGER NOT NULL DEFAULT 0,

            admin_tag_founder INTEGER NOT NULL DEFAULT 0,
            admin_tag_admins INTEGER NOT NULL DEFAULT 0,

            banned_words TEXT NOT NULL DEFAULT '',
            force_sub_channel TEXT NOT NULL DEFAULT '',
            log_channel_id INTEGER NOT NULL DEFAULT 0,
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
        CREATE TABLE IF NOT EXISTS approved_users (
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            approved_at INTEGER NOT NULL,
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


def list_registered_groups(limit: int = 50) -> List[tuple]:
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
            SELECT title, rules_text, welcome_enabled, welcome_text, goodbye_enabled, goodbye_text,
                   warn_limit, warn_action, warn_mute_minutes,
                   flood_limit, flood_window_sec,
                   link_lock, media_lock, media_action,
                   telegram_link_block, total_link_block, forwarding_block, quote_block,
                   approval_mode, night_mode, night_mode_action, night_start, night_end,
                   command_delete, service_delete, edit_checks,
                   admin_tag_founder, admin_tag_admins,
                   banned_words, force_sub_channel, log_channel_id, language
            FROM group_settings WHERE chat_id = ?
        """, (chat_id,))
        row = cur.fetchone()
        return {
            "title": row[0],
            "rules_text": row[1],
            "welcome_enabled": bool(row[2]),
            "welcome_text": row[3],
            "goodbye_enabled": bool(row[4]),
            "goodbye_text": row[5],
            "warn_limit": int(row[6]),
            "warn_action": row[7],
            "warn_mute_minutes": int(row[8]),
            "flood_limit": int(row[9]),
            "flood_window_sec": int(row[10]),
            "link_lock": bool(row[11]),
            "media_lock": bool(row[12]),
            "media_action": row[13],
            "telegram_link_block": bool(row[14]),
            "total_link_block": bool(row[15]),
            "forwarding_block": bool(row[16]),
            "quote_block": bool(row[17]),
            "approval_mode": bool(row[18]),
            "night_mode": bool(row[19]),
            "night_mode_action": row[20],
            "night_start": int(row[21]),
            "night_end": int(row[22]),
            "command_delete": bool(row[23]),
            "service_delete": bool(row[24]),
            "edit_checks": bool(row[25]),
            "admin_tag_founder": bool(row[26]),
            "admin_tag_admins": bool(row[27]),
            "banned_words": [w.strip().lower() for w in row[28].split(",") if w.strip()],
            "force_sub_channel": row[29].strip(),
            "log_channel_id": int(row[30]),
            "language": row[31] if row[31] in {"en", "bn"} else "en",
        }


def update_group_setting(chat_id: int, field: str, value):
    allowed = {
        "title", "rules_text", "welcome_enabled", "welcome_text", "goodbye_enabled", "goodbye_text",
        "warn_limit", "warn_action", "warn_mute_minutes",
        "flood_limit", "flood_window_sec",
        "link_lock", "media_lock", "media_action",
        "telegram_link_block", "total_link_block", "forwarding_block", "quote_block",
        "approval_mode", "night_mode", "night_mode_action", "night_start", "night_end",
        "command_delete", "service_delete", "edit_checks",
        "admin_tag_founder", "admin_tag_admins",
        "banned_words", "force_sub_channel", "log_channel_id", "language"
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
            SELECT user_id, warns FROM group_warns
            WHERE chat_id = ? AND warns > 0
            ORDER BY warns DESC, user_id ASC
            LIMIT ?
        """, (chat_id, limit))
        return cur.fetchall()


def approve_user(chat_id: int, user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT OR REPLACE INTO approved_users(chat_id, user_id, approved_at)
            VALUES(?, ?, ?)
        """, (chat_id, user_id, int(time.time())))
        conn.commit()


def unapprove_user(chat_id: int, user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM approved_users WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        conn.commit()


def is_approved_user(chat_id: int, user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM approved_users WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        return cur.fetchone() is not None


def list_approved_users(chat_id: int, limit: int = 30) -> List[int]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id FROM approved_users
            WHERE chat_id = ?
            ORDER BY approved_at DESC
            LIMIT ?
        """, (chat_id, limit))
        return [r[0] for r in cur.fetchall()]


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
# 8) Helpers
# =========================================================
def is_root_admin(user_id: int) -> bool:
    return user_id in CFG.admins


def get_bot_username() -> str:
    return runtime.get("bot_username", "") or ""


def safe_bool_icon(v: bool) -> str:
    return "✅" if v else "❌"


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


def text_contains_link(text: str) -> bool:
    return bool(text and re.search(r"(https?://|www\.|t\.me/)", text, re.I))


def text_contains_telegram_link(text: str) -> bool:
    return bool(text and re.search(r"(t\.me/|telegram\.me/)", text, re.I))


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


async def delayed_delete(message: Message, sec: Optional[int] = None):
    try:
        await asyncio.sleep(sec or CFG.auto_delete_seconds)
        await message.delete()
    except Exception:
        pass


async def safe_reply(message: Message, text: str, **kwargs):
    msg = await message.reply_text(text, **kwargs)
    return msg


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
    return sign_payload(f"{uid}:{page}:{chat_id}:{extra}")


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


def store_input_session(user_id: int, action: str, chat_id: int):
    pending_input_sessions[user_id] = {
        "action": action,
        "chat_id": chat_id,
        "created_at": time.time(),
    }


def pop_input_session(user_id: int) -> Optional[dict]:
    sess = pending_input_sessions.get(user_id)
    if not sess:
        return None
    if time.time() - sess["created_at"] > 900:
        pending_input_sessions.pop(user_id, None)
        return None
    pending_input_sessions.pop(user_id, None)
    return sess


# =========================================================
# 9) UI builders
# =========================================================
def build_main_panel(user_id: int) -> InlineKeyboardMarkup:
    username = get_bot_username()
    add_group_url = f"https://t.me/{username}?startgroup=true" if username else "https://t.me"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add me to a Group", url=add_group_url)],
        [InlineKeyboardButton("⚙️ Manage group settings", callback_data=make_cb(user_id, "choose_group"))],
        [InlineKeyboardButton("👥 Group", callback_data=make_cb(user_id, "choose_group")),
         InlineKeyboardButton("ℹ️ Information", callback_data=make_cb(user_id, "info"))],
        [InlineKeyboardButton("🆘 Support", callback_data=make_cb(user_id, "support")),
         InlineKeyboardButton("🌐 Languages", callback_data=make_cb(user_id, "language"))],
    ])


def build_group_list_panel(user_id: int) -> InlineKeyboardMarkup:
    groups = list_registered_groups(50)
    rows = []

    if not groups:
        username = get_bot_username()
        url = f"https://t.me/{username}?startgroup=true" if username else "https://t.me"
        rows.append([InlineKeyboardButton("➕ Add me to a Group", url=url)])
    else:
        for chat_id, title in groups[:25]:
            label = (title[:40] + "…") if len(title) > 40 else (title or str(chat_id))
            rows.append([InlineKeyboardButton(label, callback_data=make_cb(user_id, "group_home", chat_id))])

    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))])
    return InlineKeyboardMarkup(rows)


def build_group_home_panel(user_id: int, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛡 Regulation", callback_data=make_cb(user_id, "regulation", chat_id)),
         InlineKeyboardButton("👋 Welcome", callback_data=make_cb(user_id, "welcome", chat_id))],
        [InlineKeyboardButton("👋 Goodbye", callback_data=make_cb(user_id, "goodbye", chat_id)),
         InlineKeyboardButton("👮 Admin", callback_data=make_cb(user_id, "admin", chat_id))],
        [InlineKeyboardButton("🖼 Media", callback_data=make_cb(user_id, "media", chat_id)),
         InlineKeyboardButton("⚠️ Warns", callback_data=make_cb(user_id, "warns", chat_id))],
        [InlineKeyboardButton("🚫 Anti-Spam", callback_data=make_cb(user_id, "antispam", chat_id)),
         InlineKeyboardButton("🌊 Anti-Flood", callback_data=make_cb(user_id, "antiflood", chat_id))],
        [InlineKeyboardButton("⛔ Blocks", callback_data=make_cb(user_id, "blocks", chat_id)),
         InlineKeyboardButton("🌙 Night", callback_data=make_cb(user_id, "night", chat_id))],
        [InlineKeyboardButton("🔗 Link", callback_data=make_cb(user_id, "links", chat_id)),
         InlineKeyboardButton("👍 Approval", callback_data=make_cb(user_id, "approval", chat_id))],
        [InlineKeyboardButton("🗑 Deleting", callback_data=make_cb(user_id, "deleting", chat_id)),
         InlineKeyboardButton("🌐 Lang", callback_data=make_cb(user_id, "lang_group", chat_id))],
        [InlineKeyboardButton("⚙️ Other", callback_data=make_cb(user_id, "other", chat_id)),
         InlineKeyboardButton("📝 Notes", callback_data=make_cb(user_id, "notes_panel", chat_id))],
        [InlineKeyboardButton("❌ Close", callback_data=make_cb(user_id, "close_panel", chat_id)),
         InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "choose_group"))]
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


def build_panel_text(user_id: int, page: str, chat_id: int = 0) -> str:
    if page == "home":
        return "🤖 **Moderation Control Panel**\n\nChoose an option below."

    if page == "choose_group":
        return "Select a group below."

    if page == "info":
        return (
            "ℹ️ **Information**\n\n"
            "This bot provides nested admin panels, welcome/goodbye system, warns, notes, "
            "custom commands, locks, anti-flood, force-sub and safer callback-based controls."
        )

    if page == "support":
        return (
            "🆘 **Support**\n\n"
            "Use the owner contact/support group you manage. "
            "This panel is informational only."
        )

    if page == "language":
        return "🌐 **Choose your personal language**"

    if page == "close_panel":
        return "Panel closed."

    if chat_id:
        s = get_group_settings(chat_id)
        title = s["title"] or f"Chat {chat_id}"

        if page == "group_home":
            return f"⚙️ **{title}**\n\nManage your group settings."
        if page == "regulation":
            return f"🛡 **Regulation**\n\nRules:\n{s['rules_text']}"
        if page == "welcome":
            return f"👋 **Welcome**\n\nEnabled: `{s['welcome_enabled']}`\n\n{s['welcome_text']}"
        if page == "goodbye":
            return f"👋 **Goodbye**\n\nEnabled: `{s['goodbye_enabled']}`\n\n{s['goodbye_text']}"
        if page == "admin":
            return (
                f"👮 **Admin**\n\n"
                f"Tag Founder: `{s['admin_tag_founder']}`\n"
                f"Tag Admins: `{s['admin_tag_admins']}`"
            )
        if page == "media":
            return (
                f"🖼 **Media**\n\n"
                f"Media Lock: `{s['media_lock']}`\n"
                f"Action: `{s['media_action']}`"
            )
        if page == "warns":
            return (
                f"⚠️ **Warns**\n\n"
                f"Limit: `{s['warn_limit']}`\n"
                f"Action: `{s['warn_action']}`\n"
                f"Mute Minutes: `{s['warn_mute_minutes']}`"
            )
        if page == "antispam":
            return (
                f"🚫 **Anti-Spam**\n\n"
                f"Telegram Links: `{s['telegram_link_block']}`\n"
                f"Total Links: `{s['total_link_block']}`\n"
                f"Forwarding: `{s['forwarding_block']}`\n"
                f"Quote: `{s['quote_block']}`"
            )
        if page == "antiflood":
            return (
                f"🌊 **Anti-Flood**\n\n"
                f"Limit: `{s['flood_limit']}`\n"
                f"Window: `{s['flood_window_sec']}` sec"
            )
        if page == "blocks":
            return f"⛔ **Blacklist**\n\nCount: `{len(s['banned_words'])}`"
        if page == "night":
            return (
                f"🌙 **Night Mode**\n\n"
                f"Enabled: `{s['night_mode']}`\n"
                f"Action: `{s['night_mode_action']}`\n"
                f"Hours: `{s['night_start']}:00 - {s['night_end']}:00`"
            )
        if page == "links":
            return (
                f"🔗 **Links**\n\n"
                f"Link Lock: `{s['link_lock']}`\n"
                f"Telegram Links: `{s['telegram_link_block']}`\n"
                f"Total Links: `{s['total_link_block']}`"
            )
        if page == "approval":
            approved = list_approved_users(chat_id, 30)
            return (
                f"👍 **Approval Mode**\n\n"
                f"Enabled: `{s['approval_mode']}`\n"
                f"Approved users shown: `{len(approved)}`"
            )
        if page == "deleting":
            return (
                f"🗑 **Deleting**\n\n"
                f"Delete Commands: `{s['command_delete']}`\n"
                f"Delete Service Messages: `{s['service_delete']}`\n"
                f"Edit Checks: `{s['edit_checks']}`"
            )
        if page == "lang_group":
            return f"🌐 **Group Language**\n\nCurrent: `{s['language']}`"
        if page == "other":
            return (
                f"⚙️ **Other**\n\n"
                f"ForceSub: `{s['force_sub_channel'] or 'off'}`\n"
                f"Log Channel: `{s['log_channel_id']}`"
            )
        if page == "notes_panel":
            notes = list_notes(chat_id)
            return "📝 **Notes**\n\n" + ("\n".join(f"- `{x}`" for x in notes) if notes else "No notes yet.")

    return "Panel"


async def render_page(client: Client, cq: CallbackQuery, user_id: int, page: str, chat_id: int = 0):
    text = build_panel_text(user_id, page, chat_id)

    if page == "home":
        kb = build_main_panel(user_id)

    elif page == "choose_group":
        kb = build_group_list_panel(user_id)

    elif page in {"info", "support"}:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]])

    elif page == "language":
        lang = get_user_language(user_id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data=make_cb(user_id, "setlang", 0, "en")),
             InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data=make_cb(user_id, "setlang", 0, "bn"))],
            [InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]
        ])

    elif page == "group_home":
        kb = build_group_home_panel(user_id, chat_id)

    elif page == "regulation":
        kb = build_toggle_panel(user_id, "regulation_act", chat_id, [
            [("📝 Set Rules", "set_rules"), ("👀 View", "view_rules")]
        ])

    elif page == "welcome":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "welcome_act", chat_id, [
            [(f"{safe_bool_icon(s['welcome_enabled'])} Toggle", "toggle")],
            [("📝 Set Text", "set_text"), ("👀 Preview", "preview")]
        ])

    elif page == "goodbye":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "goodbye_act", chat_id, [
            [(f"{safe_bool_icon(s['goodbye_enabled'])} Toggle", "toggle")],
            [("📝 Set Text", "set_text"), ("👀 Preview", "preview")]
        ])

    elif page == "admin":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "admin_act", chat_id, [
            [(f"{safe_bool_icon(s['admin_tag_founder'])} Tag Founder", "toggle_founder")],
            [(f"{safe_bool_icon(s['admin_tag_admins'])} Tag Admins", "toggle_admins")]
        ])

    elif page == "media":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "media_act", chat_id, [
            [(f"{safe_bool_icon(s['media_lock'])} Media Lock", "toggle_lock")],
            [("🗑 Delete", "action_delete"), ("🔇 Mute", "action_mute")]
        ])

    elif page == "warns":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "warns_act", chat_id, [
            [("🚫 Off", "off"), ("👢 Kick", "kick"), ("🔇 Mute", "mute"), ("⛔ Ban", "ban")],
            [("➕ Limit", "limit_plus"), ("➖ Limit", "limit_minus")],
            [("⏱ +10m", "mute_plus"), ("⏱ -10m", "mute_minus")],
            [("📋 Warned List", "list")]
        ])

    elif page == "antispam":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "antispam_act", chat_id, [
            [(f"{safe_bool_icon(s['telegram_link_block'])} Telegram Links", "toggle_tg")],
            [(f"{safe_bool_icon(s['total_link_block'])} Total Links", "toggle_total")],
            [(f"{safe_bool_icon(s['forwarding_block'])} Forwarding", "toggle_forward")],
            [(f"{safe_bool_icon(s['quote_block'])} Quote", "toggle_quote")]
        ])

    elif page == "antiflood":
        kb = build_toggle_panel(user_id, "antiflood_act", chat_id, [
            [("➕ Limit", "limit_plus"), ("➖ Limit", "limit_minus")],
            [("➕ Window", "window_plus"), ("➖ Window", "window_minus")]
        ])

    elif page == "blocks":
        kb = build_toggle_panel(user_id, "blocks_act", chat_id, [
            [("➕ Add Word", "add"), ("➖ Remove Word", "remove")],
            [("📋 Show", "show")]
        ])

    elif page == "night":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "night_act", chat_id, [
            [(f"{safe_bool_icon(s['night_mode'])} Toggle", "toggle")],
            [("🗑 Delete", "set_delete"), ("🔇 Silence", "set_silence")],
            [("Start +1", "start_plus"), ("Start -1", "start_minus")],
            [("End +1", "end_plus"), ("End -1", "end_minus")]
        ])

    elif page == "links":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "links_act", chat_id, [
            [(f"{safe_bool_icon(s['link_lock'])} Link Lock", "toggle_lock")],
            [(f"{safe_bool_icon(s['telegram_link_block'])} Telegram Links", "toggle_tg")],
            [(f"{safe_bool_icon(s['total_link_block'])} Total Links", "toggle_total")]
        ])

    elif page == "approval":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "approval_act", chat_id, [
            [(f"{safe_bool_icon(s['approval_mode'])} Toggle", "toggle")],
            [("✅ Approve User", "approve_user"), ("❌ Unapprove User", "unapprove_user")],
            [("📋 Approved List", "list")]
        ])

    elif page == "deleting":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "deleting_act", chat_id, [
            [(f"{safe_bool_icon(s['command_delete'])} Delete Commands", "toggle_command")],
            [(f"{safe_bool_icon(s['service_delete'])} Delete Service", "toggle_service")],
            [(f"{safe_bool_icon(s['edit_checks'])} Edit Checks", "toggle_edit")]
        ])

    elif page == "lang_group":
        s = get_group_settings(chat_id)
        kb = build_toggle_panel(user_id, "lang_group_act", chat_id, [
            [(f"{'✅ ' if s['language']=='en' else ''}English", "set_en"),
             (f"{'✅ ' if s['language']=='bn' else ''}বাংলা", "set_bn")]
        ])

    elif page == "other":
        kb = build_toggle_panel(user_id, "other_act", chat_id, [
            [("📢 Set ForceSub", "set_forcesub"), ("❌ Disable ForceSub", "disable_forcesub")],
            [("📝 Set Log Channel", "set_log"), ("📄 Show Log", "show_log")]
        ])

    elif page == "notes_panel":
        kb = build_toggle_panel(user_id, "notes_act", chat_id, [
            [("➕ Add Note", "add"), ("📋 Show", "show")],
            [("➖ Delete Note", "delete")]
        ])

    elif page == "close_panel":
        kb = None

    else:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=make_cb(user_id, "home"))]])

    await cq.message.edit_text(text, reply_markup=kb)


# =========================================================
# 10) Auto actions
# =========================================================
async def apply_warn_action(client: Client, chat_id: int, user_id: int, settings: dict, warns: int):
    if warns < settings["warn_limit"]:
        return

    action = settings["warn_action"]
    try:
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
    except Exception as e:
        logger.warning(f"apply_warn_action failed: {e}")


# =========================================================
# 11) Commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)

    if is_blocked_user(user.id):
        return await message.reply_text("🚫 You are blocked from using this bot.")
    if state["maintenance_mode"] and not is_root_admin(user.id):
        return await message.reply_text("🛠️ Bot is under maintenance.")

    await message.reply_text(
        "⚡ **Welcome!**\n\nUse the panel below.",
        reply_markup=build_main_panel(user.id)
    )


@bot.on_message(filters.command("panel") & filters.private)
async def panel_cmd(client, message: Message):
    await message.reply_text("🤖 **Moderation Control Panel**", reply_markup=build_main_panel(message.from_user.id))


@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client, message: Message):
    lang = get_user_language(message.from_user.id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data=make_cb(message.from_user.id, "setlang", 0, "en")),
         InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data=make_cb(message.from_user.id, "setlang", 0, "bn"))],
        [InlineKeyboardButton("⬅️ Back", callback_data=make_cb(message.from_user.id, "home"))]
    ])
    await message.reply_text(f"🌐 Personal language: `{lang}`", reply_markup=kb)


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message: Message):
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"en", "bn"}:
        return await message.reply_text("Usage: /lang en অথবা /lang bn")
    set_user_language(message.from_user.id, parts[1].lower())
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("help"))
async def help_cmd(client, message: Message):
    await message.reply_text(
        "**Main commands**\n"
        "/start\n/panel\n/settings\n/rules\n/notes\n/getnote <name>\n"
        "/warn /unwarn /mute /unmute /ban /unban\n"
        "/approve /unapprove\n"
        "/setrules /setwelcome /setgoodbye\n"
        "/forcesub /setlog /save /delnote /setcmd\n"
    )


@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    register_group(message.chat.id, message.chat.title or "")
    s = get_group_settings(message.chat.id)
    await message.reply_text(s["rules_text"])


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


async def require_group_admin(client: Client, message: Message) -> bool:
    return bool(message.from_user and await is_group_admin(client, message.chat.id, message.from_user.id))


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


@bot.on_message(filters.command("setrules") & filters.group)
async def setrules_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /setrules your rules")
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
async def welcome_toggle_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /welcome on|off")
    update_group_setting(message.chat.id, "welcome_enabled", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("goodbye") & filters.group)
async def goodbye_toggle_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        return await message.reply_text("Usage: /goodbye on|off")
    update_group_setting(message.chat.id, "goodbye_enabled", 1 if parts[1].lower() == "on" else 0)
    await message.reply_text("✅ Updated.")


@bot.on_message(filters.command("setlog") & filters.group)
async def setlog_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split()
    if len(parts) < 2:
        return await message.reply_text("Usage: /setlog -1001234567890")
    try:
        update_group_setting(message.chat.id, "log_channel_id", int(parts[1]))
        await message.reply_text("✅ Log channel saved.")
    except Exception:
        await message.reply_text("❌ Invalid channel id.")


@bot.on_message(filters.command("forcesub") & filters.group)
async def forcesub_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    parts = message.text.split(None, 1)
    if len(parts) < 2:
        return await message.reply_text("Usage: /forcesub @channel or /forcesub off")
    value = parts[1].strip()
    if value.lower() == "off":
        update_group_setting(message.chat.id, "force_sub_channel", "")
        return await message.reply_text("✅ ForceSub disabled.")
    update_group_setting(message.chat.id, "force_sub_channel", value)
    await message.reply_text("✅ ForceSub updated.")


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
        return await message.reply_text("Usage: /setcmd hello Hello everyone")
    cmd = parts[1].strip().lstrip("/").lower()
    save_custom_command(message.chat.id, cmd, parts[2].strip())
    await message.reply_text(f"✅ Saved custom /{cmd}")


@bot.on_message(filters.command("approve") & filters.group)
async def approve_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide user id.")
    approve_user(message.chat.id, target.id)
    await message.reply_text(f"✅ Approved {target.mention}")


@bot.on_message(filters.command("unapprove") & filters.group)
async def unapprove_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide user id.")
    unapprove_user(message.chat.id, target.id)
    await message.reply_text(f"✅ Unapproved {target.mention}")


@bot.on_message(filters.command("approved") & filters.group)
async def approved_cmd(client, message: Message):
    ids = list_approved_users(message.chat.id, 30)
    txt = "✅ Approved Users\n\n" + ("\n".join(f"`{x}`" for x in ids) if ids else "Empty.")
    await message.reply_text(txt)


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide user id.")
    s = get_group_settings(message.chat.id)
    warns = get_warns(message.chat.id, target.id) + 1
    set_warns(message.chat.id, target.id, warns)
    await apply_warn_action(client, message.chat.id, target.id, s, warns)
    await message.reply_text(f"⚠️ Warned {target.mention} (`{warns}/{s['warn_limit']}`)")


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not await require_group_admin(client, message):
        return await message.reply_text("Admin only.")
    target = await extract_target_user(client, message)
    if not target:
        return await message.reply_text("Reply to a user or provide user id.")
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
            ChatPermissions(can_send_messages=True, can_send_polls=True, can_invite_users=True)
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
        await client.unban_chat_member(message.chat.id, int(parts[1]))
        await message.reply_text("✅ Unbanned.")
    except Exception as e:
        await message.reply_text(f"Unban failed: {e}")


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
    await message.reply_text("✅ Added.")


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
    await message.reply_text("✅ Removed.")


@bot.on_message(filters.command("banwords") & filters.group)
async def banwords_cmd(client, message: Message):
    s = get_group_settings(message.chat.id)
    await message.reply_text("🚫 Blacklist:\n" + (", ".join(s["banned_words"]) if s["banned_words"] else "Empty."))


@bot.on_message(filters.command("gsettings") & filters.group)
async def gsettings_cmd(client, message: Message):
    s = get_group_settings(message.chat.id)
    txt = (
        f"⚙️ **Group Settings**\n\n"
        f"Welcome: `{s['welcome_enabled']}`\n"
        f"Goodbye: `{s['goodbye_enabled']}`\n"
        f"Warn Limit: `{s['warn_limit']}`\n"
        f"Warn Action: `{s['warn_action']}`\n"
        f"Flood: `{s['flood_limit']}` / `{s['flood_window_sec']}` sec\n"
        f"Link Lock: `{s['link_lock']}`\n"
        f"Media Lock: `{s['media_lock']}`\n"
        f"Approval Mode: `{s['approval_mode']}`\n"
        f"Night: `{s['night_mode']}`\n"
        f"ForceSub: `{s['force_sub_channel'] or 'off'}`\n"
        f"Log: `{s['log_channel_id']}`"
    )
    await message.reply_text(txt)


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
# 12) Group events
# =========================================================
@bot.on_message(filters.new_chat_members)
async def welcome_new_members(client, message: Message):
    if not is_group_chat(message):
        return

    register_group(message.chat.id, message.chat.title or "")
    s = get_group_settings(message.chat.id)

    for user in message.new_chat_members:
        if s["welcome_enabled"]:
            text = s["welcome_text"].replace("{mention}", user.mention).replace("{name}", user.first_name or "User")
            try:
                await message.reply_text(text)
            except Exception:
                pass
        await log_to_channel(client, message.chat.id, f"👤 Joined\nChat: {message.chat.title}\nUser: {user.id}")


@bot.on_message(filters.left_chat_member)
async def goodbye_left_member(client, message: Message):
    if not is_group_chat(message):
        return
    register_group(message.chat.id, message.chat.title or "")
    s = get_group_settings(message.chat.id)
    user = message.left_chat_member
    if not user:
        return
    if s["goodbye_enabled"]:
        try:
            await message.reply_text(s["goodbye_text"].replace("{name}", user.first_name or "User"))
        except Exception:
            pass
    await log_to_channel(client, message.chat.id, f"👋 Left\nChat: {message.chat.title}\nUser: {user.id}")


@bot.on_message(filters.group)
async def registry_touch(client, message: Message):
    register_group(message.chat.id, message.chat.title or "")
    if message.from_user:
        upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

# =========================================================
# 13) Auto moderation
# =========================================================
@bot.on_message(filters.group & ~filters.service, group=10)
async def group_protection_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return

    register_group(message.chat.id, message.chat.title or "")
    s = get_group_settings(message.chat.id)

    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    # approval mode
    if s["approval_mode"] and not is_approved_user(message.chat.id, message.from_user.id):
        try:
            await message.delete()
            notice = await message.reply_text(
                f"🛑 {message.from_user.mention}, only approved users can speak here."
            )
            asyncio.create_task(delayed_delete(notice))
        except Exception:
            pass
        return

    # force sub
    if s["force_sub_channel"]:
        ok = await check_forcesub_membership(client, s["force_sub_channel"], message.from_user.id)
        if not ok:
            try:
                await message.delete()
            except Exception:
                pass
            btn = None
            if s["force_sub_channel"].startswith("@"):
                btn = InlineKeyboardMarkup([
                    [InlineKeyboardButton("📢 Join Required Channel", url=f"https://t.me/{s['force_sub_channel'].lstrip('@')}")]
                ])
            try:
                notice = await message.reply_text(
                    f"🛑 {message.from_user.mention}, আগে `{s['force_sub_channel']}` channel-এ join করতে হবে.",
                    reply_markup=btn
                )
                asyncio.create_task(delayed_delete(notice))
            except Exception:
                pass
            return

    text = message.text or message.caption or ""

    # forwarding
    if s["forwarding_block"] and (message.forward_date or message.forward_from or message.forward_sender_name):
        try:
            await message.delete()
        except Exception:
            pass
        return

    # quote block
    if s["quote_block"] and message.reply_to_message:
        try:
            await message.delete()
        except Exception:
            pass
        return

    # link policies
    if s["telegram_link_block"] and text_contains_telegram_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["total_link_block"] and text_contains_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["link_lock"] and text_contains_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    # media lock
    if s["media_lock"] and (message.media is not None):
        try:
            await message.delete()
            if s["media_action"] == "mute":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
        except Exception:
            pass
        return

    # night mode
    if s["night_mode"] and in_night_mode_window(s["night_start"], s["night_end"]):
        try:
            await message.delete()
            if s["night_mode_action"] == "silence":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
        except Exception:
            pass
        return

    # banned words
    hit = text_contains_banned_word(text, s["banned_words"])
    if hit:
        try:
            await message.delete()
        except Exception:
            pass
        warns = get_warns(message.chat.id, message.from_user.id) + 1
        set_warns(message.chat.id, message.from_user.id, warns)
        await apply_warn_action(client, message.chat.id, message.from_user.id, s, warns)
        await log_to_channel(client, message.chat.id, f"🚫 Banned word hit: {hit}\nUser: {message.from_user.id}\nWarns: {warns}")
        return

    # anti-flood
    key = (message.chat.id, message.from_user.id)
    now = time.time()
    arr = flood_tracker.get(key, [])
    arr = [x for x in arr if now - x <= s["flood_window_sec"]]
    arr.append(now)
    flood_tracker[key] = arr

    if len(arr) >= s["flood_limit"]:
        try:
            await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
            await log_to_channel(client, message.chat.id, f"🌊 Flood mute\nUser: {message.from_user.id}\nCount: {len(arr)}")
        except Exception as e:
            logger.warning(f"Flood action failed: {e}")


@bot.on_message(filters.command(BUILTIN_COMMANDS := [
    "start", "help", "panel", "settings", "lang", "rules", "notes", "getnote",
    "setrules", "setwelcome", "setgoodbye", "welcome", "goodbye", "setlog",
    "forcesub", "save", "delnote", "setcmd", "approve", "unapprove", "approved",
    "warn", "unwarn", "mute", "unmute", "ban", "unban", "banword", "unbanword",
    "banwords", "gsettings", "blockuser", "unblockuser"
]) & filters.group, group=15)
async def delete_command_messages_if_enabled(client, message: Message):
    s = get_group_settings(message.chat.id)
    if s["command_delete"]:
        try:
            await asyncio.sleep(2)
            await message.delete()
        except Exception:
            pass


@bot.on_edited_message(filters.group, group=16)
async def edited_message_handler(client, message: Message):
    s = get_group_settings(message.chat.id)
    if not s["edit_checks"]:
        return
    if not message.from_user or message.from_user.is_bot:
        return
    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return
    text = message.text or message.caption or ""
    if text_contains_link(text) or text_contains_banned_word(text, s["banned_words"]):
        try:
            await message.delete()
        except Exception:
            pass

# =========================================================
# 14) Custom commands
# =========================================================
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
# 15) Callback handling
# =========================================================
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

        if page in {"home", "choose_group", "info", "support", "language"}:
            return await render_page(client, cq, user_id, page)

        if page == "close_panel":
            await cq.message.edit_text("Panel closed.")
            return await cq.answer("Closed")

        if chat_id and not await is_group_admin(client, chat_id, user_id):
            return await cq.answer("You are not admin in this group.", show_alert=True)

        if page in {
            "group_home", "regulation", "welcome", "goodbye", "admin", "media",
            "warns", "antispam", "antiflood", "blocks", "night", "links",
            "approval", "deleting", "lang_group", "other", "notes_panel"
        }:
            return await render_page(client, cq, user_id, page, chat_id)

        if page == "regulation_act":
            if extra == "set_rules":
                store_input_session(user_id, "set_rules", chat_id)
                return await cq.answer("Send new rules in next message.", show_alert=True)
            if extra == "view_rules":
                await cq.message.reply_text(get_group_settings(chat_id)["rules_text"])
                return await cq.answer("Rules sent.")

        if page == "welcome_act":
            s = get_group_settings(chat_id)
            if extra == "toggle":
                update_group_setting(chat_id, "welcome_enabled", 0 if s["welcome_enabled"] else 1)
            elif extra == "set_text":
                store_input_session(user_id, "set_welcome_text", chat_id)
                return await cq.answer("Send new welcome text.", show_alert=True)
            elif extra == "preview":
                preview = s["welcome_text"].replace("{mention}", cq.from_user.mention).replace("{name}", cq.from_user.first_name or "User")
                await cq.message.reply_text(preview)
            return await render_page(client, cq, user_id, "welcome", chat_id)

        if page == "goodbye_act":
            s = get_group_settings(chat_id)
            if extra == "toggle":
                update_group_setting(chat_id, "goodbye_enabled", 0 if s["goodbye_enabled"] else 1)
            elif extra == "set_text":
                store_input_session(user_id, "set_goodbye_text", chat_id)
                return await cq.answer("Send new goodbye text.", show_alert=True)
            elif extra == "preview":
                preview = s["goodbye_text"].replace("{name}", cq.from_user.first_name or "User")
                await cq.message.reply_text(preview)
            return await render_page(client, cq, user_id, "goodbye", chat_id)

        if page == "admin_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_founder":
                update_group_setting(chat_id, "admin_tag_founder", 0 if s["admin_tag_founder"] else 1)
            elif extra == "toggle_admins":
                update_group_setting(chat_id, "admin_tag_admins", 0 if s["admin_tag_admins"] else 1)
            return await render_page(client, cq, user_id, "admin", chat_id)

        if page == "media_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_lock":
                update_group_setting(chat_id, "media_lock", 0 if s["media_lock"] else 1)
            elif extra == "action_delete":
                update_group_setting(chat_id, "media_action", "delete")
            elif extra == "action_mute":
                update_group_setting(chat_id, "media_action", "mute")
            return await render_page(client, cq, user_id, "media", chat_id)

        if page == "warns_act":
            s = get_group_settings(chat_id)
            if extra in {"off", "kick", "mute", "ban"}:
                update_group_setting(chat_id, "warn_action", extra)
            elif extra == "limit_plus":
                update_group_setting(chat_id, "warn_limit", min(20, s["warn_limit"] + 1))
            elif extra == "limit_minus":
                update_group_setting(chat_id, "warn_limit", max(1, s["warn_limit"] - 1))
            elif extra == "mute_plus":
                update_group_setting(chat_id, "warn_mute_minutes", min(1440, s["warn_mute_minutes"] + 10))
            elif extra == "mute_minus":
                update_group_setting(chat_id, "warn_mute_minutes", max(10, s["warn_mute_minutes"] - 10))
            elif extra == "list":
                rows = get_warned_list(chat_id)
                txt = "📋 Warned List\n\n" + ("\n".join(f"`{uid}` → {w}" for uid, w in rows) if rows else "Empty.")
                await cq.message.reply_text(txt)
            return await render_page(client, cq, user_id, "warns", chat_id)

        if page == "antispam_act":
            s = get_group_settings(chat_id)
            mapping = {
                "toggle_tg": "telegram_link_block",
                "toggle_total": "total_link_block",
                "toggle_forward": "forwarding_block",
                "toggle_quote": "quote_block",
            }
            field = mapping.get(extra)
            if field:
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
            if extra == "add":
                store_input_session(user_id, "add_banned_word", chat_id)
                return await cq.answer("Send the word to add.", show_alert=True)
            elif extra == "remove":
                store_input_session(user_id, "remove_banned_word", chat_id)
                return await cq.answer("Send the word to remove.", show_alert=True)
            elif extra == "show":
                s = get_group_settings(chat_id)
                await cq.message.reply_text("⛔ Blacklist\n\n" + (", ".join(s["banned_words"]) if s["banned_words"] else "Empty."))
            return await render_page(client, cq, user_id, "blocks", chat_id)

        if page == "night_act":
            s = get_group_settings(chat_id)
            if extra == "toggle":
                update_group_setting(chat_id, "night_mode", 0 if s["night_mode"] else 1)
            elif extra == "set_delete":
                update_group_setting(chat_id, "night_mode_action", "delete")
            elif extra == "set_silence":
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
            if extra == "toggle_lock":
                update_group_setting(chat_id, "link_lock", 0 if s["link_lock"] else 1)
            elif extra == "toggle_tg":
                update_group_setting(chat_id, "telegram_link_block", 0 if s["telegram_link_block"] else 1)
            elif extra == "toggle_total":
                update_group_setting(chat_id, "total_link_block", 0 if s["total_link_block"] else 1)
            return await render_page(client, cq, user_id, "links", chat_id)

        if page == "approval_act":
            s = get_group_settings(chat_id)
            if extra == "toggle":
                update_group_setting(chat_id, "approval_mode", 0 if s["approval_mode"] else 1)
            elif extra == "approve_user":
                store_input_session(user_id, "approve_user", chat_id)
                return await cq.answer("Reply না করে এখানে user id বা @username পাঠাও.", show_alert=True)
            elif extra == "unapprove_user":
                store_input_session(user_id, "unapprove_user", chat_id)
                return await cq.answer("Reply না করে এখানে user id বা @username পাঠাও.", show_alert=True)
            elif extra == "list":
                ids = list_approved_users(chat_id, 30)
                await cq.message.reply_text("✅ Approved Users\n\n" + ("\n".join(f"`{x}`" for x in ids) if ids else "Empty."))
            return await render_page(client, cq, user_id, "approval", chat_id)

        if page == "deleting_act":
            s = get_group_settings(chat_id)
            if extra == "toggle_command":
                update_group_setting(chat_id, "command_delete", 0 if s["command_delete"] else 1)
            elif extra == "toggle_service":
                update_group_setting(chat_id, "service_delete", 0 if s["service_delete"] else 1)
            elif extra == "toggle_edit":
                update_group_setting(chat_id, "edit_checks", 0 if s["edit_checks"] else 1)
            return await render_page(client, cq, user_id, "deleting", chat_id)

        if page == "lang_group_act":
            if extra == "set_en":
                update_group_setting(chat_id, "language", "en")
            elif extra == "set_bn":
                update_group_setting(chat_id, "language", "bn")
            return await render_page(client, cq, user_id, "lang_group", chat_id)

        if page == "other_act":
            s = get_group_settings(chat_id)
            if extra == "set_forcesub":
                store_input_session(user_id, "set_forcesub", chat_id)
                return await cq.answer("Send @channel or -100 id.", show_alert=True)
            elif extra == "disable_forcesub":
                update_group_setting(chat_id, "force_sub_channel", "")
            elif extra == "set_log":
                store_input_session(user_id, "set_log_channel", chat_id)
                return await cq.answer("Send log channel id.", show_alert=True)
            elif extra == "show_log":
                await cq.message.reply_text(f"Current log channel: `{s['log_channel_id']}`")
            return await render_page(client, cq, user_id, "other", chat_id)

        if page == "notes_act":
            if extra == "add":
                store_input_session(user_id, "add_note", chat_id)
                return await cq.answer("Send note as: name | text", show_alert=True)
            elif extra == "delete":
                store_input_session(user_id, "delete_note", chat_id)
                return await cq.answer("Send note name to delete.", show_alert=True)
            elif extra == "show":
                notes = list_notes(chat_id)
                await cq.message.reply_text("📝 Notes\n\n" + ("\n".join(f"- `{n}`" for n in notes) if notes else "Empty."))
            return await render_page(client, cq, user_id, "notes_panel", chat_id)

        await cq.answer("Updated.")
    except Exception as e:
        logger.exception("Callback failed")
        state["failed_actions"] += 1
        await cq.answer(f"Error: {e}", show_alert=True)


# =========================================================
# 16) Input session handler
# =========================================================
@bot.on_message(filters.private & filters.text, group=30)
async def private_input_session_handler(client, message: Message):
    if not message.from_user or message.text.startswith("/"):
        return

    sess = pop_input_session(message.from_user.id)
    if not sess:
        return

    action = sess["action"]
    chat_id = sess["chat_id"]
    text = message.text.strip()

    try:
        if action == "set_rules":
            update_group_setting(chat_id, "rules_text", text)
            return await message.reply_text("✅ Rules updated.")

        if action == "set_welcome_text":
            update_group_setting(chat_id, "welcome_text", text)
            return await message.reply_text("✅ Welcome text updated.")

        if action == "set_goodbye_text":
            update_group_setting(chat_id, "goodbye_text", text)
            return await message.reply_text("✅ Goodbye text updated.")

        if action == "add_banned_word":
            s = get_group_settings(chat_id)
            words = set(s["banned_words"])
            words.add(text.lower())
            update_group_setting(chat_id, "banned_words", ",".join(sorted(words)))
            return await message.reply_text("✅ Word added.")

        if action == "remove_banned_word":
            s = get_group_settings(chat_id)
            words = set(s["banned_words"])
            words.discard(text.lower())
            update_group_setting(chat_id, "banned_words", ",".join(sorted(words)))
            return await message.reply_text("✅ Word removed.")

        if action == "set_forcesub":
            update_group_setting(chat_id, "force_sub_channel", text)
            return await message.reply_text("✅ ForceSub updated.")

        if action == "set_log_channel":
            update_group_setting(chat_id, "log_channel_id", int(text))
            return await message.reply_text("✅ Log channel updated.")

        if action == "add_note":
            if "|" not in text:
                return await message.reply_text("Format: name | text")
            name, note_text = [x.strip() for x in text.split("|", 1)]
            save_note(chat_id, name, note_text)
            return await message.reply_text("✅ Note saved.")

        if action == "delete_note":
            del_note(chat_id, text)
            return await message.reply_text("✅ Note deleted.")

        if action in {"approve_user", "unapprove_user"}:
            raw = text.lstrip("@")
            try:
                user = await client.get_users(int(raw) if raw.isdigit() else raw)
            except Exception:
                return await message.reply_text("❌ User not found.")
            if action == "approve_user":
                approve_user(chat_id, user.id)
                return await message.reply_text(f"✅ Approved `{user.id}`")
            else:
                unapprove_user(chat_id, user.id)
                return await message.reply_text(f"✅ Unapproved `{user.id}`")

    except Exception as e:
        return await message.reply_text(f"❌ Failed: {e}")


# =========================================================
# 17) Optional service deletion
# =========================================================
@bot.on_message(filters.service, group=40)
async def service_delete_handler(client, message: Message):
    if not is_group_chat(message):
        return
    s = get_group_settings(message.chat.id)
    if s["service_delete"]:
        try:
            await asyncio.sleep(2)
            await message.delete()
        except Exception:
            pass

# =========================================================
# 18) Startup
# =========================================================
async def startup_report():
    if CFG.owner_id:
        try:
            await bot.send_message(CFG.owner_id, "✅ **Bot Started Successfully**")
        except Exception:
            pass


async def main_runner():
    init_db()
    threading.Thread(target=run_web_server, daemon=True).start()

    await bot.start()
    me = await bot.get_me()
    runtime["bot_username"] = me.username or ""
    runtime["bot_id"] = me.id
    logger.info(f"Bot started as @{runtime['bot_username']}")

    asyncio.create_task(startup_report())
    await idle()


if __name__ == "__main__":
    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        pass