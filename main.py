import os
import re
import sys
import time
import hmac
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
    Message,
)
from pyrogram.enums import ChatMemberStatus

# =========================================================
# Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("group_guard_2")

# =========================================================
# Config
# =========================================================
def env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str
    owner_id: int
    admin_ids: Set[int]
    db_path: str
    port: int
    secret_key: str
    maintenance_mode: bool
    support_channel: str
    support_group: str


def parse_ids(raw: str) -> Set[int]:
    out = set()
    for p in raw.split(","):
        p = p.strip()
        if p.isdigit():
            out.add(int(p))
    return out


def load_config() -> Config:
    required = ["API_ID", "API_HASH", "BOT_TOKEN"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")

    owner_id = int(os.environ.get("OWNER_ID", "0") or 0)
    admins = parse_ids(os.environ.get("ADMIN_IDS", ""))
    if owner_id:
        admins.add(owner_id)

    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],
        owner_id=owner_id,
        admin_ids=admins,
        db_path=os.environ.get("DB_PATH", "group_guard_2.sqlite3"),
        port=int(os.environ.get("PORT", "10000")),
        secret_key=os.environ.get("SECRET_KEY", "CHANGE_THIS_SECRET"),
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        support_channel=os.environ.get("SUPPORT_CHANNEL", "").strip(),
        support_group=os.environ.get("SUPPORT_GROUP", "").strip(),
    )


CFG = load_config()

# =========================================================
# Flask / runtime
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

runtime = {
    "bot_username": "",
    "bot_id": 0,
    "maintenance": CFG.maintenance_mode,
}
pending_inputs: Dict[int, Dict] = {}
flood_tracker: Dict[Tuple[int, int], List[float]] = {}

# =========================================================
# Flask routes
# =========================================================
@app.route("/")
def home():
    return "✅ Group Guard 2.0 is running", 200


@app.route("/healthz")
def healthz():
    return jsonify(
        {
            "ok": True,
            "uptime_sec": round(time.time() - BOOT_TIME, 2),
            "bot_username": runtime["bot_username"],
            "maintenance": runtime["maintenance"],
        }
    )


def run_web_server():
    app.run(host="0.0.0.0", port=CFG.port)

# =========================================================
# Event loop
# =========================================================
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# =========================================================
# Bot
# =========================================================
bot = Client(
    "group_guard_2",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token,
)

# =========================================================
# DB
# =========================================================
def db_connect():
    return sqlite3.connect(CFG.db_path)


def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            blocked INTEGER NOT NULL DEFAULT 0,
            lang TEXT NOT NULL DEFAULT 'en'
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS groups(
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            added_at INTEGER NOT NULL DEFAULT 0
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings(
            chat_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL DEFAULT '',
            lang TEXT NOT NULL DEFAULT 'en',

            rules_text TEXT NOT NULL DEFAULT 'No rules set.',
            rules_cmd_permission TEXT NOT NULL DEFAULT 'all',

            welcome_enabled INTEGER NOT NULL DEFAULT 0,
            welcome_text TEXT NOT NULL DEFAULT 'Welcome {mention}',
            welcome_mode TEXT NOT NULL DEFAULT 'always',
            welcome_delete_last INTEGER NOT NULL DEFAULT 0,

            goodbye_enabled INTEGER NOT NULL DEFAULT 0,
            goodbye_text TEXT NOT NULL DEFAULT 'Goodbye {name}',
            goodbye_private INTEGER NOT NULL DEFAULT 0,
            goodbye_delete_last INTEGER NOT NULL DEFAULT 0,

            captcha_enabled INTEGER NOT NULL DEFAULT 0,

            admin_status TEXT NOT NULL DEFAULT 'founder',
            admin_tag_founder INTEGER NOT NULL DEFAULT 0,
            admin_tag_admins INTEGER NOT NULL DEFAULT 0,

            media_enabled INTEGER NOT NULL DEFAULT 0,
            media_action TEXT NOT NULL DEFAULT 'allow',

            warns_action TEXT NOT NULL DEFAULT 'mute',
            warns_limit INTEGER NOT NULL DEFAULT 3,
            warns_mute_minutes INTEGER NOT NULL DEFAULT 60,

            antispam_tg_links INTEGER NOT NULL DEFAULT 0,
            antispam_forwarding INTEGER NOT NULL DEFAULT 0,
            antispam_quote INTEGER NOT NULL DEFAULT 0,
            antispam_total_links INTEGER NOT NULL DEFAULT 0,

            antiflood_messages INTEGER NOT NULL DEFAULT 5,
            antiflood_seconds INTEGER NOT NULL DEFAULT 3,
            antiflood_action TEXT NOT NULL DEFAULT 'delete',

            alphabet_arabic INTEGER NOT NULL DEFAULT 0,
            alphabet_cyrillic INTEGER NOT NULL DEFAULT 0,
            alphabet_chinese INTEGER NOT NULL DEFAULT 0,
            alphabet_latin INTEGER NOT NULL DEFAULT 0,

            check_at_join INTEGER NOT NULL DEFAULT 1,
            checks_delete_messages INTEGER NOT NULL DEFAULT 0,

            link_enabled INTEGER NOT NULL DEFAULT 0,
            group_link TEXT NOT NULL DEFAULT '',

            approval_enabled INTEGER NOT NULL DEFAULT 0,

            night_enabled INTEGER NOT NULL DEFAULT 0,
            night_delete_medias INTEGER NOT NULL DEFAULT 0,
            night_global_silence INTEGER NOT NULL DEFAULT 0,

            deleting_commands INTEGER NOT NULL DEFAULT 0,
            deleting_global_silence INTEGER NOT NULL DEFAULT 0,
            deleting_edit_checks INTEGER NOT NULL DEFAULT 0,
            deleting_service_messages INTEGER NOT NULL DEFAULT 0,
            deleting_scheduled INTEGER NOT NULL DEFAULT 0,
            deleting_block_cancellation INTEGER NOT NULL DEFAULT 0,
            deleting_all_messages INTEGER NOT NULL DEFAULT 0,
            deleting_self_destruct INTEGER NOT NULL DEFAULT 0,

            min_message_length INTEGER NOT NULL DEFAULT 0,
            max_message_length INTEGER NOT NULL DEFAULT 0,
            short_message_action TEXT NOT NULL DEFAULT 'delete',
            long_message_action TEXT NOT NULL DEFAULT 'delete',

            banned_words TEXT NOT NULL DEFAULT '',
            log_channel_id INTEGER NOT NULL DEFAULT 0,
            force_sub_channel TEXT NOT NULL DEFAULT ''
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS warns(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            warns INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, user_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS approved_users(
            chat_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            approved_at INTEGER NOT NULL,
            PRIMARY KEY(chat_id, user_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS notes(
            chat_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            text TEXT NOT NULL,
            PRIMARY KEY(chat_id, name)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS custom_commands(
            chat_id INTEGER NOT NULL,
            cmd TEXT NOT NULL,
            response TEXT NOT NULL,
            PRIMARY KEY(chat_id, cmd)
        )
        """)

        def ensure_column(name: str, ddl: str):
            cur.execute("PRAGMA table_info(settings)")
            cols = [r[1] for r in cur.fetchall()]
            if name not in cols:
                cur.execute(f"ALTER TABLE settings ADD COLUMN {ddl}")

        ensure_column("min_message_length", "min_message_length INTEGER NOT NULL DEFAULT 0")
        ensure_column("max_message_length", "max_message_length INTEGER NOT NULL DEFAULT 0")
        ensure_column("short_message_action", "short_message_action TEXT NOT NULL DEFAULT 'delete'")
        ensure_column("long_message_action", "long_message_action TEXT NOT NULL DEFAULT 'delete'")

        conn.commit()


def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO users(user_id, username, first_name, first_seen, last_seen)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=excluded.last_seen
        """, (user_id, username or "", first_name or "", now, now))
        conn.commit()


def get_user_lang(user_id: int) -> str:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT lang FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row[0] if row and row[0] in {"en", "bn"} else "en"


def set_user_lang(user_id: int, lang: str):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO users(user_id, username, first_name, first_seen, last_seen, lang)
        VALUES(?, '', '', ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            lang=excluded.lang,
            last_seen=excluded.last_seen
        """, (user_id, now, now, lang))
        conn.commit()


def register_group(chat_id: int, title: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO groups(chat_id, title, added_at)
        VALUES(?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title
        """, (chat_id, title or "", int(time.time())))
        cur.execute("INSERT OR IGNORE INTO settings(chat_id, title) VALUES(?, ?)", (chat_id, title or ""))
        cur.execute("UPDATE settings SET title = ? WHERE chat_id = ?", (title or "", chat_id))
        conn.commit()


def list_groups(limit: int = 50) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT chat_id, title FROM groups ORDER BY title ASC LIMIT ?", (limit,))
        return cur.fetchall()


def get_settings(chat_id: int) -> dict:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO settings(chat_id) VALUES(?)", (chat_id,))
        cur.execute("SELECT * FROM settings WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        data = dict(zip(cols, row))
        data["min_message_length"], data["max_message_length"] = normalize_msglen(
            int(data.get("min_message_length", 0)),
            int(data.get("max_message_length", 0)),
        )
        return data


def normalize_msglen(min_len: int, max_len: int) -> Tuple[int, int]:
    min_len = max(0, int(min_len))
    max_len = max(0, int(max_len))
    if max_len > 0 and min_len > max_len:
        min_len, max_len = max_len, min_len
    return min_len, max_len


def update_setting(chat_id: int, field: str, value):
    allowed = {
        "title", "lang",
        "rules_text", "rules_cmd_permission",
        "welcome_enabled", "welcome_text", "welcome_mode", "welcome_delete_last",
        "goodbye_enabled", "goodbye_text", "goodbye_private", "goodbye_delete_last",
        "captcha_enabled",
        "admin_status", "admin_tag_founder", "admin_tag_admins",
        "media_enabled", "media_action",
        "warns_action", "warns_limit", "warns_mute_minutes",
        "antispam_tg_links", "antispam_forwarding", "antispam_quote", "antispam_total_links",
        "antiflood_messages", "antiflood_seconds", "antiflood_action",
        "alphabet_arabic", "alphabet_cyrillic", "alphabet_chinese", "alphabet_latin",
        "check_at_join", "checks_delete_messages",
        "link_enabled", "group_link",
        "approval_enabled",
        "night_enabled", "night_delete_medias", "night_global_silence",
        "deleting_commands", "deleting_global_silence", "deleting_edit_checks",
        "deleting_service_messages", "deleting_scheduled", "deleting_block_cancellation",
        "deleting_all_messages", "deleting_self_destruct",
        "min_message_length", "max_message_length",
        "short_message_action", "long_message_action",
        "banned_words", "log_channel_id", "force_sub_channel",
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO settings(chat_id) VALUES(?)", (chat_id,))
        cur.execute(f"UPDATE settings SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()


def set_message_length_settings(chat_id: int, min_len: int, max_len: int, short_action: Optional[str] = None, long_action: Optional[str] = None):
    min_len, max_len = normalize_msglen(min_len, max_len)
    update_setting(chat_id, "min_message_length", min_len)
    update_setting(chat_id, "max_message_length", max_len)
    if short_action in {"delete", "warn"}:
        update_setting(chat_id, "short_message_action", short_action)
    if long_action in {"delete", "warn"}:
        update_setting(chat_id, "long_message_action", long_action)


def get_warns(chat_id: int, user_id: int) -> int:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT warns FROM warns WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        row = cur.fetchone()
        return row[0] if row else 0


def set_warns(chat_id: int, user_id: int, warns_count: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO warns(chat_id, user_id, warns)
        VALUES(?, ?, ?)
        ON CONFLICT(chat_id, user_id) DO UPDATE SET warns=excluded.warns
        """, (chat_id, user_id, warns_count))
        conn.commit()


def list_warned(chat_id: int, limit: int = 20) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT user_id, warns FROM warns
        WHERE chat_id = ? AND warns > 0
        ORDER BY warns DESC, user_id ASC LIMIT ?
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


def is_approved(chat_id: int, user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM approved_users WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        return cur.fetchone() is not None


def save_note(chat_id: int, name: str, text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO notes(chat_id, name, text)
        VALUES(?, ?, ?)
        ON CONFLICT(chat_id, name) DO UPDATE SET text=excluded.text
        """, (chat_id, name.lower(), text))
        conn.commit()


def delete_note(chat_id: int, name: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM notes WHERE chat_id = ? AND name = ?", (chat_id, name.lower()))
        conn.commit()


def save_custom(chat_id: int, cmd: str, response: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        INSERT INTO custom_commands(chat_id, cmd, response)
        VALUES(?, ?, ?)
        ON CONFLICT(chat_id, cmd) DO UPDATE SET response=excluded.response
        """, (chat_id, cmd.lower(), response))
        conn.commit()


def get_custom(chat_id: int, cmd: str) -> Optional[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT response FROM custom_commands WHERE chat_id = ? AND cmd = ?", (chat_id, cmd.lower()))
        row = cur.fetchone()
        return row[0] if row else None

# =========================================================
# Helpers
# =========================================================
def bot_username() -> str:
    return runtime["bot_username"] or ""


async def is_group_admin(client: Client, chat_id: int, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status in {ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR}
    except Exception:
        return False


def bool_icon(v: bool) -> str:
    return "✅" if v else "❌"


def sign_data(raw: str) -> str:
    sig = hmac.new(CFG.secret_key.encode(), raw.encode(), hashlib.sha256).hexdigest()[:8]
    return f"{raw}|{sig}"


def verify_data(data: str) -> Optional[str]:
    if "|" not in data:
        return None
    raw, sig = data.rsplit("|", 1)
    expected = hmac.new(CFG.secret_key.encode(), raw.encode(), hashlib.sha256).hexdigest()[:8]
    return raw if hmac.compare_digest(sig, expected) else None


def cb(uid: int, page: str, chat_id: int = 0, extra: str = "") -> str:
    raw = f"{uid}:{page}:{chat_id}:{extra}"
    return sign_data(raw)


def parse_cb(data: str) -> Optional[dict]:
    raw = verify_data(data)
    if not raw:
        return None
    parts = raw.split(":", 3)
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


def text_has_link(text: str) -> bool:
    return bool(text and re.search(r"(https?://|www\.|t\.me/)", text, re.I))


def text_has_tg_link(text: str) -> bool:
    return bool(text and re.search(r"(t\.me/|telegram\.me/)", text, re.I))


def text_has_cyrillic(text: str) -> bool:
    return bool(text and re.search(r"[\u0400-\u04FF]", text))


def text_has_arabic(text: str) -> bool:
    return bool(text and re.search(r"[\u0600-\u06FF]", text))


def text_has_chinese(text: str) -> bool:
    return bool(text and re.search(r"[\u4e00-\u9fff]", text))


def text_has_latin(text: str) -> bool:
    return bool(text and re.search(r"[A-Za-z]", text))


def get_add_group_url() -> str:
    u = bot_username()
    return f"https://t.me/{u}?startgroup=true" if u else "https://t.me"


async def check_forcesub(client: Client, channel_ref: str, user_id: int) -> bool:
    if not channel_ref:
        return True
    try:
        member = await client.get_chat_member(channel_ref, user_id)
        return member.status not in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED}
    except Exception:
        return False


async def apply_warn_action(client: Client, chat_id: int, user_id: int, settings: dict, warns_count: int):
    if warns_count < settings["warns_limit"]:
        return

    action = settings["warns_action"]
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
        logger.warning(f"warn action failed: {e}")


def should_check_message_length(message: Message) -> bool:
    if not message or not message.from_user or message.from_user.is_bot:
        return False
    if message.text:
        text = message.text.strip()
        if not text or text.startswith("/"):
            return False
        return True
    if message.caption and message.caption.strip():
        return True
    return False

# =========================================================
# UI text
# =========================================================
def main_text() -> str:
    return (
        "👋 **Hello!**\n"
        "Group Guard 2.0 is a complete Bot to help\n"
        "you manage your groups easily and safely!\n\n"
        "👉 Add me in a Supergroup and promote me\n"
        "as Admin to let me get in action!\n\n"
        "❓ **WHICH ARE THE COMMANDS?** ❓\n"
        "Press /help to see all the commands and how they work!"
    )


def manage_groups_text() -> str:
    return (
        "Manage group Settings\n"
        "👉 Select the group whose settings you want\n"
        "to change.\n\n"
        "If a group in which you are an administrator\n"
        "doesn't appear here:\n"
        "• Send /reload in the group and try again\n"
        "• Send /settings in the group and then press\n"
        "\"Open in pvt\""
    )


def settings_home_text(chat_id: int) -> str:
    s = get_settings(chat_id)
    title = s["title"] or f"Group {chat_id}"
    return (
        "SETTINGS\n"
        f"Group: {title}\n\n"
        "Select one of the settings that you want to\n"
        "change."
    )

# =========================================================
# Keyboards
# =========================================================
def main_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add me to a Group ➕", url=get_add_group_url())],
        [InlineKeyboardButton("⚙️ Manage group Settings ✍️", callback_data=cb(uid, "mg"))],
        [InlineKeyboardButton("👥 Group", callback_data=cb(uid, "mg")),
         InlineKeyboardButton("Channel 📢", callback_data=cb(uid, "ch"))],
        [InlineKeyboardButton("🚑 Support", callback_data=cb(uid, "su")),
         InlineKeyboardButton("Information 💬", callback_data=cb(uid, "in"))],
        [InlineKeyboardButton("🇬🇧 Languages 🇬🇧", callback_data=cb(uid, "ul"))],
    ])


def manage_groups_kb(uid: int) -> InlineKeyboardMarkup:
    rows = []
    for chat_id, title in list_groups(40):
        label = (title[:40] + "…") if len(title) > 40 else (title or str(chat_id))
        rows.append([InlineKeyboardButton(label, callback_data=cb(uid, "sh", chat_id))])
    if not rows:
        rows.append([InlineKeyboardButton("➕ Add me to a Group ➕", url=get_add_group_url())])
    return InlineKeyboardMarkup(rows)


def settings_home_kb(uid: int, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 Regulation", callback_data=cb(uid, "rg", chat_id)),
         InlineKeyboardButton("✉️ Anti-Spam", callback_data=cb(uid, "as", chat_id))],
        [InlineKeyboardButton("💬 Welcome", callback_data=cb(uid, "we", chat_id)),
         InlineKeyboardButton("🗣️ Anti-Flood", callback_data=cb(uid, "af", chat_id))],
        [InlineKeyboardButton("👋 Goodbye", callback_data=cb(uid, "gb", chat_id)),
         InlineKeyboardButton("🕉️ Alphabets", callback_data=cb(uid, "al", chat_id))],
        [InlineKeyboardButton("🧠 Captcha", callback_data=cb(uid, "cp", chat_id)),
         InlineKeyboardButton("🔦 Checks", callback_data=cb(uid, "ck", chat_id))],
        [InlineKeyboardButton("🆘 @Admin", callback_data=cb(uid, "ad", chat_id)),
         InlineKeyboardButton("🔐 Blocks", callback_data=cb(uid, "bl", chat_id))],
        [InlineKeyboardButton("📸 Media", callback_data=cb(uid, "md", chat_id)),
         InlineKeyboardButton("🔞 Porn", callback_data=cb(uid, "pn", chat_id))],
        [InlineKeyboardButton("❗ Warns", callback_data=cb(uid, "wp", chat_id)),
         InlineKeyboardButton("🌘 Night", callback_data=cb(uid, "ng", chat_id))],
        [InlineKeyboardButton("🔔 Tag", callback_data=cb(uid, "tg", chat_id)),
         InlineKeyboardButton("🔗 Link", callback_data=cb(uid, "gl", chat_id))],
        [InlineKeyboardButton("📬 Approval mode", callback_data=cb(uid, "ap", chat_id))],
        [InlineKeyboardButton("🗑️ Deleting Messages", callback_data=cb(uid, "dl", chat_id))],
        [InlineKeyboardButton("🇬🇧 Lang", callback_data=cb(uid, "lg", chat_id)),
         InlineKeyboardButton("✅ Close", callback_data=cb(uid, "cl")),
         InlineKeyboardButton("▶️ Other", callback_data=cb(uid, "ot", chat_id))],
    ])


def back_kb(uid: int, page: str, chat_id: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=cb(uid, page, chat_id))]])

# =========================================================
# Render helper
# =========================================================
async def render_settings_page(cq: CallbackQuery, uid: int, chat_id: int, page: str):
    s = get_settings(chat_id)

    if page == "we":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "wea", chat_id, "off")),
             InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "wea", chat_id, "on"))],
            [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "wea", chat_id, "c"))],
            [InlineKeyboardButton("🔔 Always send", callback_data=cb(uid, "wea", chat_id, "a")),
             InlineKeyboardButton("Send 1st join", callback_data=cb(uid, "wea", chat_id, "f"))],
            [InlineKeyboardButton(f"Delete last message {'✅' if s['welcome_delete_last'] else '✖️'}", callback_data=cb(uid, "wea", chat_id, "d"))],
            [InlineKeyboardButton("Back", callback_data=cb(uid, "sh", chat_id))]
        ])
        txt = f"💬 Welcome Message\nStatus: {'On ✅' if s['welcome_enabled'] else 'Off ❌'}"
        return await cq.message.edit_text(txt, reply_markup=kb)

    if page == "gb":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "gba", chat_id, "off")),
             InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "gba", chat_id, "on"))],
            [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "gba", chat_id, "c"))],
            [InlineKeyboardButton(f"💌 Send in private chat {'✅' if s['goodbye_private'] else '✖️'}", callback_data=cb(uid, "gba", chat_id, "p"))],
            [InlineKeyboardButton(f"♻️ Delete last message {'✅' if s['goodbye_delete_last'] else '✖️'}", callback_data=cb(uid, "gba", chat_id, "d"))],
            [InlineKeyboardButton("Back", callback_data=cb(uid, "sh", chat_id))]
        ])
        txt = f"👋 Goodbye\nStatus: {'On ✅' if s['goodbye_enabled'] else 'Off ❌'}"
        return await cq.message.edit_text(txt, reply_markup=kb)

    if page == "cp":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Activate" if not s["captcha_enabled"] else "✖️ Deactivate", callback_data=cb(uid, "cpa", chat_id, "t"))],
            [InlineKeyboardButton("Back", callback_data=cb(uid, "sh", chat_id))]
        ])
        txt = f"🧠 Captcha\nStatus: {'On ✅' if s['captcha_enabled'] else 'Off ❌'}"
        return await cq.message.edit_text(txt, reply_markup=kb)

    if page == "ot":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔤 Banned Words", callback_data=cb(uid, "ots", chat_id, "bw"))],
            [InlineKeyboardButton("Message length", callback_data=cb(uid, "ots", chat_id, "ml"))],
            [InlineKeyboardButton("Log Channel", callback_data=cb(uid, "ots", chat_id, "lc"))],
            [InlineKeyboardButton("Back", callback_data=cb(uid, "sh", chat_id)),
             InlineKeyboardButton("✅ Close", callback_data=cb(uid, "cl")),
             InlineKeyboardButton("🇬🇧 Lang", callback_data=cb(uid, "lg", chat_id))]
        ])
        return await cq.message.edit_text(settings_home_text(chat_id), reply_markup=kb)

# =========================================================
# Core commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    parts = message.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].startswith("settings_"):
        try:
            chat_id = int(parts[1].split("_", 1)[1])
            if await is_group_admin(client, chat_id, message.from_user.id):
                return await message.reply_text(
                    settings_home_text(chat_id),
                    reply_markup=settings_home_kb(message.from_user.id, chat_id)
                )
        except Exception:
            pass

    await message.reply_text(main_text(), reply_markup=main_kb(message.from_user.id))


@bot.on_message(filters.command("help") & filters.private)
async def help_private(client, message: Message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👮🏻‍♂️ Basic commands", callback_data=cb(message.from_user.id, "hb")),
         InlineKeyboardButton("Advanced 👮🏻", callback_data=cb(message.from_user.id, "ha"))],
        [InlineKeyboardButton("🕵🏻 Experts", callback_data=cb(message.from_user.id, "he")),
         InlineKeyboardButton("Pro Guides 🧝🏻", callback_data=cb(message.from_user.id, "hp"))],
    ])
    await message.reply_text("Welcome to the help menu!", reply_markup=kb)


@bot.on_message(filters.command("help") & filters.group)
async def help_group(client, message: Message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👮🏻‍♂️ Basic commands", callback_data=cb(message.from_user.id, "hb")),
         InlineKeyboardButton("Advanced 👮🏻", callback_data=cb(message.from_user.id, "ha"))],
        [InlineKeyboardButton("🕵🏻 Experts", callback_data=cb(message.from_user.id, "he")),
         InlineKeyboardButton("Pro Guides 🧝🏻", callback_data=cb(message.from_user.id, "hp"))],
    ])
    await message.reply_text("Welcome to the help menu!", reply_markup=kb)

# =========================================================
# Group commands
# =========================================================
@bot.on_message(filters.command("reload") & filters.group)
async def reload_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    register_group(message.chat.id, message.chat.title or "")
    await message.reply_text("✅ Reloaded.")


@bot.on_message(filters.command("settings") & filters.group)
async def settings_group(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    register_group(message.chat.id, message.chat.title or "")
    url = f"https://t.me/{bot_username()}?start=settings_{message.chat.id}" if bot_username() else ""
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open in pvt", url=url)]]) if url else None
    await message.reply_text("Open settings in private chat.", reply_markup=kb)


@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    s = get_settings(message.chat.id)
    if s["rules_cmd_permission"] == "admins":
        if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
            return await message.reply_text("Only admins can use /rules here.")
    await message.reply_text(s["rules_text"])


@bot.on_message(filters.command("link") & filters.group)
async def link_cmd(client, message: Message):
    s = get_settings(message.chat.id)
    if not s["link_enabled"] or not s["group_link"]:
        return await message.reply_text("Group link is deactivated.")
    await message.reply_text(s["group_link"])


@bot.on_message(filters.command("msglen") & filters.group)
async def msglen_cmd(client, message: Message):
    s = get_settings(message.chat.id)
    await message.reply_text(
        f"Message length settings\n\n"
        f"Min: {s['min_message_length']}\n"
        f"Max: {s['max_message_length']}\n"
        f"Short action: {s['short_message_action']}\n"
        f"Long action: {s['long_message_action']}"
    )


@bot.on_message(filters.command("setmsglen") & filters.group)
async def setmsglen_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")

    parts = message.text.split()
    if len(parts) < 3 or len(parts) > 5:
        return await message.reply_text(
            "Usage: /setmsglen MIN MAX [short_action] [long_action]\n"
            "Example: /setmsglen 3 300 delete warn"
        )

    try:
        min_len = int(parts[1])
        max_len = int(parts[2])
        short_action = parts[3].lower() if len(parts) >= 4 else None
        long_action = parts[4].lower() if len(parts) >= 5 else None

        if short_action and short_action not in {"delete", "warn"}:
            return await message.reply_text("short_action must be delete or warn")
        if long_action and long_action not in {"delete", "warn"}:
            return await message.reply_text("long_action must be delete or warn")

        set_message_length_settings(message.chat.id, min_len, max_len, short_action, long_action)
        s = get_settings(message.chat.id)
        await message.reply_text(
            f"✅ Message length updated.\n"
            f"Min: {s['min_message_length']}\n"
            f"Max: {s['max_message_length']}\n"
            f"Short action: {s['short_message_action']}\n"
            f"Long action: {s['long