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
from pyrogram.enums import ChatMemberStatus, ChatType

# =========================================================
# Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("grouphelp_ui_clone")

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
        db_path=os.environ.get("DB_PATH", "bot.sqlite3"),
        port=int(os.environ.get("PORT", "10000")),
        secret_key=os.environ.get("SECRET_KEY", "CHANGE_ME_SECRET"),
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
    )


CFG = load_config()

# =========================================================
# App / Runtime
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

runtime = {
    "bot_username": "",
    "bot_id": 0,
    "maintenance": CFG.maintenance_mode,
}
flood_tracker: Dict[Tuple[int, int], List[float]] = {}
pending_inputs: Dict[int, Dict] = {}

# =========================================================
# Flask
# =========================================================
@app.route("/")
def home():
    return "✅ Bot running", 200


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
    "grouphelp_like_bot",
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
        return dict(zip(cols, row))


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
        "banned_words", "log_channel_id", "force_sub_channel",
    }
    if field not in allowed:
        raise ValueError("Invalid field")
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO settings(chat_id) VALUES(?)", (chat_id,))
        cur.execute(f"UPDATE settings SET {field} = ? WHERE chat_id = ?", (value, chat_id))
        conn.commit()


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


def list_approved(chat_id: int, limit: int = 30) -> List[int]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT user_id FROM approved_users
        WHERE chat_id = ?
        ORDER BY approved_at DESC
        LIMIT ?
        """, (chat_id, limit))
        return [r[0] for r in cur.fetchall()]


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


def list_notes(chat_id: int, limit: int = 50) -> List[str]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM notes WHERE chat_id = ? ORDER BY name ASC LIMIT ?", (chat_id, limit))
        return [r[0] for r in cur.fetchall()]


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
def is_root_admin(user_id: int) -> bool:
    return user_id in CFG.admin_ids


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
    sig = hmac.new(CFG.secret_key.encode(), raw.encode(), hashlib.sha256).hexdigest()[:12]
    return f"{raw}|{sig}"


def verify_data(data: str) -> Optional[str]:
    if "|" not in data:
        return None
    raw, sig = data.rsplit("|", 1)
    expected = hmac.new(CFG.secret_key.encode(), raw.encode(), hashlib.sha256).hexdigest()[:12]
    return raw if hmac.compare_digest(sig, expected) else None


def cb(uid: int, page: str, chat_id: int = 0, extra: str = "") -> str:
    return sign_data(f"{uid}:{page}:{chat_id}:{extra}")


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


async def log_to_channel(client: Client, chat_id: int, text: str):
    s = get_settings(chat_id)
    if not s["log_channel_id"]:
        return
    try:
        await client.send_message(s["log_channel_id"], text)
    except Exception:
        pass


# =========================================================
# UI text
# =========================================================
def main_text() -> str:
    return (
        "👋 **Hello!**\n"
        "Group Help is the most complete Bot to help\n"
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
    title = s["title"] or f"Chat {chat_id}"
    return (
        "SETTINGS\n"
        f"Group: {title}\n\n"
        "Select one of the settings that you want to\n"
        "change."
    )


def help_menu_text() -> str:
    return "Welcome to the help menu!"


def basic_commands_text() -> str:
    return (
        "Base Commands\n\n"
        "👮 Available to Admins&Moderators\n"
        "🕵️ Available to Admins\n\n"
        "👮 /reload updates the Admins list and their privileges\n\n"
        "🕵️ /settings lets you manage all the Bot settings in a group\n\n"
        "👮 /ban lets you ban a user from the group\n"
        "without giving him the possibility to join again using the link of the group\n\n"
        "👮 /mute puts a user in read-only mode. He can\n"
        "read but he can't send any messages\n\n"
        "👮 /kick bans a user from the group, giving\n"
        "him the possibility to join again with the link of the group\n\n"
        "👮 /unban lets you remove a user from group's\n"
        "blacklist, giving them the possibility to join again with the link of the group\n\n"
        "👮 /info gives information about a user\n"
        "👮 /infopvt is the same of /info, but sends\n"
        "infos in private chat\n\n"
        "◻️ /staff gives the complete List of group Staff"
    )


def advanced_commands_text() -> str:
    return (
        "Advanced Commands\n\n"
        "🕵️ Available to Admins\n"
        "👮 Available to Admins&Moderators\n"
        "🧹 Available to Admins&Cleaners\n\n"
        "WARN MANAGEMENT\n"
        "👮 /warn adds a warn to the user\n"
        "👮 /unwarn removes a warn to the user\n"
        "👮 /warns lets you see and manage user Warns\n"
        "🕵️ /delwarn deletes the message and add a warn to the user\n\n"
        "🧹 /del deletes the selected message\n"
        "🧹 /logdel deletes the selected message and sends it to the Log Channel\n\n"
        "◻️ /me sends in private chat a message with his own infos, group infos, warns received,\n"
        "rules of the group, banned words list...\n\n"
        "🕵️ /send permits to send a post using HTML in the group, through the Bot\n"
        "➡️ Example: /send Hello World!\n\n"
        "👮 /intervention lets you request the intervention of a member of Official Bot Support"
    )


def experts_text() -> str:
    return (
        "Experts Commands\n\n"
        "👥 Available to all users\n"
        "👮 Available to Admins&Moderators\n"
        "🕵️ Available to Admins\n\n"
        "👥 /geturl, by replying to a message, you receive the link that refers directly to that message.\n\n"
        "🕵️ /inactives [days] sends in private chat the list of users who have not sent a message in the last [days].\n\n"
        "Pinned Messages\n"
        "🕵️ /pin [message] sends the message through the Bot and pins it.\n"
        "🕵️ /pin pins the message in reply.\n"
        "🕵️ /editpin [message] edits the current pinned message.\n"
        "🕵️ /delpin removes the pinned message.\n"
        "🕵️ /repin removes and pins again the current pinned message.\n"
        "👥 /pinned refers to the current pinned message.\n\n"
        "🕵️ /list sends in private chat the list of users of the group with the number of messages sent by them.\n"
        "🕵️ /list roles sends in private chat the list of all the special roles assigned to users\n"
        "🕵️ /graphic sends a graph showing the trend of the group members.\n"
        "🕵️ /trend sends the group's growth statistics."
    )


def pro_guides_text() -> str:
    return (
        "Pro Guides\n"
        "In this menu you will find some guides for very\n"
        "advanced Group Help functions.\n"
        "I recommend you to use them only if you\n"
        "know what you are doing and following them\n"
        "completely and correctly."
    )


# =========================================================
# UI builders
# =========================================================
def main_kb(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add me to a Group ➕", url=get_add_group_url())],
        [InlineKeyboardButton("⚙️ Manage group Settings ✍️", callback_data=cb(uid, "manage_groups"))],
        [InlineKeyboardButton("👥 Group", callback_data=cb(uid, "manage_groups")),
         InlineKeyboardButton("Channel 📢", callback_data=cb(uid, "channel"))],
        [InlineKeyboardButton("🚑 Support", callback_data=cb(uid, "support")),
         InlineKeyboardButton("Information 💬", callback_data=cb(uid, "information"))],
        [InlineKeyboardButton("🇬🇧 Languages 🇬🇧", callback_data=cb(uid, "languages"))],
    ])


def manage_groups_kb(uid: int) -> InlineKeyboardMarkup:
    rows = []
    groups = list_groups(40)
    for chat_id, title in groups:
        label = (title[:40] + "…") if len(title) > 40 else (title or str(chat_id))
        rows.append([InlineKeyboardButton(label, callback_data=cb(uid, "settings_home", chat_id))])
    if not rows:
        rows.append([InlineKeyboardButton("➕ Add me to a Group ➕", url=get_add_group_url())])
    return InlineKeyboardMarkup(rows)


def settings_home_kb(uid: int, chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📜 Regulation", callback_data=cb(uid, "regulation", chat_id)),
         InlineKeyboardButton("✉️ Anti-Spam", callback_data=cb(uid, "anti_spam", chat_id))],
        [InlineKeyboardButton("💬 Welcome", callback_data=cb(uid, "welcome", chat_id)),
         InlineKeyboardButton("🗣️ Anti-Flood", callback_data=cb(uid, "anti_flood", chat_id))],
        [InlineKeyboardButton("👋 Goodbye", callback_data=cb(uid, "goodbye", chat_id)),
         InlineKeyboardButton("🕉️ Alphabets", callback_data=cb(uid, "alphabets", chat_id))],
        [InlineKeyboardButton("🧠 Captcha", callback_data=cb(uid, "captcha", chat_id)),
         InlineKeyboardButton("🔦 Checks", callback_data=cb(uid, "checks", chat_id))],
        [InlineKeyboardButton("🆘 @Admin", callback_data=cb(uid, "admin_cmd", chat_id)),
         InlineKeyboardButton("🔐 Blocks", callback_data=cb(uid, "blocks", chat_id))],
        [InlineKeyboardButton("📸 Media", callback_data=cb(uid, "media", chat_id)),
         InlineKeyboardButton("🔞 Porn", callback_data=cb(uid, "porn", chat_id))],
        [InlineKeyboardButton("❗ Warns", callback_data=cb(uid, "warns_panel", chat_id)),
         InlineKeyboardButton("🌘 Night", callback_data=cb(uid, "night", chat_id))],
        [InlineKeyboardButton("🔔 Tag", callback_data=cb(uid, "tag", chat_id)),
         InlineKeyboardButton("🔗 Link", callback_data=cb(uid, "group_link", chat_id))],
        [InlineKeyboardButton("📬 Approval mode", callback_data=cb(uid, "approval", chat_id))],
        [InlineKeyboardButton("🗑️ Deleting Messages", callback_data=cb(uid, "deleting", chat_id))],
        [InlineKeyboardButton("🇬🇧 Lang", callback_data=cb(uid, "group_lang", chat_id)),
         InlineKeyboardButton("✅ Close", callback_data=cb(uid, "close")),
         InlineKeyboardButton("▶️ Other", callback_data=cb(uid, "other", chat_id))],
    ])


# =========================================================
# Renderers
# =========================================================
async def edit_panel(cq: CallbackQuery, text: str, kb: Optional[InlineKeyboardMarkup]):
    await cq.message.edit_text(text, reply_markup=kb)


async def render_main(cq: CallbackQuery, uid: int):
    await edit_panel(cq, main_text(), main_kb(uid))


async def render_manage_groups(cq: CallbackQuery, uid: int):
    await edit_panel(cq, manage_groups_text(), manage_groups_kb(uid))


async def render_settings_home(cq: CallbackQuery, uid: int, chat_id: int):
    await edit_panel(cq, settings_home_text(chat_id), settings_home_kb(uid, chat_id))


def back_kb(uid: int, back_page: str, chat_id: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back", callback_data=cb(uid, back_page, chat_id))]])


# =========================================================
# Commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message: Message):
    upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    parts = message.text.split(maxsplit=1)
    if len(parts) > 1 and parts[1].startswith("settings_"):
        try:
            chat_id = int(parts[1].split("_", 1)[1])
            if await is_group_admin(client, chat_id, message.from_user.id):
                text = settings_home_text(chat_id)
                kb = settings_home_kb(message.from_user.id, chat_id)
                return await message.reply_text(text, reply_markup=kb)
        except Exception:
            pass

    await message.reply_text(main_text(), reply_markup=main_kb(message.from_user.id))


@bot.on_message(filters.command("help") & filters.private)
async def help_cmd_private(client, message: Message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👮🏻‍♂️ Basic commands", callback_data=cb(message.from_user.id, "help_basic")),
         InlineKeyboardButton("Advanced 👮🏻", callback_data=cb(message.from_user.id, "help_advanced"))],
        [InlineKeyboardButton("🕵🏻 Experts", callback_data=cb(message.from_user.id, "help_experts")),
         InlineKeyboardButton("Pro Guides 🧝🏻", callback_data=cb(message.from_user.id, "help_pro"))],
    ])
    await message.reply_text(help_menu_text(), reply_markup=kb)


@bot.on_message(filters.command("help") & filters.group)
async def help_cmd_group(client, message: Message):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("👮🏻‍♂️ Basic commands", callback_data=cb(message.from_user.id, "help_basic")),
         InlineKeyboardButton("Advanced 👮🏻", callback_data=cb(message.from_user.id, "help_advanced"))],
        [InlineKeyboardButton("🕵🏻 Experts", callback_data=cb(message.from_user.id, "help_experts")),
         InlineKeyboardButton("Pro Guides 🧝🏻", callback_data=cb(message.from_user.id, "help_pro"))],
    ])
    await message.reply_text(help_menu_text(), reply_markup=kb)


@bot.on_message(filters.command("reload") & filters.group)
async def reload_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    register_group(message.chat.id, message.chat.title or "")
    await message.reply_text("✅ Reloaded admins/groups cache for this group.")


@bot.on_message(filters.command("settings") & filters.group)
async def settings_cmd_group(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    register_group(message.chat.id, message.chat.title or "")
    link = f"https://t.me/{bot_username()}?start=settings_{message.chat.id}" if bot_username() else ""
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Open in pvt", url=link)]]) if link else None
    await message.reply_text("Open settings in private chat.", reply_markup=kb)


@bot.on_message(filters.command("rules") & filters.group)
async def rules_cmd(client, message: Message):
    s = get_settings(message.chat.id)
    await message.reply_text(s["rules_text"])


@bot.on_message(filters.command("link") & filters.group)
async def link_cmd(client, message: Message):
    s = get_settings(message.chat.id)
    if not s["link_enabled"] or not s["group_link"]:
        return await message.reply_text("Group link is deactivated.")
    await message.reply_text(s["group_link"])


@bot.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user.")
    target = message.reply_to_message.from_user
    s = get_settings(message.chat.id)
    wc = get_warns(message.chat.id, target.id) + 1
    set_warns(message.chat.id, target.id, wc)
    await apply_warn_action(client, message.chat.id, target.id, s, wc)
    await message.reply_text(f"⚠️ Warn added: `{wc}/{s['warns_limit']}`")


@bot.on_message(filters.command("unwarn") & filters.group)
async def unwarn_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user.")
    target = message.reply_to_message.from_user
    wc = max(0, get_warns(message.chat.id, target.id) - 1)
    set_warns(message.chat.id, target.id, wc)
    await message.reply_text(f"✅ Warns: {wc}")


@bot.on_message(filters.command("warns") & filters.group)
async def warns_cmd(client, message: Message):
    rows = list_warned(message.chat.id)
    txt = "Warned List\n\n" + ("\n".join(f"`{uid}` → {w}" for uid, w in rows) if rows else "Empty.")
    await message.reply_text(txt)


@bot.on_message(filters.command("approve") & filters.group)
async def approve_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user.")
    approve_user(message.chat.id, message.reply_to_message.from_user.id)
    await message.reply_text("✅ Approved.")


@bot.on_message(filters.command("unapprove") & filters.group)
async def unapprove_cmd(client, message: Message):
    if not message.from_user or not await is_group_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("Admin only.")
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return await message.reply_text("Reply to a user.")
    unapprove_user(message.chat.id, message.reply_to_message.from_user.id)
    await message.reply_text("✅ Unapproved.")


# =========================================================
# Group registry / welcome / moderation
# =========================================================
@bot.on_message(filters.group)
async def group_registry_touch(client, message: Message):
    register_group(message.chat.id, message.chat.title or "")
    if message.from_user:
        upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)


@bot.on_message(filters.new_chat_members)
async def new_members_handler(client, message: Message):
    register_group(message.chat.id, message.chat.title or "")
    s = get_settings(message.chat.id)

    for user in message.new_chat_members:
        if s["welcome_enabled"]:
            text = s["welcome_text"].replace("{mention}", user.mention).replace("{name}", user.first_name or "User")
            try:
                await message.reply_text(text)
            except Exception:
                pass

        if s["captcha_enabled"]:
            try:
                await client.restrict_chat_member(message.chat.id, user.id, ChatPermissions(can_send_messages=False))
                await message.reply_text(f"🧠 {user.mention}, verify that you are not a robot.")
            except Exception:
                pass

        if s["check_at_join"]:
            name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            low_name = name.lower()

            blocked_name = False
            if re.search(r"[\u0600-\u06FF]", name) and "arabic name" in low_name:
                blocked_name = True

            if blocked_name and s["checks_delete_messages"]:
                try:
                    await client.ban_chat_member(message.chat.id, user.id)
                except Exception:
                    pass


@bot.on_message(filters.left_chat_member)
async def left_handler(client, message: Message):
    s = get_settings(message.chat.id)
    if s["goodbye_enabled"] and message.left_chat_member:
        text = s["goodbye_text"].replace("{name}", message.left_chat_member.first_name or "User")
        try:
            await message.reply_text(text)
        except Exception:
            pass


@bot.on_message(filters.group & ~filters.service, group=10)
async def moderation_handler(client, message: Message):
    if not message.from_user or message.from_user.is_bot:
        return
    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return

    s = get_settings(message.chat.id)
    text = message.text or message.caption or ""

    if s["approval_enabled"] and not is_approved(message.chat.id, message.from_user.id):
        try:
            await message.delete()
            await message.reply_text(f"🛑 {message.from_user.mention}, only approved users can speak here.")
        except Exception:
            pass
        return

    if s["force_sub_channel"]:
        ok = await check_forcesub(client, s["force_sub_channel"], message.from_user.id)
        if not ok:
            try:
                await message.delete()
            except Exception:
                pass
            btn = None
            if s["force_sub_channel"].startswith("@"):
                btn = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Join Channel", url=f"https://t.me/{s['force_sub_channel'].lstrip('@')}")]
                ])
            try:
                await message.reply_text("You must join the required channel first.", reply_markup=btn)
            except Exception:
                pass
            return

    if s["antispam_forwarding"] and (message.forward_date or message.forward_from or message.forward_sender_name):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["antispam_quote"] and message.reply_to_message:
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["antispam_tg_links"] and text_has_tg_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["antispam_total_links"] and text_has_link(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    if s["link_enabled"] and s["group_link"] and text.strip() == s["group_link"].strip():
        pass

    if s["media_enabled"] and message.media is not None:
        try:
            await message.delete()
            if s["media_action"] == "mute":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
        except Exception:
            pass
        return

    if s["night_enabled"]:
        if s["night_delete_medias"] and message.media is not None:
            try:
                await message.delete()
            except Exception:
                pass
            return
        if s["night_global_silence"]:
            try:
                await message.delete()
            except Exception:
                pass
            return

    banned_words = [w.strip().lower() for w in s["banned_words"].split(",") if w.strip()]
    low = text.lower()

    for bw in banned_words:
        if bw and re.search(rf"\b{re.escape(bw)}\b", low):
            try:
                await message.delete()
            except Exception:
                pass
            wc = get_warns(message.chat.id, message.from_user.id) + 1
            set_warns(message.chat.id, message.from_user.id, wc)
            await apply_warn_action(client, message.chat.id, message.from_user.id, s, wc)
            return

    if s["alphabet_arabic"] and text_has_arabic(text):
        try:
            await message.delete()
        except Exception:
            pass
        return
    if s["alphabet_cyrillic"] and text_has_cyrillic(text):
        try:
            await message.delete()
        except Exception:
            pass
        return
    if s["alphabet_chinese"] and text_has_chinese(text):
        try:
            await message.delete()
        except Exception:
            pass
        return
    if s["alphabet_latin"] and text_has_latin(text):
        try:
            await message.delete()
        except Exception:
            pass
        return

    key = (message.chat.id, message.from_user.id)
    now = time.time()
    arr = [x for x in flood_tracker.get(key, []) if now - x <= s["antiflood_seconds"]]
    arr.append(now)
    flood_tracker[key] = arr

    if len(arr) >= s["antiflood_messages"]:
        try:
            action = s["antiflood_action"]
            if action == "off":
                return
            if action == "warn":
                wc = get_warns(message.chat.id, message.from_user.id) + 1
                set_warns(message.chat.id, message.from_user.id, wc)
                await apply_warn_action(client, message.chat.id, message.from_user.id, s, wc)
            elif action == "kick":
                await client.ban_chat_member(message.chat.id, message.from_user.id)
                await client.unban_chat_member(message.chat.id, message.from_user.id)
            elif action == "mute":
                await client.restrict_chat_member(message.chat.id, message.from_user.id, ChatPermissions(can_send_messages=False))
            elif action == "ban":
                await client.ban_chat_member(message.chat.id, message.from_user.id)
            elif action == "delete":
                await message.delete()
        except Exception:
            pass


@bot.on_message(filters.command(["start", "help", "settings", "rules", "link", "warn", "unwarn", "warns", "approve", "unapprove", "reload"]) & filters.group, group=15)
async def delete_command_messages(client, message: Message):
    s = get_settings(message.chat.id)
    if s["deleting_commands"]:
        try:
            await asyncio.sleep(2)
            await message.delete()
        except Exception:
            pass


@bot.on_edited_message(filters.group, group=16)
async def edited_message_checks(client, message: Message):
    s = get_settings(message.chat.id)
    if not s["deleting_edit_checks"]:
        return
    if not message.from_user or message.from_user.is_bot:
        return
    if await is_group_admin(client, message.chat.id, message.from_user.id):
        return
    text = message.text or message.caption or ""
    if text_has_link(text):
        try:
            await message.delete()
        except Exception:
            pass


# =========================================================
# Custom command handler
# =========================================================
BUILTINS = {
    "start", "help", "settings", "reload", "rules", "link",
    "warn", "unwarn", "warns", "approve", "unapprove",
}

@bot.on_message(filters.group & filters.text, group=20)
async def custom_commands_handler(client, message: Message):
    if not message.text or not message.text.startswith("/"):
        return
    cmd = message.text.split()[0].lstrip("/").split("@")[0].lower()
    if cmd in BUILTINS:
        return
    res = get_custom(message.chat.id, cmd)
    if res:
        await message.reply_text(res)

# =========================================================
# Pending input session handler
# =========================================================
@bot.on_message(filters.private & filters.text, group=30)
async def pending_input_handler(client, message: Message):
    if not message.from_user or message.text.startswith("/"):
        return
    sess = pending_inputs.pop(message.from_user.id, None)
    if not sess:
        return

    action = sess["action"]
    chat_id = sess["chat_id"]
    text = message.text.strip()

    try:
        if action == "set_rules":
            update_setting(chat_id, "rules_text", text)
            return await message.reply_text("✅ Rules updated.")
        if action == "set_welcome":
            update_setting(chat_id, "welcome_text", text)
            return await message.reply_text("✅ Welcome message updated.")
        if action == "set_goodbye":
            update_setting(chat_id, "goodbye_text", text)
            return await message.reply_text("✅ Goodbye message updated.")
        if action == "set_group_link":
            update_setting(chat_id, "group_link", text)
            update_setting(chat_id, "link_enabled", 1)
            return await message.reply_text("✅ Group link saved.")
        if action == "add_banned_word":
            s = get_settings(chat_id)
            words = [w.strip().lower() for w in s["banned_words"].split(",") if w.strip()]
            if text.lower() not in words:
                words.append(text.lower())
            update_setting(chat_id, "banned_words", ",".join(words))
            return await message.reply_text("✅ Banned word added.")
        if action == "remove_banned_word":
            s = get_settings(chat_id)
            words = [w.strip().lower() for w in s["banned_words"].split(",") if w.strip()]
            words = [w for w in words if w != text.lower()]
            update_setting(chat_id, "banned_words", ",".join(words))
            return await message.reply_text("✅ Banned word removed.")
        if action == "set_log_channel":
            update_setting(chat_id, "log_channel_id", int(text))
            return await message.reply_text("✅ Log channel updated.")
        if action == "set_forcesub":
            update_setting(chat_id, "force_sub_channel", text)
            return await message.reply_text("✅ ForceSub updated.")
    except Exception as e:
        return await message.reply_text(f"❌ Failed: {e}")


# =========================================================
# Callback handler
# =========================================================
@bot.on_callback_query()
async def callback_handler(client, cq: CallbackQuery):
    p = parse_cb(cq.data)
    if not p:
        return await cq.answer("Invalid panel.", show_alert=True)

    uid = cq.from_user.id
    if p["uid"] != uid:
        return await cq.answer("This panel is not for you.", show_alert=True)

    page = p["page"]
    chat_id = p["chat_id"]
    extra = p["extra"]

    try:
        if page == "close":
            await cq.message.edit_text("Closed.")
            return await cq.answer("Closed")

        if page == "manage_groups":
            return await render_manage_groups(cq, uid)

        if page == "settings_home":
            if not await is_group_admin(client, chat_id, uid):
                return await cq.answer("You are not admin in this group.", show_alert=True)
            return await render_settings_home(cq, uid, chat_id)

        if page == "channel":
            return await edit_panel(
                cq,
                "Channel 📢\n\nThis section is informational in this build.",
                back_kb(uid, "home")
            )

        if page == "support":
            return await edit_panel(
                cq,
                "🚑 Support\n\nUse your own support group / owner contact.",
                back_kb(uid, "home")
            )

        if page == "information":
            return await edit_panel(
                cq,
                "Information 💬\n\nScreenshot-matched GroupHelp-style control panel build.",
                back_kb(uid, "home")
            )

        if page == "languages":
            lang = get_user_lang(uid)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'✅ ' if lang=='en' else ''}English", callback_data=cb(uid, "set_user_lang", 0, "en")),
                 InlineKeyboardButton(f"{'✅ ' if lang=='bn' else ''}বাংলা", callback_data=cb(uid, "set_user_lang", 0, "bn"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "home"))]
            ])
            return await edit_panel(cq, "🇬🇧 Languages 🇬🇧", kb)

        if page == "set_user_lang":
            set_user_lang(uid, extra if extra in {"en", "bn"} else "en")
            lang = get_user_lang(uid)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'✅ ' if lang=='en' else ''}English", callback_data=cb(uid, "set_user_lang", 0, "en")),
                 InlineKeyboardButton(f"{'✅ ' if lang=='bn' else ''}বাংলা", callback_data=cb(uid, "set_user_lang", 0, "bn"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "home"))]
            ])
            return await edit_panel(cq, "🇬🇧 Languages 🇬🇧", kb)

        if page == "home":
            return await render_main(cq, uid)

        # Admin check for group settings
        if chat_id and not await is_group_admin(client, chat_id, uid):
            return await cq.answer("You are not admin in this group.", show_alert=True)

        s = get_settings(chat_id)

        if page == "regulation":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "regulation_act", chat_id, "customize"))],
                [InlineKeyboardButton("📍 Commands Permissions", callback_data=cb(uid, "regulation_act", chat_id, "permission"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "📜 Group's regulations\n"
                "From this menu you can manage the group's\n"
                "regulations, that will be shown with the\n"
                "command /rules.\n\n"
                "To edit who can use the /rules command, go to\n"
                "the \"Commands permissions\" section."
            )
            return await edit_panel(cq, text, kb)

        if page == "regulation_act":
            if extra == "customize":
                pending_inputs[uid] = {"action": "set_rules", "chat_id": chat_id}
                return await cq.answer("Send the new rules text in private chat.", show_alert=True)
            if extra == "permission":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{'✅ ' if s['rules_cmd_permission']=='all' else ''}All users", callback_data=cb(uid, "reg_perm", chat_id, "all"))],
                    [InlineKeyboardButton(f"{'✅ ' if s['rules_cmd_permission']=='admins' else ''}Admins only", callback_data=cb(uid, "reg_perm", chat_id, "admins"))],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "regulation", chat_id))]
                ])
                return await edit_panel(cq, "Commands Permissions", kb)

        if page == "reg_perm":
            update_setting(chat_id, "rules_cmd_permission", extra)
            return await cq.answer("Updated")

        if page == "welcome":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "welcome_act", chat_id, "off")),
                 InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "welcome_act", chat_id, "on"))],
                [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "welcome_act", chat_id, "customize"))],
                [InlineKeyboardButton("🔔 Always send", callback_data=cb(uid, "welcome_act", chat_id, "always")),
                 InlineKeyboardButton("Send 1st join", callback_data=cb(uid, "welcome_act", chat_id, "first"))],
                [InlineKeyboardButton(f"Delete last message {'✅' if s['welcome_delete_last'] else '✖️'}", callback_data=cb(uid, "welcome_act", chat_id, "toggle_delete_last"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "💬 Welcome Message\n"
                "From this menu you can set a welcome\n"
                "message that will be sent when someone joins\n"
                "the group.\n\n"
                f"Status: {'On ✅' if s['welcome_enabled'] else 'Off ❌'}\n"
                f"Mode: {'Send the welcome message at every join of the users in the group' if s['welcome_mode']=='always' else 'Send only at first join'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "welcome_act":
            if extra == "off":
                update_setting(chat_id, "welcome_enabled", 0)
            elif extra == "on":
                update_setting(chat_id, "welcome_enabled", 1)
            elif extra == "customize":
                pending_inputs[uid] = {"action": "set_welcome", "chat_id": chat_id}
                return await cq.answer("Send welcome text in private chat.", show_alert=True)
            elif extra == "always":
                update_setting(chat_id, "welcome_mode", "always")
            elif extra == "first":
                update_setting(chat_id, "welcome_mode", "first")
            elif extra == "toggle_delete_last":
                update_setting(chat_id, "welcome_delete_last", 0 if s["welcome_delete_last"] else 1)
            s = get_settings(chat_id)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "welcome_act", chat_id, "off")),
                 InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "welcome_act", chat_id, "on"))],
                [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "welcome_act", chat_id, "customize"))],
                [InlineKeyboardButton(f"{'✅ ' if s['welcome_mode']=='always' else ''}🔔 Always send", callback_data=cb(uid, "welcome_act", chat_id, "always")),
                 InlineKeyboardButton(f"{'✅ ' if s['welcome_mode']=='first' else ''}Send 1st join", callback_data=cb(uid, "welcome_act", chat_id, "first"))],
                [InlineKeyboardButton(f"Delete last message {'✅' if s['welcome_delete_last'] else '✖️'}", callback_data=cb(uid, "welcome_act", chat_id, "toggle_delete_last"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "💬 Welcome Message\n"
                "From this menu you can set a welcome\n"
                "message that will be sent when someone joins\n"
                "the group.\n\n"
                f"Status: {'On ✅' if s['welcome_enabled'] else 'Off ❌'}\n"
                f"Mode: {'Send the welcome message at every join of the users in the group' if s['welcome_mode']=='always' else 'Send only at first join'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "goodbye":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "goodbye_act", chat_id, "off")),
                 InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "goodbye_act", chat_id, "on"))],
                [InlineKeyboardButton("✍🏻 Customize message", callback_data=cb(uid, "goodbye_act", chat_id, "customize"))],
                [InlineKeyboardButton(f"💌 Send in private chat {'✅' if s['goodbye_private'] else '✖️'}", callback_data=cb(uid, "goodbye_act", chat_id, "toggle_private"))],
                [InlineKeyboardButton(f"♻️ Delete last message {'✅' if s['goodbye_delete_last'] else '✖️'}", callback_data=cb(uid, "goodbye_act", chat_id, "toggle_delete_last"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "👋 Goodbye\n"
                "From this menu you can set a goodbye\n"
                "message that will be sent when someone\n"
                "leaves the group.\n\n"
                f"Status: {'On ✅' if s['goodbye_enabled'] else 'Off ❌'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "goodbye_act":
            if extra == "off":
                update_setting(chat_id, "goodbye_enabled", 0)
            elif extra == "on":
                update_setting(chat_id, "goodbye_enabled", 1)
            elif extra == "customize":
                pending_inputs[uid] = {"action": "set_goodbye", "chat_id": chat_id}
                return await cq.answer("Send goodbye text in private chat.", show_alert=True)
            elif extra == "toggle_private":
                update_setting(chat_id, "goodbye_private", 0 if s["goodbye_private"] else 1)
            elif extra == "toggle_delete_last":
                update_setting(chat_id, "goodbye_delete_last", 0 if s["goodbye_delete_last"] else 1)
            return await cq.answer("Updated")

        if page == "captcha":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Activate" if not s["captcha_enabled"] else "✖️ Deactivate", callback_data=cb(uid, "captcha_act", chat_id, "toggle"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🧠 Captcha\n"
                "By activating the captcha, when a user\n"
                "enters the group he will not be able to send\n"
                "messages until he has confirmed that he is not\n"
                "a robot.\n\n"
                "⏰ You can also decide to set a punishment\n"
                "down below for those who will not resolve the\n"
                "captcha within the desired time.\n\n"
                f"Status: {'On ✅' if s['captcha_enabled'] else 'Off ❌'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "captcha_act":
            update_setting(chat_id, "captcha_enabled", 0 if s["captcha_enabled"] else 1)
            return await cq.answer("Updated")

        if page == "admin_cmd":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✖️ Nobody", callback_data=cb(uid, "admin_act", chat_id, "nobody")),
                 InlineKeyboardButton("👑 Founder", callback_data=cb(uid, "admin_act", chat_id, "founder"))],
                [InlineKeyboardButton("👥 Staff Group", callback_data=cb(uid, "admin_act", chat_id, "staffgroup"))],
                [InlineKeyboardButton(f"🔔 Tag Founder {'✅' if s['admin_tag_founder'] else '❌'}", callback_data=cb(uid, "admin_act", chat_id, "tag_founder"))],
                [InlineKeyboardButton(f"🔔 Tag Admins {'✅' if s['admin_tag_admins'] else '❌'}", callback_data=cb(uid, "admin_act", chat_id, "tag_admins"))],
                [InlineKeyboardButton("🛠️ Advanced settings", callback_data=cb(uid, "admin_act", chat_id, "advanced"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🆘 @admin command\n"
                "@admin (or /report) is a command available to\n"
                "users to attract the attention of the group's\n"
                "staff.\n\n"
                "Status: Active\n"
                f"Send to: {'Nobody' if s['admin_status']=='nobody' else '👑 Founder' if s['admin_status']=='founder' else '👥 Staff Group'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "admin_act":
            if extra in {"nobody", "founder", "staffgroup"}:
                update_setting(chat_id, "admin_status", extra)
            elif extra == "tag_founder":
                update_setting(chat_id, "admin_tag_founder", 0 if s["admin_tag_founder"] else 1)
            elif extra == "tag_admins":
                update_setting(chat_id, "admin_tag_admins", 0 if s["admin_tag_admins"] else 1)
            elif extra == "advanced":
                return await edit_panel(cq, "Advanced settings\n\nThis section is not expanded in this build.", back_kb(uid, "admin_cmd", chat_id))
            return await cq.answer("Updated")

        if page == "media":
            text = (
                "📸 Media\n\n"
                "This simplified build supports media block as a whole.\n"
                f"Status: {'On ✅' if s['media_enabled'] else 'Off ❌'}\n"
                f"Action: {s['media_action'].title()}"
            )
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Allow", callback_data=cb(uid, "media_act", chat_id, "allow")),
                 InlineKeyboardButton("❗ Warn", callback_data=cb(uid, "media_act", chat_id, "warn")),
                 InlineKeyboardButton("🔇 Mute", callback_data=cb(uid, "media_act", chat_id, "mute"))],
                [InlineKeyboardButton("🚫 Block", callback_data=cb(uid, "media_act", chat_id, "block")),
                 InlineKeyboardButton("🗑️ Delete", callback_data=cb(uid, "media_act", chat_id, "delete"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            return await edit_panel(cq, text, kb)

        if page == "media_act":
            if extra == "allow":
                update_setting(chat_id, "media_enabled", 0)
                update_setting(chat_id, "media_action", "allow")
            elif extra == "warn":
                update_setting(chat_id, "media_enabled", 1)
                update_setting(chat_id, "media_action", "warn")
            elif extra == "mute":
                update_setting(chat_id, "media_enabled", 1)
                update_setting(chat_id, "media_action", "mute")
            elif extra == "block":
                update_setting(chat_id, "media_enabled", 1)
                update_setting(chat_id, "media_action", "block")
            elif extra == "delete":
                update_setting(chat_id, "media_enabled", 1)
                update_setting(chat_id, "media_action", "delete")
            return await cq.answer("Updated")

        if page == "warns_panel":
            row_numbers = []
            for n in [2, 3, 4, 5, 6]:
                label = f"{n}{' ✅' if s['warns_limit']==n else ''}"
                row_numbers.append(InlineKeyboardButton(label, callback_data=cb(uid, "warns_act", chat_id, f"limit_{n}")))
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🧾 Warned List", callback_data=cb(uid, "warns_act", chat_id, "list"))],
                [InlineKeyboardButton("✖️ Off", callback_data=cb(uid, "warns_act", chat_id, "off")),
                 InlineKeyboardButton("❗ Kick", callback_data=cb(uid, "warns_act", chat_id, "kick"))],
                [InlineKeyboardButton("🔇 Mute", callback_data=cb(uid, "warns_act", chat_id, "mute")),
                 InlineKeyboardButton("🚫 Ban", callback_data=cb(uid, "warns_act", chat_id, "ban"))],
                [InlineKeyboardButton("🔇⏱️ Set mute duration", callback_data=cb(uid, "warns_act", chat_id, "mute_duration"))],
                row_numbers,
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "❗ User warnings\n"
                "The warning system allows you to give\n"
                "warnings to users for incorrect behavior in the\n"
                "group, before actually punishing them.\n\n"
                f"Punishment: {s['warns_action'].title()}\n"
                f"Max Warns allowed: {s['warns_limit']}"
            )
            return await edit_panel(cq, text, kb)

        if page == "warns_act":
            if extra == "list":
                rows = list_warned(chat_id)
                txt = "Warned List\n\n" + ("\n".join(f"`{uid2}` → {w}" for uid2, w in rows) if rows else "Empty.")
                await cq.message.reply_text(txt)
                return await cq.answer("Sent.")
            if extra in {"off", "kick", "mute", "ban"}:
                update_setting(chat_id, "warns_action", extra)
            elif extra.startswith("limit_"):
                update_setting(chat_id, "warns_limit", int(extra.split("_", 1)[1]))
            elif extra == "mute_duration":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("10m", callback_data=cb(uid, "warns_mute_set", chat_id, "10")),
                     InlineKeyboardButton("30m", callback_data=cb(uid, "warns_mute_set", chat_id, "30")),
                     InlineKeyboardButton("60m", callback_data=cb(uid, "warns_mute_set", chat_id, "60"))],
                    [InlineKeyboardButton("120m", callback_data=cb(uid, "warns_mute_set", chat_id, "120")),
                     InlineKeyboardButton("240m", callback_data=cb(uid, "warns_mute_set", chat_id, "240"))],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "warns_panel", chat_id))]
                ])
                return await edit_panel(cq, "Set mute duration", kb)
            return await cq.answer("Updated")

        if page == "warns_mute_set":
            update_setting(chat_id, "warns_mute_minutes", int(extra))
            return await cq.answer("Updated")

        if page == "tag":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🪪 Buy the PRO", url="https://t.me")],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🔔 Tag alert\n"
                "When a user will be tagged, he will be notified\n"
                "with a private alert.\n\n"
                "⚠️ Available only for PRO groups"
            )
            return await edit_panel(cq, text, kb)

        if page == "anti_spam":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Telegram links", callback_data=cb(uid, "anti_spam_toggle", chat_id, "tg"))],
                [InlineKeyboardButton("Forwarding", callback_data=cb(uid, "anti_spam_toggle", chat_id, "forward")) ,
                 InlineKeyboardButton("Quote", callback_data=cb(uid, "anti_spam_toggle", chat_id, "quote"))],
                [InlineKeyboardButton("Total links block", callback_data=cb(uid, "anti_spam_toggle", chat_id, "total"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "✉️ Anti-Spam\n"
                "In this menu you can decide whether to\n"
                "protect your groups from unnecessary links,\n"
                "forwards, and quotes.\n\n"
                f"Telegram links: {bool_icon(bool(s['antispam_tg_links']))}\n"
                f"Forwarding: {bool_icon(bool(s['antispam_forwarding']))}\n"
                f"Quote: {bool_icon(bool(s['antispam_quote']))}\n"
                f"Total links block: {bool_icon(bool(s['antispam_total_links']))}"
            )
            return await edit_panel(cq, text, kb)

        if page == "anti_spam_toggle":
            mapping = {
                "tg": "antispam_tg_links",
                "forward": "antispam_forwarding",
                "quote": "antispam_quote",
                "total": "antispam_total_links",
            }
            field = mapping[extra]
            update_setting(chat_id, field, 0 if s[field] else 1)
            return await cq.answer("Updated")

        if page == "anti_flood":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📄 Messages", callback_data=cb(uid, "antiflood_conf", chat_id, "messages")),
                 InlineKeyboardButton("🕒 Time", callback_data=cb(uid, "antiflood_conf", chat_id, "time"))],
                [InlineKeyboardButton("❌ Off", callback_data=cb(uid, "antiflood_action", chat_id, "off")),
                 InlineKeyboardButton("❕ Warn", callback_data=cb(uid, "antiflood_action", chat_id, "warn"))],
                [InlineKeyboardButton("❗ Kick", callback_data=cb(uid, "antiflood_action", chat_id, "kick")),
                 InlineKeyboardButton("🔇 Mute", callback_data=cb(uid, "antiflood_action", chat_id, "mute")),
                 InlineKeyboardButton("🚫 Ban", callback_data=cb(uid, "antiflood_action", chat_id, "ban"))],
                [InlineKeyboardButton("🗑️ Delete Messages", callback_data=cb(uid, "antiflood_action", chat_id, "delete"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🗣️ Antiflood\n"
                "From this menu you can set a punishment for\n"
                "those who send many messages in a short\n"
                "time.\n\n"
                f"Currently the antiflood is triggered when {s['antiflood_messages']} messages are sent within {s['antiflood_seconds']} seconds.\n\n"
                f"Punishment: {s['antiflood_action'].title()}"
            )
            return await edit_panel(cq, text, kb)

        if page == "antiflood_action":
            update_setting(chat_id, "antiflood_action", extra)
            return await cq.answer("Updated")

        if page == "antiflood_conf":
            if extra == "messages":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(str(n), callback_data=cb(uid, "antiflood_set_messages", chat_id, str(n))) for n in [3, 4, 5, 6, 7]],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "anti_flood", chat_id))]
                ])
                return await edit_panel(cq, "Choose messages threshold", kb)
            if extra == "time":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton(str(n), callback_data=cb(uid, "antiflood_set_time", chat_id, str(n))) for n in [2, 3, 4, 5, 6]],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "anti_flood", chat_id))]
                ])
                return await edit_panel(cq, "Choose seconds threshold", kb)

        if page == "antiflood_set_messages":
            update_setting(chat_id, "antiflood_messages", int(extra))
            return await cq.answer("Updated")
        if page == "antiflood_set_time":
            update_setting(chat_id, "antiflood_seconds", int(extra))
            return await cq.answer("Updated")

        if page == "alphabets":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🟣 ARABIC", callback_data=cb(uid, "alpha_toggle", chat_id, "arabic")),
                 InlineKeyboardButton("🇷🇺 CYRILLIC", callback_data=cb(uid, "alpha_toggle", chat_id, "cyrillic"))],
                [InlineKeyboardButton("CHINESE", callback_data=cb(uid, "alpha_toggle", chat_id, "chinese")),
                 InlineKeyboardButton("LATIN", callback_data=cb(uid, "alpha_toggle", chat_id, "latin"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🕉️ Alphabets\n"
                "Select punishment for any user who send\n"
                "messages written in certain alphabets.\n\n"
                f"Arabic — Status: {'On' if s['alphabet_arabic'] else 'Off'}\n"
                f"Cyrillic — Status: {'On' if s['alphabet_cyrillic'] else 'Off'}\n"
                f"Chinese — Status: {'On' if s['alphabet_chinese'] else 'Off'}\n"
                f"Latin — Status: {'On' if s['alphabet_latin'] else 'Off'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "alpha_toggle":
            field = {
                "arabic": "alphabet_arabic",
                "cyrillic": "alphabet_cyrillic",
                "chinese": "alphabet_chinese",
                "latin": "alphabet_latin",
            }[extra]
            update_setting(chat_id, field, 0 if s[field] else 1)
            return await cq.answer("Updated")

        if page == "checks":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("OBLIGATIONS", callback_data=cb(uid, "checks_info", chat_id, "obligations")),
                 InlineKeyboardButton("NAME BLOCKS", callback_data=cb(uid, "checks_info", chat_id, "nameblocks"))],
                [InlineKeyboardButton(f"📜 Check at the join {'✅' if s['check_at_join'] else '✖️'}", callback_data=cb(uid, "checks_toggle", chat_id, "join"))],
                [InlineKeyboardButton(f"🗑️ Delete Messages {'✅' if s['checks_delete_messages'] else '✖️'}", callback_data=cb(uid, "checks_toggle", chat_id, "delete"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "OBLIGATION OF...\n"
                "• Surname: Off\n"
                "• Username: Off\n"
                "• Profile picture: Off\n"
                "• Channel obligation: Off\n"
                "• Obligation to add: Off\n\n"
                "BLOCK...\n"
                "• Arabic name: Off\n"
                "• Chinese name: Off\n"
                "• Russian Name: Off\n"
                "• Spam name: Off\n\n"
                "📜 Check at the join\n"
                f"Status: {'Active ✅' if s['check_at_join'] else 'Off ✖️'}\n\n"
                "🗑️ Delete Messages\n"
                f"Status: {'Active ✅' if s['checks_delete_messages'] else 'Off ✖️'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "checks_info":
            return await edit_panel(cq, f"{extra.title()}\n\nThis subpanel is informational in this build.", back_kb(uid, "checks", chat_id))

        if page == "checks_toggle":
            if extra == "join":
                update_setting(chat_id, "check_at_join", 0 if s["check_at_join"] else 1)
            elif extra == "delete":
                update_setting(chat_id, "checks_delete_messages", 0 if s["checks_delete_messages"] else 1)
            return await cq.answer("Updated")

        if page == "blocks":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⛔ Blacklist", callback_data=cb(uid, "blocks_sub", chat_id, "blacklist"))],
                [InlineKeyboardButton("🤖 Bot block", callback_data=cb(uid, "blocks_sub", chat_id, "botblock"))],
                [InlineKeyboardButton("🧑 Join block", callback_data=cb(uid, "blocks_sub", chat_id, "joinblock"))],
                [InlineKeyboardButton("🚪 Leave block", callback_data=cb(uid, "blocks_sub", chat_id, "leaveblock"))],
                [InlineKeyboardButton("🏃 Join-Leave block", callback_data=cb(uid, "blocks_sub", chat_id, "joinleave"))],
                [InlineKeyboardButton("Multiple joins bloc", callback_data=cb(uid, "blocks_sub", chat_id, "multiple"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            return await edit_panel(cq, "🔐 Blocks", kb)

        if page == "blocks_sub":
            if extra == "blacklist":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add word", callback_data=cb(uid, "blacklist_action", chat_id, "add")),
                     InlineKeyboardButton("➖ Remove word", callback_data=cb(uid, "blacklist_action", chat_id, "remove"))],
                    [InlineKeyboardButton("📋 Show", callback_data=cb(uid, "blacklist_action", chat_id, "show"))],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "blocks", chat_id))]
                ])
                return await edit_panel(cq, "Blacklist", kb)
            return await edit_panel(cq, f"{extra.title()}\n\nNot implemented deeply in this build.", back_kb(uid, "blocks", chat_id))

        if page == "blacklist_action":
            if extra == "add":
                pending_inputs[uid] = {"action": "add_banned_word", "chat_id": chat_id}
                return await cq.answer("Send the word in private chat.", show_alert=True)
            if extra == "remove":
                pending_inputs[uid] = {"action": "remove_banned_word", "chat_id": chat_id}
                return await cq.answer("Send the word in private chat.", show_alert=True)
            if extra == "show":
                words = [w.strip() for w in s["banned_words"].split(",") if w.strip()]
                await cq.message.reply_text("Blacklist\n\n" + (", ".join(words) if words else "Empty."))
                return await cq.answer("Sent")

        if page == "porn":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🪪 Buy the PRO", url="https://t.me")],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            return await edit_panel(cq, "🔞 Porn\n\nAvailable only for PRO groups.", kb)

        if page == "night":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📸 Delete medias", callback_data=cb(uid, "night_act", chat_id, "media")),
                 InlineKeyboardButton("🤫 Global Silence", callback_data=cb(uid, "night_act", chat_id, "silence"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🌘 Night mode\n"
                "Select the actions you want to limit every\n"
                "night.\n\n"
                f"Status: {'On ✅' if s['night_enabled'] else '✖️ Off'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "night_act":
            update_setting(chat_id, "night_enabled", 1)
            if extra == "media":
                update_setting(chat_id, "night_delete_medias", 0 if s["night_delete_medias"] else 1)
            elif extra == "silence":
                update_setting(chat_id, "night_global_silence", 0 if s["night_global_silence"] else 1)
            return await cq.answer("Updated")

        if page == "group_link":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✍🏻 Set", callback_data=cb(uid, "group_link_set", chat_id))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "🔗 Group link\n"
                "Here you can set the link of the group, which\n"
                "will be visible with the command /link.\n\n"
                f"Status: {'Activated' if s['link_enabled'] and s['group_link'] else 'Deactivated'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "group_link_set":
            pending_inputs[uid] = {"action": "set_group_link", "chat_id": chat_id}
            return await cq.answer("Send the group link in private chat.", show_alert=True)

        if page == "approval":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔓 Auto-approval ⬇️", callback_data=cb(uid, "approval_toggle", chat_id))],
                [InlineKeyboardButton("✖️ Turn off", callback_data=cb(uid, "approval_set", chat_id, "off")),
                 InlineKeyboardButton("✔️ Turn on", callback_data=cb(uid, "approval_set", chat_id, "on"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            text = (
                "📬 Approval mode\n"
                "Through this menu you can decide to delegate\n"
                "the management of group approvals to the\n"
                "bot.\n\n"
                "💡 Status:\n"
                f"• Auto-approval: {'Activated' if s['approval_enabled'] else 'Deactivated'}"
            )
            return await edit_panel(cq, text, kb)

        if page == "approval_toggle":
            update_setting(chat_id, "approval_enabled", 0 if s["approval_enabled"] else 1)
            return await cq.answer("Updated")

        if page == "approval_set":
            update_setting(chat_id, "approval_enabled", 1 if extra == "on" else 0)
            return await cq.answer("Updated")

        if page == "deleting":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🤖 Commands", callback_data=cb(uid, "deleting_toggle", chat_id, "commands"))],
                [InlineKeyboardButton("🤫 Global Silence", callback_data=cb(uid, "deleting_toggle", chat_id, "globalsilence"))],
                [InlineKeyboardButton("✍🏻 Edit Checks", callback_data=cb(uid, "deleting_toggle", chat_id, "editchecks"))],
                [InlineKeyboardButton("💥 Service Messages", callback_data=cb(uid, "deleting_toggle", chat_id, "servicemsg"))],
                [InlineKeyboardButton("🕒 Scheduled deletion", callback_data=cb(uid, "deleting_toggle", chat_id, "scheduled"))],
                [InlineKeyboardButton("Block cancellation", callback_data=cb(uid, "deleting_toggle", chat_id, "blockcancel"))],
                [InlineKeyboardButton("Delete all messages", callback_data=cb(uid, "deleting_toggle", chat_id, "allmsg"))],
                [InlineKeyboardButton("♻️ Messages self-destruction", callback_data=cb(uid, "deleting_toggle", chat_id, "selfdestruct"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            return await edit_panel(cq, "🗑️ Deleting Messages\nWhat messages do you want the Bot to delete?", kb)

        if page == "deleting_toggle":
            mapping = {
                "commands": "deleting_commands",
                "globalsilence": "deleting_global_silence",
                "editchecks": "deleting_edit_checks",
                "servicemsg": "deleting_service_messages",
                "scheduled": "deleting_scheduled",
                "blockcancel": "deleting_block_cancellation",
                "allmsg": "deleting_all_messages",
                "selfdestruct": "deleting_self_destruct",
            }
            field = mapping[extra]
            update_setting(chat_id, field, 0 if s[field] else 1)
            return await cq.answer("Updated")

        if page == "group_lang":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{'✅ ' if s['lang']=='en' else ''}English", callback_data=cb(uid, "set_group_lang", chat_id, "en")),
                 InlineKeyboardButton(f"{'✅ ' if s['lang']=='bn' else ''}বাংলা", callback_data=cb(uid, "set_group_lang", chat_id, "bn"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id))]
            ])
            return await edit_panel(cq, "🇬🇧 Lang", kb)

        if page == "set_group_lang":
            update_setting(chat_id, "lang", extra if extra in {"en", "bn"} else "en")
            return await cq.answer("Updated")

        if page == "other":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🗂️ Topic", callback_data=cb(uid, "other_sub", chat_id, "topic"))],
                [InlineKeyboardButton("🔤 Banned Words", callback_data=cb(uid, "other_sub", chat_id, "bannedwords"))],
                [InlineKeyboardButton("🕒 Recurring messages", callback_data=cb(uid, "other_sub", chat_id, "recurring"))],
                [InlineKeyboardButton("👥 Members Management", callback_data=cb(uid, "other_sub", chat_id, "members"))],
                [InlineKeyboardButton("🫥 Masked users", callback_data=cb(uid, "other_sub", chat_id, "masked"))],
                [InlineKeyboardButton("📣 Discussion group", callback_data=cb(uid, "other_sub", chat_id, "discussion"))],
                [InlineKeyboardButton("Personal Commands", callback_data=cb(uid, "other_sub", chat_id, "personal"))],
                [InlineKeyboardButton("🎭 Magic Stickers&GIFs", callback_data=cb(uid, "other_sub", chat_id, "magic"))],
                [InlineKeyboardButton("Message length", callback_data=cb(uid, "other_sub", chat_id, "length"))],
                [InlineKeyboardButton("📢 Channels management", callback_data=cb(uid, "other_sub", chat_id, "channels"))],
                [InlineKeyboardButton("Permissions", callback_data=cb(uid, "other_sub", chat_id, "permissions")),
                 InlineKeyboardButton("Log Channel", callback_data=cb(uid, "other_sub", chat_id, "logchannel"))],
                [InlineKeyboardButton("Back", callback_data=cb(uid, "settings_home", chat_id)),
                 InlineKeyboardButton("✅ Close", callback_data=cb(uid, "close")),
                 InlineKeyboardButton("🇬🇧 Lang", callback_data=cb(uid, "group_lang", chat_id))]
            ])
            return await edit_panel(cq, settings_home_text(chat_id), kb)

        if page == "other_sub":
            if extra == "bannedwords":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Add word", callback_data=cb(uid, "blacklist_action", chat_id, "add")),
                     InlineKeyboardButton("➖ Remove word", callback_data=cb(uid, "blacklist_action", chat_id, "remove"))],
                    [InlineKeyboardButton("📋 Show", callback_data=cb(uid, "blacklist_action", chat_id, "show"))],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "other", chat_id))]
                ])
                return await edit_panel(cq, "Banned Words", kb)
            if extra == "logchannel":
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("Set Log Channel", callback_data=cb(uid, "set_log_channel", chat_id))],
                    [InlineKeyboardButton("Back", callback_data=cb(uid, "other", chat_id))]
                ])
                return await edit_panel(cq, f"Log Channel\n\nCurrent: `{s['log_channel_id']}`", kb)
            return await edit_panel(cq, f"{extra.title()}\n\nNot expanded in this build.", back_kb(uid, "other", chat_id))

        if page == "set_log_channel":
            pending_inputs[uid] = {"action": "set_log_channel", "chat_id": chat_id}
            return await cq.answer("Send channel id in private chat.", show_alert=True)

        if page == "help_basic":
            return await edit_panel(cq, basic_commands_text(), InlineKeyboardMarkup([[InlineKeyboardButton("Back to Help", callback_data=cb(uid, "help_menu"))]]))
        if page == "help_advanced":
            return await edit_panel(cq, advanced_commands_text(), InlineKeyboardMarkup([[InlineKeyboardButton("Back to Help", callback_data=cb(uid, "help_menu"))]]))
        if page == "help_experts":
            return await edit_panel(cq, experts_text(), InlineKeyboardMarkup([[InlineKeyboardButton("Back to Help", callback_data=cb(uid, "help_menu"))]]))
        if page == "help_pro":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Set up Staff group", callback_data=cb(uid, "pro_sub", 0, "staff"))],
                [InlineKeyboardButton("👾 How to create a Clone", callback_data=cb(uid, "pro_sub", 0, "clone"))],
                [InlineKeyboardButton("Users Roles", callback_data=cb(uid, "pro_sub", 0, "roles"))],
                [InlineKeyboardButton("Back to Help", callback_data=cb(uid, "help_menu"))]
            ])
            return await edit_panel(cq, pro_guides_text(), kb)

        if page == "pro_sub":
            return await edit_panel(cq, f"{extra.title()}\n\nGuide placeholder.", InlineKeyboardMarkup([[InlineKeyboardButton("Back to Help", callback_data=cb(uid, "help_pro"))]]))

        if page == "help_menu":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("👮🏻‍♂️ Basic commands", callback_data=cb(uid, "help_basic")),
                 InlineKeyboardButton("Advanced 👮🏻", callback_data=cb(uid, "help_advanced"))],
                [InlineKeyboardButton("🕵🏻 Experts", callback_data=cb(uid, "help_experts")),
                 InlineKeyboardButton("Pro Guides 🧝🏻", callback_data=cb(uid, "help_pro"))],
            ])
            return await edit_panel(cq, help_menu_text(), kb)

        return await cq.answer("Unknown panel", show_alert=True)

    except Exception as e:
        logger.exception("callback error")
        await cq.answer(f"Error: {e}", show_alert=True)

# =========================================================
# Startup
# =========================================================
async def startup_report():
    if CFG.owner_id:
        try:
            await bot.send_message(CFG.owner_id, "✅ Bot started.")
        except Exception:
            pass


async def main():
    init_db()
    threading.Thread(target=run_web_server, daemon=True).start()

    await bot.start()
    me = await bot.get_me()
    runtime["bot_username"] = me.username or ""
    runtime["bot_id"] = me.id

    logger.info(f"Started as @{runtime['bot_username']}")
    asyncio.create_task(startup_report())
    await idle()


if __name__ == "__main__":
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass