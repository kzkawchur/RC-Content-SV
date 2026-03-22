import os
import re
import sys
import time
import math
import sqlite3
import shutil
import logging
import asyncio
import threading
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple, List

from flask import Flask, jsonify
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatPermissions, Message, ChatMemberUpdated
)
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant, ChatAdminRequired
from pyrogram.enums import ChatMemberStatus, ChatType

# =========================================================
# 1) Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("grouphelp_bot")

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

_rh = RecentLogHandler()
_rh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logging.getLogger().addHandler(_rh)

# =========================================================
# 2) Config
# =========================================================
def env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}

@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str
    string_session: str
    port: int
    download_dir: str
    db_path: str
    force_sub_channel: str
    custom_caption: str
    owner_id: int
    admins: Set[int]
    max_file_size: int
    max_queue_size: int
    max_pending_per_user: int
    user_cooldown_sec: int
    task_timeout_sec: int
    maintenance_mode: bool
    log_channel: int
    support_chat: str

def parse_admins(raw: str) -> Set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids

def load_config() -> Config:
    required = ["API_ID", "API_HASH", "BOT_TOKEN", "STRING_SESSION"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise ValueError(f"Missing env vars: {', '.join(missing)}")
    owner_id = int(os.environ.get("OWNER_ID", "0") or 0)
    admins = parse_admins(os.environ.get("ADMIN_IDS", ""))
    if owner_id:
        admins.add(owner_id)
    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],
        string_session=os.environ["STRING_SESSION"],
        port=int(os.environ.get("PORT", "10000")),
        download_dir=os.environ.get("DOWNLOAD_DIR", "downloads"),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),
        force_sub_channel=os.environ.get("FORCE_SUB_CHANNEL", "").strip(),
        custom_caption=os.environ.get("CUSTOM_CAPTION", "").strip(),
        owner_id=owner_id,
        admins=admins,
        max_file_size=int(os.environ.get("MAX_FILE_SIZE", str(2 * 1024 * 1024 * 1024))),
        max_queue_size=int(os.environ.get("MAX_QUEUE_SIZE", "25")),
        max_pending_per_user=int(os.environ.get("MAX_PENDING_PER_USER", "2")),
        user_cooldown_sec=int(os.environ.get("USER_COOLDOWN_SEC", "15")),
        task_timeout_sec=int(os.environ.get("TASK_TIMEOUT_SEC", "900")),
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        log_channel=int(os.environ.get("LOG_CHANNEL", "0") or 0),
        support_chat=os.environ.get("SUPPORT_CHAT", "").strip(),
    )

CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

@app.route("/")
def home():
    return "âœ… GroupHelp Bot is running", 200

@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "queue_size": task_queue.qsize() if "task_queue" in globals() else 0,
        "maintenance": state["maintenance_mode"],
        "active_task": runtime["active_task_id"],
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
# 5) Bot & Userbot clients
# =========================================================
bot = Client("grouphelp_bot", api_id=CFG.api_id, api_hash=CFG.api_hash, bot_token=CFG.bot_token)
userbot = Client("userbot_helper", api_id=CFG.api_id, api_hash=CFG.api_hash, session_string=CFG.string_session)

# =========================================================
# 6) Runtime state
# =========================================================
state = {
    "maintenance_mode": CFG.maintenance_mode,
    "started_at": time.time(),
    "total_tasks": 0,
    "success_tasks": 0,
    "failed_tasks": 0,
}
runtime = {"active_task_id": None, "active_user_id": None}
task_queue: asyncio.Queue = asyncio.Queue(maxsize=CFG.max_queue_size)
user_pending_count: Dict[int, int] = {}
user_last_request: Dict[int, float] = {}
task_registry: Dict[str, Dict] = {}
flood_tracker: Dict[str, list] = {}  # key: f"{chat_id}:{user_id}"

TG_LINK_RE = re.compile(
    r"^(https?://)?t\.me/(c/\d+/\d+|[A-Za-z0-9_]{4,}/\d+)(\?.*)?$",
    re.IGNORECASE
)

# =========================================================
# 7) Multilingual texts
# =========================================================
TEXTS = {
    "en": {
        "welcome_default": "ðŸ‘‹ Welcome {mention} to {title}!\nMember #{count}",
        "goodbye_default": "ðŸ‘‹ {mention} has left the group.",
        "maintenance": "ðŸ› ï¸ Bot is under maintenance.",
        "blocked": "ðŸš« You are blocked from using this bot.",
        "join_required": "ðŸ›‘ Please join the required channel first.",
        "cooldown": "â³ Cooldown: `{remain}`s remaining.",
        "busy": "ðŸš¦ Server is busy. Try again later.",
        "pending_limit": "ðŸ“Œ You have `{pending}` pending task(s). Wait.",
        "task_cancelled": "ðŸ›‘ Task cancelled.",
        "no_permission": "âŒ You don't have permission.",
        "bot_no_admin": "âš ï¸ I need admin rights for this.",
        "user_not_found": "âŒ User not found.",
        "cant_action_admin": "âŒ Cannot perform action on an admin.",
        "banned_success": "âœ… **{user}** has been banned.\nðŸ“ Reason: {reason}",
        "unbanned_success": "âœ… **{user}** has been unbanned.",
        "kicked_success": "âœ… **{user}** has been kicked.",
        "muted_success": "ðŸ”‡ **{user}** has been muted.\nðŸ“ Reason: {reason}",
        "unmuted_success": "ðŸ”Š **{user}** has been unmuted.",
        "warned_success": "âš ï¸ **{user}** warned!\nCount: `{count}/{max}`\nðŸ“ Reason: {reason}",
        "warn_limit_reached": "ðŸš« **{user}** has reached max warnings and has been banned!",
        "unwarned_success": "âœ… Warning removed for **{user}**.",
        "no_warnings": "âœ… **{user}** has no warnings.",
        "warn_list": "âš ï¸ **{user}** has `{count}/{max}` warnings:\n{reasons}",
        "rules_not_set": "ðŸ“‹ No rules set for this group.",
        "rules_set": "âœ… Rules updated!",
        "note_saved": "ðŸ“ Note `{name}` saved!",
        "note_not_found": "âŒ Note `{name}` not found.",
        "note_deleted": "ðŸ—‘ï¸ Note `{name}` deleted.",
        "no_notes": "ðŸ“ No notes saved.",
        "filter_saved": "âœ… Filter `{keyword}` saved!",
        "filter_deleted": "âœ… Filter `{keyword}` deleted.",
        "no_filters": "ðŸ“‹ No filters in this group.",
        "antiflood_set": "âœ… Anti-flood set to `{count}` messages in `{time}`s â†’ `{action}`",
        "antiflood_off": "âœ… Anti-flood disabled.",
        "captcha_on": "âœ… Captcha verification enabled.",
        "captcha_off": "âœ… Captcha verification disabled.",
        "captcha_challenge": "ðŸ‘‹ Welcome {mention}!\n\nPlease verify you're human within **60 seconds**.\nClick the button below:",
        "captcha_pass": "âœ… Verified! Welcome to the group!",
        "captcha_fail": "âŒ Verification failed. You've been kicked.",
        "report_sent": "âœ… Report sent to admins.",
        "report_msg": "ðŸš¨ **Report**\nFrom: {reporter}\nAbout: {reported}\nMessage: {msg}",
        "locked": "ðŸ”’ **{perm}** locked.",
        "unlocked": "ðŸ”“ **{perm}** unlocked.",
        "pinned": "ðŸ“Œ Message pinned!",
        "unpinned": "ðŸ“Œ Message unpinned!",
        "lang_set": "âœ… Language set to `{lang}`.",
    },
    "bn": {
        "welcome_default": "ðŸ‘‹ {mention} à¦•à§‡ {title}-à¦ à¦¸à§à¦¬à¦¾à¦—à¦¤à¦®!\nà¦¸à¦¦à¦¸à§à¦¯ #{count}",
        "goodbye_default": "ðŸ‘‹ {mention} à¦—à§à¦°à§à¦ª à¦›à§‡à¦¡à¦¼à§‡ à¦šà¦²à§‡ à¦—à§‡à¦›à§‡à¦¨à¥¤",
        "maintenance": "ðŸ› ï¸ à¦¬à¦Ÿ à¦à¦–à¦¨ maintenance-à¦ à¦†à¦›à§‡à¥¤",
        "blocked": "ðŸš« à¦¤à§à¦®à¦¿ à¦à¦‡ à¦¬à¦Ÿ à¦¬à§à¦¯à¦¬à¦¹à¦¾à¦° à¦•à¦°à¦¤à§‡ à¦ªà¦¾à¦°à¦¬à§‡ à¦¨à¦¾à¥¤",
        "join_required": "ðŸ›‘ à¦†à¦—à§‡ à¦šà§à¦¯à¦¾à¦¨à§‡à¦²à§‡ à¦œà¦¯à¦¼à§‡à¦¨ à¦•à¦°à§à¦¨à¥¤",
        "cooldown": "â³ Cooldown: à¦†à¦° `{remain}` à¦¸à§‡à¦•à§‡à¦¨à§à¦¡ à¦…à¦ªà§‡à¦•à§à¦·à¦¾ à¦•à¦°à§‹à¥¤",
        "busy": "ðŸš¦ à¦¸à¦¾à¦°à§à¦­à¦¾à¦° à¦¬à§à¦¯à¦¸à§à¦¤à¥¤ à¦à¦•à¦Ÿà§ à¦ªà¦°à§‡ à¦šà§‡à¦·à§à¦Ÿà¦¾ à¦•à¦°à§‹à¥¤",
        "pending_limit": "ðŸ“Œ à¦¤à§‹à¦®à¦¾à¦° `{pending}`à¦Ÿà¦¾ task pending à¦†à¦›à§‡à¥¤",
        "task_cancelled": "ðŸ›‘ Task à¦¬à¦¾à¦¤à¦¿à¦² à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "no_permission": "âŒ à¦¤à§‹à¦®à¦¾à¦° à¦à¦‡ à¦•à¦¾à¦œà§‡à¦° à¦…à¦¨à§à¦®à¦¤à¦¿ à¦¨à§‡à¦‡à¥¤",
        "bot_no_admin": "âš ï¸ à¦†à¦®à¦¾à¦•à§‡ admin à¦•à¦°à§‹ à¦à¦‡ à¦•à¦¾à¦œà§‡à¦° à¦œà¦¨à§à¦¯à¥¤",
        "user_not_found": "âŒ à¦‡à¦‰à¦œà¦¾à¦° à¦ªà¦¾à¦“à¦¯à¦¼à¦¾ à¦¯à¦¾à¦¯à¦¼à¦¨à¦¿à¥¤",
        "cant_action_admin": "âŒ Admin-à¦à¦° à¦‰à¦ªà¦° à¦à¦‡ à¦•à¦¾à¦œ à¦•à¦°à¦¾ à¦¯à¦¾à¦¬à§‡ à¦¨à¦¾à¥¤",
        "banned_success": "âœ… **{user}** à¦•à§‡ ban à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤\nðŸ“ à¦•à¦¾à¦°à¦£: {reason}",
        "unbanned_success": "âœ… **{user}** à¦•à§‡ unban à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "kicked_success": "âœ… **{user}** à¦•à§‡ kick à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "muted_success": "ðŸ”‡ **{user}** à¦•à§‡ mute à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤\nðŸ“ à¦•à¦¾à¦°à¦£: {reason}",
        "unmuted_success": "ðŸ”Š **{user}** à¦•à§‡ unmute à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "warned_success": "âš ï¸ **{user}** à¦•à§‡ à¦¸à¦¤à¦°à§à¦• à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡!\nCount: `{count}/{max}`\nðŸ“ à¦•à¦¾à¦°à¦£: {reason}",
        "warn_limit_reached": "ðŸš« **{user}** à¦¸à¦°à§à¦¬à§‹à¦šà§à¦š warning à¦ªà§‡à¦¯à¦¼à§‡ ban à¦¹à¦¯à¦¼à§‡ à¦—à§‡à¦›à§‡!",
        "unwarned_success": "âœ… **{user}** à¦à¦° warning à¦®à§à¦›à§‡ à¦¦à§‡à¦“à¦¯à¦¼à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "no_warnings": "âœ… **{user}** à¦à¦° à¦•à§‹à¦¨à§‹ warning à¦¨à§‡à¦‡à¥¤",
        "warn_list": "âš ï¸ **{user}** à¦à¦° `{count}/{max}` warning:\n{reasons}",
        "rules_not_set": "ðŸ“‹ à¦à¦‡ à¦—à§à¦°à§à¦ªà§‡ à¦•à§‹à¦¨à§‹ à¦¨à¦¿à¦¯à¦¼à¦® à¦¨à§‡à¦‡à¥¤",
        "rules_set": "âœ… à¦¨à¦¿à¦¯à¦¼à¦® à¦†à¦ªà¦¡à§‡à¦Ÿ à¦¹à¦¯à¦¼à§‡à¦›à§‡!",
        "note_saved": "ðŸ“ Note `{name}` à¦¸à§‡à¦­ à¦¹à¦¯à¦¼à§‡à¦›à§‡!",
        "note_not_found": "âŒ Note `{name}` à¦ªà¦¾à¦“à¦¯à¦¼à¦¾ à¦¯à¦¾à¦¯à¦¼à¦¨à¦¿à¥¤",
        "note_deleted": "ðŸ—‘ï¸ Note `{name}` à¦¡à¦¿à¦²à§‡à¦Ÿ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "no_notes": "ðŸ“ à¦•à§‹à¦¨à§‹ note à¦¨à§‡à¦‡à¥¤",
        "filter_saved": "âœ… Filter `{keyword}` à¦¸à§‡à¦­ à¦¹à¦¯à¦¼à§‡à¦›à§‡!",
        "filter_deleted": "âœ… Filter `{keyword}` à¦¡à¦¿à¦²à§‡à¦Ÿ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "no_filters": "ðŸ“‹ à¦à¦‡ à¦—à§à¦°à§à¦ªà§‡ à¦•à§‹à¦¨à§‹ filter à¦¨à§‡à¦‡à¥¤",
        "antiflood_set": "âœ… Anti-flood à¦¸à§‡à¦Ÿ: `{count}` à¦®à§‡à¦¸à§‡à¦œ `{time}`à¦¸à§‡à¦•à§‡à¦¨à§à¦¡à§‡ â†’ `{action}`",
        "antiflood_off": "âœ… Anti-flood à¦¬à¦¨à§à¦§à¥¤",
        "captcha_on": "âœ… Captcha à¦šà¦¾à¦²à§ à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "captcha_off": "âœ… Captcha à¦¬à¦¨à§à¦§ à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "captcha_challenge": "ðŸ‘‹ {mention} à¦•à§‡ à¦¸à§à¦¬à¦¾à¦—à¦¤à¦®!\n\n**à§¬à§¦ à¦¸à§‡à¦•à§‡à¦¨à§à¦¡à§‡à¦° à¦®à¦§à§à¦¯à§‡** à¦¨à¦¿à¦šà§‡à¦° à¦¬à¦¾à¦Ÿà¦¨à§‡ à¦•à§à¦²à¦¿à¦• à¦•à¦°à§‡ verify à¦•à¦°à§‹:",
        "captcha_pass": "âœ… Verified! à¦—à§à¦°à§à¦ªà§‡ à¦¸à§à¦¬à¦¾à¦—à¦¤à¦®!",
        "captcha_fail": "âŒ Verify à¦¹à¦¯à¦¼à¦¨à¦¿à¥¤ à¦¤à§‹à¦®à¦¾à¦•à§‡ kick à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "report_sent": "âœ… à¦°à¦¿à¦ªà§‹à¦°à§à¦Ÿ admins-à¦¦à§‡à¦° à¦•à¦¾à¦›à§‡ à¦ªà¦¾à¦ à¦¾à¦¨à§‹ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "report_msg": "ðŸš¨ **à¦°à¦¿à¦ªà§‹à¦°à§à¦Ÿ**\nFrom: {reporter}\nAbout: {reported}\nMessage: {msg}",
        "locked": "ðŸ”’ **{perm}** à¦²à¦• à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "unlocked": "ðŸ”“ **{perm}** à¦†à¦¨à¦²à¦• à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
        "pinned": "ðŸ“Œ à¦®à§‡à¦¸à§‡à¦œ pin à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡!",
        "unpinned": "ðŸ“Œ à¦®à§‡à¦¸à§‡à¦œ unpin à¦•à¦°à¦¾ à¦¹à¦¯à¦¼à§‡à¦›à§‡!",
        "lang_set": "âœ… à¦­à¦¾à¦·à¦¾ `{lang}` à¦¸à§‡à¦Ÿ à¦¹à¦¯à¦¼à§‡à¦›à§‡à¥¤",
    }
}

# =========================================================
# 8) Database
# =========================================================
def cleanup_storage():
    if os.path.exists(CFG.download_dir):
        shutil.rmtree(CFG.download_dir, ignore_errors=True)
    os.makedirs(CFG.download_dir, exist_ok=True)

def db_connect():
    return sqlite3.connect(CFG.db_path, check_same_thread=False)

def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        # Core tables
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_seen INTEGER, last_seen INTEGER,
            username TEXT, first_name TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY, user_id INTEGER, username TEXT,
            created_at INTEGER, status TEXT, input_text TEXT, error_text TEXT)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY, reason TEXT, banned_at INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'en')""")
        # Group tables
        cur.execute("""CREATE TABLE IF NOT EXISTS group_settings (
            chat_id INTEGER PRIMARY KEY,
            language TEXT DEFAULT 'en',
            welcome_text TEXT DEFAULT '',
            welcome_buttons TEXT DEFAULT '',
            goodbye_text TEXT DEFAULT '',
            rules TEXT DEFAULT '',
            captcha_enabled INTEGER DEFAULT 0,
            antiflood_count INTEGER DEFAULT 0,
            antiflood_time INTEGER DEFAULT 10,
            antiflood_action TEXT DEFAULT 'mute',
            max_warnings INTEGER DEFAULT 3,
            media_lock INTEGER DEFAULT 0,
            sticker_lock INTEGER DEFAULT 0,
            link_lock INTEGER DEFAULT 0,
            forward_lock INTEGER DEFAULT 0,
            bot_lock INTEGER DEFAULT 0)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, chat_id INTEGER, reason TEXT, warned_at INTEGER)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, name TEXT, content TEXT,
            UNIQUE(chat_id, name))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER, keyword TEXT, response TEXT,
            UNIQUE(chat_id, keyword))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS captcha_pending (
            user_id INTEGER, chat_id INTEGER, msg_id INTEGER,
            created_at INTEGER, PRIMARY KEY(user_id, chat_id))""")
        conn.commit()

# ---- User helpers ----
def upsert_user(user_id, username, first_name):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO users(user_id, first_seen, last_seen, username, first_name)
            VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET
            last_seen=excluded.last_seen, username=excluded.username, first_name=excluded.first_name""",
            (user_id, now, now, username or "", first_name or ""))
        conn.commit()

def safe_total_users():
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]

def latest_users(limit=10):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, username, first_name, last_seen FROM users ORDER BY last_seen DESC LIMIT ?", (limit,))
        return cur.fetchall()

def is_globally_banned(user_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bans WHERE user_id=?", (user_id,))
        return cur.fetchone() is not None

def ban_user_global(user_id, reason=""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO bans(user_id, reason, banned_at) VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, banned_at=excluded.banned_at""",
            (user_id, reason, int(time.time())))
        conn.commit()

def unban_user_global(user_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
        conn.commit()

def get_user_language(user_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT language FROM user_settings WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return row[0] if row and row[0] in {"en", "bn"} else "en"

def set_user_language(user_id, language):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO user_settings(user_id, language) VALUES(?,?)
            ON CONFLICT(user_id) DO UPDATE SET language=excluded.language""", (user_id, language))
        conn.commit()

# ---- Group settings helpers ----
def ensure_group(chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO group_settings(chat_id) VALUES(?)", (chat_id,))
        conn.commit()

def get_group_setting(chat_id, key):
    ensure_group(chat_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT {key} FROM group_settings WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None

def set_group_setting(chat_id, key, value):
    ensure_group(chat_id)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE group_settings SET {key}=? WHERE chat_id=?", (value, chat_id))
        conn.commit()

def get_group_language(chat_id):
    val = get_group_setting(chat_id, "language")
    return val if val in {"en", "bn"} else "en"

def set_group_language(chat_id, lang):
    set_group_setting(chat_id, "language", lang)

# ---- Task helpers ----
def add_task_record(task_id, user_id, username, input_text):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO tasks(id, user_id, username, created_at, status, input_text, error_text)
            VALUES(?,?,?,?,?,?,?)""", (task_id, user_id, username or "", int(time.time()), "queued", input_text, ""))
        conn.commit()

def update_task_record(task_id, status, error_text=""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE tasks SET status=?, error_text=? WHERE id=?", (status, error_text, task_id))
        conn.commit()

# ---- Warning helpers ----
def add_warning(user_id, chat_id, reason=""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO warnings(user_id, chat_id, reason, warned_at) VALUES(?,?,?,?)",
            (user_id, chat_id, reason, int(time.time())))
        conn.commit()

def get_warnings(user_id, chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT reason FROM warnings WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        return [r[0] for r in cur.fetchall()]

def clear_warnings(user_id, chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM warnings WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        conn.commit()

def remove_last_warning(user_id, chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""DELETE FROM warnings WHERE id = (
            SELECT id FROM warnings WHERE user_id=? AND chat_id=? ORDER BY warned_at DESC LIMIT 1)""",
            (user_id, chat_id))
        conn.commit()

# ---- Notes helpers ----
def save_note(chat_id, name, content):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO notes(chat_id, name, content) VALUES(?,?,?)
            ON CONFLICT(chat_id, name) DO UPDATE SET content=excluded.content""",
            (chat_id, name.lower(), content))
        conn.commit()

def get_note(chat_id, name):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT content FROM notes WHERE chat_id=? AND name=?", (chat_id, name.lower()))
        row = cur.fetchone()
        return row[0] if row else None

def delete_note(chat_id, name):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM notes WHERE chat_id=? AND name=?", (chat_id, name.lower()))
        conn.commit()

def list_notes(chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM notes WHERE chat_id=? ORDER BY name", (chat_id,))
        return [r[0] for r in cur.fetchall()]

# ---- Filters helpers ----
def save_filter(chat_id, keyword, response):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT INTO filters(chat_id, keyword, response) VALUES(?,?,?)
            ON CONFLICT(chat_id, keyword) DO UPDATE SET response=excluded.response""",
            (chat_id, keyword.lower(), response))
        conn.commit()

def delete_filter(chat_id, keyword):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM filters WHERE chat_id=? AND keyword=?", (chat_id, keyword.lower()))
        conn.commit()

def get_filters(chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT keyword, response FROM filters WHERE chat_id=?", (chat_id,))
        return {r[0]: r[1] for r in cur.fetchall()}

# ---- Captcha helpers ----
def set_captcha_pending(user_id, chat_id, msg_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""INSERT OR REPLACE INTO captcha_pending(user_id, chat_id, msg_id, created_at)
            VALUES(?,?,?,?)""", (user_id, chat_id, msg_id, int(time.time())))
        conn.commit()

def remove_captcha_pending(user_id, chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM captcha_pending WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        conn.commit()

def is_captcha_pending(user_id, chat_id):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT msg_id FROM captcha_pending WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        row = cur.fetchone()
        return row[0] if row else None

# =========================================================
# 9) Utility helpers
# =========================================================
def is_bot_admin(user_id: int) -> bool:
    return user_id in CFG.admins

def tl(chat_id_or_user_id: int, key: str, is_group: bool = False, **kwargs) -> str:
    if is_group:
        lang = get_group_language(chat_id_or_user_id)
    else:
        lang = get_user_language(chat_id_or_user_id)
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)

def humanbytes(size: int) -> str:
    if not size:
        return "0 B"
    power = 1024
    unit = 0
    units = ["B", "KB", "MB", "GB", "TB"]
    while size >= power and unit < len(units) - 1:
        size /= power
        unit += 1
    return f"{size:.2f} {units[unit]}"

def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def valid_tg_link(text: str) -> bool:
    return bool(TG_LINK_RE.match(text.strip()))

def make_task_id(user_id: int) -> str:
    return f"{user_id}_{int(time.time() * 1000)}"

def user_on_cooldown(user_id: int) -> Tuple[bool, int]:
    last = user_last_request.get(user_id, 0)
    remain = CFG.user_cooldown_sec - int(time.time() - last)
    return (remain > 0, max(remain, 0))

def register_task(task_id, user_id, input_text):
    task_registry[task_id] = {
        "user_id": user_id, "input_text": input_text,
        "status": "queued", "created_at": int(time.time()), "cancelled": False
    }

def set_task_status(task_id, status):
    if task_id in task_registry:
        task_registry[task_id]["status"] = status

def get_user_tasks(user_id, limit=5):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, created_at, status, input_text FROM tasks WHERE user_id=? ORDER BY created_at DESC LIMIT ?", (user_id, limit))
        return cur.fetchall()

async def check_fsub(client, message) -> bool:
    if not CFG.force_sub_channel:
        return True
    try:
        await client.get_chat_member(CFG.force_sub_channel, message.from_user.id)
        return True
    except Exception:
        return False

async def get_target_user(client, message: Message):
    """Extract target user from reply or argument."""
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        return None
    identifier = parts[1].lstrip("@")
    try:
        if identifier.lstrip("-").isdigit():
            return await client.get_users(int(identifier))
        return await client.get_users(identifier)
    except Exception:
        return None

async def get_reason(message: Message, default="No reason given") -> str:
    parts = message.text.split(maxsplit=2 if message.reply_to_message else 3)
    if message.reply_to_message:
        return parts[1].strip() if len(parts) > 1 else default
    return parts[2].strip() if len(parts) > 2 else default

async def is_group_admin(client, chat_id, user_id) -> bool:
    try:
        member = await client.get_chat_member(chat_id, user_id)
        return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False

async def send_log(client, text):
    if CFG.log_channel:
        try:
            await client.send_message(CFG.log_channel, text)
        except Exception:
            pass

# =========================================================
# 10) Progress bar (for downloader)
# =========================================================
async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total if total else 0
        completed = math.floor(percentage / 5)
        bar = "[{0}{1}]".format("â–ˆ" * completed, "â–’" * (20 - completed))
        speed = current / diff if diff > 0 else 0
        tmp = (
            f"**{ud_type}**\n\n"
            f"ðŸ“Š Progress: `{round(percentage, 2)}%`\n"
            f"ðŸš€ `{bar}`\n\n"
            f"ðŸ“ Size: `{humanbytes(current)} / {humanbytes(total)}`\n"
            f"âš¡ Speed: `{humanbytes(speed)}/s`"
        )
        try:
            await message.edit_text(text=tmp)
        except Exception:
            pass

# =========================================================
# 11) UI Builders
# =========================================================
def build_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ðŸ“Š Stats", callback_data="admin_stats"),
         InlineKeyboardButton("ðŸ“¦ Queue", callback_data="admin_queue")],
        [InlineKeyboardButton("ðŸ‘¥ Users", callback_data="admin_users"),
         InlineKeyboardButton("ðŸ›  Maintenance", callback_data="admin_maint")],
        [InlineKeyboardButton("ðŸ§¹ Clear Queue", callback_data="admin_clearqueue"),
         InlineKeyboardButton("ðŸ“œ Logs", callback_data="admin_logs")]
    ])

def build_settings_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'âœ… ' if lang == 'en' else ''}English", callback_data="setlang_en"),
         InlineKeyboardButton(f"{'âœ… ' if lang == 'bn' else ''}à¦¬à¦¾à¦‚à¦²à¦¾", callback_data="setlang_bn")]
    ])

def build_group_settings_panel(chat_id: int) -> InlineKeyboardMarkup:
    cap = get_group_setting(chat_id, "captcha_enabled")
    afc = get_group_setting(chat_id, "antiflood_count")
    ml = get_group_setting(chat_id, "media_lock")
    sl = get_group_setting(chat_id, "sticker_lock")
    ll = get_group_setting(chat_id, "link_lock")
    fl = get_group_setting(chat_id, "forward_lock")
    lang = get_group_language(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'ðŸŸ¢' if cap else 'ðŸ”´'} Captcha", callback_data=f"gs_captcha_{chat_id}"),
         InlineKeyboardButton(f"{'ðŸŸ¢' if afc else 'ðŸ”´'} Anti-Flood", callback_data=f"gs_flood_{chat_id}")],
        [InlineKeyboardButton(f"{'ðŸ”’' if ml else 'ðŸ”“'} Media", callback_data=f"gs_media_{chat_id}"),
         InlineKeyboardButton(f"{'ðŸ”’' if sl else 'ðŸ”“'} Stickers", callback_data=f"gs_sticker_{chat_id}")],
        [InlineKeyboardButton(f"{'ðŸ”’' if ll else 'ðŸ”“'} Links", callback_data=f"gs_link_{chat_id}"),
         InlineKeyboardButton(f"{'ðŸ”’' if fl else 'ðŸ”“'} Forwards", callback_data=f"gs_forward_{chat_id}")],
        [InlineKeyboardButton(f"{'âœ… ' if lang == 'en' else ''}EN", callback_data=f"gs_lang_en_{chat_id}"),
         InlineKeyboardButton(f"{'âœ… ' if lang == 'bn' else ''}BN", callback_data=f"gs_lang_bn_{chat_id}")],
        [InlineKeyboardButton("ðŸ”™ Close", callback_data="gs_close")]
    ])

def build_start_buttons() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("âž• Add to Group", url=f"https://t.me/YOUR_BOT?startgroup=true"),
         InlineKeyboardButton("â“ Help", callback_data="help_main")]
    ]
    if CFG.support_chat:
        buttons.append([InlineKeyboardButton("ðŸ’¬ Support", url=f"https://t.me/{CFG.support_chat.lstrip('@')}")])
    return InlineKeyboardMarkup(buttons)

def build_help_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("ðŸ›¡ Moderation", callback_data="help_mod"),
         InlineKeyboardButton("ðŸ‘‹ Welcome", callback_data="help_welcome")],
        [InlineKeyboardButton("âš ï¸ Warnings", callback_data="help_warn"),
         InlineKeyboardButton("ðŸ“ Notes", callback_data="help_notes")],
        [InlineKeyboardButton("ðŸ” Filters", callback_data="help_filters"),
         InlineKeyboardButton("ðŸ”’ Locks", callback_data="help_locks")],
        [InlineKeyboardButton("ðŸ¤– Captcha", callback_data="help_captcha"),
         InlineKeyboardButton("ðŸŒŠ Anti-Flood", callback_data="help_flood")],
        [InlineKeyboardButton("ðŸ“¥ Downloader", callback_data="help_downloader"),
         InlineKeyboardButton("âš™ï¸ Settings", callback_data="help_settings")],
        [InlineKeyboardButton("ðŸ”™ Back", callback_data="help_back")]
    ])

HELP_TEXTS = {
    "main": """ðŸ¤– **GroupHelp Bot**

I'm a powerful group management bot with a built-in Telegram content extractor!

Choose a category below to learn more ðŸ‘‡""",
    "mod": """ðŸ›¡ **Moderation Commands**

`/ban [@user] [reason]` â€” Ban a user
`/unban [@user]` â€” Unban a user
`/kick [@user] [reason]` â€” Kick a user
`/mute [@user] [reason]` â€” Mute a user
`/unmute [@user]` â€” Unmute a user
`/promote [@user]` â€” Promote to admin
`/demote [@user]` â€” Demote admin
`/adminlist` â€” List all admins
`/pin` â€” Pin a message (reply)
`/unpin` â€” Unpin current pinned message""",
    "welcome": """ðŸ‘‹ **Welcome & Goodbye**

`/setwelcome <text>` â€” Set welcome message
`/resetwelcome` â€” Reset to default
`/welcome` â€” Show current welcome
`/setgoodbye <text>` â€” Set goodbye message
`/resetgoodbye` â€” Reset goodbye

**Variables:**
`{mention}` â€” User mention
`{first}` â€” First name
`{last}` â€” Last name
`{username}` â€” Username
`{title}` â€” Group name
`{count}` â€” Member count""",
    "warn": """âš ï¸ **Warning System**

`/warn [@user] [reason]` â€” Warn a user
`/unwarn [@user]` â€” Remove last warning
`/warnlist [@user]` â€” View user's warnings
`/clearwarns [@user]` â€” Clear all warnings
`/setwarnlimit <n>` â€” Set max warnings (default 3)

After hitting the limit, user gets **banned**.""",
    "notes": """ðŸ“ **Notes**

`/save <name> <content>` â€” Save a note
`/note <name>` or `#name` â€” Get a note
`/notes` â€” List all notes
`/delnote <name>` â€” Delete a note""",
    "filters": """ðŸ” **Filters**

`/filter <keyword> <response>` â€” Add filter
`/filters` â€” List filters
`/stop <keyword>` â€” Remove a filter

When someone sends the keyword, bot auto-replies!""",
    "locks": """ðŸ”’ **Lock System**

`/lock <type>` â€” Lock a feature
`/unlock <type>` â€” Unlock it
`/locks` â€” View lock status

**Types:** `media`, `stickers`, `links`, `forwards`, `bots`, `all`""",
    "captcha": """ðŸ¤– **Captcha Verification**

`/captcha on|off` â€” Enable/disable captcha
New members must click a button within 60s or they get kicked.""",
    "flood": """ðŸŒŠ **Anti-Flood**

`/antiflood <count> <time> <action>` â€” Set flood limit
`/antiflood off` â€” Disable anti-flood

**Example:** `/antiflood 5 10 mute`
(5 msgs in 10s â†’ mute)
**Actions:** `mute`, `kick`, `ban`""",
    "downloader": """ðŸ“¥ **Restricted Content Downloader**

Send a private message to the bot with any restricted Telegram post link:
`https://t.me/c/12345/678`
`https://t.me/channelname/123`

The bot will download and send you the file!""",
    "settings": """âš™ï¸ **Settings**

`/settings` â€” Group settings panel (in groups)
`/lang en|bn` â€” Change language
`/rules [text]` â€” Set or view rules
`/setrules <text>` â€” Update rules""",
}

# =========================================================
# 12) Anti-flood logic
# =========================================================
async def check_flood(client, message: Message) -> bool:
    """Returns True if user is flooding (action taken)."""
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        return False

    flood_count = get_group_setting(chat_id, "antiflood_count")
    flood_time = get_group_setting(chat_id, "antiflood_time")
    if not flood_count or flood_count <= 0:
        return False

    key = f"{chat_id}:{user_id}"
    now = time.time()
    msgs = flood_tracker.get(key, [])
    msgs = [t for t in msgs if now - t < flood_time]
    msgs.append(now)
    flood_tracker[key] = msgs

    if len(msgs) >= flood_count:
        flood_tracker[key] = []
        action = get_group_setting(chat_id, "antiflood_action") or "mute"
        try:
            if action == "ban":
                await client.ban_chat_member(chat_id, user_id)
                await message.reply_text(f"ðŸš« **{message.from_user.mention}** was banned for flooding!")
            elif action == "kick":
                await client.ban_chat_member(chat_id, user_id)
                await asyncio.sleep(1)
                await client.unban_chat_member(chat_id, user_id)
                await message.reply_text(f"ðŸ‘¢ **{message.from_user.mention}** was kicked for flooding!")
            else:
                await client.restrict_chat_member(chat_id, user_id, ChatPermissions(can_send_messages=False))
                await message.reply_text(f"ðŸ”‡ **{message.from_user.mention}** was muted for flooding!")
        except Exception:
            pass
        return True
    return False

# =========================================================
# 13) Private commands (Start, Help, Settings, Lang)
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)
    if is_globally_banned(user.id):
        return await message.reply_text(tl(user.id, "blocked"))
    if state["maintenance_mode"] and not is_bot_admin(user.id):
        return await message.reply_text(tl(user.id, "maintenance"))
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("ðŸ“¢ Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(tl(user.id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))

    welcome_text = (
        f"ðŸ‘‹ **Hello, {user.first_name}!**\n\n"
        f"I'm **GroupHelp Bot** â€” a powerful group manager + content extractor!\n\n"
        f"**In Groups:** I manage moderation, welcome messages, filters, notes & more.\n"
        f"**In Private:** Send me any restricted Telegram link and I'll extract it!\n\n"
        f"Use the buttons below to learn more ðŸ‘‡"
    )
    await message.reply_text(welcome_text, reply_markup=build_start_buttons())

@bot.on_message(filters.command("start") & (filters.group))
async def start_group_cmd(client, message):
    await message.reply_text("ðŸ‘‹ I'm online! Use /help to see what I can do.")

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd_private(client, message):
    await message.reply_text(HELP_TEXTS["main"], reply_markup=build_help_menu())

@bot.on_message(filters.command("help") & (filters.group))
async def help_cmd_group(client, message):
    await message.reply_text(HELP_TEXTS["main"], reply_markup=build_help_menu())

@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd_private(client, message):
    lang = get_user_language(message.from_user.id)
    await message.reply_text(
        f"âš™ï¸ **Personal Settings**\n\nðŸŒ Language: `{lang}`",
        reply_markup=build_settings_panel(message.from_user.id)
    )

@bot.on_message(filters.command("settings") & (filters.group))
async def settings_cmd_group(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_group_admin(client, chat_id, user_id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    ensure_group(chat_id)
    await message.reply_text("âš™ï¸ **Group Settings**", reply_markup=build_group_settings_panel(chat_id))

@bot.on_message(filters.command("lang"))
async def lang_cmd(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/lang en` or `/lang bn`", quote=True)
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only `en` or `bn`.")
    if message.chat.type in (ChatType.GROUP,):
        if not await is_group_admin(client, message.chat.id, message.from_user.id):
            return await message.reply_text(tl(message.chat.id, "no_permission", is_group=True))
        set_group_language(message.chat.id, lang)
    else:
        set_user_language(message.from_user.id, lang)
    await message.reply_text(tl(message.from_user.id, "lang_set", lang=lang))

# =========================================================
# 14) Group Moderation Commands
# =========================================================
@bot.on_message(filters.command("ban") & (filters.group))
async def ban_cmd(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if not await is_group_admin(client, chat_id, user_id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    if await is_group_admin(client, chat_id, target.id):
        return await message.reply_text(tl(chat_id, "cant_action_admin", is_group=True))
    reason = await get_reason(message)
    try:
        await client.ban_chat_member(chat_id, target.id)
        text = tl(chat_id, "banned_success", is_group=True, user=target.mention, reason=reason)
        await message.reply_text(text)
        await send_log(client, f"ðŸš« **BAN** | Chat: {message.chat.title}\nUser: {target.mention}\nBy: {message.from_user.mention}\nReason: {reason}")
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("unban") & (filters.group))
async def unban_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    try:
        await client.unban_chat_member(chat_id, target.id)
        await message.reply_text(tl(chat_id, "unbanned_success", is_group=True, user=target.mention))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("kick") & (filters.group))
async def kick_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    if await is_group_admin(client, chat_id, target.id):
        return await message.reply_text(tl(chat_id, "cant_action_admin", is_group=True))
    try:
        await client.ban_chat_member(chat_id, target.id)
        await asyncio.sleep(1)
        await client.unban_chat_member(chat_id, target.id)
        await message.reply_text(tl(chat_id, "kicked_success", is_group=True, user=target.mention))
        await send_log(client, f"ðŸ‘¢ **KICK** | Chat: {message.chat.title}\nUser: {target.mention}\nBy: {message.from_user.mention}")
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("mute") & (filters.group))
async def mute_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    if await is_group_admin(client, chat_id, target.id):
        return await message.reply_text(tl(chat_id, "cant_action_admin", is_group=True))
    reason = await get_reason(message)
    try:
        await client.restrict_chat_member(chat_id, target.id, ChatPermissions(can_send_messages=False))
        await message.reply_text(tl(chat_id, "muted_success", is_group=True, user=target.mention, reason=reason))
        await send_log(client, f"ðŸ”‡ **MUTE** | Chat: {message.chat.title}\nUser: {target.mention}\nBy: {message.from_user.mention}\nReason: {reason}")
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("unmute") & (filters.group))
async def unmute_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    try:
        await client.restrict_chat_member(chat_id, target.id, ChatPermissions(
            can_send_messages=True, can_send_media_messages=True,
            can_send_other_messages=True, can_add_web_page_previews=True
        ))
        await message.reply_text(tl(chat_id, "unmuted_success", is_group=True, user=target.mention))
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("promote") & (filters.group))
async def promote_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    try:
        await client.promote_chat_member(chat_id, target.id,
            can_change_info=True, can_delete_messages=True, can_restrict_members=True,
            can_invite_users=True, can_pin_messages=True)
        await message.reply_text(f"â­ **{target.mention}** has been promoted to admin!")
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("demote") & (filters.group))
async def demote_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    try:
        await client.promote_chat_member(chat_id, target.id,
            can_change_info=False, can_delete_messages=False, can_restrict_members=False,
            can_invite_users=False, can_pin_messages=False)
        await message.reply_text(f"â¬‡ï¸ **{target.mention}** has been demoted.")
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("adminlist") & (filters.group))
async def adminlist_cmd(client, message):
    chat_id = message.chat.id
    try:
        admins = []
        async for member in client.get_chat_members(chat_id, filter="administrators"):
            title = member.custom_title or ""
            name = member.user.first_name
            mention = f"[{name}](tg://user?id={member.user.id})"
            badge = "ðŸ‘‘" if member.status == ChatMemberStatus.OWNER else "â­"
            admins.append(f"{badge} {mention}" + (f" â€” `{title}`" if title else ""))
        text = "**ðŸ‘® Admin List:**\n\n" + "\n".join(admins)
        await message.reply_text(text)
    except Exception as e:
        await message.reply_text(f"âŒ Error: {e}")

@bot.on_message(filters.command("pin") & (filters.group))
async def pin_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    if not message.reply_to_message:
        return await message.reply_text("â“ Reply to a message to pin it.")
    try:
        await client.pin_chat_message(chat_id, message.reply_to_message.id, disable_notification=False)
        await message.reply_text(tl(chat_id, "pinned", is_group=True))
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))

@bot.on_message(filters.command("unpin") & (filters.group))
async def unpin_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    try:
        await client.unpin_chat_message(chat_id)
        await message.reply_text(tl(chat_id, "unpinned", is_group=True))
    except ChatAdminRequired:
        await message.reply_text(tl(chat_id, "bot_no_admin", is_group=True))

# =========================================================
# 15) Warning System
# =========================================================
@bot.on_message(filters.command("warn") & (filters.group))
async def warn_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    if await is_group_admin(client, chat_id, target.id):
        return await message.reply_text(tl(chat_id, "cant_action_admin", is_group=True))

    reason = await get_reason(message)
    add_warning(target.id, chat_id, reason)
    warns = get_warnings(target.id, chat_id)
    max_w = get_group_setting(chat_id, "max_warnings") or 3
    count = len(warns)

    if count >= max_w:
        clear_warnings(target.id, chat_id)
        try:
            await client.ban_chat_member(chat_id, target.id)
        except Exception:
            pass
        await message.reply_text(tl(chat_id, "warn_limit_reached", is_group=True, user=target.mention))
        await send_log(client, f"ðŸš« **AUTO-BAN (warnings)** | {message.chat.title}\nUser: {target.mention}")
    else:
        await message.reply_text(tl(chat_id, "warned_success", is_group=True,
            user=target.mention, count=count, max=max_w, reason=reason))

@bot.on_message(filters.command("unwarn") & (filters.group))
async def unwarn_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    remove_last_warning(target.id, chat_id)
    await message.reply_text(tl(chat_id, "unwarned_success", is_group=True, user=target.mention))

@bot.on_message(filters.command(["warnlist", "warnings"]) & (filters.group))
async def warnlist_cmd(client, message):
    chat_id = message.chat.id
    target = await get_target_user(client, message)
    if not target:
        target = message.from_user
    warns = get_warnings(target.id, chat_id)
    max_w = get_group_setting(chat_id, "max_warnings") or 3
    if not warns:
        return await message.reply_text(tl(chat_id, "no_warnings", is_group=True, user=target.mention))
    reasons = "\n".join(f"{i+1}. {r or 'No reason'}" for i, r in enumerate(warns))
    await message.reply_text(tl(chat_id, "warn_list", is_group=True,
        user=target.mention, count=len(warns), max=max_w, reasons=reasons))

@bot.on_message(filters.command("clearwarns") & (filters.group))
async def clearwarns_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    target = await get_target_user(client, message)
    if not target:
        return await message.reply_text(tl(chat_id, "user_not_found", is_group=True))
    clear_warnings(target.id, chat_id)
    await message.reply_text(f"âœ… All warnings cleared for **{target.mention}**.")

@bot.on_message(filters.command("setwarnlimit") & (filters.group))
async def setwarnlimit_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return await message.reply_text("Usage: `/setwarnlimit <number>`")
    limit = int(parts[1].strip())
    if limit < 1 or limit > 10:
        return await message.reply_text("Limit must be between 1 and 10.")
    set_group_setting(chat_id, "max_warnings", limit)
    await message.reply_text(f"âœ… Warning limit set to `{limit}`.")

# =========================================================
# 16) Welcome & Goodbye
# =========================================================
@bot.on_message(filters.command("setwelcome") & (filters.group))
async def setwelcome_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/setwelcome <text>`\n\nVariables: `{mention}`, `{first}`, `{title}`, `{count}`")
    set_group_setting(chat_id, "welcome_text", parts[1].strip())
    await message.reply_text("âœ… Welcome message updated!")

@bot.on_message(filters.command("welcome") & (filters.group))
async def welcome_cmd(client, message):
    chat_id = message.chat.id
    text = get_group_setting(chat_id, "welcome_text") or tl(chat_id, "welcome_default", is_group=True, mention="@you", title=message.chat.title, count="?")
    await message.reply_text(f"**Current Welcome:**\n\n{text}")

@bot.on_message(filters.command("resetwelcome") & (filters.group))
async def resetwelcome_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    set_group_setting(chat_id, "welcome_text", "")
    await message.reply_text("âœ… Welcome message reset to default.")

@bot.on_message(filters.command("setgoodbye") & (filters.group))
async def setgoodbye_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/setgoodbye <text>`")
    set_group_setting(chat_id, "goodbye_text", parts[1].strip())
    await message.reply_text("âœ… Goodbye message updated!")

@bot.on_chat_member_updated()
async def member_update_handler(client, update: ChatMemberUpdated):
    chat_id = update.chat.id
    if not update.new_chat_member:
        return

    new_status = update.new_chat_member.status
    old_status = update.old_chat_member.status if update.old_chat_member else None
    user = update.new_chat_member.user

    # Member joined
    if new_status == ChatMemberStatus.MEMBER and old_status not in (
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER
    ):
        upsert_user(user.id, user.username, user.first_name)

        # Captcha check
        if get_group_setting(chat_id, "captcha_enabled"):
            try:
                await client.restrict_chat_member(chat_id, user.id, ChatPermissions(can_send_messages=False))
            except Exception:
                pass
            mention = f"[{user.first_name}](tg://user?id={user.id})"
            btn = InlineKeyboardMarkup([[
                InlineKeyboardButton("âœ… I'm Human", callback_data=f"captcha_{user.id}_{chat_id}")
            ]])
            sent = await client.send_message(
                chat_id,
                tl(chat_id, "captcha_challenge", is_group=True, mention=mention),
                reply_markup=btn
            )
            set_captcha_pending(user.id, chat_id, sent.id)
            asyncio.create_task(captcha_timeout(client, chat_id, user.id, sent.id))
            return

        # Welcome message
        welcome_text = get_group_setting(chat_id, "welcome_text")
        try:
            chat = await client.get_chat(chat_id)
            count = chat.members_count
        except Exception:
            count = "?"
        mention = f"[{user.first_name}](tg://user?id={user.id})"
        if not welcome_text:
            welcome_text = tl(chat_id, "welcome_default", is_group=True, mention=mention, title=update.chat.title, count=count)
        else:
            welcome_text = welcome_text.format(
                mention=mention, first=user.first_name or "",
                last=user.last_name or "", username=f"@{user.username}" if user.username else user.first_name,
                title=update.chat.title, count=count
            )
        try:
            await client.send_message(chat_id, welcome_text)
        except Exception:
            pass

    # Member left
    elif new_status == ChatMemberStatus.LEFT and old_status == ChatMemberStatus.MEMBER:
        goodbye_text = get_group_setting(chat_id, "goodbye_text")
        mention = f"[{user.first_name}](tg://user?id={user.id})"
        if not goodbye_text:
            goodbye_text = tl(chat_id, "goodbye_default", is_group=True, mention=mention)
        else:
            goodbye_text = goodbye_text.format(mention=mention, first=user.first_name or "")
        try:
            await client.send_message(chat_id, goodbye_text)
        except Exception:
            pass

async def captcha_timeout(client, chat_id, user_id, msg_id):
    await asyncio.sleep(60)
    pending = is_captcha_pending(user_id, chat_id)
    if pending is not None:
        remove_captcha_pending(user_id, chat_id)
        try:
            await client.ban_chat_member(chat_id, user_id)
            await asyncio.sleep(1)
            await client.unban_chat_member(chat_id, user_id)
            await client.delete_messages(chat_id, msg_id)
            await client.send_message(chat_id, tl(chat_id, "captcha_fail", is_group=True))
        except Exception:
            pass

# =========================================================
# 17) Notes
# =========================================================
@bot.on_message(filters.command("save") & (filters.group))
async def save_note_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply_text("Usage: `/save <name> <content>`")
    name, content = parts[1], parts[2]
    save_note(chat_id, name, content)
    await message.reply_text(tl(chat_id, "note_saved", is_group=True, name=name))

@bot.on_message(filters.command("note") & (filters.group))
async def get_note_cmd(client, message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/note <name>`")
    name = parts[1].strip()
    content = get_note(chat_id, name)
    if not content:
        return await message.reply_text(tl(chat_id, "note_not_found", is_group=True, name=name))
    await message.reply_text(f"ðŸ“ **#{name}**\n\n{content}")

@bot.on_message(filters.command("notes") & (filters.group))
async def list_notes_cmd(client, message):
    chat_id = message.chat.id
    notes = list_notes(chat_id)
    if not notes:
        return await message.reply_text(tl(chat_id, "no_notes", is_group=True))
    text = "ðŸ“ **Saved Notes:**\n\n" + "\n".join(f"â€¢ `#{n}`" for n in notes)
    await message.reply_text(text)

@bot.on_message(filters.command("delnote") & (filters.group))
async def del_note_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/delnote <name>`")
    name = parts[1].strip()
    delete_note(chat_id, name)
    await message.reply_text(tl(chat_id, "note_deleted", is_group=True, name=name))

# =========================================================
# 18) Filters
# =========================================================
@bot.on_message(filters.command("filter") & (filters.group))
async def set_filter_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply_text("Usage: `/filter <keyword> <response>`")
    keyword, response = parts[1], parts[2]
    save_filter(chat_id, keyword, response)
    await message.reply_text(tl(chat_id, "filter_saved", is_group=True, keyword=keyword))

@bot.on_message(filters.command("filters") & (filters.group))
async def list_filters_cmd(client, message):
    chat_id = message.chat.id
    f = get_filters(chat_id)
    if not f:
        return await message.reply_text(tl(chat_id, "no_filters", is_group=True))
    text = "ðŸ” **Active Filters:**\n\n" + "\n".join(f"â€¢ `{k}`" for k in f.keys())
    await message.reply_text(text)

@bot.on_message(filters.command("stop") & (filters.group))
async def stop_filter_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/stop <keyword>`")
    keyword = parts[1].strip()
    delete_filter(chat_id, keyword)
    await message.reply_text(tl(chat_id, "filter_deleted", is_group=True, keyword=keyword))

# =========================================================
# 19) Locks
# =========================================================
LOCK_TYPES = {
    "media": "media_lock",
    "stickers": "sticker_lock",
    "sticker": "sticker_lock",
    "links": "link_lock",
    "link": "link_lock",
    "forwards": "forward_lock",
    "forward": "forward_lock",
    "bots": "bot_lock",
    "bot": "bot_lock",
}

@bot.on_message(filters.command("lock") & (filters.group))
async def lock_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text(f"Usage: `/lock <type>`\nTypes: {', '.join(set(LOCK_TYPES.keys()))}")
    lock_type = parts[1].strip().lower()
    if lock_type == "all":
        for col in set(LOCK_TYPES.values()):
            set_group_setting(chat_id, col, 1)
        return await message.reply_text("ðŸ”’ All locks enabled!")
    col = LOCK_TYPES.get(lock_type)
    if not col:
        return await message.reply_text(f"Unknown type. Use: {', '.join(set(LOCK_TYPES.keys()))}")
    set_group_setting(chat_id, col, 1)
    await message.reply_text(tl(chat_id, "locked", is_group=True, perm=lock_type))

@bot.on_message(filters.command("unlock") & (filters.group))
async def unlock_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text(f"Usage: `/unlock <type>`")
    lock_type = parts[1].strip().lower()
    if lock_type == "all":
        for col in set(LOCK_TYPES.values()):
            set_group_setting(chat_id, col, 0)
        return await message.reply_text("ðŸ”“ All locks disabled!")
    col = LOCK_TYPES.get(lock_type)
    if not col:
        return await message.reply_text(f"Unknown type.")
    set_group_setting(chat_id, col, 0)
    await message.reply_text(tl(chat_id, "unlocked", is_group=True, perm=lock_type))

@bot.on_message(filters.command("locks") & (filters.group))
async def locks_status_cmd(client, message):
    chat_id = message.chat.id
    ensure_group(chat_id)
    status = {
        "Media": get_group_setting(chat_id, "media_lock"),
        "Stickers": get_group_setting(chat_id, "sticker_lock"),
        "Links": get_group_setting(chat_id, "link_lock"),
        "Forwards": get_group_setting(chat_id, "forward_lock"),
        "Bots": get_group_setting(chat_id, "bot_lock"),
    }
    lines = "\n".join(f"{'ðŸ”’' if v else 'ðŸ”“'} {k}" for k, v in status.items())
    await message.reply_text(f"**ðŸ”’ Lock Status:**\n\n{lines}")

# =========================================================
# 20) Anti-Flood config
# =========================================================
@bot.on_message(filters.command("antiflood") & (filters.group))
async def antiflood_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=3)
    if len(parts) == 2 and parts[1].lower() == "off":
        set_group_setting(chat_id, "antiflood_count", 0)
        return await message.reply_text(tl(chat_id, "antiflood_off", is_group=True))
    if len(parts) < 4:
        return await message.reply_text("Usage: `/antiflood <count> <time_secs> <action>`\nExample: `/antiflood 5 10 mute`\nActions: mute, kick, ban\n`/antiflood off` to disable")
    count, t_secs, action = parts[1], parts[2], parts[3].lower()
    if not count.isdigit() or not t_secs.isdigit():
        return await message.reply_text("Count and time must be numbers.")
    if action not in {"mute", "kick", "ban"}:
        return await message.reply_text("Action must be: mute, kick, or ban")
    set_group_setting(chat_id, "antiflood_count", int(count))
    set_group_setting(chat_id, "antiflood_time", int(t_secs))
    set_group_setting(chat_id, "antiflood_action", action)
    await message.reply_text(tl(chat_id, "antiflood_set", is_group=True, count=count, time=t_secs, action=action))

# =========================================================
# 21) Captcha config
# =========================================================
@bot.on_message(filters.command("captcha") & (filters.group))
async def captcha_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or parts[1].lower() not in {"on", "off"}:
        current = get_group_setting(chat_id, "captcha_enabled")
        return await message.reply_text(f"Usage: `/captcha on|off`\nCurrent: `{'on' if current else 'off'}`")
    enabled = parts[1].lower() == "on"
    set_group_setting(chat_id, "captcha_enabled", 1 if enabled else 0)
    key = "captcha_on" if enabled else "captcha_off"
    await message.reply_text(tl(chat_id, key, is_group=True))

# =========================================================
# 22) Rules
# =========================================================
@bot.on_message(filters.command("rules") & (filters.group))
async def rules_cmd(client, message):
    chat_id = message.chat.id
    parts = message.text.split(maxsplit=1)
    if len(parts) > 1:
        if not await is_group_admin(client, chat_id, message.from_user.id):
            return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
        set_group_setting(chat_id, "rules", parts[1].strip())
        return await message.reply_text(tl(chat_id, "rules_set", is_group=True))
    rules = get_group_setting(chat_id, "rules")
    if not rules:
        return await message.reply_text(tl(chat_id, "rules_not_set", is_group=True))
    await message.reply_text(f"ðŸ“‹ **Group Rules:**\n\n{rules}")

@bot.on_message(filters.command("setrules") & (filters.group))
async def setrules_cmd(client, message):
    chat_id = message.chat.id
    if not await is_group_admin(client, chat_id, message.from_user.id):
        return await message.reply_text(tl(chat_id, "no_permission", is_group=True))
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/setrules <rules text>`")
    set_group_setting(chat_id, "rules", parts[1].strip())
    await message.reply_text(tl(chat_id, "rules_set", is_group=True))

# =========================================================
# 23) Report
# =========================================================
@bot.on_message(filters.command("report") & (filters.group))
async def report_cmd(client, message):
    chat_id = message.chat.id
    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to report it.")
    reporter = message.from_user.mention
    reported = message.reply_to_message.from_user.mention if message.reply_to_message.from_user else "Unknown"
    msg_link = f"Message from {reported}"
    report_text = tl(chat_id, "report_msg", is_group=True,
        reporter=reporter, reported=reported, msg=msg_link)
    # Notify all admins
    try:
        async for member in client.get_chat_members(chat_id, filter="administrators"):
            if not member.user.is_bot:
                try:
                    await client.send_message(member.user.id, report_text)
                except Exception:
                    pass
    except Exception:
        pass
    await message.reply_text(tl(chat_id, "report_sent", is_group=True))

# =========================================================
# 24) Admin Bot Panel
# =========================================================
@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message):
    if not is_bot_admin(message.from_user.id):
        return await message.reply_text(tl(message.from_user.id, "no_permission"))
    await message.reply_text("ðŸ§© **Admin Panel**", reply_markup=build_admin_panel())

@bot.on_message(filters.command("stats") & filters.private)
async def stats_cmd(client, message):
    if not is_bot_admin(message.from_user.id):
        return
    uptime = format_duration(time.time() - state["started_at"])
    text = (
        f"ðŸ“Š **Bot Statistics**\n\n"
        f"ðŸ‘¥ Total Users: `{safe_total_users()}`\n"
        f"ðŸ“¦ Queue: `{task_queue.qsize()}`\n"
        f"âœ… Tasks Done: `{state['success_tasks']}`\n"
        f"âŒ Tasks Failed: `{state['failed_tasks']}`\n"
        f"â± Uptime: `{uptime}`\n"
        f"ðŸ›  Maintenance: `{state['maintenance_mode']}`"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client, message):
    if not is_bot_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/broadcast <message>`")
    msg = parts[1].strip()
    users = latest_users(limit=9999)
    sent, failed = 0, 0
    for (uid, _, _, _) in users:
        try:
            await client.send_message(uid, f"ðŸ“¢ **Broadcast**\n\n{msg}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await message.reply_text(f"ðŸ“¢ Broadcast done!\nâœ… Sent: {sent} | âŒ Failed: {failed}")

# =========================================================
# 25) Callback Query Handler
# =========================================================
@bot.on_callback_query()
async def callback_handler(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data

    # Help navigation
    if data == "help_main":
        await cq.message.edit_text(HELP_TEXTS["main"], reply_markup=build_help_menu())
        return await cq.answer()
    if data == "help_back":
        await cq.message.edit_text(HELP_TEXTS["main"], reply_markup=build_help_menu())
        return await cq.answer()
    for key in ["mod", "welcome", "warn", "notes", "filters", "locks", "captcha", "flood", "downloader", "settings"]:
        if data == f"help_{key}":
            back_btn = InlineKeyboardMarkup([[InlineKeyboardButton("ðŸ”™ Back", callback_data="help_back")]])
            await cq.message.edit_text(HELP_TEXTS.get(key, ""), reply_markup=back_btn)
            return await cq.answer()

    # Captcha verification
    if data.startswith("captcha_"):
        parts = data.split("_")
        target_user_id = int(parts[1])
        chat_id = int(parts[2])
        if user_id != target_user_id:
            return await cq.answer("This button is not for you!", show_alert=True)
        pending = is_captcha_pending(target_user_id, chat_id)
        if pending is None:
            return await cq.answer("Already verified or expired.", show_alert=True)
        remove_captcha_pending(target_user_id, chat_id)
        try:
            await client.restrict_chat_member(chat_id, target_user_id, ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_other_messages=True, can_add_web_page_previews=True
            ))
        except Exception:
            pass
        await cq.message.edit_text(tl(chat_id, "captcha_pass", is_group=True))
        return await cq.answer("âœ… Verified!", show_alert=False)

    # Group settings panel
    if data.startswith("gs_"):
        parts = data.split("_")
        if len(parts) >= 3:
            action = parts[1]
            chat_id_str = parts[-1]
            if not chat_id_str.lstrip("-").isdigit():
                return await cq.answer()
            chat_id = int(chat_id_str)
            if not await is_group_admin(client, chat_id, user_id):
                return await cq.answer("Admins only!", show_alert=True)
            if action == "captcha":
                cur_val = get_group_setting(chat_id, "captcha_enabled")
                set_group_setting(chat_id, "captcha_enabled", 0 if cur_val else 1)
            elif action == "flood":
                cur_val = get_group_setting(chat_id, "antiflood_count")
                set_group_setting(chat_id, "antiflood_count", 0 if cur_val else 5)
                if not cur_val:
                    set_group_setting(chat_id, "antiflood_time", 10)
                    set_group_setting(chat_id, "antiflood_action", "mute")
            elif action == "media":
                cur_val = get_group_setting(chat_id, "media_lock")
                set_group_setting(chat_id, "media_lock", 0 if cur_val else 1)
            elif action == "sticker":
                cur_val = get_group_setting(chat_id, "sticker_lock")
                set_group_setting(chat_id, "sticker_lock", 0 if cur_val else 1)
            elif action == "link":
                cur_val = get_group_setting(chat_id, "link_lock")
                set_group_setting(chat_id, "link_lock", 0 if cur_val else 1)
            elif action == "forward":
                cur_val = get_group_setting(chat_id, "forward_lock")
                set_group_setting(chat_id, "forward_lock", 0 if cur_val else 1)
            elif action == "lang":
                lang = parts[2]
                set_group_language(chat_id, lang)
            elif action == "close":
                return await cq.message.delete()
            try:
                await cq.message.edit_reply_markup(build_group_settings_panel(chat_id))
            except Exception:
                pass
            return await cq.answer("âœ… Updated!")

    # Language settings (private)
    if data.startswith("setlang_"):
        lang = data.split("_", 1)[1]
        set_user_language(user_id, lang)
        await cq.message.edit_reply_markup(build_settings_panel(user_id))
        return await cq.answer("Language updated!")

    # Admin panel callbacks
    if not is_bot_admin(user_id):
        return await cq.answer("Admin only!", show_alert=True)

    if data == "admin_stats":
        await cq.message.edit_text(
            f"ðŸ“Š Users: {safe_total_users()}\nâœ… Success: {state['success_tasks']}\n"
            f"âŒ Failed: {state['failed_tasks']}\nðŸ“¦ Queue: {task_queue.qsize()}",
            reply_markup=build_admin_panel()
        )
    elif data == "admin_queue":
        active = runtime["active_task_id"]
        lines = [f"â–¶ï¸ Running: `{active}`"] if active else []
        count = 0
        for tid, meta in sorted(task_registry.items(), key=lambda x: x[1]["created_at"]):
            if meta["status"] == "queued" and not meta["cancelled"]:
                count += 1
                lines.append(f"{count}. `{tid}` | user `{meta['user_id']}`")
                if count >= 10: break
        text = "ðŸ“¦ Queue:\n\n" + ("\n".join(lines) if lines else "Empty.")
        await cq.message.edit_text(text, reply_markup=build_admin_panel())
    elif data == "admin_maint":
        state["maintenance_mode"] = not state["maintenance_mode"]
        await cq.message.edit_text(f"ðŸ›  Maintenance: `{state['maintenance_mode']}`", reply_markup=build_admin_panel())
    elif data == "admin_clearqueue":
        for tid, meta in list(task_registry.items()):
            if meta["status"] == "queued" and not meta["cancelled"]:
                meta["cancelled"] = True
                meta["status"] = "cancelled"
                update_task_record(tid, "cancelled", "Cleared by admin")
        await cq.message.edit_text("ðŸ§¹ Queue cleared.", reply_markup=build_admin_panel())
    elif data == "admin_logs":
        logs = "\n".join(recent_logs[-20:]) or "No logs yet."
        await cq.message.edit_text(f"```\n{logs[:3500]}\n```", reply_markup=build_admin_panel())
    elif data == "admin_users":
        users = latest_users(5)
        lines = [f"â€¢ [{row[2] or 'User'}](tg://user?id={row[0]}) â€” `{row[0]}`" for row in users]
        await cq.message.edit_text("ðŸ‘¥ **Recent Users:**\n\n" + "\n".join(lines), reply_markup=build_admin_panel())
    await cq.answer()

# =========================================================
# 26) Group message handler (filters, locks, flood, #notes)
# =========================================================
@bot.on_message(filters.text & (filters.group) &
    ~filters.command(["ban","unban","kick","mute","unmute","warn","unwarn","warnlist","clearwarns",
                      "setwarnlimit","promote","demote","adminlist","pin","unpin","setwelcome",
                      "resetwelcome","welcome","setgoodbye","save","note","notes","delnote",
                      "filter","filters","stop","lock","unlock","locks","antiflood","captcha",
                      "rules","setrules","report","help","settings","lang","start"]))
async def group_text_handler(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        return

    # Skip admins for most checks
    is_admin_user = await is_group_admin(client, chat_id, user_id)

    # Anti-flood
    if not is_admin_user:
        if await check_flood(client, message):
            return

    # Lock: links
    if not is_admin_user and get_group_setting(chat_id, "link_lock"):
        if re.search(r"https?://|t\.me/|@\w+", message.text or ""):
            try:
                await message.delete()
            except Exception:
                pass
            return

    # Filters: check and respond
    active_filters = get_filters(chat_id)
    text_lower = (message.text or "").lower()
    for keyword, response in active_filters.items():
        if keyword in text_lower:
            await message.reply_text(response)
            return

    # #note shortcut
    if message.text and message.text.startswith("#"):
        note_name = message.text[1:].strip().lower().split()[0]
        if note_name:
            content = get_note(chat_id, note_name)
            if content:
                await message.reply_text(f"ðŸ“ **#{note_name}**\n\n{content}")

@bot.on_message(filters.media & (filters.group))
async def group_media_handler(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id:
        return
    if await is_group_admin(client, chat_id, user_id):
        return

    # Flood check for media
    if await check_flood(client, message):
        return

    # Lock: media
    if get_group_setting(chat_id, "media_lock"):
        if message.photo or message.video or message.document or message.audio:
            try:
                await message.delete()
            except Exception:
                pass
            return

    # Lock: stickers
    if get_group_setting(chat_id, "sticker_lock") and message.sticker:
        try:
            await message.delete()
        except Exception:
            pass
        return

    # Lock: forwards
    if get_group_setting(chat_id, "forward_lock") and message.forward_from:
        try:
            await message.delete()
        except Exception:
            pass
        return

# =========================================================
# 27) Core task processing (Downloader - Private only)
# =========================================================
async def process_safe_task(client, message, text_input, status_msg, task_id):
    update_task_record(task_id, "running")
    set_task_status(task_id, "running")
    runtime["active_task_id"] = task_id
    runtime["active_user_id"] = message.from_user.id

    try:
        if task_registry.get(task_id, {}).get("cancelled"):
            update_task_record(task_id, "cancelled")
            set_task_status(task_id, "cancelled")
            await status_msg.edit_text("ðŸ›‘ Task was cancelled before processing.")
            return

        await status_msg.edit_text("ðŸ” **Task started** â€” Validating link...")

        if not valid_tg_link(text_input):
            await status_msg.edit_text("âŒ **Invalid Link:** Please send a valid Telegram post link.")
            update_task_record(task_id, "failed", "Invalid link")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            return

        link = text_input.strip()
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            if chat_id.lstrip("-").isdigit():
                chat_id = int(chat_id)
            msg_id = int(parts[-1].split("?")[0])

        if not userbot.is_connected:
            await userbot.start()

        target_msg = await userbot.get_messages(chat_id, msg_id)

        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CFG.custom_caption}" if CFG.custom_caption else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()

        elif target_msg.media:
            media = (target_msg.document or target_msg.video or target_msg.audio or
                     target_msg.voice or target_msg.photo)
            file_size = getattr(media, "file_size", 0)

            if file_size and file_size > CFG.max_file_size:
                err = f"File size ({humanbytes(file_size)}) exceeds limit."
                await status_msg.edit_text(
                    f"â›” **File Too Large!**\n\n"
                    f"**Size:** `{humanbytes(file_size)}`\n"
                    f"**Limit:** `{humanbytes(CFG.max_file_size)}`"
                )
                update_task_record(task_id, "failed", err)
                set_task_status(task_id, "failed")
                state["failed_tasks"] += 1
                return

            start_time = time.time()
            file_path = await userbot.download_media(
                target_msg,
                progress=progress_bar,
                progress_args=("ðŸ“¥ DOWNLOADING...", status_msg, start_time)
            )

            final_caption = ""
            if target_msg.caption and CFG.custom_caption:
                final_caption = f"{target_msg.caption}\n\n{CFG.custom_caption}"
            elif target_msg.caption:
                final_caption = target_msg.caption
            elif CFG.custom_caption:
                final_caption = CFG.custom_caption

            await status_msg.edit_text("ðŸ”„ **Uploading...**")
            start_time = time.time()

            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption,
                        progress=progress_bar, progress_args=("ðŸ“¤ UPLOADING VIDEO...", status_msg, start_time))
                elif target_msg.audio:
                    await client.send_audio(message.chat.id, audio=file_path, caption=final_caption)
                elif target_msg.voice:
                    await client.send_voice(message.chat.id, voice=file_path, caption=final_caption)
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=final_caption,
                        progress=progress_bar, progress_args=("ðŸ“¤ UPLOADING...", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)

            await status_msg.delete()
            await message.reply_text(
                "âœ… **Task Completed!**\n_Your file has been delivered._",
                quote=True
            )
        else:
            await status_msg.edit_text("âš ï¸ **No extractable content found.**")
            update_task_record(task_id, "failed", "No content")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            return

        update_task_record(task_id, "done")
        set_task_status(task_id, "done")
        state["success_tasks"] += 1

    except FloodWait as e:
        await asyncio.sleep(e.value)
        update_task_record(task_id, "failed", f"FloodWait {e.value}")
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text(f"âš ï¸ Rate limited. Waited `{e.value}`s.")
        except Exception:
            pass

    except RPCError as e:
        logger.exception("RPC error in task")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("âŒ Telegram API error.")
        except Exception:
            pass

    except Exception as e:
        logger.exception("Task error")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("âŒ Unexpected error.")
        except Exception:
            pass

    finally:
        runtime["active_task_id"] = None
        runtime["active_user_id"] = None

async def process_worker():
    logger.info("Queue worker started")
    while True:
        client, message, text_input, status_msg, task_id = await task_queue.get()
        user_id = message.from_user.id
        try:
            if task_registry.get(task_id, {}).get("cancelled"):
                update_task_record(task_id, "cancelled")
                set_task_status(task_id, "cancelled")
                try:
                    await status_msg.edit_text("ðŸ›‘ Task cancelled.")
                except Exception:
                    pass
                continue
            await asyncio.wait_for(
                process_safe_task(client, message, text_input, status_msg, task_id),
                timeout=CFG.task_timeout_sec
            )
        except asyncio.TimeoutError:
            update_task_record(task_id, "failed", "timeout")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            try:
                await status_msg.edit_text("â° Task timed out.")
            except Exception:
                pass
        finally:
            user_pending_count[user_id] = max(user_pending_count.get(user_id, 1) - 1, 0)
            task_queue.task_done()

# =========================================================
# 28) Private text handler (Downloader)
# =========================================================
@bot.on_message(
    filters.text & filters.private &
    ~filters.command(["start","help","settings","lang","admin","stats","broadcast"])
)
async def handle_private_text(client, message):
    user_id = message.from_user.id
    text_input = message.text.strip()
    upsert_user(user_id, message.from_user.username, message.from_user.first_name)

    if is_globally_banned(user_id):
        return await message.reply_text(tl(user_id, "blocked"))
    if state["maintenance_mode"] and not is_bot_admin(user_id):
        return await message.reply_text(tl(user_id, "maintenance"))
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("ðŸ“¢ Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(tl(user_id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))

    if not valid_tg_link(text_input):
        return await message.reply_text(
            "ðŸ¤” **Send me a Telegram link!**\n\n"
            "Example: `https://t.me/c/12345/678`\n\n"
            "Use /help for all commands."
        )

    on_cd, remain = user_on_cooldown(user_id)
    if on_cd and not is_bot_admin(user_id):
        return await message.reply_text(tl(user_id, "cooldown", remain=remain))
    if task_queue.full() and not is_bot_admin(user_id):
        return await message.reply_text(tl(user_id, "busy"))

    pending = user_pending_count.get(user_id, 0)
    if pending >= CFG.max_pending_per_user and not is_bot_admin(user_id):
        return await message.reply_text(tl(user_id, "pending_limit", pending=pending))

    task_id = make_task_id(user_id)
    add_task_record(task_id, user_id, message.from_user.username or "", text_input)
    register_task(task_id, user_id, text_input)

    user_pending_count[user_id] = pending + 1
    user_last_request[user_id] = time.time()
    state["total_tasks"] += 1

    position = task_queue.qsize() + 1
    status_msg = await message.reply_text(
        f"ðŸ“ **Task Queued**\n\n"
        f"ðŸ†” **ID:** `{task_id}`\n"
        f"ðŸ“ **Position:** `{position}`\n"
        f"â³ Please wait..."
    )
    await task_queue.put((client, message, text_input, status_msg, task_id))

# =========================================================
# 29) Startup & Main
# =========================================================
async def startup_report():
    if not CFG.owner_id:
        return
    try:
        await bot.send_message(
            CFG.owner_id,
            f"âœ… **GroupHelp Bot Started!**\n\n"
            f"ðŸ• Time: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\n"
            f"ðŸ“¦ Max Queue: `{CFG.max_queue_size}`\n"
            f"ðŸ›  Maintenance: `{state['maintenance_mode']}`"
        )
    except Exception:
        pass

async def main_runner():
    cleanup_storage()
    init_db()

    await bot.start()
    await userbot.start()
    logger.info("Bot and Userbot started successfully!")

    asyncio.create_task(process_worker())
    asyncio.create_task(startup_report())

    await idle()

if __name__ == "__main__":
    # âœ… Flask MUST start first so Render sees the port immediately
    flask_thread = threading.Thread(target=run_web_server, daemon=True)
    flask_thread.start()
    logger.info(f"Web server started on port {CFG.port}")

    # Give Flask a moment to bind the port before bot starts
    time.sleep(2)

    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}")