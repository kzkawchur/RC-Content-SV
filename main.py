import os
import re
import sys
import time
import sqlite3
import shutil
import logging
import asyncio
import threading
import json
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple, List
from pathlib import Path

from flask import Flask, jsonify, request
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pyrogram.errors import FloodWait, RPCError, ChannelPrivate, ChatAdminRequired
from pyrogram.enums import ChatType

# =========================================================
# 1) Logging
# =========================================================
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("premium_bot")

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


@dataclass
class Config:
    api_id: int
    api_hash: str
    bot_token: str

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
    extract_allowed_users: Set[int]


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
    
    extract_users = parse_admins(os.environ.get("EXTRACT_ALLOWED_USERS", ""))

    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],

        port=int(os.environ.get("PORT", "10000")),
        download_dir=os.environ.get("DOWNLOAD_DIR", "downloads"),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),

        force_sub_channel=os.environ.get("FORCE_SUB_CHANNEL", "").strip(),
        custom_caption=os.environ.get("CUSTOM_CAPTION", "").strip(),

        owner_id=owner_id,
        admins=admins,

        max_file_size=int(os.environ.get("MAX_FILE_SIZE", str(800 * 1024 * 1024))),
        max_queue_size=int(os.environ.get("MAX_QUEUE_SIZE", "25")),
        max_pending_per_user=int(os.environ.get("MAX_PENDING_PER_USER", "2")),
        user_cooldown_sec=int(os.environ.get("USER_COOLDOWN_SEC", "15")),
        task_timeout_sec=int(os.environ.get("TASK_TIMEOUT_SEC", "900")),

        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        extract_allowed_users=extract_users,
    )


CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()


@app.route("/")
def home():
    return "✅ Bot system is running", 200


@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "queue_size": task_queue.qsize() if "task_queue" in globals() else 0,
        "maintenance": state["maintenance_mode"],
        "active_task": runtime["active_task_id"],
        "active_user": runtime["active_user_id"],
    }), 200


@app.route("/summary")
def summary():
    return jsonify({
        "users": safe_total_users(),
        "success_tasks": state["success_tasks"],
        "failed_tasks": state["failed_tasks"],
        "queue_size": task_queue.qsize(),
        "maintenance": state["maintenance_mode"],
        "file_limit_bytes": CFG.max_file_size,
        "extracted_items": state["extracted_items"],
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
    "safe_system_bot",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token
)

# =========================================================
# 6) Runtime state
# =========================================================
state = {
    "maintenance_mode": CFG.maintenance_mode,
    "started_at": time.time(),
    "total_tasks": 0,
    "success_tasks": 0,
    "failed_tasks": 0,
    "extracted_items": 0,
}

runtime = {
    "active_task_id": None,
    "active_user_id": None,
}

task_queue: asyncio.Queue = asyncio.Queue(maxsize=CFG.max_queue_size)
user_pending_count: Dict[int, int] = {}
user_last_request: Dict[int, float] = {}
task_registry: Dict[str, Dict] = {}

TG_LINK_RE = re.compile(
    r"^(https?://)?t\.me/(c/\d+/\d+|[A-Za-z0-9_]{4,}/\d+)(\?.*)?$",
    re.IGNORECASE
)

CHANNEL_GROUP_LINK_RE = re.compile(
    r"^(https?://)?t\.me/(c/\d+|[A-Za-z0-9_]{4,})/?(\?.*)?$",
    re.IGNORECASE
)

# =========================================================
# 7) Language text
# =========================================================
TEXTS = {
    "en": {
        "welcome": "⚡ **Welcome, {name}!**\n\nThis professional bot includes:\n• Queue system\n• Cancel tools\n• Admin panel\n• Content extraction (restricted/private)\n• Advanced stats\n• Settings\n• Bangla/English language\n• SQLite persistence\n\nUse /help to see commands.",
        "maintenance": "🛠️ Bot is under maintenance. Please try again later.",
        "blocked": "🚫 You are blocked from using this bot.",
        "join_required": "🛑 Please join the required channel first.",
        "cooldown": "⏳ Cooldown active. Try again in `{remain}` seconds.",
        "busy": "🚦 Server busy right now. Please try again later.",
        "pending_limit": "📌 You already have `{pending}` pending task(s). Please wait for them to finish.",
        "queue_empty": "📭 Queue is empty.",
        "task_empty": "📭 You have no recent tasks.",
        "admin_only": "🔒 Admin only.",
        "extract_not_allowed": "🚫 You don't have permission to extract content.",
        "task_not_found": "Task not found.",
        "cannot_view_task": "You cannot view this task.",
        "settings_saved": "✅ Settings updated successfully.",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "task_cancelled": "🛑 Task cancelled successfully.",
        "task_cancel_fail": "⚠️ {msg}",
        "extract_invalid_link": "❌ Invalid channel/group link format.",
        "extract_no_access": "❌ Cannot access this chat (private/restricted).",
        "extract_started": "🚀 Extraction started for: {chat_name}",
        "extract_progress": "📊 Extracted {count} items from {total}",
        "extract_completed": "✅ Extraction completed!\n\n📊 **Summary:**\n• Messages: {messages}\n• Photos: {photos}\n• Videos: {videos}\n• Documents: {documents}\n• Audio: {audio}\n• Others: {others}",
        "extract_failed": "❌ Extraction failed: {error}",
    },
    "bn": {
        "welcome": "⚡ **স্বাগতম, {name}!**\n\nএই professional bot-এ আছে:\n• Queue system\n• Cancel tools\n• Admin panel\n• Content extraction (restricted/private)\n• Advanced stats\n• Settings\n• বাংলা/ইংরেজি language\n• SQLite persistence\n\nকমান্ড দেখতে /help দাও।",
        "maintenance": "🛠️ বট maintenance-এ আছে। পরে আবার চেষ্টা করো।",
        "blocked": "🚫 তুমি এই বট ব্যবহার করতে পারবে না।",
        "join_required": "🛑 আগে required channel-এ join করো।",
        "cooldown": "⏳ Cooldown চলছে। `{remain}` সেকেন্ড পরে আবার চেষ্টা করো।",
        "busy": "🚦 সার্ভার এখন ব্যস্ত। একটু পরে আবার চেষ্টা করো।",
        "pending_limit": "📌 তোমার আগে থেকেই `{pending}`টা pending task আছে।",
        "queue_empty": "📭 Queue খালি আছে।",
        "task_empty": "📭 তোমার কোনো recent task নেই।",
        "admin_only": "🔒 এটা শুধু admin-এর জন্য।",
        "extract_not_allowed": "🚫 তোমার এই কন্টেন্ট extract করার অনুমতি নেই।",
        "task_not_found": "Task পাওয়া যায়নি।",
        "cannot_view_task": "তুমি এই task দেখতে পারবে না।",
        "settings_saved": "✅ Settings সফলভাবে save হয়েছে।",
        "lang_set_en": "✅ Language set to English.",
        "lang_set_bn": "✅ ভাষা বাংলা করা হয়েছে।",
        "task_cancelled": "🛑 Task সফলভাবে cancel করা হয়েছে।",
        "task_cancel_fail": "⚠️ {msg}",
        "extract_invalid_link": "❌ Invalid channel/group link।",
        "extract_no_access": "❌ এই চ্যাটে অ্যাক্সেস নেই (private/restricted)।",
        "extract_started": "🚀 Extraction শুরু হয়েছে: {chat_name}",
        "extract_progress": "📊 {total}-এর মধ্যে {count}টি item extract করা হয়েছে",
        "extract_completed": "✅ Extraction সম্পূর্ণ!\n\n📊 **Summary:**\n• Messages: {messages}\n• Photos: {photos}\n• Videos: {videos}\n• Documents: {documents}\n• Audio: {audio}\n• Others: {others}",
        "extract_failed": "❌ Extraction ব্যর্থ: {error}",
    }
}

# =========================================================
# 8) Storage setup
# =========================================================
def cleanup_storage() -> None:
    if os.path.exists(CFG.download_dir):
        shutil.rmtree(CFG.download_dir, ignore_errors=True)
        logger.info("Cleared previous download cache")
    os.makedirs(CFG.download_dir, exist_ok=True)

# =========================================================
# 9) SQLite
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
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                username TEXT,
                created_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                input_text TEXT,
                error_text TEXT,
                task_type TEXT
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
            CREATE TABLE IF NOT EXISTS extraction_records (
                id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_name TEXT,
                total_items INTEGER DEFAULT 0,
                extracted_messages INTEGER DEFAULT 0,
                extracted_photos INTEGER DEFAULT 0,
                extracted_videos INTEGER DEFAULT 0,
                extracted_documents INTEGER DEFAULT 0,
                extracted_audio INTEGER DEFAULT 0,
                extracted_others INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
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
            FROM users
            ORDER BY last_seen DESC
            LIMIT ?
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


def add_task_record(task_id: str, user_id: int, username: str, input_text: str, task_type: str = "process"):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tasks(id, user_id, username, created_at, status, input_text, error_text, task_type)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, user_id, username or "", int(time.time()), "queued", input_text, "", task_type))
        conn.commit()


def update_task_record(task_id: str, status: str, error_text: str = ""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE tasks
            SET status = ?, error_text = ?
            WHERE id = ?
        """, (status, error_text, task_id))
        conn.commit()


def add_extraction_record(task_id: str, user_id: int, chat_id: int, chat_name: str):
    import uuid
    extract_id = str(uuid.uuid4())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO extraction_records(id, task_id, user_id, chat_id, chat_name, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
        """, (extract_id, task_id, user_id, chat_id, chat_name, int(time.time())))
        conn.commit()
    return extract_id


def update_extraction_record(extract_id: str, **kwargs):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        fields = []
        values = []
        for k, v in kwargs.items():
            fields.append(f"{k} = ?")
            values.append(v)
        values.append(extract_id)
        query = f"UPDATE extraction_records SET {', '.join(fields)} WHERE id = ?"
        cur.execute(query, values)
        conn.commit()


def get_user_tasks(user_id: int, limit: int = 5) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, created_at, status, input_text
            FROM tasks
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (user_id, limit))
        return cur.fetchall()


def get_task_row(task_id: str) -> Optional[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, username, created_at, status, input_text, error_text, task_type
            FROM tasks
            WHERE id = ?
        """, (task_id,))
        return cur.fetchone()


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

# =========================================================
# 10) Helpers
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id in CFG.admins


def can_extract(user_id: int) -> bool:
    return is_admin(user_id) or user_id in CFG.extract_allowed_users


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_language(user_id)
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


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


def valid_telegram_post_link(text: str) -> bool:
    return bool(TG_LINK_RE.match(text.strip()))


def valid_channel_group_link(text: str) -> bool:
    return bool(CHANNEL_GROUP_LINK_RE.match(text.strip()))


def parse_chat_identifier(link: str) -> Optional[str]:
    """Extract chat identifier from Telegram link"""
    match = CHANNEL_GROUP_LINK_RE.match(link.strip())
    if match:
        return match.group(2)
    return None


async def check_fsub(client, message) -> bool:
    if not CFG.force_sub_channel:
        return True
    try:
        await client.get_chat_member(CFG.force_sub_channel, message.from_user.id)
        return True
    except Exception:
        return False


def make_task_id(user_id: int) -> str:
    return f"{user_id}_{int(time.time() * 1000)}"


def user_on_cooldown(user_id: int) -> Tuple[bool, int]:
    last = user_last_request.get(user_id, 0)
    remain = CFG.user_cooldown_sec - int(time.time() - last)
    return (remain > 0, max(remain, 0))


def register_task(task_id: str, user_id: int, input_text: str):
    task_registry[task_id] = {
        "user_id": user_id,
        "input_text": input_text,
        "status": "queued",
        "created_at": int(time.time()),
        "cancelled": False,
    }


def set_task_status(task_id: str, status: str):
    if task_id in task_registry:
        task_registry[task_id]["status"] = status


def cancel_task(task_id: str, requester_id: int) -> Tuple[bool, str]:
    task = task_registry.get(task_id)
    if not task:
        return False, "Task not found."
    if task["user_id"] != requester_id and not is_admin(requester_id):
        return False, "You cannot cancel this task."
    if task["status"] in {"done", "failed", "cancelled"}:
        return False, f"Task already `{task['status']}`."
    if task["status"] == "running":
        return False, "Task is already running and cannot be cancelled safely."
    task["cancelled"] = True
    task["status"] = "cancelled"
    update_task_record(task_id, "cancelled", "Cancelled by user/admin")
    return True, "Task cancelled successfully."


def queued_task_lines(limit: int = 15) -> List[str]:
    lines = []
    active = runtime["active_task_id"]
    if active:
        lines.append(f"▶️ Running: `{active}` (user `{runtime['active_user_id']}`)")
    count = 0
    for task_id, meta in sorted(task_registry.items(), key=lambda x: x[1]["created_at"]):
        if meta["status"] == "queued" and not meta["cancelled"]:
            count += 1
            lines.append(f"{count}. `{task_id}` | user `{meta['user_id']}`")
            if count >= limit:
                break
    return lines


def build_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Stats", callback_data="admin_stats"),
            InlineKeyboardButton("📦 Queue", callback_data="admin_queue"),
        ],
        [
            InlineKeyboardButton("👥 Users", callback_data="admin_users"),
            InlineKeyboardButton("🛠 Maintenance", callback_data="admin_maint"),
        ],
        [
            InlineKeyboardButton("🧹 Clear Queue", callback_data="admin_clearqueue"),
            InlineKeyboardButton("📜 Logs", callback_data="admin_logs"),
        ]
    ])


def build_settings_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"{'✅ ' if lang == 'en' else ''}English",
                callback_data="setlang_en"
            ),
            InlineKeyboardButton(
                f"{'✅ ' if lang == 'bn' else ''}বাংলা",
                callback_data="setlang_bn"
            ),
        ]
    ])

# =========================================================
# 11) Content Extraction Logic
# =========================================================
async def extract_channel_content(
    client: Client,
    chat_identifier: str,
    status_msg: Message,
    task_id: str,
    user_id: int,
    extract_id: str,
) -> Dict:
    """Extract content from channel/group (including restricted/private)"""
    
    stats = {
        "messages": 0,
        "photos": 0,
        "videos": 0,
        "documents": 0,
        "audio": 0,
        "others": 0,
        "total": 0,
    }
    
    try:
        # Get chat info
        try:
            chat = await client.get_chat(chat_identifier)
        except Exception as e:
            logger.error(f"Cannot access chat {chat_identifier}: {e}")
            return {"error": f"Cannot access chat: {str(e)}"}
        
        chat_name = chat.title or chat.username or str(chat.id)
        logger.info(f"Extracting from {chat_name} (ID: {chat.id})")
        
        # Get total message count
        try:
            async with closing(db_connect()) as conn:
                cur = conn.cursor()
                # This is approximate for channels
                pass
        except:
            pass
        
        # Create extraction directory
        extract_dir = os.path.join(CFG.download_dir, extract_id)
        os.makedirs(extract_dir, exist_ok=True)
        
        # Extract messages and media
        message_count = 0
        extraction_data = {
            "chat_id": chat.id,
            "chat_name": chat_name,
            "chat_type": str(chat.type),
            "messages": [],
        }
        
        try:
            async for message in client.get_chat_history(chat.id, limit=None):
                if task_registry.get(task_id, {}).get("cancelled"):
                    break
                
                message_count += 1
                stats["total"] += 1
                
                msg_data = {
                    "message_id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "text": message.text or "",
                    "caption": message.caption or "",
                }
                
                # Handle media
                if message.photo:
                    stats["photos"] += 1
                    file_path = await client.download_media(
                        message.photo,
                        file_name=os.path.join(extract_dir, f"photo_{message.id}.jpg")
                    )
                    msg_data["media_type"] = "photo"
                    msg_data["media_file"] = os.path.basename(file_path) if file_path else None
                    
                elif message.video:
                    stats["videos"] += 1
                    file_path = await client.download_media(
                        message.video,
                        file_name=os.path.join(extract_dir, f"video_{message.id}.mp4")
                    )
                    msg_data["media_type"] = "video"
                    msg_data["media_file"] = os.path.basename(file_path) if file_path else None
                    msg_data["video_duration"] = message.video.duration
                    
                elif message.document:
                    stats["documents"] += 1
                    file_path = await client.download_media(
                        message.document,
                        file_name=os.path.join(extract_dir, f"doc_{message.id}_{message.document.file_name}")
                    )
                    msg_data["media_type"] = "document"
                    msg_data["media_file"] = os.path.basename(file_path) if file_path else None
                    msg_data["file_name"] = message.document.file_name
                    
                elif message.audio:
                    stats["audio"] += 1
                    file_path = await client.download_media(
                        message.audio,
                        file_name=os.path.join(extract_dir, f"audio_{message.id}.mp3")
                    )
                    msg_data["media_type"] = "audio"
                    msg_data["media_file"] = os.path.basename(file_path) if file_path else None
                    msg_data["audio_duration"] = message.audio.duration
                    
                elif message.voice:
                    stats["audio"] += 1
                    file_path = await client.download_media(
                        message.voice,
                        file_name=os.path.join(extract_dir, f"voice_{message.id}.ogg")
                    )
                    msg_data["media_type"] = "voice"
                    msg_data["media_file"] = os.path.basename(file_path) if file_path else None
                    
                elif message.text or message.caption:
                    stats["messages"] += 1
                else:
                    stats["others"] += 1
                
                # Add user info if available
                if message.from_user:
                    msg_data["from_user_id"] = message.from_user.id
                    msg_data["from_user_name"] = message.from_user.first_name or ""
                
                extraction_data["messages"].append(msg_data)
                
                # Update progress every 10 messages
                if message_count % 10 == 0:
                    try:
                        await status_msg.edit_text(
                            t(user_id, "extract_progress", count=message_count, total=message_count)
                        )
                    except:
                        pass
                    await asyncio.sleep(0.5)
                        
        except Exception as e:
            logger.error(f"Error during extraction: {e}")
            stats["error"] = str(e)
        
        # Save extraction metadata
        metadata_file = os.path.join(extract_dir, "metadata.json")
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(extraction_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Extraction complete: {stats}")
        return stats
        
    except Exception as e:
        logger.exception(f"Extraction failed: {e}")
        return {"error": str(e)}


async def process_extraction_task(
    client,
    message,
    chat_link: str,
    status_msg,
    task_id: str
):
    """Process content extraction task"""
    update_task_record(task_id, "running")
    set_task_status(task_id, "running")
    runtime["active_task_id"] = task_id
    runtime["active_user_id"] = message.from_user.id
    user_id = message.from_user.id

    try:
        if task_registry.get(task_id, {}).get("cancelled"):
            update_task_record(task_id, "cancelled", "Cancelled before processing")
            set_task_status(task_id, "cancelled")
            await status_msg.edit_text("🛑 Task was cancelled before processing.")
            return

        # Parse chat identifier
        chat_id = parse_chat_identifier(chat_link)
        if not chat_id:
            await status_msg.edit_text(t(user_id, "extract_invalid_link"))
            update_task_record(task_id, "failed", "Invalid link format")
            state["failed_tasks"] += 1
            return

        await status_msg.edit_text(t(user_id, "extract_started", chat_name=chat_id))
        
        # Create extraction record
        extract_id = add_extraction_record(task_id, user_id, 0, chat_id)
        
        # Extract content
        stats = await extract_channel_content(
            client, chat_id, status_msg, task_id, user_id, extract_id
        )

        if "error" in stats:
            error_msg = stats.get("error", "Unknown error")
            await status_msg.edit_text(t(user_id, "extract_failed", error=error_msg))
            update_task_record(task_id, "failed", error_msg)
            state["failed_tasks"] += 1
            return

        # Update extraction record
        update_extraction_record(
            extract_id,
            total_items=stats.get("total", 0),
            extracted_messages=stats.get("messages", 0),
            extracted_photos=stats.get("photos", 0),
            extracted_videos=stats.get("videos", 0),
            extracted_documents=stats.get("documents", 0),
            extracted_audio=stats.get("audio", 0),
            extracted_others=stats.get("others", 0),
            completed_at=int(time.time()),
        )

        # Send completion message
        await status_msg.edit_text(
            t(user_id, "extract_completed",
              messages=stats.get("messages", 0),
              photos=stats.get("photos", 0),
              videos=stats.get("videos", 0),
              documents=stats.get("documents", 0),
              audio=stats.get("audio", 0),
              others=stats.get("others", 0))
        )

        update_task_record(task_id, "done")
        set_task_status(task_id, "done")
        state["success_tasks"] += 1
        state["extracted_items"] += stats.get("total", 0)

    except FloodWait as e:
        await asyncio.sleep(e.value)
        update_task_record(task_id, "failed", f"FloodWait {e.value}")
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text(f"⚠️ Telegram rate limit. Waited `{e.value}` seconds.")
        except Exception:
            pass

    except RPCError as e:
        logger.exception("Telegram RPC error")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("❌ Telegram API error occurred.")
        except Exception:
            pass

    except Exception as e:
        logger.exception("Task failed")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("❌ Unexpected task error.")
        except Exception:
            pass

    finally:
        runtime["active_task_id"] = None
        runtime["active_user_id"] = None


async def process_safe_task(client, message, text_input: str, status_msg, task_id: str):
    update_task_record(task_id, "running")
    set_task_status(task_id, "running")
    runtime["active_task_id"] = task_id
    runtime["active_user_id"] = message.from_user.id

    try:
        if task_registry.get(task_id, {}).get("cancelled"):
            update_task_record(task_id, "cancelled", "Cancelled before processing")
            set_task_status(task_id, "cancelled")
            await status_msg.edit_text("🛑 Task was cancelled before processing.")
            return

        await status_msg.edit_text("🔍 **Task started**\nValidating input...")
        await asyncio.sleep(1.0)

        if valid_telegram_post_link(text_input):
            await status_msg.edit_text(
                "✅ **System check passed**\n\n"
                "Link format looks valid.\n"
                "Professional system layer is active."
            )
        else:
            await status_msg.edit_text(
                "✅ **Task completed**\n\n"
                "Input received and logged.\n"
                "Professional safe system layer is active."
            )

        update_task_record(task_id, "done")
        set_task_status(task_id, "done")
        state["success_tasks"] += 1

    except FloodWait as e:
        await asyncio.sleep(e.value)
        update_task_record(task_id, "failed", f"FloodWait {e.value}")
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text(f"⏰ Telegram rate limit. Waited `{e.value}` seconds.")
        except Exception:
            pass

    except RPCError as e:
        logger.exception("Telegram RPC error")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("❌ Telegram API error occurred.")
        except Exception:
            pass

    except Exception as e:
        logger.exception("Task failed")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try:
            await status_msg.edit_text("❌ Unexpected task error.")
        except Exception:
            pass

    finally:
        runtime["active_task_id"] = None
        runtime["active_user_id"] = None


async def process_worker():
    logger.info("Queue worker started")
    while True:
        client, message, task_type, text_input, status_msg, task_id = await task_queue.get()
        user_id = message.from_user.id

        try:
            if task_registry.get(task_id, {}).get("cancelled"):
                update_task_record(task_id, "cancelled", "Cancelled in queue")
                set_task_status(task_id, "cancelled")
                try:
                    await status_msg.edit_text("🛑 Task cancelled.")
                except Exception:
                    pass
                continue

            if task_type == "extract":
                await asyncio.wait_for(
                    process_extraction_task(client, message, text_input, status_msg, task_id),
                    timeout=CFG.task_timeout_sec
                )
            else:
                await asyncio.wait_for(
                    process_safe_task(client, message, text_input, status_msg, task_id),
                    timeout=CFG.task_timeout_sec
                )
        except asyncio.TimeoutError:
            logger.warning("Task timeout: %s", task_id)
            update_task_record(task_id, "failed", "timeout")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            try:
                await status_msg.edit_text("⏰ Task timed out.")
            except Exception:
                pass
        finally:
            user_pending_count[user_id] = max(user_pending_count.get(user_id, 1) - 1, 0)
            task_queue.task_done()

# =========================================================
# 12) Commands
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)

    if is_banned(user.id):
        return await message.reply_text(t(user.id, "blocked"))

    if state["maintenance_mode"] and not is_admin(user.id):
        return await message.reply_text(t(user.id, "maintenance"))

    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(t(user.id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))

    await message.reply_text(t(user.id, "welcome", name=user.first_name or "User"))


@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    user_id = message.from_user.id
    can_ext = can_extract(user_id)
    
    text = (
        "**Commands**\n\n"
        "/start - Start bot\n"
        "/help - Help panel\n"
        "/about - Bot info\n"
        "/ping - Check responsiveness\n"
        "/settings - User settings\n"
        "/lang bn অথবা /lang en\n"
        "/mytasks - Your recent tasks\n"
        "/task TASK_ID - Task details\n"
        "/cancel TASK_ID - Cancel your queued task\n"
    )
    
    if can_ext:
        text += (
            "\n**Content Extraction**\n"
            "/extract <channel/group_link> - Extract from channel/group\n"
            "Example: /extract https://t.me/mychannel\n"
            "or: /extract @mychannel\n"
        )
    
    if is_admin(user_id):
        text += (
            "\n**Admin**\n"
            "/stats\n"
            "/users\n"
            "/queue\n"
            "/admin\n"
            "/broadcast your message\n"
            "/clearqueue\n"
            "/removequeue TASK_ID\n"
            "/logs\n"
            "/maintenance on|off\n"
            "/ban USER_ID reason\n"
            "/unban USER_ID\n"
        )
    
    await message.reply_text(text)


@bot.on_message(filters.command("about") & filters.private)
async def about_cmd(client, message):
    uptime = format_duration(time.time() - state["started_at"])
    await message.reply_text(
        f"🤖 **Identity:** Professional Telegram Bot with Content Extraction\n"
        f"⚙️ **Core:** Pyrogram + Flask + SQLite\n"
        f"📦 **Queue Limit:** `{CFG.max_queue_size}`\n"
        f"📁 **Practical File Limit:** `{humanbytes(CFG.max_file_size)}`\n"
        f"🧊 **Cooldown:** `{CFG.user_cooldown_sec}s`\n"
        f"📊 **Extracted Items:** `{state['extracted_items']}`\n"
        f"⏱️ **Uptime:** `{uptime}`"
    )


@bot.on_message(filters.command("ping") & filters.private)
async def ping_cmd(client, message):
    start = time.time()
    msg = await message.reply_text("🏓 Pong...")
    ms = int((time.time() - start) * 1000)
    await msg.edit_text(f"🏓 Pong! `{ms} ms`")


@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client, message):
    lang = get_user_language(message.from_user.id)
    text = (
        "**Settings**\n\n"
        f"🌐 Language: `{lang}`\n"
        f"📁 Practical Limit: `{humanbytes(CFG.max_file_size)}`\n"
    )
    await message.reply_text(text, reply_markup=build_settings_panel(message.from_user.id))


@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/lang en` or `/lang bn`", quote=True)

    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}:
        return await message.reply_text("Use only `en` or `bn`.")

    set_user_language(message.from_user.id, lang)
    if lang == "bn":
        return await message.reply_text(t(message.from_user.id, "lang_set_bn"))
    return await message.reply_text(t(message.from_user.id, "lang_set_en"))


@bot.on_message(filters.command("stats") & filters.private)
async def stats_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    uptime = format_duration(time.time() - state["started_at"])
    total_done = state["success_tasks"] + state["failed_tasks"]
    success_rate = (state["success_tasks"] / total_done * 100) if total_done else 0

    text = (
        "**Advanced Bot Stats**\n\n"
        f"👥 Users: `{safe_total_users()}`\n"
        f"📝 Processed: `{total_done}`\n"
        f"✅ Success: `{state['success_tasks']}`\n"
        f"❌ Failed: `{state['failed_tasks']}`\n"
        f"📈 Success Rate: `{success_rate:.2f}%`\n"
        f"📦 Queue Size: `{task_queue.qsize()}`\n"
        f"▶️ Active Task: `{runtime['active_task_id']}`\n"
        f"👤 Active User: `{runtime['active_user_id']}`\n"
        f"📊 Extracted Items: `{state['extracted_items']}`\n"
        f"🛠️ Maintenance: `{state['maintenance_mode']}`\n"
        f"📁 Limit: `{humanbytes(CFG.max_file_size)}`\n"
        f"⏱️ Uptime: `{uptime}`"
    )
    await message.reply_text(text)


@bot.on_message(filters.command("users") & filters.private)
async def users_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    rows = latest_users(15)
    if not rows:
        return await message.reply_text("No users found.")

    lines = ["**Latest Users**\n"]
    for user_id, username, first_name, last_seen in rows:
        name = first_name or "Unknown"
        uname = f"@{username}" if username else "no_username"
        lines.append(f"• `{user_id}` | {name} | {uname} | last `{format_duration(time.time() - last_seen)}` ago")

    await message.reply_text("\n".join(lines))


@bot.on_message(filters.command("queue") & filters.private)
async def queue_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    lines = queued_task_lines(limit=15)
    if not lines:
        return await message.reply_text(t(message.from_user.id, "queue_empty"))

    await message.reply_text("**Queue Overview**\n\n" + "\n".join(lines))


@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    await message.reply_text("🧩 **Admin Panel**", reply_markup=build_admin_panel())


@bot.on_callback_query()
async def callback_handler(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data

    if data.startswith("setlang_"):
        lang = data.split("_", 1)[1]
        if lang in {"en", "bn"}:
            set_user_language(user_id, lang)
            txt = t(user_id, "lang_set_bn") if lang == "bn" else t(user_id, "lang_set_en")
            await callback_query.message.edit_text(
                f"**Settings**\n\n🌐 Language: `{lang}`\n📁 Practical Limit: `{humanbytes(CFG.max_file_size)}`",
                reply_markup=build_settings_panel(user_id)
            )
            await callback_query.answer(txt)
            return

    if not is_admin(user_id):
        return await callback_query.answer("Admin only", show_alert=True)

    if data == "admin_stats":
        uptime = format_duration(time.time() - state["started_at"])
        total_done = state["success_tasks"] + state["failed_tasks"]
        success_rate = (state["success_tasks"] / total_done * 100) if total_done else 0
        text = (
            f"📊 Users: {safe_total_users()}\n"
            f"✅ Success: {state['success_tasks']}\n"
            f"❌ Failed: {state['failed_tasks']}\n"
            f"📈 Success Rate: {success_rate:.2f}%\n"
            f"📦 Queue: {task_queue.qsize()}\n"
            f"📊 Extracted: {state['extracted_items']}\n"
            f"⏱ Uptime: {uptime}"
        )
        await callback_query.message.edit_text(text, reply_markup=build_admin_panel())

    elif data == "admin_queue":
        lines = queued_task_lines(limit=10)
        text = "📦 Queue Overview\n\n" + ("\n".join(lines) if lines else "Queue is empty.")
        await callback_query.message.edit_text(text, reply_markup=build_admin_panel())

    elif data == "admin_users":
        rows = latest_users(10)
        if rows:
            text = "👥 Latest Users\n\n" + "\n".join(
                f"• {first_name or 'Unknown'} | {('@' + username) if username else 'no_username'} | {uid}"
                for uid, username, first_name, _ in rows
            )
        else:
            text = "No users found."
        await callback_query.message.edit_text(text, reply_markup=build_admin_panel())

    elif data == "admin_maint":
        state["maintenance_mode"] = not state["maintenance_mode"]
        text = f"🛠 Maintenance mode: `{state['maintenance_mode']}`"
        await callback_query.message.edit_text(text, reply_markup=build_admin_panel())

    elif data == "admin_clearqueue":
        removed = 0
        for task_id, meta in list(task_registry.items()):
            if meta["status"] == "queued" and not meta["cancelled"]:
                meta["cancelled"] = True
                meta["status"] = "cancelled"
                update_task_record(task_id, "cancelled", "Cleared from admin panel")
                removed += 1
        await callback_query.message.edit_text(
            f"🧹 Cleared `{removed}` queued task(s).",
            reply_markup=build_admin_panel()
        )

    elif data == "admin_logs":
        text = "**Recent Logs**\n\n" + "\n".join(f"`{x[-90:]}`" for x in recent_logs[-10:])
        await callback_query.message.edit_text(text[:4000], reply_markup=build_admin_panel())

    await callback_query.answer()


@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/broadcast your message here`", quote=True)

    broadcast_text = parts[1]
    sent = 0
    failed = 0
    status = await message.reply_text("📢 Broadcast started...")

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users")
        rows = cur.fetchall()

    for i, (uid,) in enumerate(rows, start=1):
        try:
            await bot.send_message(uid, broadcast_text)
            sent += 1
        except Exception:
            failed += 1

        if i % 20 == 0:
            try:
                await status.edit_text(
                    f"📢 Broadcast running...\n\nProcessed: `{i}`\n✅ Sent: `{sent}`\n❌ Failed: `{failed}`"
                )
            except Exception:
                pass
        await asyncio.sleep(0.08)

    await status.edit_text(f"📢 **Broadcast completed**\n\n✅ Sent: `{sent}`\n❌ Failed: `{failed}`")


@bot.on_message(filters.command("extract") & filters.private)
async def extract_cmd(client, message):
    user_id = message.from_user.id
    user = message.from_user
    
    upsert_user(user_id, user.username, user.first_name)

    if is_banned(user_id):
        return await message.reply_text(t(user_id, "blocked"))

    if not can_extract(user_id):
        return await message.reply_text(t(user_id, "extract_not_allowed"))

    if state["maintenance_mode"] and not is_admin(user_id):
        return await message.reply_text(t(user_id, "maintenance"))

    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(t(user_id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/extract <channel_or_group_link>`\nExample: `/extract @mychannel`", quote=True)

    chat_link = parts[1].strip()

    if not valid_channel_group_link(chat_link):
        return await message.reply_text(t(user_id, "extract_invalid_link"))

    on_cd, remain = user_on_cooldown(user_id)
    if on_cd and not is_admin(user_id):
        return await message.reply_text(t(user_id, "cooldown", remain=remain))

    if task_queue.full() and not is_admin(user_id):
        return await message.reply_text(t(user_id, "busy"))

    pending = user_pending_count.get(user_id, 0)
    if pending >= CFG.max_pending_per_user and not is_admin(user_id):
        return await message.reply_text(t(user_id, "pending_limit", pending=pending))

    task_id = make_task_id(user_id)
    add_task_record(task_id, user_id, user.username or "", chat_link, "extract")
    register_task(task_id, user_id, chat_link)

    user_pending_count[user_id] = pending + 1
    user_last_request[user_id] = time.time()
    state["total_tasks"] += 1

    position = task_queue.qsize() + 1
    status_msg = await message.reply_text(
        "📝 **Extraction queued**\n\n"
        f"🆔 **Task ID:** `{task_id}`\n"
        f"📍 **Position:** `{position}`\n"
        f"🔗 **Target:** `{chat_link}`\n"
        "⏳ Processing will begin automatically."
    )

    await task_queue.put((client, message, "extract", chat_link, status_msg, task_id))


@bot.on_message(filters.command("clearqueue") & filters.private)
async def clearqueue_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    removed = 0
    for task_id, meta in list(task_registry.items()):
        if meta["status"] == "queued" and not meta["cancelled"]:
            meta["cancelled"] = True
            meta["status"] = "cancelled"
            update_task_record(task_id, "cancelled", "Cleared by admin")
            removed += 1

    await message.reply_text(f"🧹 Cleared `{removed}` queued task(s).")


@bot.on_message(filters.command("removequeue") & filters.private)
async def removequeue_cmd(client, message):
    if not is_admin(message.from_user.id):
        return await message.reply_text(t(message.from_user.id, "admin_only"))

    parts = message.text.split(maxsplit=1)
    if