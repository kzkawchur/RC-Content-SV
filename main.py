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
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, RPCError

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
        except Exception: pass

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

def parse_admins(raw: str) -> Set[int]:
    ids = set()
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            ids.add(int(part))
    return ids

def load_config() -> Config:
    required = ["API_ID", "API_HASH", "BOT_TOKEN", "STRING_SESSION"]
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
        string_session=os.environ["STRING_SESSION"],

        port=int(os.environ.get("PORT", "10000")),
        download_dir=os.environ.get("DOWNLOAD_DIR", "downloads"),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),

        force_sub_channel=os.environ.get("FORCE_SUB_CHANNEL", "").strip(),
        custom_caption=os.environ.get("CUSTOM_CAPTION", "").strip(),

        owner_id=owner_id,
        admins=admins,

        max_file_size=int(os.environ.get("MAX_FILE_SIZE", str(1 * 1024 * 1024 * 1024))), # 1 GB
        max_queue_size=int(os.environ.get("MAX_QUEUE_SIZE", "25")),
        max_pending_per_user=int(os.environ.get("MAX_PENDING_PER_USER", "2")),
        user_cooldown_sec=int(os.environ.get("USER_COOLDOWN_SEC", "15")),
        task_timeout_sec=int(os.environ.get("TASK_TIMEOUT_SEC", "900")),

        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
    )

CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

@app.route("/")
def home(): return "✅ Bot system is running", 200

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
# 5) Bot client & Userbot
# =========================================================
bot = Client("safe_system_bot", api_id=CFG.api_id, api_hash=CFG.api_hash, bot_token=CFG.bot_token)
userbot = Client("userbot_helper", api_id=CFG.api_id, api_hash=CFG.api_hash, session_string=CFG.string_session)

# =========================================================
# 6) Runtime state
# =========================================================
state = {"maintenance_mode": CFG.maintenance_mode, "started_at": time.time(), "total_tasks": 0, "success_tasks": 0, "failed_tasks": 0}
runtime = {"active_task_id": None, "active_user_id": None}
task_queue: asyncio.Queue = asyncio.Queue(maxsize=CFG.max_queue_size)
user_pending_count: Dict[int, int] = {}
user_last_request: Dict[int, float] = {}
task_registry: Dict[str, Dict] = {}

TG_LINK_RE = re.compile(r"^(https?://)?t\.me/(c/\d+/\d+|[A-Za-z0-9_]{4,}/\d+)(\?.*)?$", re.IGNORECASE)

# =========================================================
# 7) Language text
# =========================================================
TEXTS = {
    "en": {
        "welcome": "⚡ **Welcome, {name}!**\n\nSend me any restricted Telegram link and I will extract it for you.",
        "maintenance": "🛠️ Bot is under maintenance. Please try again later.",
        "blocked": "🚫 You are blocked from using this bot.",
        "join_required": "🛑 Please join the required channel first.",
        "cooldown": "⏳ Cooldown active. Try again in `{remain}` seconds.",
        "busy": "🚦 Server busy right now. Please try again later.",
        "pending_limit": "📌 You already have `{pending}` pending task(s). Please wait for them to finish.",
        "task_cancelled": "🛑 Task cancelled successfully.",
    },
    "bn": {
        "welcome": "⚡ **স্বাগতম, {name}!**\n\nযেকোনো রেস্ট্রিক্টেড টেলিগ্রাম লিংক আমাকে দিন, আমি সেটি আপনাকে ডাউনলোড করে দেব।",
        "maintenance": "🛠️ বট maintenance-এ আছে। পরে আবার চেষ্টা করো।",
        "blocked": "🚫 তুমি এই বট ব্যবহার করতে পারবে না।",
        "join_required": "🛑 আগে চ্যানেলটিতে জয়েন করুন।",
        "cooldown": "⏳ Cooldown চলছে। `{remain}` সেকেন্ড পরে আবার চেষ্টা করো।",
        "busy": "🚦 সার্ভার এখন ব্যস্ত। একটু পরে আবার চেষ্টা করো।",
        "pending_limit": "📌 তোমার আগে থেকেই `{pending}`টা pending task আছে। এগুলো শেষ হলে আবার দাও।",
        "task_cancelled": "🛑 Task সফলভাবে cancel করা হয়েছে।",
    }
}

# =========================================================
# 8) Storage setup & DB
# =========================================================
def cleanup_storage() -> None:
    if os.path.exists(CFG.download_dir):
        shutil.rmtree(CFG.download_dir, ignore_errors=True)
    os.makedirs(CFG.download_dir, exist_ok=True)

def db_connect(): return sqlite3.connect(CFG.db_path)

def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, username TEXT, first_name TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, username TEXT, created_at INTEGER NOT NULL, status TEXT NOT NULL, input_text TEXT, error_text TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS bans (user_id INTEGER PRIMARY KEY, reason TEXT, banned_at INTEGER NOT NULL)")
        cur.execute("CREATE TABLE IF NOT EXISTS user_settings (user_id INTEGER PRIMARY KEY, language TEXT NOT NULL DEFAULT 'en')")
        conn.commit()

def upsert_user(user_id: int, username: Optional[str], first_name: Optional[str]):
    now = int(time.time())
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO users(user_id, first_seen, last_seen, username, first_name) VALUES(?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen, username=excluded.username, first_name=excluded.first_name", (user_id, now, now, username or "", first_name or ""))
        conn.commit()

def safe_total_users() -> int:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]

def latest_users(limit: int = 10) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, username, first_name, last_seen FROM users ORDER BY last_seen DESC LIMIT ?", (limit,))
        return cur.fetchall()

def is_banned(user_id: int) -> bool:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM bans WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None

def ban_user(user_id: int, reason: str = ""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO bans(user_id, reason, banned_at) VALUES(?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, banned_at=excluded.banned_at", (user_id, reason, int(time.time())))
        conn.commit()

def unban_user(user_id: int):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        conn.commit()

def add_task_record(task_id: str, user_id: int, username: str, input_text: str):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO tasks(id, user_id, username, created_at, status, input_text, error_text) VALUES(?, ?, ?, ?, ?, ?, ?)", (task_id, user_id, username or "", int(time.time()), "queued", input_text, ""))
        conn.commit()

def update_task_record(task_id: str, status: str, error_text: str = ""):
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE tasks SET status = ?, error_text = ? WHERE id = ?", (status, error_text, task_id))
        conn.commit()

def get_user_tasks(user_id: int, limit: int = 5) -> List[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, created_at, status, input_text FROM tasks WHERE user_id = ? ORDER BY created_at DESC LIMIT ?", (user_id, limit))
        return cur.fetchall()

def get_task_row(task_id: str) -> Optional[tuple]:
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, user_id, username, created_at, status, input_text, error_text FROM tasks WHERE id = ?", (task_id,))
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
        cur.execute("INSERT INTO user_settings(user_id, language) VALUES(?, ?) ON CONFLICT(user_id) DO UPDATE SET language=excluded.language", (user_id, language))
        conn.commit()

# =========================================================
# 10) Helpers & Progress Bar
# =========================================================
def is_admin(user_id: int) -> bool: return user_id in CFG.admins

def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_user_language(user_id)
    text = TEXTS.get(lang, TEXTS["en"]).get(key, key)
    return text.format(**kwargs)

def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def humanbytes(size: int) -> str:
    if not size: return "0 B"
    power = 1024
    unit = 0
    units = ["B", "KB", "MB", "GB", "TB"]
    while size >= power and unit < len(units) - 1:
        size /= power
        unit += 1
    return f"{size:.2f} {units[unit]}"

def valid_telegram_post_link(text: str) -> bool: return bool(TG_LINK_RE.match(text.strip()))

async def check_fsub(client, message) -> bool:
    if not CFG.force_sub_channel: return True
    try:
        await client.get_chat_member(CFG.force_sub_channel, message.from_user.id)
        return True
    except Exception: return False

def make_task_id(user_id: int) -> str: return f"{user_id}_{int(time.time() * 1000)}"

def user_on_cooldown(user_id: int) -> Tuple[bool, int]:
    last = user_last_request.get(user_id, 0)
    remain = CFG.user_cooldown_sec - int(time.time() - last)
    return (remain > 0, max(remain, 0))

def register_task(task_id: str, user_id: int, input_text: str):
    task_registry[task_id] = {"user_id": user_id, "input_text": input_text, "status": "queued", "created_at": int(time.time()), "cancelled": False}

def set_task_status(task_id: str, status: str):
    if task_id in task_registry: task_registry[task_id]["status"] = status

def cancel_task(task_id: str, requester_id: int) -> Tuple[bool, str]:
    task = task_registry.get(task_id)
    if not task: return False, "Task not found."
    if task["user_id"] != requester_id and not is_admin(requester_id): return False, "You cannot cancel this task."
    if task["status"] in {"done", "failed", "cancelled"}: return False, f"Task already `{task['status']}`."
    if task["status"] == "running": return False, "Task is already running and cannot be cancelled safely."
    task["cancelled"] = True
    task["status"] = "cancelled"
    update_task_record(task_id, "cancelled", "Cancelled by user/admin")
    return True, "Task cancelled successfully."

def queued_task_lines(limit: int = 15) -> List[str]:
    lines = []
    active = runtime["active_task_id"]
    if active: lines.append(f"▶️ Running: `{active}`")
    count = 0
    for task_id, meta in sorted(task_registry.items(), key=lambda x: x[1]["created_at"]):
        if meta["status"] == "queued" and not meta["cancelled"]:
            count += 1
            lines.append(f"{count}. `{task_id}` | user `{meta['user_id']}`")
            if count >= limit: break
    return lines

def build_admin_panel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="admin_stats"), InlineKeyboardButton("📦 Queue", callback_data="admin_queue")],
        [InlineKeyboardButton("👥 Users", callback_data="admin_users"), InlineKeyboardButton("🛠 Maintenance", callback_data="admin_maint")],
        [InlineKeyboardButton("🧹 Clear Queue", callback_data="admin_clearqueue"), InlineKeyboardButton("📜 Logs", callback_data="admin_logs")]
    ])

def build_settings_panel(user_id: int) -> InlineKeyboardMarkup:
    lang = get_user_language(user_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if lang == 'en' else ''}English", callback_data="setlang_en"), InlineKeyboardButton(f"{'✅ ' if lang == 'bn' else ''}বাংলা", callback_data="setlang_bn")]
    ])

async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total if total else 0
        completed = math.floor(percentage / 5)
        remaining = 20 - completed
        bar = "[{0}{1}{2}]".format("█" * completed, "", "▒" * remaining)
        speed = current / diff if diff > 0 else 0
        
        tmp = (
            f"**{ud_type}**\n\n"
            f"📊 **Progress:** `{round(percentage, 2)}%`\n"
            f"🚀 `{bar}`\n\n"
            f"📁 **Size:** `{humanbytes(current)} / {humanbytes(total)}`\n"
            f"⚡ **Speed:** `{humanbytes(speed)}/s`"
        )
        try: await message.edit_text(text=tmp)
        except Exception: pass

# =========================================================
# 11) Safe task processing (CORE EXTRACTION LOGIC)
# =========================================================
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

        await status_msg.edit_text("🔍 **Task started**\nValidating and extracting content...")

        if not valid_telegram_post_link(text_input):
            await status_msg.edit_text("❌ **Invalid Link:** Please provide a valid Telegram post link.")
            update_task_record(task_id, "failed", "Invalid link format")
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
            if chat_id.isdigit(): chat_id = int(chat_id)
            msg_id = int(parts[-1].split("?")[0])

        if not userbot.is_connected:
            await userbot.start()
        
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CFG.custom_caption}" if CFG.custom_caption else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
            
        elif target_msg.media:
            media = target_msg.document or target_msg.video or target_msg.audio or target_msg.voice or target_msg.photo
            file_size = getattr(media, 'file_size', 0)
            
            if file_size > CFG.max_file_size:
                error_txt = f"File size ({humanbytes(file_size)}) exceeds limit."
                await status_msg.edit_text(f"⛔ **System Alert!**\n\n**File Size:** `{humanbytes(file_size)}`\n⚠️ Limit exceeded! Maximum allowed size is `{humanbytes(CFG.max_file_size)}`.")
                update_task_record(task_id, "failed", error_txt)
                set_task_status(task_id, "failed")
                state["failed_tasks"] += 1
                return

            start_time = time.time()
            file_path = await userbot.download_media(
                target_msg, 
                progress=progress_bar, 
                progress_args=("📥 DOWNLOADING CONTENT...", status_msg, start_time)
            )
            
            final_caption = f"{target_msg.caption or ''}\n\n{CFG.custom_caption}" if CFG.custom_caption else (target_msg.caption or "")
            start_time = time.time()
            
            await status_msg.edit_text("🔄 **Preparing to upload...**")
            
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 UPLOADING VIDEO...", status_msg, start_time))
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 UPLOADING FILE...", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path): 
                    os.remove(file_path)
            
            await status_msg.delete()
            await message.reply_text("✅ **Task Completed Successfully!**\n_File has been delivered._", quote=True)
        else:
            await status_msg.edit_text("⚠️ **Notice:** No extractable content found in this link.")
            update_task_record(task_id, "failed", "No extractable content")
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
        try: await status_msg.edit_text(f"⚠️ Telegram rate limit. Waited `{e.value}` seconds.")
        except Exception: pass

    except RPCError as e:
        logger.exception("Telegram RPC error")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try: await status_msg.edit_text("❌ Telegram API error occurred.")
        except Exception: pass

    except Exception as e:
        logger.exception("Task failed")
        update_task_record(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try: await status_msg.edit_text("❌ Unexpected task error.")
        except Exception: pass

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
                update_task_record(task_id, "cancelled", "Cancelled in queue")
                set_task_status(task_id, "cancelled")
                try: await status_msg.edit_text("🛑 Task cancelled.")
                except Exception: pass
                continue

            await asyncio.wait_for(process_safe_task(client, message, text_input, status_msg, task_id), timeout=CFG.task_timeout_sec)
        except asyncio.TimeoutError:
            update_task_record(task_id, "failed", "timeout")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            try: await status_msg.edit_text("⏰ Task timed out.")
            except Exception: pass
        finally:
            user_pending_count[user_id] = max(user_pending_count.get(user_id, 1) - 1, 0)
            task_queue.task_done()

# =========================================================
# 12) Commands (Skipped extra ones for brevity, all are same as your code)
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def start_cmd(client, message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)
    if is_banned(user.id): return await message.reply_text(t(user.id, "blocked"))
    if state["maintenance_mode"] and not is_admin(user.id): return await message.reply_text(t(user.id, "maintenance"))
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(t(user.id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))
    await message.reply_text(t(user.id, "welcome", name=user.first_name or "User"))

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    text = "**Commands**\n/start - Start\n/settings - Settings\n/lang bn|en\n/mytasks - Tasks\n/task ID\n/cancel ID\n\nAdmin:\n/admin\n/stats\n/users\n/clearqueue\n/broadcast MSG"
    await message.reply_text(text)

@bot.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client, message):
    lang = get_user_language(message.from_user.id)
    await message.reply_text(f"**Settings**\n\n🌐 Language: `{lang}`", reply_markup=build_settings_panel(message.from_user.id))

@bot.on_message(filters.command("lang") & filters.private)
async def lang_cmd(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/lang en` or `/lang bn`", quote=True)
    lang = parts[1].strip().lower()
    if lang not in {"en", "bn"}: return await message.reply_text("Use only `en` or `bn`.")
    set_user_language(message.from_user.id, lang)
    await message.reply_text(t(message.from_user.id, "lang_set_bn") if lang == "bn" else t(message.from_user.id, "lang_set_en"))

@bot.on_message(filters.command("admin") & filters.private)
async def admin_cmd(client, message):
    if not is_admin(message.from_user.id): return await message.reply_text(t(message.from_user.id, "admin_only"))
    await message.reply_text("🧩 **Admin Panel**", reply_markup=build_admin_panel())

@bot.on_callback_query()
async def callback_handler(client, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if data.startswith("setlang_"):
        lang = data.split("_", 1)[1]
        set_user_language(user_id, lang)
        await callback_query.message.edit_text(f"**Settings**\n🌐 Language: `{lang}`", reply_markup=build_settings_panel(user_id))
        await callback_query.answer("Language updated.")
        return
    if not is_admin(user_id): return await callback_query.answer("Admin only", show_alert=True)
    if data == "admin_stats":
        await callback_query.message.edit_text(f"📊 Users: {safe_total_users()}\n✅ Success: {state['success_tasks']}\n❌ Failed: {state['failed_tasks']}\n📦 Queue: {task_queue.qsize()}", reply_markup=build_admin_panel())
    elif data == "admin_queue":
        lines = queued_task_lines(limit=10)
        await callback_query.message.edit_text("📦 Queue Overview\n\n" + ("\n".join(lines) if lines else "Empty."), reply_markup=build_admin_panel())
    elif data == "admin_maint":
        state["maintenance_mode"] = not state["maintenance_mode"]
        await callback_query.message.edit_text(f"🛠 Maintenance: `{state['maintenance_mode']}`", reply_markup=build_admin_panel())
    elif data == "admin_clearqueue":
        for task_id, meta in list(task_registry.items()):
            if meta["status"] == "queued" and not meta["cancelled"]:
                meta["cancelled"] = True; meta["status"] = "cancelled"
                update_task_record(task_id, "cancelled", "Cleared")
        await callback_query.message.edit_text("🧹 Cleared queue.", reply_markup=build_admin_panel())
    await callback_query.answer()

# =========================================================
# 13) Generic input handler
# =========================================================
@bot.on_message(filters.text & filters.private & ~filters.command(["start", "help", "settings", "lang", "admin"]))
async def handle_text(client, message):
    user_id = message.from_user.id
    text_input = message.text.strip()
    upsert_user(user_id, message.from_user.username, message.from_user.first_name)

    if is_banned(user_id): return await message.reply_text(t(user_id, "blocked"))
    if state["maintenance_mode"] and not is_admin(user_id): return await message.reply_text(t(user.id, "maintenance"))
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")]]
        return await message.reply_text(t(user_id, "join_required"), reply_markup=InlineKeyboardMarkup(btn))

    on_cd, remain = user_on_cooldown(user_id)
    if on_cd and not is_admin(user_id): return await message.reply_text(t(user_id, "cooldown", remain=remain))
    if task_queue.full() and not is_admin(user_id): return await message.reply_text(t(user_id, "busy"))

    pending = user_pending_count.get(user_id, 0)
    if pending >= CFG.max_pending_per_user and not is_admin(user_id): return await message.reply_text(t(user_id, "pending_limit", pending=pending))

    task_id = make_task_id(user_id)
    add_task_record(task_id, user_id, message.from_user.username or "", text_input)
    register_task(task_id, user_id, text_input)

    user_pending_count[user_id] = pending + 1
    user_last_request[user_id] = time.time()
    state["total_tasks"] += 1

    position = task_queue.qsize() + 1
    status_msg = await message.reply_text(f"📝 **Task queued**\n\n🆔 **ID:** `{task_id}`\n📍 **Position:** `{position}`\n⏳ Please wait.")
    await task_queue.put((client, message, text_input, status_msg, task_id))

# =========================================================
# 14 & 15) Main & Startup Report
# =========================================================
async def startup_report():
    if not CFG.owner_id: return
    try: await bot.send_message(CFG.owner_id, f"✅ **Bot Started**\n\nQueue Limit: {CFG.max_queue_size}")
    except Exception: pass

async def main_runner():
    cleanup_storage()
    init_db()
    threading.Thread(target=run_web_server, daemon=True).start()

    await bot.start()
    await userbot.start() # <-- Userbot Started Here!
    logger.info("Bot and Userbot started successfully")

    asyncio.create_task(process_worker())
    asyncio.create_task(startup_report())
    await idle()

if __name__ == "__main__":
    try: loop.run_until_complete(main_runner())
    except KeyboardInterrupt: pass
