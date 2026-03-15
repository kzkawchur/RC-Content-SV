import os
import re
import sys
import time
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
from pyrogram.errors import FloodWait, RPCError, SessionPasswordNeeded, PhoneCodeInvalid

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
    string_session: Optional[str] # Added for restricted content
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
    daily_task_limit: int
    support_text: str
    version: str

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
        api_hash=os.environ["API_HASH"]),
        bot_token=os.environ["BOT_TOKEN"],
        string_session=os.environ.get("STRING_SESSION"),
        port=int(os.environ.get("PORT", "10000")),
        download_dir=os.environ.get("DOWNLOAD_DIR", "downloads"),
        db_path=os.environ.get("DB_PATH", "bot_data.sqlite3"),
        force_sub_channel=os.environ.get("FORCE_SUB_CHANNEL", "").strip(),
        custom_caption=os.environ.get("CUSTOM_CAPTION", "").strip(),
        owner_id=owner_id,
        admins=admins,
        max_file_size=int(os.environ.get("MAX_FILE_SIZE", str(1024 * 1024 * 1024))),  # 1 GB
        max_queue_size=int(os.environ.get("MAX_QUEUE_SIZE", "50")),
        max_pending_per_user=int(os.environ.get("MAX_PENDING_PER_USER", "3")),
        user_cooldown_sec=int(os.environ.get("USER_COOLDOWN_SEC", "10")),
        task_timeout_sec=int(os.environ.get("TASK_TIMEOUT_SEC", "1800")),
        maintenance_mode=env_bool("MAINTENANCE_MODE", False),
        daily_task_limit=int(os.environ.get("DAILY_TASK_LIMIT", "30")),
        support_text=os.environ.get("SUPPORT_TEXT", "Contact @Admin for help."),
        version=os.environ.get("BOT_VERSION", "4.5.0-Extractor"),
    )

CFG = load_config()

# =========================================================
# 3) Flask health server
# =========================================================
app = Flask(__name__)
BOOT_TIME = time.time()

@app.route("/")
def home():
    return "✅ Extraction System Active", 200

@app.route("/healthz")
def healthz():
    return jsonify({
        "ok": True,
        "uptime_sec": round(time.time() - BOOT_TIME, 2),
        "active_task": runtime["active_task_id"],
    }), 200

def run_web_server():
    app.run(host="0.0.0.0", port=CFG.port)

# =========================================================
# 4) Bot Clients (Bot + Userbot)
# =========================================================
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

bot = Client(
    "premium_bot_v4",
    api_id=CFG.api_id,
    api_hash=CFG.api_hash,
    bot_token=CFG.bot_token
)

# Userbot is required to bypass restricted content
user_bot = None
if CFG.string_session:
    user_bot = Client(
        "premium_userbot",
        api_id=CFG.api_id,
        api_hash=CFG.api_hash,
        session_string=CFG.string_session
    )

# =========================================================
# 5) Runtime state & Helpers
# =========================================================
state = {
    "maintenance_mode": CFG.maintenance_mode,
    "queue_paused": False,
    "started_at": time.time(),
    "success_tasks": 0,
    "failed_tasks": 0,
    "startup_checks_ok": False,
}

runtime = {"active_task_id": None, "active_user_id": None}
task_registry: Dict[str, Dict] = {}
user_pending_count: Dict[int, int] = {}
user_last_request: Dict[int, float] = {}

# Regex for private and public links
TG_LINK_RE = re.compile(r"https?://t\.me/(c/)?([^/]+)/(\d+)")

def parse_tg_link(link: str) -> Tuple[Optional[str], Optional[int], bool]:
    match = TG_LINK_RE.search(link)
    if not match:
        return None, None, False
    
    is_private = bool(match.group(1))
    chat_id = match.group(2)
    msg_id = int(match.group(3))
    
    if is_private:
        # Private links look like /c/12345678/1 - actual chat_id needs -100 prefix
        chat_id = int(f"-100{chat_id}")
    elif chat_id.isdigit():
        chat_id = int(f"-100{chat_id}")
    
    return chat_id, msg_id, is_private

# [Language mapping and other helper functions remain identical to your previous version]
# (Skipped for brevity, but include all your TEXTS, db_connect, init_db, upsert_user, etc. here)

# =========================================================
# 6) Core Extraction Logic
# =========================================================

async def extract_restricted_content(client, user_id, chat_id, msg_id, status_msg):
    """Downloads media from restricted/private source and re-uploads."""
    if not user_bot:
        return False, "STRING_SESSION is missing. Cannot access restricted content."

    try:
        await status_msg.edit_text("🛰 **Accessing remote channel...**")
        msg = await user_bot.get_messages(chat_id, msg_id)
        
        if msg.empty:
            return False, "Message not found or Bot has no access."

        if not msg.media:
            # Handle text-only content
            await bot.send_message(user_id, msg.text or "Empty Message")
            return True, None

        # Determine file size
        file_size = 0
        media_obj = getattr(msg, msg.media.value)
        if hasattr(media_obj, "file_size"):
            file_size = media_obj.file_size

        if file_size > CFG.max_file_size:
            return False, f"File size ({humanbytes(file_size)}) exceeds limit."

        await status_msg.edit_text(f"📥 **Downloading media...** ({humanbytes(file_size)})")
        
        # Download locally
        start_time = time.time()
        file_path = await user_bot.download_media(
            msg,
            file_name=os.path.join(CFG.download_dir, f"{user_id}_{msg_id}/")
        )

        if not file_path or not os.path.exists(file_path):
            return False, "Download failed."

        await status_msg.edit_text("📤 **Uploading to you...**")
        
        # Upload via Bot
        caption = CFG.custom_caption or (msg.caption if msg.caption else "")
        await bot.send_document(
            user_id,
            document=file_path,
            caption=caption
        )

        # Cleanup
        if os.path.exists(os.path.dirname(file_path)):
            shutil.rmtree(os.path.dirname(file_path))
            
        return True, None

    except Exception as e:
        logger.exception("Extraction error")
        return False, str(e)

# =========================================================
# 7) Task Processor (Integrated)
# =========================================================

async def process_safe_task(client, message, text_input: str, status_msg, task_id: str):
    update_task_record(task_id, "running")
    set_task_status(task_id, "running")
    runtime["active_task_id"] = task_id
    runtime["active_user_id"] = message.from_user.id

    try:
        chat_target, msg_target, is_restricted = parse_tg_link(text_input)

        if chat_target and msg_target:
            await status_msg.edit_text("🔍 **Link detected.** Initializing extraction...")
            success, err = await extract_restricted_content(client, message.from_user.id, chat_target, msg_target, status_msg)
            
            if success:
                await status_msg.edit_text("✅ **Success!** Restricted content has been saved.")
                update_task_record(task_id, "done")
                state["success_tasks"] += 1
            else:
                await status_msg.edit_text(f"❌ **Failed:** {err}")
                update_task_record(task_id, "failed", err)
                state["failed_tasks"] += 1
        else:
            # Normal text processing
            await status_msg.edit_text("✅ **Task logged.** (No Telegram link found)")
            update_task_record(task_id, "done")
            state["success_tasks"] += 1

    except Exception as e:
        update_task_record(task_id, "failed", str(e))
        await status_msg.edit_text(f"❌ **Error:** {e}")
    finally:
        runtime["active_task_id"] = None

# =========================================================
# 8) Main Runner
# =========================================================

async def main_runner():
    # Cleanup and DB Init
    if os.path.exists(CFG.download_dir):
        shutil.rmtree(CFG.download_dir)
    os.makedirs(CFG.download_dir, exist_ok=True)
    init_db()

    # Web Server
    threading.Thread(target=run_web_server, daemon=True).start()

    # Start Bot
    await bot.start()
    logger.info("Main Bot started.")

    # Start Userbot if session exists
    if user_bot:
        try:
            await user_bot.start()
            logger.info("Extraction Userbot started.")
        except Exception as e:
            logger.error(f"Userbot failed: {e}. Restricted links won't work.")

    # Start Worker
    asyncio.create_task(process_worker())
    
    logger.info("System is live.")
    await idle()

if __name__ == "__main__":
    try:
        loop.run_until_complete(main_runner())
    except KeyboardInterrupt:
        pass
