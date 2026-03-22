# -*- coding: utf-8 -*-
"""
GroupHelp Bot - Advanced Telegram Group Manager + Content Extractor
"""
import sys, os
sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)
os.environ["PYTHONIOENCODING"] = "utf-8"
os.environ["LANG"] = "en_US.UTF-8"

import re, time, math, sqlite3, shutil, logging, asyncio, hashlib, threading, json
from contextlib import closing
from dataclasses import dataclass
from typing import Optional, Dict, Set, Tuple, List

from flask import Flask, jsonify
from pyrogram import Client, filters, idle
from pyrogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ChatPermissions, Message, ChatMemberUpdated
)
from pyrogram.errors import FloodWait, RPCError, ChatAdminRequired
from pyrogram.enums import ChatMemberStatus, ChatType

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("grouphelp")
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
# CONFIG
# =========================================================
def env_bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in {"1","true","yes","on"}

def parse_ids(raw):
    ids = set()
    for p in raw.split(","):
        p = p.strip()
        if p.lstrip("-").isdigit():
            ids.add(int(p))
    return ids

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

def load_config():
    for k in ["API_ID","API_HASH","BOT_TOKEN","STRING_SESSION"]:
        if not os.environ.get(k):
            raise ValueError(f"Missing: {k}")
    owner_id = int(os.environ.get("OWNER_ID","0") or 0)
    admins = parse_ids(os.environ.get("ADMIN_IDS",""))
    if owner_id: admins.add(owner_id)
    return Config(
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
        bot_token=os.environ["BOT_TOKEN"],
        string_session=os.environ["STRING_SESSION"],
        port=int(os.environ.get("PORT","10000")),
        download_dir=os.environ.get("DOWNLOAD_DIR","downloads"),
        db_path=os.environ.get("DB_PATH","bot_data.sqlite3"),
        force_sub_channel=os.environ.get("FORCE_SUB_CHANNEL","").strip(),
        custom_caption=os.environ.get("CUSTOM_CAPTION","").strip(),
        owner_id=owner_id,
        admins=admins,
        max_file_size=int(os.environ.get("MAX_FILE_SIZE", str(2*1024*1024*1024))),
        max_queue_size=int(os.environ.get("MAX_QUEUE_SIZE","25")),
        max_pending_per_user=int(os.environ.get("MAX_PENDING_PER_USER","2")),
        user_cooldown_sec=int(os.environ.get("USER_COOLDOWN_SEC","15")),
        task_timeout_sec=int(os.environ.get("TASK_TIMEOUT_SEC","900")),
        maintenance_mode=env_bool("MAINTENANCE_MODE",False),
        log_channel=int(os.environ.get("LOG_CHANNEL","0") or 0),
        support_chat=os.environ.get("SUPPORT_CHAT","").strip(),
    )

CFG = load_config()

# =========================================================
# FLASK
# =========================================================
flask_app = Flask(__name__)
BOOT_TIME = time.time()

@flask_app.route("/")
def home(): return "Bot is running", 200

@flask_app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "uptime": round(time.time()-BOOT_TIME,2)}), 200

def run_flask():
    flask_app.run(host="0.0.0.0", port=CFG.port, use_reloader=False)

# =========================================================
# EVENT LOOP & CLIENTS
# =========================================================
import sys as _sys
if _sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

bot = Client("bot_session", api_id=CFG.api_id, api_hash=CFG.api_hash, bot_token=CFG.bot_token)
userbot = Client("user_session", api_id=CFG.api_id, api_hash=CFG.api_hash, session_string=CFG.string_session)

# =========================================================
# RUNTIME STATE
# =========================================================
state = {
    "maintenance": CFG.maintenance_mode,
    "started_at": time.time(),
    "total_tasks": 0, "success_tasks": 0, "failed_tasks": 0,
}
runtime = {"active_task_id": None, "active_user_id": None}
task_queue: asyncio.Queue = asyncio.Queue(maxsize=CFG.max_queue_size)
user_pending: Dict[int, int] = {}
user_last_req: Dict[int, float] = {}
task_reg: Dict[str, Dict] = {}
flood_tracker: Dict[str, list] = {}
raid_tracker: Dict[int, list] = {}
dup_tracker: Dict[str, int] = {}
bot_username = ""

TG_LINK = re.compile(
    r"^(https?://)?t\.me/(c/\d+/\d+|[A-Za-z0-9_]{4,}/\d+)(\?.*)?$", re.I
)

# =========================================================
# DATABASE
# =========================================================
def db():
    c = sqlite3.connect(CFG.db_path, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    return c

def init_db():
    with closing(db()) as c:
        cur = c.cursor()
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, first_seen INTEGER, last_seen INTEGER,
            username TEXT, first_name TEXT, total_tasks INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY, user_id INTEGER, username TEXT,
            created_at INTEGER, status TEXT, input_text TEXT, error_text TEXT
        );
        CREATE TABLE IF NOT EXISTS global_bans (
            user_id INTEGER PRIMARY KEY, reason TEXT, banned_at INTEGER, banned_by INTEGER
        );
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'en'
        );
        CREATE TABLE IF NOT EXISTS group_settings (
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
            bot_lock INTEGER DEFAULT 0,
            arabic_lock INTEGER DEFAULT 0,
            nsfw_lock INTEGER DEFAULT 0,
            welcome_enabled INTEGER DEFAULT 1,
            goodbye_enabled INTEGER DEFAULT 1,
            log_channel INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS warnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER, chat_id INTEGER, reason TEXT, warned_at INTEGER, warned_by INTEGER
        );
        CREATE TABLE IF NOT EXISTS notes (
            chat_id INTEGER, name TEXT, content TEXT, created_by INTEGER,
            PRIMARY KEY(chat_id, name)
        );
        CREATE TABLE IF NOT EXISTS filters (
            chat_id INTEGER, keyword TEXT, response TEXT, response_type TEXT DEFAULT 'text',
            PRIMARY KEY(chat_id, keyword)
        );
        CREATE TABLE IF NOT EXISTS captcha_pending (
            user_id INTEGER, chat_id INTEGER, msg_id INTEGER, created_at INTEGER,
            PRIMARY KEY(user_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS force_tasks (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0,
            tasks TEXT DEFAULT '[]',
            reward_text TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS force_task_done (
            user_id INTEGER, chat_id INTEGER,
            PRIMARY KEY(user_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS timed_mutes (
            user_id INTEGER, chat_id INTEGER, unmute_at INTEGER,
            PRIMARY KEY(user_id, chat_id)
        );
        """)
        c.commit()

# ---- User helpers ----
def upsert_user(user_id, username, first_name):
    now = int(time.time())
    with closing(db()) as c:
        c.execute("""INSERT INTO users(user_id,first_seen,last_seen,username,first_name)
            VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET
            last_seen=excluded.last_seen,username=excluded.username,first_name=excluded.first_name""",
            (user_id, now, now, username or "", first_name or ""))
        c.commit()

def total_users():
    with closing(db()) as c:
        return c.execute("SELECT COUNT(*) FROM users").fetchone()[0]

def latest_users(limit=10):
    with closing(db()) as c:
        return c.execute("SELECT user_id,username,first_name,last_seen FROM users ORDER BY last_seen DESC LIMIT ?", (limit,)).fetchall()

def all_user_ids():
    with closing(db()) as c:
        return [r[0] for r in c.execute("SELECT user_id FROM users").fetchall()]

def is_gbanned(user_id):
    with closing(db()) as c:
        return c.execute("SELECT 1 FROM global_bans WHERE user_id=?", (user_id,)).fetchone() is not None

def gban(user_id, reason="", banned_by=0):
    with closing(db()) as c:
        c.execute("""INSERT INTO global_bans(user_id,reason,banned_at,banned_by) VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason,banned_at=excluded.banned_at""",
            (user_id, reason, int(time.time()), banned_by))
        c.commit()

def ungban(user_id):
    with closing(db()) as c:
        c.execute("DELETE FROM global_bans WHERE user_id=?", (user_id,))
        c.commit()

def get_lang(user_id):
    with closing(db()) as c:
        row = c.execute("SELECT language FROM user_settings WHERE user_id=?", (user_id,)).fetchone()
        return row[0] if row and row[0] in {"en","bn"} else "en"

def set_lang(user_id, lang):
    with closing(db()) as c:
        c.execute("INSERT INTO user_settings(user_id,language) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET language=excluded.language", (user_id, lang))
        c.commit()

# ---- Group helpers ----
def ensure_group(chat_id):
    with closing(db()) as c:
        c.execute("INSERT OR IGNORE INTO group_settings(chat_id) VALUES(?)", (chat_id,))
        c.commit()

def gsetting(chat_id, key):
    ensure_group(chat_id)
    with closing(db()) as c:
        row = c.execute(f"SELECT {key} FROM group_settings WHERE chat_id=?", (chat_id,)).fetchone()
        return row[0] if row else None

def set_gsetting(chat_id, key, value):
    ensure_group(chat_id)
    with closing(db()) as c:
        c.execute(f"UPDATE group_settings SET {key}=? WHERE chat_id=?", (value, chat_id))
        c.commit()

def get_group_lang(chat_id):
    v = gsetting(chat_id, "language")
    return v if v in {"en","bn"} else "en"

# ---- Task records ----
def add_task_rec(task_id, user_id, username, input_text):
    with closing(db()) as c:
        c.execute("INSERT INTO tasks(id,user_id,username,created_at,status,input_text,error_text) VALUES(?,?,?,?,?,?,?)",
            (task_id, user_id, username or "", int(time.time()), "queued", input_text, ""))
        c.commit()

def upd_task(task_id, status, error=""):
    with closing(db()) as c:
        c.execute("UPDATE tasks SET status=?,error_text=? WHERE id=?", (status, error, task_id))
        c.commit()

# ---- Warnings ----
def add_warn(user_id, chat_id, reason="", warned_by=0):
    with closing(db()) as c:
        c.execute("INSERT INTO warnings(user_id,chat_id,reason,warned_at,warned_by) VALUES(?,?,?,?,?)",
            (user_id, chat_id, reason, int(time.time()), warned_by))
        c.commit()

def get_warns(user_id, chat_id):
    with closing(db()) as c:
        return c.execute("SELECT id,reason,warned_at FROM warnings WHERE user_id=? AND chat_id=? ORDER BY warned_at", (user_id, chat_id)).fetchall()

def del_warn(warn_id):
    with closing(db()) as c:
        c.execute("DELETE FROM warnings WHERE id=?", (warn_id,))
        c.commit()

def clear_warns(user_id, chat_id):
    with closing(db()) as c:
        c.execute("DELETE FROM warnings WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        c.commit()

def del_last_warn(user_id, chat_id):
    with closing(db()) as c:
        row = c.execute("SELECT id FROM warnings WHERE user_id=? AND chat_id=? ORDER BY warned_at DESC LIMIT 1", (user_id, chat_id)).fetchone()
        if row:
            c.execute("DELETE FROM warnings WHERE id=?", (row[0],))
            c.commit()

# ---- Notes ----
def save_note(chat_id, name, content, created_by=0):
    with closing(db()) as c:
        c.execute("INSERT INTO notes(chat_id,name,content,created_by) VALUES(?,?,?,?) ON CONFLICT(chat_id,name) DO UPDATE SET content=excluded.content", (chat_id, name.lower(), content, created_by))
        c.commit()

def get_note(chat_id, name):
    with closing(db()) as c:
        row = c.execute("SELECT content FROM notes WHERE chat_id=? AND name=?", (chat_id, name.lower())).fetchone()
        return row[0] if row else None

def del_note(chat_id, name):
    with closing(db()) as c:
        c.execute("DELETE FROM notes WHERE chat_id=? AND name=?", (chat_id, name.lower()))
        c.commit()

def list_notes(chat_id):
    with closing(db()) as c:
        return [r[0] for r in c.execute("SELECT name FROM notes WHERE chat_id=? ORDER BY name", (chat_id,)).fetchall()]

# ---- Filters ----
def save_filter(chat_id, keyword, response):
    with closing(db()) as c:
        c.execute("INSERT INTO filters(chat_id,keyword,response) VALUES(?,?,?) ON CONFLICT(chat_id,keyword) DO UPDATE SET response=excluded.response", (chat_id, keyword.lower(), response))
        c.commit()

def del_filter(chat_id, keyword):
    with closing(db()) as c:
        c.execute("DELETE FROM filters WHERE chat_id=? AND keyword=?", (chat_id, keyword.lower()))
        c.commit()

def get_filters(chat_id):
    with closing(db()) as c:
        return {r[0]: r[1] for r in c.execute("SELECT keyword,response FROM filters WHERE chat_id=?", (chat_id,)).fetchall()}

# ---- Captcha ----
def set_captcha_pending(user_id, chat_id, msg_id):
    with closing(db()) as c:
        c.execute("INSERT OR REPLACE INTO captcha_pending(user_id,chat_id,msg_id,created_at) VALUES(?,?,?,?)", (user_id, chat_id, msg_id, int(time.time())))
        c.commit()

def get_captcha_pending(user_id, chat_id):
    with closing(db()) as c:
        row = c.execute("SELECT msg_id FROM captcha_pending WHERE user_id=? AND chat_id=?", (user_id, chat_id)).fetchone()
        return row[0] if row else None

def del_captcha_pending(user_id, chat_id):
    with closing(db()) as c:
        c.execute("DELETE FROM captcha_pending WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        c.commit()

# ---- Force Task ----
def get_force_tasks(chat_id):
    with closing(db()) as c:
        row = c.execute("SELECT enabled,tasks,reward_text FROM force_tasks WHERE chat_id=?", (chat_id,)).fetchone()
        if not row:
            return {"enabled": 0, "tasks": [], "reward_text": ""}
        return {"enabled": row[0], "tasks": json.loads(row[1] or "[]"), "reward_text": row[2] or ""}

def set_force_tasks(chat_id, data):
    with closing(db()) as c:
        c.execute("INSERT INTO force_tasks(chat_id,enabled,tasks,reward_text) VALUES(?,?,?,?) ON CONFLICT(chat_id) DO UPDATE SET enabled=excluded.enabled,tasks=excluded.tasks,reward_text=excluded.reward_text",
            (chat_id, data["enabled"], json.dumps(data["tasks"]), data.get("reward_text","")))
        c.commit()

def has_done_force_task(user_id, chat_id):
    with closing(db()) as c:
        return c.execute("SELECT 1 FROM force_task_done WHERE user_id=? AND chat_id=?", (user_id, chat_id)).fetchone() is not None

def mark_force_task_done(user_id, chat_id):
    with closing(db()) as c:
        c.execute("INSERT OR IGNORE INTO force_task_done(user_id,chat_id) VALUES(?,?)", (user_id, chat_id))
        c.commit()

def reset_force_task(user_id, chat_id):
    with closing(db()) as c:
        c.execute("DELETE FROM force_task_done WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        c.commit()

# ---- Timed mute ----
def set_timed_mute(user_id, chat_id, unmute_at):
    with closing(db()) as c:
        c.execute("INSERT OR REPLACE INTO timed_mutes(user_id,chat_id,unmute_at) VALUES(?,?,?)", (user_id, chat_id, unmute_at))
        c.commit()

def get_expired_mutes():
    now = int(time.time())
    with closing(db()) as c:
        rows = c.execute("SELECT user_id,chat_id FROM timed_mutes WHERE unmute_at<=?", (now,)).fetchall()
        c.execute("DELETE FROM timed_mutes WHERE unmute_at<=?", (now,))
        c.commit()
        return rows

# =========================================================
# UTILITIES
# =========================================================
def is_bot_admin(user_id): return user_id in CFG.admins

def humanbytes(size):
    if not size: return "0 B"
    units = ["B","KB","MB","GB","TB"]
    i = 0
    while size >= 1024 and i < len(units)-1:
        size /= 1024; i += 1
    return f"{size:.2f} {units[i]}"

def fmt_time(seconds):
    seconds = int(seconds)
    h,r = divmod(seconds,3600); m,s = divmod(r,60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def parse_time_arg(arg):
    """Parse time like 10m, 2h, 1d -> seconds"""
    match = re.match(r"^(\d+)([smhd])$", arg.lower())
    if not match: return None
    val, unit = int(match[1]), match[2]
    return val * {"s":1,"m":60,"h":3600,"d":86400}[unit]

def valid_tg_link(text):
    return bool(TG_LINK.match(text.strip()))

def make_task_id(user_id):
    return f"{user_id}_{int(time.time()*1000)}"

def msg_hash(text):
    return hashlib.md5(text.strip().lower().encode()).hexdigest()

def user_on_cooldown(user_id):
    last = user_last_req.get(user_id, 0)
    remain = CFG.user_cooldown_sec - int(time.time() - last)
    return (remain > 0, max(remain, 0))

def reg_task(task_id, user_id, input_text):
    task_reg[task_id] = {"user_id": user_id, "input_text": input_text, "status": "queued", "created_at": int(time.time()), "cancelled": False}

def set_task_status(task_id, status):
    if task_id in task_reg: task_reg[task_id]["status"] = status

SCAM_KEYWORDS = [
    "free nitro","claim your prize","you have been selected","click here to claim",
    "crypto giveaway","send 0.1 btc","double your","investment profit",
    "airdrop claim","@everyone","adult content","18+ group","free premium",
    "earn money fast","work from home earn","urgently needed","wire transfer"
]

ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F]")

# =========================================================
# ASYNC HELPERS
# =========================================================
async def is_admin(client, chat_id, user_id):
    try:
        m = await client.get_chat_member(chat_id, user_id)
        return m.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER)
    except Exception:
        return False

async def get_target(client, message: Message):
    if message.reply_to_message and message.reply_to_message.from_user:
        return message.reply_to_message.from_user
    parts = message.text.split(maxsplit=2) if message.text else []
    if len(parts) < 2: return None
    ident = parts[1].lstrip("@")
    try:
        return await client.get_users(int(ident) if ident.lstrip("-").isdigit() else ident)
    except Exception:
        return None

async def get_reason(message: Message, default="No reason given"):
    parts = (message.text or "").split(maxsplit=2 if message.reply_to_message else 3)
    if message.reply_to_message:
        return parts[1].strip() if len(parts) > 1 else default
    return parts[2].strip() if len(parts) > 2 else default

async def send_log(client, text):
    if CFG.log_channel:
        try: await client.send_message(CFG.log_channel, text)
        except Exception: pass

async def auto_del(client, chat_id, msg_id, delay=30):
    await asyncio.sleep(delay)
    try: await client.delete_messages(chat_id, msg_id)
    except Exception: pass

async def check_fsub(client, message):
    if not CFG.force_sub_channel: return True
    try:
        await client.get_chat_member(CFG.force_sub_channel, message.from_user.id)
        return True
    except Exception:
        return False

async def check_cas(user_id):
    try:
        import urllib.request
        with urllib.request.urlopen(f"https://api.cas.chat/check?user_id={user_id}", timeout=4) as r:
            return json.loads(r.read()).get("ok", False)
    except Exception:
        return False

async def check_raid(client, chat_id):
    now = time.time()
    joins = [t for t in raid_tracker.get(chat_id,[]) if now-t < 10]
    joins.append(now)
    raid_tracker[chat_id] = joins
    if len(joins) >= 5:
        try: await client.set_slow_mode(chat_id, 30)
        except Exception: pass
        return True
    return False

async def check_flood(client, message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id: return False
    fc = gsetting(chat_id, "antiflood_count")
    ft = gsetting(chat_id, "antiflood_time")
    if not fc or fc <= 0: return False
    key = f"{chat_id}:{user_id}"
    now = time.time()
    msgs = [t for t in flood_tracker.get(key,[]) if now-t < ft]
    msgs.append(now)
    flood_tracker[key] = msgs
    if len(msgs) >= fc:
        flood_tracker[key] = []
        action = gsetting(chat_id, "antiflood_action") or "mute"
        try:
            if action == "ban":
                await client.ban_chat_member(chat_id, user_id)
            elif action == "kick":
                await client.ban_chat_member(chat_id, user_id)
                await asyncio.sleep(0.5)
                await client.unban_chat_member(chat_id, user_id)
            else:
                await client.restrict_chat_member(chat_id, user_id, ChatPermissions(can_send_messages=False))
            m = await client.send_message(chat_id, f"Flood detected! Action: {action} on {message.from_user.mention}")
            asyncio.create_task(auto_del(client, chat_id, m.id, 15))
        except Exception: pass
        return True
    return False

async def check_dup_spam(client, message: Message):
    if not message.text or len(message.text) < 8: return False
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id: return False
    key = f"{chat_id}:{user_id}:{msg_hash(message.text)}"
    count = dup_tracker.get(key, 0) + 1
    dup_tracker[key] = count
    if len(dup_tracker) > 1000: dup_tracker.clear()
    if count >= 3:
        dup_tracker.pop(key, None)
        try:
            await message.delete()
            await client.restrict_chat_member(chat_id, user_id, ChatPermissions(can_send_messages=False))
            m = await client.send_message(chat_id, f"Duplicate spam detected! {message.from_user.mention} has been muted.")
            asyncio.create_task(auto_del(client, chat_id, m.id, 20))
        except Exception: pass
        return True
    return False

# =========================================================
# FORCE TASK SYSTEM
# =========================================================
async def check_force_task(client, user_id, chat_id, message=None):
    """Returns True if user has completed force tasks, False if still pending."""
    ft = get_force_tasks(chat_id)
    if not ft["enabled"] or not ft["tasks"]:
        return True
    if has_done_force_task(user_id, chat_id):
        return True
    # Show task panel
    tasks = ft["tasks"]
    buttons = []
    for i, task in enumerate(tasks):
        label = task.get("label","Subscribe/Follow")
        url = task.get("url","")
        if url:
            buttons.append([InlineKeyboardButton(f"{'YouTube' if 'youtube' in url.lower() or 'youtu.be' in url.lower() else 'Facebook' if 'facebook' in url.lower() or 'fb.com' in url.lower() else 'Telegram' if 't.me' in url.lower() else 'Visit'}: {label}", url=url)])
    buttons.append([InlineKeyboardButton("Done! Verify Now", callback_data=f"ft_verify_{chat_id}")])
    text = (
        "**Complete the following tasks to use this bot:**\n\n"
        + "\n".join(f"{i+1}. {t.get('label','Task')}" for i,t in enumerate(tasks))
        + "\n\nAfter completing, click **Done! Verify Now**"
    )
    try:
        if message:
            await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await client.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception: pass
    return False

# =========================================================
# UI BUILDERS
# =========================================================
def start_btns(uname="bot"):
    btns = [
        [InlineKeyboardButton("Add me to Group", url=f"https://t.me/{uname}?startgroup=true"),
         InlineKeyboardButton("Help", callback_data="help_main")],
    ]
    if CFG.support_chat:
        btns.append([InlineKeyboardButton("Support Chat", url=f"https://t.me/{CFG.support_chat.lstrip('@')}")])
    return InlineKeyboardMarkup(btns)

def help_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Moderation", callback_data="help_mod"),
         InlineKeyboardButton("Welcome/Bye", callback_data="help_welcome")],
        [InlineKeyboardButton("Warnings", callback_data="help_warn"),
         InlineKeyboardButton("Notes", callback_data="help_notes")],
        [InlineKeyboardButton("Filters", callback_data="help_filters"),
         InlineKeyboardButton("Locks", callback_data="help_locks")],
        [InlineKeyboardButton("Captcha", callback_data="help_captcha"),
         InlineKeyboardButton("Anti-Flood", callback_data="help_flood")],
        [InlineKeyboardButton("Force Task", callback_data="help_forcetask"),
         InlineKeyboardButton("Downloader", callback_data="help_dl")],
        [InlineKeyboardButton("Security", callback_data="help_security"),
         InlineKeyboardButton("Settings", callback_data="help_settings")],
        [InlineKeyboardButton("Close", callback_data="close")]
    ])

def back_btn():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Back to Menu", callback_data="help_main")]])

def admin_panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Stats", callback_data="adm_stats"),
         InlineKeyboardButton("Queue", callback_data="adm_queue")],
        [InlineKeyboardButton("Users", callback_data="adm_users"),
         InlineKeyboardButton("Maintenance", callback_data="adm_maint")],
        [InlineKeyboardButton("Clear Queue", callback_data="adm_clrq"),
         InlineKeyboardButton("Logs", callback_data="adm_logs")],
        [InlineKeyboardButton("Close", callback_data="close")]
    ])

def group_settings_menu(chat_id):
    cap = gsetting(chat_id, "captcha_enabled")
    afc = gsetting(chat_id, "antiflood_count")
    ml = gsetting(chat_id, "media_lock")
    sl = gsetting(chat_id, "sticker_lock")
    ll = gsetting(chat_id, "link_lock")
    fl = gsetting(chat_id, "forward_lock")
    al = gsetting(chat_id, "arabic_lock")
    wl = gsetting(chat_id, "welcome_enabled")
    gl = gsetting(chat_id, "goodbye_enabled")
    ft = get_force_tasks(chat_id)
    lang = get_group_lang(chat_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'ON' if cap else 'OFF'} Captcha", callback_data=f"gs_captcha_{chat_id}"),
         InlineKeyboardButton(f"{'ON' if afc else 'OFF'} Anti-Flood", callback_data=f"gs_flood_{chat_id}")],
        [InlineKeyboardButton(f"{'LOCK' if ml else 'OPEN'} Media", callback_data=f"gs_media_{chat_id}"),
         InlineKeyboardButton(f"{'LOCK' if sl else 'OPEN'} Stickers", callback_data=f"gs_sticker_{chat_id}")],
        [InlineKeyboardButton(f"{'LOCK' if ll else 'OPEN'} Links", callback_data=f"gs_link_{chat_id}"),
         InlineKeyboardButton(f"{'LOCK' if fl else 'OPEN'} Forwards", callback_data=f"gs_fwd_{chat_id}")],
        [InlineKeyboardButton(f"{'LOCK' if al else 'OPEN'} Arabic", callback_data=f"gs_arabic_{chat_id}"),
         InlineKeyboardButton(f"{'ON' if wl else 'OFF'} Welcome", callback_data=f"gs_welcome_{chat_id}")],
        [InlineKeyboardButton(f"{'ON' if ft['enabled'] else 'OFF'} Force Task", callback_data=f"gs_ft_{chat_id}"),
         InlineKeyboardButton(f"{'ON' if gl else 'OFF'} Goodbye", callback_data=f"gs_bye_{chat_id}")],
        [InlineKeyboardButton(f"EN {'(active)' if lang=='en' else ''}", callback_data=f"gs_lang_en_{chat_id}"),
         InlineKeyboardButton(f"BN {'(active)' if lang=='bn' else ''}", callback_data=f"gs_lang_bn_{chat_id}")],
        [InlineKeyboardButton("Close", callback_data="close")]
    ])

HELP = {
    "main": (
        "**GroupHelp Bot**\n\n"
        "A powerful group manager + restricted content downloader.\n\n"
        "Choose a category below:"
    ),
    "mod": (
        "**Moderation Commands:**\n\n"
        "`/ban [@user] [reason]` - Ban a user\n"
        "`/unban [@user]` - Unban a user\n"
        "`/kick [@user] [reason]` - Kick from group\n"
        "`/mute [@user] [time] [reason]` - Mute user\n"
        "  Time: `10m`, `2h`, `1d` (optional)\n"
        "`/unmute [@user]` - Unmute user\n"
        "`/ban [@user] del [reason]` - Ban + delete messages\n"
        "`/promote [@user]` - Promote to admin\n"
        "`/demote [@user]` - Remove admin rights\n"
        "`/pin` - Pin replied message\n"
        "`/unpin` - Unpin message\n"
        "`/purge` - Delete all messages from reply\n"
        "`/del` - Delete replied message\n"
        "`/adminlist` - Show all admins\n"
        "`/info [@user]` - User info + stats\n"
        "`/id` - Get user/chat ID"
    ),
    "warn": (
        "**Warning System:**\n\n"
        "`/warn [@user] [reason]` - Warn a user\n"
        "`/dwarn [@user]` - Warn + delete message\n"
        "`/unwarn [@user]` - Remove last warning\n"
        "`/warnlist [@user]` - View warnings\n"
        "`/clearwarns [@user]` - Clear all warnings\n"
        "`/setwarnlimit <n>` - Set max warnings (default: 3)\n"
        "`/warnmode ban|kick|mute` - Action at limit\n\n"
        "When limit is reached: auto ban/kick/mute!"
    ),
    "welcome": (
        "**Welcome & Goodbye:**\n\n"
        "`/setwelcome <text>` - Set welcome message\n"
        "`/resetwelcome` - Reset to default\n"
        "`/welcome` - Preview welcome message\n"
        "`/setgoodbye <text>` - Set goodbye message\n"
        "`/resetgoodbye` - Reset goodbye\n\n"
        "**Variables:**\n"
        "`{mention}` - User mention\n"
        "`{first}` - First name\n"
        "`{last}` - Last name\n"
        "`{username}` - Username\n"
        "`{title}` - Group name\n"
        "`{count}` - Member count\n"
        "`{id}` - User ID"
    ),
    "notes": (
        "**Notes System:**\n\n"
        "`/save <name> <content>` - Save a note\n"
        "`/note <name>` or `#name` - Get a note\n"
        "`/notes` - List all notes\n"
        "`/delnote <name>` - Delete a note\n\n"
        "Notes support text, buttons, and markdown!"
    ),
    "filters": (
        "**Auto-Filter System:**\n\n"
        "`/filter <keyword> <response>` - Add keyword filter\n"
        "`/filters` - List all active filters\n"
        "`/stop <keyword>` - Remove a filter\n"
        "`/stopall` - Remove all filters\n\n"
        "Bot auto-replies when keyword is detected!"
    ),
    "locks": (
        "**Lock System:**\n\n"
        "`/lock <type>` - Lock something\n"
        "`/unlock <type>` - Unlock it\n"
        "`/locks` - View all lock status\n\n"
        "**Lock Types:**\n"
        "`media` `stickers` `links` `forwards` `arabic` `bots` `all`"
    ),
    "captcha": (
        "**Captcha System:**\n\n"
        "`/captcha on|off` - Enable/disable\n\n"
        "New members must click a button within 60s to verify.\n"
        "If they fail, they are auto-kicked!\n\n"
        "Also includes **CAS Ban** check on join."
    ),
    "flood": (
        "**Anti-Flood System:**\n\n"
        "`/antiflood <count> <time> <action>` - Set limit\n"
        "`/antiflood off` - Disable\n\n"
        "**Actions:** `mute`, `kick`, `ban`\n"
        "**Example:** `/antiflood 5 10 mute`\n"
        "(5 messages in 10 seconds = mute)"
    ),
    "forcetask": (
        "**Force Task System:**\n\n"
        "`/addtask <label> <url>` - Add a task (YouTube sub, FB follow, etc.)\n"
        "`/deltask <number>` - Remove a task\n"
        "`/tasklist` - View current tasks\n"
        "`/setreward <text>` - Message shown after completion\n"
        "`/forcetask on|off` - Enable/disable\n"
        "`/resetuser [@user]` - Reset task for a user\n\n"
        "Users must complete all tasks before using the bot in the group!"
    ),
    "dl": (
        "**Restricted Content Downloader:**\n\n"
        "Send me any restricted Telegram link in private:\n"
        "`https://t.me/c/12345/678`\n"
        "`https://t.me/channelname/123`\n\n"
        "I will download and send you the file!\n"
        "Supports: Videos, Photos, Documents, Audio, Voice"
    ),
    "security": (
        "**Advanced Security (Auto-Active):**\n\n"
        "**On Join:**\n"
        "- CAS Ban check (global spam database)\n"
        "- Anti-Raid (5+ joins in 10s -> slow mode)\n"
        "- Captcha verification (if enabled)\n\n"
        "**In Chat:**\n"
        "- Anti-flood protection\n"
        "- Duplicate message spam detection\n"
        "- Scam/phishing keyword auto-delete\n"
        "- Arabic text lock (optional)\n"
        "- Link lock\n\n"
        "**Global:**\n"
        "- `/gban` - Global ban across all chats\n"
        "- `/ungban` - Remove global ban"
    ),
    "settings": (
        "**Group Settings:**\n\n"
        "`/settings` - Open settings panel\n"
        "`/lang en|bn` - Change bot language\n"
        "`/rules [text]` - View or set rules\n"
        "`/setrules <text>` - Update rules\n"
        "`/warnmode ban|kick|mute` - Warn action\n"
        "`/setlogchannel <id>` - Set log channel\n"
        "`/forcetask on|off` - Force task toggle"
    ),
}

# =========================================================
# COMMANDS - PRIVATE
# =========================================================
@bot.on_message(filters.command("start") & filters.private)
async def cmd_start(client, message):
    user = message.from_user
    upsert_user(user.id, user.username, user.first_name)
    if is_gbanned(user.id):
        return await message.reply_text("You are globally banned from using this bot.")
    if state["maintenance"] and not is_bot_admin(user.id):
        return await message.reply_text("Bot is under maintenance. Try again later.")
    if not await check_fsub(client, message):
        return await message.reply_text(
            "Please join our channel first!",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")
            ]])
        )
    lang = get_lang(user.id)
    if lang == "bn":
        text = (
            f"**à¦¸à§à¦¬à¦¾à¦—à¦¤à¦®, {user.first_name}!**\n\n"
            "à¦†à¦®à¦¿ **GroupHelp Bot** - à¦‰à¦¨à§à¦¨à¦¤ à¦—à§à¦°à§à¦ª à¦®à§à¦¯à¦¾à¦¨à§‡à¦œà¦¾à¦° + à¦•à¦¨à§à¦Ÿà§‡à¦¨à§à¦Ÿ à¦¡à¦¾à¦‰à¦¨à¦²à§‹à¦¡à¦¾à¦°!\n\n"
            "**à¦—à§à¦°à§à¦ªà§‡:** Ban, Mute, Warn, Notes, Filters, Captcha, Anti-Flood à¦à¦¬à¦‚ à¦†à¦°à¦“!\n"
            "**Private-à¦:** Restricted Telegram link à¦ªà¦¾à¦ à¦¾à¦“, à¦†à¦®à¦¿ à¦¡à¦¾à¦‰à¦¨à¦²à§‹à¦¡ à¦•à¦°à§‡ à¦¦à§‡à¦¬!\n\n"
            "à¦¨à¦¿à¦šà§‡à¦° à¦¬à¦¾à¦Ÿà¦¨à§‡ à¦•à§à¦²à¦¿à¦• à¦•à¦°à§‹:"
        )
    else:
        text = (
            f"**Welcome, {user.first_name}!**\n\n"
            "I'm **GroupHelp Bot** - Advanced group manager + content downloader!\n\n"
            "**In Groups:** Ban, Mute, Warn, Notes, Filters, Captcha, Anti-Flood & more!\n"
            "**In Private:** Send any restricted Telegram link, I'll download it!\n\n"
            "Use the buttons below:"
        )
    await message.reply_text(text, reply_markup=start_btns(bot_username))

@bot.on_message(filters.command("help"))
async def cmd_help(client, message):
    await message.reply_text(HELP["main"], reply_markup=help_menu())

@bot.on_message(filters.command("settings") & filters.private)
async def cmd_settings_private(client, message):
    lang = get_lang(message.from_user.id)
    await message.reply_text(
        f"**Personal Settings**\n\nLanguage: `{lang}`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("English", callback_data="sl_en"),
             InlineKeyboardButton("Bangla", callback_data="sl_bn")]
        ])
    )

@bot.on_message(filters.command("admin") & filters.private)
async def cmd_admin(client, message):
    if not is_bot_admin(message.from_user.id): return
    await message.reply_text("**Admin Panel**", reply_markup=admin_panel())

@bot.on_message(filters.command("gban") & filters.private)
async def cmd_gban(client, message):
    if not is_bot_admin(message.from_user.id): return
    target = await get_target(client, message)
    if not target: return await message.reply_text("Usage: `/gban @user [reason]` or reply to user")
    reason = await get_reason(message, "Global ban")
    gban(target.id, reason, message.from_user.id)
    banned_in = 0
    with closing(db()) as c:
        chats = [r[0] for r in c.execute("SELECT chat_id FROM group_settings").fetchall()]
    for cid in chats:
        try:
            await client.ban_chat_member(cid, target.id)
            banned_in += 1
            await asyncio.sleep(0.1)
        except Exception: pass
    await message.reply_text(
        f"**Global Ban Applied**\n\nUser: {target.mention}\nReason: {reason}\nBanned in: {banned_in} chats"
    )
    await send_log(client, f"GBAN: {target.mention} (`{target.id}`) by {message.from_user.mention}\nReason: {reason}")

@bot.on_message(filters.command("ungban") & filters.private)
async def cmd_ungban(client, message):
    if not is_bot_admin(message.from_user.id): return
    target = await get_target(client, message)
    if not target: return await message.reply_text("Usage: `/ungban @user`")
    ungban(target.id)
    await message.reply_text(f"Global ban removed for {target.mention}.")

@bot.on_message(filters.command("broadcast") & filters.private)
async def cmd_broadcast(client, message):
    if not is_bot_admin(message.from_user.id): return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/broadcast <message>`")
    msg = parts[1]
    sent, failed = 0, 0
    for uid in all_user_ids():
        try:
            await client.send_message(uid, f"**Broadcast:**\n\n{msg}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception: failed += 1
    await message.reply_text(f"Broadcast done!\nSent: {sent} | Failed: {failed}")

@bot.on_message(filters.command("stats") & filters.private)
async def cmd_stats(client, message):
    if not is_bot_admin(message.from_user.id): return
    await message.reply_text(
        f"**Bot Statistics**\n\n"
        f"Users: `{total_users()}`\n"
        f"Queue: `{task_queue.qsize()}`\n"
        f"Tasks Done: `{state['success_tasks']}`\n"
        f"Tasks Failed: `{state['failed_tasks']}`\n"
        f"Uptime: `{fmt_time(time.time()-state['started_at'])}`\n"
        f"Maintenance: `{state['maintenance']}`"
    )

# =========================================================
# COMMANDS - GROUPS: MODERATION
# =========================================================
@bot.on_message(filters.command("ban") & filters.group)
async def cmd_ban(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    if await is_admin(client, chat_id, target.id):
        return await message.reply_text("Cannot ban an admin.")
    reason = await get_reason(message)
    # Check for 'del' flag to delete messages
    delete_msgs = "del" in (message.text or "").lower().split()
    try:
        await client.ban_chat_member(chat_id, target.id)
        text = f"**Banned:** {target.mention}\n**Reason:** {reason}"
        await message.reply_text(text)
        if delete_msgs:
            await message.delete()
        await send_log(client, f"BAN | {message.chat.title}\nUser: {target.mention} (`{target.id}`)\nBy: {message.from_user.mention}\nReason: {reason}")
    except ChatAdminRequired:
        await message.reply_text("I need ban rights!")

@bot.on_message(filters.command("unban") & filters.group)
async def cmd_unban(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    try:
        await client.unban_chat_member(chat_id, target.id)
        await message.reply_text(f"**Unbanned:** {target.mention}")
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("kick") & filters.group)
async def cmd_kick(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    if await is_admin(client, chat_id, target.id):
        return await message.reply_text("Cannot kick an admin.")
    reason = await get_reason(message)
    try:
        await client.ban_chat_member(chat_id, target.id)
        await asyncio.sleep(0.5)
        await client.unban_chat_member(chat_id, target.id)
        await message.reply_text(f"**Kicked:** {target.mention}\n**Reason:** {reason}")
        await send_log(client, f"KICK | {message.chat.title}\nUser: {target.mention}\nBy: {message.from_user.mention}\nReason: {reason}")
    except ChatAdminRequired:
        await message.reply_text("I need kick rights!")

@bot.on_message(filters.command("mute") & filters.group)
async def cmd_mute(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    if await is_admin(client, chat_id, target.id):
        return await message.reply_text("Cannot mute an admin.")
    # Parse time argument
    parts = (message.text or "").split()
    mute_secs = None
    reason = "No reason given"
    for p in parts[2:]:
        t = parse_time_arg(p)
        if t:
            mute_secs = t
        elif p not in parts[:2]:
            reason = p
    try:
        until = int(time.time()) + mute_secs if mute_secs else 0
        await client.restrict_chat_member(chat_id, target.id, ChatPermissions(can_send_messages=False), until_date=until if until else None)
        dur_text = f" for {fmt_time(mute_secs)}" if mute_secs else ""
        await message.reply_text(f"**Muted:** {target.mention}{dur_text}\n**Reason:** {reason}")
        if mute_secs:
            set_timed_mute(target.id, chat_id, int(time.time()) + mute_secs)
        await send_log(client, f"MUTE | {message.chat.title}\nUser: {target.mention}\nDuration: {fmt_time(mute_secs) if mute_secs else 'Permanent'}\nReason: {reason}")
    except ChatAdminRequired:
        await message.reply_text("I need restrict rights!")

@bot.on_message(filters.command("unmute") & filters.group)
async def cmd_unmute(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    try:
        await client.restrict_chat_member(chat_id, target.id, ChatPermissions(
            can_send_messages=True, can_send_media_messages=True,
            can_send_other_messages=True, can_add_web_page_previews=True
        ))
        await message.reply_text(f"**Unmuted:** {target.mention}")
    except ChatAdminRequired:
        await message.reply_text("I need restrict rights!")

@bot.on_message(filters.command("promote") & filters.group)
async def cmd_promote(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    parts = (message.text or "").split(maxsplit=2)
    title = parts[2].strip() if len(parts) > 2 else ""
    try:
        await client.promote_chat_member(chat_id, target.id,
            can_change_info=True, can_delete_messages=True,
            can_restrict_members=True, can_invite_users=True, can_pin_messages=True)
        if title:
            await client.set_administrator_title(chat_id, target.id, title)
        await message.reply_text(f"**Promoted:** {target.mention}" + (f"\nTitle: `{title}`" if title else ""))
    except ChatAdminRequired:
        await message.reply_text("I need promote rights!")

@bot.on_message(filters.command("demote") & filters.group)
async def cmd_demote(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    try:
        await client.promote_chat_member(chat_id, target.id,
            can_change_info=False, can_delete_messages=False,
            can_restrict_members=False, can_invite_users=False, can_pin_messages=False)
        await message.reply_text(f"**Demoted:** {target.mention}")
    except ChatAdminRequired:
        await message.reply_text("I need promote rights!")

@bot.on_message(filters.command("pin") & filters.group)
async def cmd_pin(client, message):
    if not await is_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to pin it.")
    try:
        await client.pin_chat_message(message.chat.id, message.reply_to_message.id)
        await message.reply_text("Message pinned!")
    except ChatAdminRequired:
        await message.reply_text("I need pin rights!")

@bot.on_message(filters.command("unpin") & filters.group)
async def cmd_unpin(client, message):
    if not await is_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    try:
        await client.unpin_chat_message(message.chat.id)
        await message.reply_text("Message unpinned!")
    except ChatAdminRequired:
        await message.reply_text("I need pin rights!")

@bot.on_message(filters.command("purge") & filters.group)
async def cmd_purge(client, message):
    if not await is_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    if not message.reply_to_message:
        return await message.reply_text("Reply to the message to start purging from.")
    from_id = message.reply_to_message.id
    to_id = message.id
    ids = list(range(from_id, to_id + 1))
    try:
        for i in range(0, len(ids), 100):
            await client.delete_messages(message.chat.id, ids[i:i+100])
        m = await client.send_message(message.chat.id, f"Purged {len(ids)} messages.")
        asyncio.create_task(auto_del(client, message.chat.id, m.id, 5))
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("del") & filters.group)
async def cmd_del(client, message):
    if not await is_admin(client, message.chat.id, message.from_user.id): return
    if message.reply_to_message:
        try:
            await message.reply_to_message.delete()
            await message.delete()
        except Exception: pass

@bot.on_message(filters.command("adminlist") & filters.group)
async def cmd_adminlist(client, message):
    try:
        admins = []
        async for m in client.get_chat_members(message.chat.id, filter="administrators"):
            badge = "Owner" if m.status == ChatMemberStatus.OWNER else "Admin"
            name = m.user.first_name
            title = f" [{m.custom_title}]" if m.custom_title else ""
            admins.append(f"[{badge}] [{name}](tg://user?id={m.user.id}){title}")
        await message.reply_text("**Admin List:**\n\n" + "\n".join(admins))
    except Exception as e:
        await message.reply_text(f"Error: {e}")

@bot.on_message(filters.command("id"))
async def cmd_id(client, message):
    if message.reply_to_message and message.reply_to_message.from_user:
        u = message.reply_to_message.from_user
        await message.reply_text(f"User ID: `{u.id}`\nName: {u.first_name}")
    else:
        text = f"Your ID: `{message.from_user.id}`"
        if message.chat.type == ChatType.GROUP:
            text += f"\nChat ID: `{message.chat.id}`"
        await message.reply_text(text)

@bot.on_message(filters.command("info"))
async def cmd_info(client, message):
    target = await get_target(client, message)
    user = target or message.from_user
    warns = get_warns(user.id, message.chat.id) if message.chat.type == ChatType.GROUP else []
    max_w = gsetting(message.chat.id, "max_warnings") if message.chat.type == ChatType.GROUP else 3
    gbanned = is_gbanned(user.id)
    upsert_user(user.id, user.username, user.first_name)
    await message.reply_text(
        f"**User Info**\n\n"
        f"ID: `{user.id}`\n"
        f"Name: {user.first_name or ''} {user.last_name or ''}\n"
        f"Username: @{user.username or 'None'}\n"
        f"Bot: `{user.is_bot}`\n"
        f"Warnings: `{len(warns)}/{max_w or 3}`\n"
        f"Globally Banned: `{gbanned}`"
    )

# =========================================================
# COMMANDS - GROUPS: WARNINGS
# =========================================================
@bot.on_message(filters.command("warn") & filters.group)
async def cmd_warn(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    if await is_admin(client, chat_id, target.id):
        return await message.reply_text("Cannot warn an admin.")
    reason = await get_reason(message)
    add_warn(target.id, chat_id, reason, message.from_user.id)
    warns = get_warns(target.id, chat_id)
    max_w = gsetting(chat_id, "max_warnings") or 3
    count = len(warns)
    if count >= max_w:
        clear_warns(target.id, chat_id)
        warnmode = gsetting(chat_id, "antiflood_action") or "ban"
        try:
            if warnmode == "kick":
                await client.ban_chat_member(chat_id, target.id)
                await asyncio.sleep(0.5)
                await client.unban_chat_member(chat_id, target.id)
                action_text = "kicked"
            elif warnmode == "mute":
                await client.restrict_chat_member(chat_id, target.id, ChatPermissions(can_send_messages=False))
                action_text = "muted"
            else:
                await client.ban_chat_member(chat_id, target.id)
                action_text = "banned"
            await message.reply_text(
                f"**{target.mention}** reached `{max_w}` warnings and has been **{action_text}**!"
            )
        except Exception: pass
        await send_log(client, f"AUTO-ACTION (warn limit) | {message.chat.title}\nUser: {target.mention}")
    else:
        warn_btns = InlineKeyboardMarkup([[
            InlineKeyboardButton("Remove Last Warn", callback_data=f"rmwarn_{target.id}_{chat_id}")
        ]])
        await message.reply_text(
            f"**Warned:** {target.mention}\n"
            f"**Count:** `{count}/{max_w}`\n"
            f"**Reason:** {reason}",
            reply_markup=warn_btns
        )

@bot.on_message(filters.command("dwarn") & filters.group)
async def cmd_dwarn(client, message):
    """Warn + delete the replied message."""
    if not await is_admin(client, message.chat.id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    if message.reply_to_message:
        try: await message.reply_to_message.delete()
        except Exception: pass
    # Reuse warn logic
    await cmd_warn(client, message)

@bot.on_message(filters.command("unwarn") & filters.group)
async def cmd_unwarn(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    del_last_warn(target.id, chat_id)
    await message.reply_text(f"Last warning removed for {target.mention}.")

@bot.on_message(filters.command("warnlist") & filters.group)
async def cmd_warnlist(client, message):
    target = await get_target(client, message)
    user = target or message.from_user
    warns = get_warns(user.id, message.chat.id)
    max_w = gsetting(message.chat.id, "max_warnings") or 3
    if not warns:
        return await message.reply_text(f"{user.mention} has no warnings.")
    lines = "\n".join(f"{i+1}. {r[1] or 'No reason'}" for i,r in enumerate(warns))
    await message.reply_text(f"**Warnings for {user.mention}:**\n`{len(warns)}/{max_w}`\n\n{lines}")

@bot.on_message(filters.command("clearwarns") & filters.group)
async def cmd_clearwarns(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    clear_warns(target.id, chat_id)
    await message.reply_text(f"All warnings cleared for {target.mention}.")

@bot.on_message(filters.command("setwarnlimit") & filters.group)
async def cmd_setwarnlimit(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return await message.reply_text("Usage: `/setwarnlimit <1-10>`")
    n = int(parts[1].strip())
    if not 1 <= n <= 10: return await message.reply_text("Must be between 1 and 10.")
    set_gsetting(chat_id, "max_warnings", n)
    await message.reply_text(f"Warning limit set to `{n}`.")

@bot.on_message(filters.command("warnmode") & filters.group)
async def cmd_warnmode(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].lower() not in {"ban","kick","mute"}:
        return await message.reply_text("Usage: `/warnmode ban|kick|mute`")
    set_gsetting(chat_id, "antiflood_action", parts[1].lower())
    await message.reply_text(f"Warn action set to `{parts[1].lower()}`.")

# =========================================================
# COMMANDS - GROUPS: WELCOME
# =========================================================
@bot.on_message(filters.command("setwelcome") & filters.group)
async def cmd_setwelcome(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/setwelcome <text>`\n\nVariables: `{mention}` `{first}` `{title}` `{count}` `{id}`")
    set_gsetting(chat_id, "welcome_text", parts[1].strip())
    await message.reply_text("Welcome message updated!")

@bot.on_message(filters.command("welcome") & filters.group)
async def cmd_welcome(client, message):
    chat_id = message.chat.id
    text = gsetting(chat_id, "welcome_text") or "Default: Welcome {mention} to {title}!"
    await message.reply_text(f"**Current welcome message:**\n\n{text}")

@bot.on_message(filters.command("resetwelcome") & filters.group)
async def cmd_resetwelcome(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    set_gsetting(chat_id, "welcome_text", "")
    await message.reply_text("Welcome message reset to default.")

@bot.on_message(filters.command("setgoodbye") & filters.group)
async def cmd_setgoodbye(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/setgoodbye <text>`")
    set_gsetting(chat_id, "goodbye_text", parts[1].strip())
    await message.reply_text("Goodbye message updated!")

@bot.on_message(filters.command("resetgoodbye") & filters.group)
async def cmd_resetgoodbye(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    set_gsetting(chat_id, "goodbye_text", "")
    await message.reply_text("Goodbye message reset.")

# =========================================================
# COMMANDS - GROUPS: NOTES
# =========================================================
@bot.on_message(filters.command("save") & filters.group)
async def cmd_save(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3: return await message.reply_text("Usage: `/save <name> <content>`")
    save_note(chat_id, parts[1], parts[2], message.from_user.id)
    await message.reply_text(f"Note `#{parts[1]}` saved!")

@bot.on_message(filters.command("note") & filters.group)
async def cmd_note(client, message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/note <name>`")
    content = get_note(message.chat.id, parts[1].strip())
    if not content: return await message.reply_text(f"Note `#{parts[1]}` not found.")
    await message.reply_text(f"**#{parts[1]}**\n\n{content}")

@bot.on_message(filters.command("notes") & filters.group)
async def cmd_notes(client, message):
    notes = list_notes(message.chat.id)
    if not notes: return await message.reply_text("No saved notes.")
    await message.reply_text("**Saved Notes:**\n\n" + "\n".join(f"- `#{n}`" for n in notes))

@bot.on_message(filters.command("delnote") & filters.group)
async def cmd_delnote(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/delnote <name>`")
    del_note(chat_id, parts[1].strip())
    await message.reply_text(f"Note `#{parts[1]}` deleted.")

# =========================================================
# COMMANDS - GROUPS: FILTERS
# =========================================================
@bot.on_message(filters.command("filter") & filters.group)
async def cmd_filter(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3: return await message.reply_text("Usage: `/filter <keyword> <response>`")
    save_filter(chat_id, parts[1], parts[2])
    await message.reply_text(f"Filter `{parts[1]}` saved!")

@bot.on_message(filters.command("filters") & filters.group)
async def cmd_filters(client, message):
    f = get_filters(message.chat.id)
    if not f: return await message.reply_text("No active filters.")
    await message.reply_text("**Active Filters:**\n\n" + "\n".join(f"- `{k}`" for k in f.keys()))

@bot.on_message(filters.command("stop") & filters.group)
async def cmd_stop(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/stop <keyword>`")
    del_filter(chat_id, parts[1].strip())
    await message.reply_text(f"Filter `{parts[1]}` removed.")

@bot.on_message(filters.command("stopall") & filters.group)
async def cmd_stopall(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    with closing(db()) as c:
        c.execute("DELETE FROM filters WHERE chat_id=?", (chat_id,))
        c.commit()
    await message.reply_text("All filters removed.")

# =========================================================
# COMMANDS - GROUPS: LOCKS
# =========================================================
LOCK_MAP = {
    "media": "media_lock", "stickers": "sticker_lock", "sticker": "sticker_lock",
    "links": "link_lock", "link": "link_lock", "forwards": "forward_lock",
    "forward": "forward_lock", "arabic": "arabic_lock", "bots": "bot_lock", "bot": "bot_lock",
}

@bot.on_message(filters.command("lock") & filters.group)
async def cmd_lock(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text(f"Usage: `/lock <type>`\nTypes: {', '.join(set(LOCK_MAP.keys()))}, all")
    t = parts[1].strip().lower()
    if t == "all":
        for col in set(LOCK_MAP.values()): set_gsetting(chat_id, col, 1)
        return await message.reply_text("All locks enabled!")
    col = LOCK_MAP.get(t)
    if not col: return await message.reply_text(f"Unknown type. Try: {', '.join(set(LOCK_MAP.keys()))}")
    set_gsetting(chat_id, col, 1)
    await message.reply_text(f"`{t}` locked!")

@bot.on_message(filters.command("unlock") & filters.group)
async def cmd_unlock(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/unlock <type>` or `/unlock all`")
    t = parts[1].strip().lower()
    if t == "all":
        for col in set(LOCK_MAP.values()): set_gsetting(chat_id, col, 0)
        return await message.reply_text("All locks disabled!")
    col = LOCK_MAP.get(t)
    if not col: return await message.reply_text("Unknown type.")
    set_gsetting(chat_id, col, 0)
    await message.reply_text(f"`{t}` unlocked!")

@bot.on_message(filters.command("locks") & filters.group)
async def cmd_locks(client, message):
    chat_id = message.chat.id
    ensure_group(chat_id)
    statuses = [
        ("Media", gsetting(chat_id, "media_lock")),
        ("Stickers", gsetting(chat_id, "sticker_lock")),
        ("Links", gsetting(chat_id, "link_lock")),
        ("Forwards", gsetting(chat_id, "forward_lock")),
        ("Arabic", gsetting(chat_id, "arabic_lock")),
        ("Bots", gsetting(chat_id, "bot_lock")),
    ]
    lines = "\n".join(f"{'LOCKED' if v else 'OPEN'} {k}" for k, v in statuses)
    await message.reply_text(f"**Lock Status:**\n\n{lines}")

# =========================================================
# COMMANDS - GROUPS: RULES
# =========================================================
@bot.on_message(filters.command("rules") & filters.group)
async def cmd_rules(client, message):
    chat_id = message.chat.id
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        if not await is_admin(client, chat_id, message.from_user.id):
            return await message.reply_text("You need admin rights.")
        set_gsetting(chat_id, "rules", parts[1].strip())
        return await message.reply_text("Rules updated!")
    rules = gsetting(chat_id, "rules")
    if not rules: return await message.reply_text("No rules set. Use `/rules <text>` to set them.")
    await message.reply_text(f"**Group Rules:**\n\n{rules}")

@bot.on_message(filters.command("setrules") & filters.group)
async def cmd_setrules(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/setrules <rules text>`")
    set_gsetting(chat_id, "rules", parts[1].strip())
    await message.reply_text("Rules updated!")

# =========================================================
# COMMANDS - GROUPS: ANTI-FLOOD & CAPTCHA
# =========================================================
@bot.on_message(filters.command("antiflood") & filters.group)
async def cmd_antiflood(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=3)
    if len(parts) == 2 and parts[1].lower() == "off":
        set_gsetting(chat_id, "antiflood_count", 0)
        return await message.reply_text("Anti-flood disabled.")
    if len(parts) < 4:
        return await message.reply_text(
            "Usage: `/antiflood <count> <time_secs> <action>`\n"
            "Example: `/antiflood 5 10 mute`\n"
            "Actions: `mute`, `kick`, `ban`\n"
            "Disable: `/antiflood off`"
        )
    count, t_secs, action = parts[1], parts[2], parts[3].lower()
    if not count.isdigit() or not t_secs.isdigit():
        return await message.reply_text("Count and time must be numbers.")
    if action not in {"mute","kick","ban"}:
        return await message.reply_text("Action: mute, kick, or ban")
    set_gsetting(chat_id, "antiflood_count", int(count))
    set_gsetting(chat_id, "antiflood_time", int(t_secs))
    set_gsetting(chat_id, "antiflood_action", action)
    await message.reply_text(f"Anti-flood: `{count}` msgs in `{t_secs}`s -> `{action}`")

@bot.on_message(filters.command("captcha") & filters.group)
async def cmd_captcha(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].lower() not in {"on","off"}:
        cur = gsetting(chat_id, "captcha_enabled")
        return await message.reply_text(f"Captcha is currently `{'on' if cur else 'off'}`.\nUsage: `/captcha on|off`")
    enabled = parts[1].lower() == "on"
    set_gsetting(chat_id, "captcha_enabled", 1 if enabled else 0)
    await message.reply_text(f"Captcha {'enabled' if enabled else 'disabled'}!")

# =========================================================
# COMMANDS - GROUPS: FORCE TASK
# =========================================================
@bot.on_message(filters.command("addtask") & filters.group)
async def cmd_addtask(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 3:
        return await message.reply_text(
            "Usage: `/addtask <label> <url>`\n\n"
            "Examples:\n"
            "`/addtask Subscribe on YouTube https://youtube.com/@yourchannel`\n"
            "`/addtask Follow on Facebook https://facebook.com/yourpage`\n"
            "`/addtask Join our Channel https://t.me/yourchannel`"
        )
    label, url = parts[1], parts[2]
    if not url.startswith("http"): return await message.reply_text("URL must start with http.")
    ft = get_force_tasks(chat_id)
    ft["tasks"].append({"label": label, "url": url})
    set_force_tasks(chat_id, ft)
    await message.reply_text(f"Task added!\n\nLabel: `{label}`\nURL: {url}")

@bot.on_message(filters.command("deltask") & filters.group)
async def cmd_deltask(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip().isdigit():
        return await message.reply_text("Usage: `/deltask <number>` (use /tasklist to see numbers)")
    idx = int(parts[1].strip()) - 1
    ft = get_force_tasks(chat_id)
    if idx < 0 or idx >= len(ft["tasks"]):
        return await message.reply_text("Invalid task number.")
    removed = ft["tasks"].pop(idx)
    set_force_tasks(chat_id, ft)
    await message.reply_text(f"Task `{removed['label']}` removed.")

@bot.on_message(filters.command("tasklist") & filters.group)
async def cmd_tasklist(client, message):
    chat_id = message.chat.id
    ft = get_force_tasks(chat_id)
    if not ft["tasks"]: return await message.reply_text("No tasks configured. Use `/addtask` to add.")
    lines = [f"{i+1}. [{t['label']}]({t['url']})" for i,t in enumerate(ft["tasks"])]
    status = "ON" if ft["enabled"] else "OFF"
    await message.reply_text(
        f"**Force Tasks [{status}]:**\n\n" + "\n".join(lines) +
        (f"\n\n**Reward:** {ft['reward_text']}" if ft.get("reward_text") else "")
    )

@bot.on_message(filters.command("setreward") & filters.group)
async def cmd_setreward(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2: return await message.reply_text("Usage: `/setreward <message>`")
    ft = get_force_tasks(chat_id)
    ft["reward_text"] = parts[1].strip()
    set_force_tasks(chat_id, ft)
    await message.reply_text("Reward message set!")

@bot.on_message(filters.command("forcetask") & filters.group)
async def cmd_forcetask(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].lower() not in {"on","off"}:
        ft = get_force_tasks(chat_id)
        return await message.reply_text(f"Force task is `{'on' if ft['enabled'] else 'off'}`.\nUsage: `/forcetask on|off`")
    ft = get_force_tasks(chat_id)
    if not ft["tasks"] and parts[1].lower() == "on":
        return await message.reply_text("Add tasks first using `/addtask`!")
    ft["enabled"] = 1 if parts[1].lower() == "on" else 0
    set_force_tasks(chat_id, ft)
    await message.reply_text(f"Force task {'enabled' if ft['enabled'] else 'disabled'}!")

@bot.on_message(filters.command("resetuser") & filters.group)
async def cmd_resetuser(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    target = await get_target(client, message)
    if not target: return await message.reply_text("Reply to a user or provide username.")
    reset_force_task(target.id, chat_id)
    await message.reply_text(f"Force task reset for {target.mention}. They will need to complete tasks again.")

# =========================================================
# COMMANDS - GROUPS: SETTINGS & MISC
# =========================================================
@bot.on_message(filters.command("settings") & filters.group)
async def cmd_settings_group(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    ensure_group(chat_id)
    await message.reply_text("**Group Settings:**", reply_markup=group_settings_menu(chat_id))

@bot.on_message(filters.command("lang"))
async def cmd_lang(client, message):
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or parts[1].lower() not in {"en","bn"}:
        return await message.reply_text("Usage: `/lang en` or `/lang bn`")
    lang = parts[1].lower()
    if message.chat.type == ChatType.GROUP:
        if not await is_admin(client, message.chat.id, message.from_user.id):
            return await message.reply_text("You need admin rights.")
        with closing(db()) as c:
            ensure_group(message.chat.id)
            c.execute("UPDATE group_settings SET language=? WHERE chat_id=?", (lang, message.chat.id))
            c.commit()
    else:
        set_lang(message.from_user.id, lang)
    await message.reply_text(f"Language set to `{lang}`.")

@bot.on_message(filters.command("report") & filters.group)
async def cmd_report(client, message):
    chat_id = message.chat.id
    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to report it.")
    reporter = message.from_user.mention
    reported = message.reply_to_message.from_user.mention if message.reply_to_message.from_user else "Unknown"
    async for member in client.get_chat_members(chat_id, filter="administrators"):
        if not member.user.is_bot:
            try:
                await client.send_message(member.user.id,
                    f"**Report in {message.chat.title}**\nFrom: {reporter}\nAbout: {reported}")
            except Exception: pass
    await message.reply_text("Report sent to admins!")

@bot.on_message(filters.command("setlogchannel") & filters.group)
async def cmd_setlogchannel(client, message):
    chat_id = message.chat.id
    if not await is_admin(client, chat_id, message.from_user.id):
        return await message.reply_text("You need admin rights.")
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        return await message.reply_text("Usage: `/setlogchannel <channel_id>`")
    try:
        log_id = int(parts[1].strip())
        set_gsetting(chat_id, "log_channel", log_id)
        await message.reply_text(f"Log channel set to `{log_id}`.")
    except ValueError:
        await message.reply_text("Invalid channel ID.")

# =========================================================
# MEMBER JOIN/LEAVE HANDLER
# =========================================================
@bot.on_chat_member_updated()
async def on_member_update(client, update: ChatMemberUpdated):
    chat_id = update.chat.id
    if not update.new_chat_member: return
    new_s = update.new_chat_member.status
    old_s = update.old_chat_member.status if update.old_chat_member else None
    user = update.new_chat_member.user

    if new_s == ChatMemberStatus.MEMBER and old_s not in (
        ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER
    ):
        upsert_user(user.id, user.username, user.first_name)

        # CAS ban check
        if await check_cas(user.id):
            try:
                await client.ban_chat_member(chat_id, user.id)
                m = await client.send_message(chat_id,
                    f"**Auto-banned:** {user.mention} is in the global spam database (CAS).")
                asyncio.create_task(auto_del(client, chat_id, m.id, 30))
                await send_log(client, f"CAS-BAN: {user.first_name} (`{user.id}`) in {update.chat.title}")
            except Exception: pass
            return

        # Anti-raid check
        if await check_raid(client, chat_id):
            m = await client.send_message(chat_id, "Raid detected! Slow mode enabled for 30 seconds.")
            asyncio.create_task(auto_del(client, chat_id, m.id, 35))

        # Captcha
        if gsetting(chat_id, "captcha_enabled"):
            try:
                await client.restrict_chat_member(chat_id, user.id, ChatPermissions(can_send_messages=False))
            except Exception: pass
            btn = InlineKeyboardMarkup([[
                InlineKeyboardButton("I am not a robot - Click to verify", callback_data=f"cap_{user.id}_{chat_id}")
            ]])
            sent = await client.send_message(
                chat_id,
                f"**Welcome, {user.mention}!**\n\nPlease verify you are human within 60 seconds.",
                reply_markup=btn
            )
            set_captcha_pending(user.id, chat_id, sent.id)
            asyncio.create_task(captcha_timeout(client, chat_id, user.id, sent.id))
            return

        # Welcome message
        if gsetting(chat_id, "welcome_enabled"):
            welcome_text = gsetting(chat_id, "welcome_text")
            try:
                count = (await client.get_chat(chat_id)).members_count
            except Exception:
                count = "?"
            mention = f"[{user.first_name}](tg://user?id={user.id})"
            fmt_vars = {
                "mention": mention, "first": user.first_name or "",
                "last": user.last_name or "", "title": update.chat.title or "",
                "count": count, "id": user.id,
                "username": f"@{user.username}" if user.username else user.first_name
            }
            if not welcome_text:
                welcome_text = f"Welcome {mention} to {update.chat.title}! Member #{count}"
            else:
                try: welcome_text = welcome_text.format(**fmt_vars)
                except Exception: pass
            try:
                await client.send_message(chat_id, welcome_text)
            except Exception: pass

    elif new_s == ChatMemberStatus.LEFT and old_s == ChatMemberStatus.MEMBER:
        if not gsetting(chat_id, "goodbye_enabled"): return
        goodbye_text = gsetting(chat_id, "goodbye_text")
        mention = f"[{user.first_name}](tg://user?id={user.id})"
        if not goodbye_text:
            goodbye_text = f"{mention} has left the group."
        else:
            try: goodbye_text = goodbye_text.format(mention=mention, first=user.first_name or "")
            except Exception: pass
        try: await client.send_message(chat_id, goodbye_text)
        except Exception: pass

async def captcha_timeout(client, chat_id, user_id, msg_id):
    await asyncio.sleep(60)
    if get_captcha_pending(user_id, chat_id) is not None:
        del_captcha_pending(user_id, chat_id)
        try:
            await client.ban_chat_member(chat_id, user_id)
            await asyncio.sleep(0.5)
            await client.unban_chat_member(chat_id, user_id)
            await client.delete_messages(chat_id, msg_id)
            m = await client.send_message(chat_id, "User failed captcha verification and was kicked.")
            asyncio.create_task(auto_del(client, chat_id, m.id, 15))
        except Exception: pass

# =========================================================
# GROUP MESSAGE HANDLER (filters, locks, flood, notes)
# =========================================================
@bot.on_message(filters.group & filters.text & ~filters.command([
    "ban","unban","kick","mute","unmute","promote","demote","pin","unpin","purge","del",
    "warn","dwarn","unwarn","warnlist","clearwarns","setwarnlimit","warnmode","adminlist",
    "setwelcome","resetwelcome","welcome","setgoodbye","resetgoodbye","save","note","notes",
    "delnote","filter","filters","stop","stopall","lock","unlock","locks","rules","setrules",
    "antiflood","captcha","addtask","deltask","tasklist","setreward","forcetask","resetuser",
    "settings","lang","report","setlogchannel","id","info","help","start"
]))
async def on_group_text(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id: return
    is_adm = await is_admin(client, chat_id, user_id)

    if not is_adm:
        # Force task check
        ft = get_force_tasks(chat_id)
        if ft["enabled"] and not has_done_force_task(user_id, chat_id):
            try: await message.delete()
            except Exception: pass
            await check_force_task(client, user_id, chat_id, message)
            return

        if await check_flood(client, message): return
        if await check_dup_spam(client, message): return

        # Scam detection
        text_lower = (message.text or "").lower()
        for kw in SCAM_KEYWORDS:
            if kw in text_lower:
                try:
                    await message.delete()
                    add_warn(user_id, chat_id, f"Auto: scam keyword", message.from_user.id)
                    warns = get_warns(user_id, chat_id)
                    max_w = gsetting(chat_id, "max_warnings") or 3
                    m = await client.send_message(chat_id,
                        f"Scam/spam message removed from {message.from_user.mention}. Warning `{len(warns)}/{max_w}`.")
                    asyncio.create_task(auto_del(client, chat_id, m.id, 20))
                    if len(warns) >= max_w:
                        clear_warns(user_id, chat_id)
                        await client.ban_chat_member(chat_id, user_id)
                except Exception: pass
                return

        # Arabic lock
        if gsetting(chat_id, "arabic_lock") and ARABIC_RE.search(message.text or ""):
            try: await message.delete()
            except Exception: pass
            return

        # Link lock
        if gsetting(chat_id, "link_lock"):
            if re.search(r"https?://|t\.me/|www\.", message.text or ""):
                try:
                    await message.delete()
                    m = await client.send_message(chat_id, f"{message.from_user.mention} - Links are not allowed here!")
                    asyncio.create_task(auto_del(client, chat_id, m.id, 8))
                except Exception: pass
                return

    # Active filters
    text_lower = (message.text or "").lower()
    for kw, resp in get_filters(chat_id).items():
        if kw in text_lower:
            await message.reply_text(resp)
            return

    # #note shortcut
    if message.text and message.text.startswith("#"):
        note_name = message.text[1:].split()[0].lower() if " " in message.text else message.text[1:].lower()
        if note_name:
            content = get_note(chat_id, note_name)
            if content:
                await message.reply_text(f"**#{note_name}**\n\n{content}")

@bot.on_message(filters.group & filters.media)
async def on_group_media(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id if message.from_user else None
    if not user_id or await is_admin(client, chat_id, user_id): return

    if await check_flood(client, message): return

    if gsetting(chat_id, "media_lock") and (message.photo or message.video or message.document or message.audio):
        try: await message.delete()
        except Exception: pass
        return
    if gsetting(chat_id, "sticker_lock") and message.sticker:
        try: await message.delete()
        except Exception: pass
        return
    if gsetting(chat_id, "forward_lock") and message.forward_from:
        try: await message.delete()
        except Exception: pass
        return

# =========================================================
# CALLBACK QUERY HANDLER
# =========================================================
@bot.on_callback_query()
async def on_callback(client, cq: CallbackQuery):
    user_id = cq.from_user.id
    data = cq.data

    if data == "close":
        try: await cq.message.delete()
        except Exception: pass
        return

    # Help navigation
    if data == "help_main":
        await cq.message.edit_text(HELP["main"], reply_markup=help_menu())
        return await cq.answer()
    for key in HELP:
        if data == f"help_{key}":
            await cq.message.edit_text(HELP[key], reply_markup=back_btn())
            return await cq.answer()

    # Language selection (private)
    if data in ("sl_en","sl_bn"):
        lang = data.split("_")[1]
        set_lang(user_id, lang)
        await cq.answer(f"Language set to {lang}!")
        await cq.message.edit_reply_markup(InlineKeyboardMarkup([
            [InlineKeyboardButton("English", callback_data="sl_en"),
             InlineKeyboardButton("Bangla", callback_data="sl_bn")]
        ]))
        return

    # Captcha verification
    if data.startswith("cap_"):
        _, target_uid, chat_id_str = data.split("_", 2)
        target_uid = int(target_uid); chat_id = int(chat_id_str)
        if user_id != target_uid:
            return await cq.answer("This button is not for you!", show_alert=True)
        pending = get_captcha_pending(target_uid, chat_id)
        if pending is None:
            return await cq.answer("Already verified or expired.", show_alert=True)
        del_captcha_pending(target_uid, chat_id)
        try:
            await client.restrict_chat_member(chat_id, target_uid, ChatPermissions(
                can_send_messages=True, can_send_media_messages=True,
                can_send_other_messages=True, can_add_web_page_previews=True
            ))
        except Exception: pass
        await cq.message.edit_text(f"Verified! Welcome to the group, {cq.from_user.mention}!")
        return await cq.answer("Verified!")

    # Force task verify
    if data.startswith("ft_verify_"):
        chat_id = int(data.split("_", 2)[2])
        ft = get_force_tasks(chat_id)
        if not ft["tasks"]:
            mark_force_task_done(user_id, chat_id)
            return await cq.answer("All done!")
        # We trust the user clicked all links (honor system + admin can reset)
        mark_force_task_done(user_id, chat_id)
        reward = ft.get("reward_text","") or "You have completed all tasks! You can now use the bot in the group."
        await cq.message.edit_text(f"**Tasks Completed!**\n\n{reward}")
        return await cq.answer("Verified!")

    # Remove warn button
    if data.startswith("rmwarn_"):
        parts = data.split("_")
        target_uid = int(parts[1]); chat_id = int(parts[2])
        if not await is_admin(client, chat_id, user_id):
            return await cq.answer("Admins only!", show_alert=True)
        del_last_warn(target_uid, chat_id)
        await cq.answer("Warning removed!")
        try: await cq.message.edit_reply_markup(None)
        except Exception: pass
        return

    # Group settings panel
    if data.startswith("gs_"):
        parts = data.split("_")
        action = parts[1]
        chat_id_str = parts[-1]
        if not chat_id_str.lstrip("-").isdigit(): return await cq.answer()
        chat_id = int(chat_id_str)
        if not await is_admin(client, chat_id, user_id):
            return await cq.answer("Admins only!", show_alert=True)
        toggle_map = {
            "captcha": "captcha_enabled",
            "media": "media_lock",
            "sticker": "sticker_lock",
            "link": "link_lock",
            "fwd": "forward_lock",
            "arabic": "arabic_lock",
            "welcome": "welcome_enabled",
            "bye": "goodbye_enabled",
        }
        if action == "flood":
            cur = gsetting(chat_id, "antiflood_count")
            if cur:
                set_gsetting(chat_id, "antiflood_count", 0)
            else:
                set_gsetting(chat_id, "antiflood_count", 5)
                set_gsetting(chat_id, "antiflood_time", 10)
                set_gsetting(chat_id, "antiflood_action", "mute")
        elif action == "ft":
            ft = get_force_tasks(chat_id)
            ft["enabled"] = 0 if ft["enabled"] else 1
            set_force_tasks(chat_id, ft)
        elif action == "lang":
            lang = parts[2]
            with closing(db()) as c:
                ensure_group(chat_id)
                c.execute("UPDATE group_settings SET language=? WHERE chat_id=?", (lang, chat_id))
                c.commit()
        elif action in toggle_map:
            col = toggle_map[action]
            cur = gsetting(chat_id, col)
            set_gsetting(chat_id, col, 0 if cur else 1)
        try:
            await cq.message.edit_reply_markup(group_settings_menu(chat_id))
        except Exception: pass
        return await cq.answer("Updated!")

    # Admin panel
    if not is_bot_admin(user_id):
        return await cq.answer("Admin only!", show_alert=True)

    if data == "adm_stats":
        await cq.message.edit_text(
            f"**Stats**\nUsers: {total_users()}\nDone: {state['success_tasks']}\n"
            f"Failed: {state['failed_tasks']}\nQueue: {task_queue.qsize()}",
            reply_markup=admin_panel()
        )
    elif data == "adm_maint":
        state["maintenance"] = not state["maintenance"]
        await cq.message.edit_text(f"Maintenance: `{state['maintenance']}`", reply_markup=admin_panel())
    elif data == "adm_clrq":
        for tid, meta in list(task_reg.items()):
            if meta["status"] == "queued":
                meta["cancelled"] = True; meta["status"] = "cancelled"
                upd_task(tid, "cancelled", "Cleared")
        await cq.message.edit_text("Queue cleared.", reply_markup=admin_panel())
    elif data == "adm_logs":
        logs = "\n".join(recent_logs[-15:]) or "No logs."
        await cq.message.edit_text(f"```\n{logs[:3500]}\n```", reply_markup=admin_panel())
    elif data == "adm_users":
        users = latest_users(5)
        lines = [f"[{r[2] or 'User'}](tg://user?id={r[0]}) - `{r[0]}`" for r in users]
        await cq.message.edit_text("**Recent Users:**\n\n" + "\n".join(lines), reply_markup=admin_panel())
    elif data == "adm_queue":
        active = runtime["active_task_id"]
        lines = [f"Running: `{active}`"] if active else []
        c = 0
        for tid, meta in sorted(task_reg.items(), key=lambda x: x[1]["created_at"]):
            if meta["status"] == "queued" and not meta["cancelled"]:
                c += 1
                lines.append(f"{c}. `{tid}`")
                if c >= 8: break
        await cq.message.edit_text("**Queue:**\n\n" + ("\n".join(lines) or "Empty."), reply_markup=admin_panel())
    await cq.answer()

# =========================================================
# TIMED MUTE WORKER
# =========================================================
async def timed_mute_worker():
    while True:
        try:
            for user_id, chat_id in get_expired_mutes():
                try:
                    await bot.restrict_chat_member(chat_id, user_id, ChatPermissions(
                        can_send_messages=True, can_send_media_messages=True,
                        can_send_other_messages=True, can_add_web_page_previews=True
                    ))
                except Exception: pass
        except Exception: pass
        await asyncio.sleep(30)

# =========================================================
# DOWNLOADER TASK PROCESSING
# =========================================================
async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        pct = current * 100 / total if total else 0
        done = math.floor(pct / 5)
        bar = "[" + "#"*done + "."*(20-done) + "]"
        speed = current / diff if diff > 0 else 0
        try:
            await message.edit_text(
                f"**{ud_type}**\n\n"
                f"Progress: `{round(pct,2)}%`\n"
                f"`{bar}`\n\n"
                f"Size: `{humanbytes(current)} / {humanbytes(total)}`\n"
                f"Speed: `{humanbytes(speed)}/s`"
            )
        except Exception: pass

async def process_task(client, message, text_input, status_msg, task_id):
    upd_task(task_id, "running")
    set_task_status(task_id, "running")
    runtime["active_task_id"] = task_id
    runtime["active_user_id"] = message.from_user.id

    try:
        if task_reg.get(task_id, {}).get("cancelled"):
            upd_task(task_id, "cancelled")
            set_task_status(task_id, "cancelled")
            await status_msg.edit_text("Task cancelled.")
            return

        await status_msg.edit_text("Validating link...")

        if not valid_tg_link(text_input):
            await status_msg.edit_text("Invalid link. Please send a valid Telegram post link.")
            upd_task(task_id, "failed", "Invalid link")
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
            if chat_id.lstrip("-").isdigit(): chat_id = int(chat_id)
            msg_id = int(parts[-1].split("?")[0])

        if not userbot.is_connected: await userbot.start()
        target_msg = await userbot.get_messages(chat_id, msg_id)

        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CFG.custom_caption}" if CFG.custom_caption else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
        elif target_msg.media:
            media = target_msg.document or target_msg.video or target_msg.audio or target_msg.voice or target_msg.photo
            file_size = getattr(media, "file_size", 0)
            if file_size and file_size > CFG.max_file_size:
                await status_msg.edit_text(f"File too large! Size: `{humanbytes(file_size)}` | Limit: `{humanbytes(CFG.max_file_size)}`")
                upd_task(task_id, "failed", "Too large")
                set_task_status(task_id, "failed")
                state["failed_tasks"] += 1
                return

            start_time = time.time()
            file_path = await userbot.download_media(target_msg, progress=progress_bar,
                progress_args=("Downloading...", status_msg, start_time))

            caption = ""
            if target_msg.caption and CFG.custom_caption:
                caption = f"{target_msg.caption}\n\n{CFG.custom_caption}"
            elif target_msg.caption: caption = target_msg.caption
            elif CFG.custom_caption: caption = CFG.custom_caption

            await status_msg.edit_text("Uploading...")
            start_time = time.time()
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=caption,
                        progress=progress_bar, progress_args=("Uploading video...", status_msg, start_time))
                elif target_msg.audio:
                    await client.send_audio(message.chat.id, audio=file_path, caption=caption)
                elif target_msg.voice:
                    await client.send_voice(message.chat.id, voice=file_path, caption=caption)
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=caption,
                        progress=progress_bar, progress_args=("Uploading...", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path): os.remove(file_path)
            await status_msg.delete()
            await message.reply_text("Done! File delivered.")
        else:
            await status_msg.edit_text("No extractable content found.")
            upd_task(task_id, "failed", "No content")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            return

        upd_task(task_id, "done")
        set_task_status(task_id, "done")
        state["success_tasks"] += 1

    except FloodWait as e:
        await asyncio.sleep(e.value)
        upd_task(task_id, "failed", f"FloodWait {e.value}")
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try: await status_msg.edit_text(f"Rate limited. Waited {e.value}s.")
        except Exception: pass
    except Exception as e:
        logger.exception("Task error")
        upd_task(task_id, "failed", str(e))
        set_task_status(task_id, "failed")
        state["failed_tasks"] += 1
        try: await status_msg.edit_text("An error occurred.")
        except Exception: pass
    finally:
        runtime["active_task_id"] = None
        runtime["active_user_id"] = None

async def queue_worker():
    while True:
        client, message, text_input, status_msg, task_id = await task_queue.get()
        user_id = message.from_user.id
        try:
            if task_reg.get(task_id, {}).get("cancelled"):
                upd_task(task_id, "cancelled")
                set_task_status(task_id, "cancelled")
                try: await status_msg.edit_text("Task cancelled.")
                except Exception: pass
                continue
            await asyncio.wait_for(
                process_task(client, message, text_input, status_msg, task_id),
                timeout=CFG.task_timeout_sec
            )
        except asyncio.TimeoutError:
            upd_task(task_id, "failed", "timeout")
            set_task_status(task_id, "failed")
            state["failed_tasks"] += 1
            try: await status_msg.edit_text("Task timed out.")
            except Exception: pass
        finally:
            user_pending[user_id] = max(user_pending.get(user_id, 1) - 1, 0)
            task_queue.task_done()

# =========================================================
# PRIVATE TEXT HANDLER (DOWNLOADER)
# =========================================================
@bot.on_message(filters.private & filters.text & ~filters.command([
    "start","help","settings","lang","admin","stats","broadcast","gban","ungban"
]))
async def on_private_text(client, message):
    user_id = message.from_user.id
    text = message.text.strip()
    upsert_user(user_id, message.from_user.username, message.from_user.first_name)

    if is_gbanned(user_id): return await message.reply_text("You are globally banned.")
    if state["maintenance"] and not is_bot_admin(user_id): return await message.reply_text("Under maintenance.")
    if not await check_fsub(client, message):
        return await message.reply_text("Please join our channel first!",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Join Channel", url=f"https://t.me/{CFG.force_sub_channel.lstrip('@')}")
            ]]))

    if not valid_tg_link(text):
        return await message.reply_text(
            "Send me a Telegram post link to download it.\n\n"
            "Example: `https://t.me/c/12345/678`\n\n"
            "Use /help for all commands."
        )

    on_cd, remain = user_on_cooldown(user_id)
    if on_cd and not is_bot_admin(user_id):
        return await message.reply_text(f"Cooldown: wait `{remain}` seconds.")
    if task_queue.full() and not is_bot_admin(user_id):
        return await message.reply_text("Server busy. Try again later.")

    pending = user_pending.get(user_id, 0)
    if pending >= CFG.max_pending_per_user and not is_bot_admin(user_id):
        return await message.reply_text(f"You have `{pending}` pending tasks. Wait.")

    task_id = make_task_id(user_id)
    add_task_rec(task_id, user_id, message.from_user.username or "", text)
    reg_task(task_id, user_id, text)

    user_pending[user_id] = pending + 1
    user_last_req[user_id] = time.time()
    state["total_tasks"] += 1

    pos = task_queue.qsize() + 1
    status_msg = await message.reply_text(
        f"**Task Queued**\n\nID: `{task_id}`\nPosition: `{pos}`\nPlease wait..."
    )
    await task_queue.put((client, message, text, status_msg, task_id))

# =========================================================
# STARTUP
# =========================================================
async def startup_report():
    if not CFG.owner_id: return
    try:
        await bot.send_message(CFG.owner_id,
            f"**Bot Started!**\n\nTime: `{time.strftime('%Y-%m-%d %H:%M:%S')}`\nPort: `{CFG.port}`")
    except Exception: pass

async def main():
    global bot_username
    cleanup_storage()
    init_db()

    await bot.start()
    me = await bot.get_me()
    bot_username = me.username or "bot"
    logger.info(f"Bot started as @{bot_username}")

    await userbot.start()
    logger.info("Userbot started")

    asyncio.create_task(queue_worker())
    asyncio.create_task(timed_mute_worker())
    asyncio.create_task(startup_report())
    await idle()

def cleanup_storage():
    if os.path.exists(CFG.download_dir):
        shutil.rmtree(CFG.download_dir, ignore_errors=True)
    os.makedirs(CFG.download_dir, exist_ok=True)

if __name__ == "__main__":
    # Flask must start first so Render sees the port
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    logger.info(f"Flask started on port {CFG.port}")
    time.sleep(2)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Stopped.")
    except Exception as e:
        logger.critical(f"Fatal: {e}")