import asyncio
import logging
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

import requests
from flask import Flask, jsonify, request

import pyrogram.errors as pyro_errors

if not hasattr(pyro_errors, "GroupcallForbidden"):
    pyro_errors.GroupcallForbidden = pyro_errors.Forbidden

from pyrogram import Client
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("WebhookLibraryMusicBot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"].strip()
SESSION_STRING = (os.environ.get("SESSION_STRING") or os.environ.get("STRING_SESSION") or "").strip()
WEBHOOK_URL = os.environ["WEBHOOK_URL"].strip().rstrip("/")
PORT = int(os.environ.get("PORT", 8080))

if not SESSION_STRING:
    raise RuntimeError("Missing SESSION_STRING or STRING_SESSION")

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
WEBHOOK_PATH = "/telegram/webhook"
FULL_WEBHOOK_URL = f"{WEBHOOK_URL}{WEBHOOK_PATH}"

DB_PATH = os.environ.get("DB_PATH", "music_library.db")
TMP_DIR = Path(os.environ.get("TMP_DIR", "/tmp/music_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)

# -----------------------------
# Telegram Bot API helpers
# -----------------------------
def tg_get(method: str, params: dict[str, Any] | None = None) -> requests.Response:
    url = f"{API_BASE}/{method}"
    r = requests.get(url, params=params or {}, timeout=30)
    logger.info("Telegram GET %s -> %s", method, r.text[:700])
    return r


def tg_post(method: str, payload: dict[str, Any] | None = None) -> requests.Response:
    url = f"{API_BASE}/{method}"
    r = requests.post(url, json=payload or {}, timeout=30)
    logger.info("Telegram POST %s -> %s", method, r.text[:700])
    return r


def send_message(chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> Optional[int]:
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    try:
        r = tg_post("sendMessage", payload)
        data = r.json()
        if data.get("ok") and data.get("result"):
            return data["result"]["message_id"]
    except Exception:
        logger.exception("send_message failed")
    return None


def edit_message(chat_id: int, message_id: int, text: str) -> None:
    try:
        tg_post(
            "editMessageText",
            {"chat_id": chat_id, "message_id": message_id, "text": text},
        )
    except Exception:
        logger.exception("edit_message failed")


def get_chat_member_status(chat_id: int, user_id: int) -> Optional[str]:
    try:
        r = tg_get("getChatMember", {"chat_id": chat_id, "user_id": user_id})
        data = r.json()
        if data.get("ok") and data.get("result"):
            return data["result"].get("status")
    except Exception:
        logger.exception("get_chat_member_status failed")
    return None


def is_group_admin(chat_id: int, user_id: int) -> bool:
    status = get_chat_member_status(chat_id, user_id)
    return status in {"creator", "administrator"}


def set_my_commands() -> None:
    try:
        tg_post(
            "setMyCommands",
            {
                "commands": [
                    {"command": "start", "description": "Start the bot"},
                    {"command": "ping", "description": "Health check"},
                    {"command": "addsong", "description": "Reply to media and save it"},
                    {"command": "listsongs", "description": "Show saved songs"},
                    {"command": "searchsong", "description": "Search saved songs"},
                    {"command": "delsong", "description": "Delete a saved song"},
                    {"command": "play", "description": "Play a saved song"},
                    {"command": "stop", "description": "Stop current stream"},
                    {"command": "nowplaying", "description": "Show current song"},
                ]
            },
        )
    except Exception:
        logger.exception("setMyCommands failed")


def delete_webhook() -> None:
    try:
        tg_post("deleteWebhook", {"drop_pending_updates": True})
    except Exception:
        logger.exception("deleteWebhook failed")


def set_webhook_once() -> bool:
    try:
        delete_webhook()
        time.sleep(1)
        resp = tg_post(
            "setWebhook",
            {
                "url": FULL_WEBHOOK_URL,
                "allowed_updates": ["message", "edited_message"],
            },
        )
        ok = resp.ok and '"ok":true' in resp.text.replace(" ", "").lower()
        info = tg_get("getWebhookInfo")
        logger.info("Webhook info after set: %s", info.text[:1000])
        if ok:
            set_my_commands()
            return True
        return False
    except Exception:
        logger.exception("set_webhook_once failed")
        return False


def setup_webhook_with_retry() -> None:
    logger.info("Starting webhook setup. Target: %s", FULL_WEBHOOK_URL)
    for attempt in range(1, 13):
        logger.info("Webhook setup attempt %s/12", attempt)
        if set_webhook_once():
            logger.info("Webhook setup succeeded")
            return
        time.sleep(5)
    logger.error("Webhook setup failed after all retries")


def get_file_path(file_id: str) -> str:
    r = tg_get("getFile", {"file_id": file_id})
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"getFile failed: {data}")
    return data["result"]["file_path"]


def download_bot_file(file_id: str, destination: str) -> None:
    file_path = get_file_path(file_id)
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    logger.info("Downloading bot file: %s", url)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(destination, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

# -----------------------------
# DB
# -----------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db_connect() as conn:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                file_id TEXT NOT NULL,
                original_name TEXT,
                mime_type TEXT,
                added_by INTEGER,
                created_at INTEGER NOT NULL
            )
            '''
        )
        conn.commit()


def normalize_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"\s+", " ", name)
    return name


def add_song_to_db(
    name: str,
    file_id: str,
    original_name: Optional[str],
    mime_type: Optional[str],
    added_by: Optional[int],
) -> None:
    song_name = normalize_name(name)
    with db_connect() as conn:
        conn.execute(
            '''
            INSERT OR REPLACE INTO songs (name, file_id, original_name, mime_type, added_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (song_name, file_id, original_name, mime_type, added_by, int(time.time())),
        )
        conn.commit()


def get_song(name: str) -> Optional[sqlite3.Row]:
    song_name = normalize_name(name)
    with db_connect() as conn:
        return conn.execute("SELECT * FROM songs WHERE name = ?", (song_name,)).fetchone()


def search_songs(keyword: str, limit: int = 10):
    q = f"%{normalize_name(keyword)}%"
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM songs WHERE name LIKE ? ORDER BY name ASC LIMIT ?",
            (q, limit),
        ).fetchall()


def list_songs(limit: int = 50):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM songs ORDER BY name ASC LIMIT ?",
            (limit,),
        ).fetchall()


def delete_song(name: str) -> bool:
    song_name = normalize_name(name)
    with db_connect() as conn:
        cur = conn.execute("DELETE FROM songs WHERE name = ?", (song_name,))
        conn.commit()
        return cur.rowcount > 0

# -----------------------------
# Helpers
# -----------------------------
def split_long_text(text: str, limit: int = 3500) -> list[str]:
    parts = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        parts.append(text[:cut])
        text = text[cut:].lstrip()
    if text:
        parts.append(text)
    return parts


def parse_command(text: str) -> tuple[Optional[str], str]:
    if not text or not text.startswith("/"):
        return None, ""
    parts = text.split(maxsplit=1)
    cmd = parts[0][1:].split("@")[0].lower()
    rest = parts[1].strip() if len(parts) > 1 else ""
    return cmd, rest


def safe_file_ext(original_name: Optional[str], mime_type: Optional[str]) -> str:
    if original_name and "." in original_name:
        ext = Path(original_name).suffix.lower()
        if ext:
            return ext
    if mime_type:
        if "mpeg" in mime_type or "mp3" in mime_type:
            return ".mp3"
        if "ogg" in mime_type:
            return ".ogg"
        if "aac" in mime_type:
            return ".aac"
        if "wav" in mime_type:
            return ".wav"
        if "m4a" in mime_type or "mp4" in mime_type:
            return ".m4a"
        if "flac" in mime_type:
            return ".flac"
        if "video/" in mime_type:
            return ".mp4"
    return ".mp3"


def extract_replied_media(reply_msg: dict) -> tuple[Optional[dict], Optional[str]]:
    if not reply_msg:
        return None, None
    if reply_msg.get("audio"):
        return reply_msg["audio"], "audio"
    if reply_msg.get("voice"):
        return reply_msg["voice"], "voice"
    if reply_msg.get("video"):
        return reply_msg["video"], "video"
    doc = reply_msg.get("document")
    if doc and doc.get("mime_type"):
        mt = doc["mime_type"]
        if mt.startswith("audio/") or mt.startswith("video/"):
            return doc, "document"
    return None, None

# -----------------------------
# Voice / PyTgCalls
# -----------------------------
user = None
call_py = None
VOICE_LOOP = asyncio.new_event_loop()
VOICE_READY = threading.Event()
ACTIVE_STREAMS: dict[int, dict[str, Any]] = {}


async def cleanup_chat_file(chat_id: int) -> None:
    info = ACTIVE_STREAMS.get(chat_id)
    if not info:
        return
    path = info.get("local_path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
            logger.info("Removed temp file: %s", path)
        except Exception:
            logger.exception("Failed to remove temp file: %s", path)


async def resolve_voice_chat_id(chat_id: int) -> int:
    global user
    if user is None:
        raise RuntimeError("Voice session is not ready")
    chat_id = int(chat_id)
    try:
        resolved_chat = await user.get_chat(chat_id)
        logger.info("Resolved voice peer directly: raw=%s resolved=%s", chat_id, resolved_chat.id)
        return resolved_chat.id
    except Exception:
        logger.exception("Direct get_chat failed, trying dialogs scan")
    async for dialog in user.get_dialogs(limit=500):
        try:
            if int(dialog.chat.id) == chat_id:
                logger.info("Resolved voice peer from dialogs: raw=%s resolved=%s", chat_id, dialog.chat.id)
                return int(dialog.chat.id)
        except Exception:
            continue
    raise RuntimeError(f"Could not resolve peer for chat id {chat_id}")


async def play_saved_song(chat_id: int, song_name: str, status_chat_id: int, status_message_id: int) -> None:
    global user, call_py
    if user is None or call_py is None:
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, "Voice engine is not ready.")
        return
    try:
        chat_id = await resolve_voice_chat_id(chat_id)
    except Exception as e:
        logger.exception("Failed to resolve peer")
        await asyncio.to_thread(
            edit_message,
            status_chat_id,
            status_message_id,
            f"Play failed:\nCould not resolve this group for the session account.\n{e}",
        )
        return
    row = get_song(song_name)
    if not row:
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, "Song not found in library.")
        return

    ext = safe_file_ext(row["original_name"], row["mime_type"])
    local_path = TMP_DIR / f"{abs(chat_id)}_{int(time.time())}{ext}"
    try:
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, f"Preparing: {row['name']}")
        await asyncio.to_thread(download_bot_file, row["file_id"], str(local_path))
        try:
            await call_py.leave_call(chat_id)
        except Exception:
            pass
        await cleanup_chat_file(chat_id)
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, "Starting voice chat stream...")
        await call_py.play(chat_id, MediaStream(str(local_path)))
        ACTIVE_STREAMS[chat_id] = {
            "name": row["name"],
            "local_path": str(local_path),
        }
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, f"Now playing: {row['name']}")
    except Exception as e:
        logger.exception("play_saved_song failed")
        try:
            if local_path.exists():
                local_path.unlink(missing_ok=True)
        except Exception:
            pass
        await asyncio.to_thread(edit_message, status_chat_id, status_message_id, f"Play failed:\n{e}")


async def stop_current_stream(chat_id: int) -> None:
    global user, call_py
    if user is None or call_py is None:
        return
    try:
        chat_id = await resolve_voice_chat_id(chat_id)
    except Exception:
        logger.exception("Failed to resolve peer for stop")
    try:
        await call_py.leave_call(chat_id)
    except Exception:
        logger.exception("leave_call failed")
    await cleanup_chat_file(chat_id)
    ACTIVE_STREAMS.pop(chat_id, None)


async def voice_boot() -> None:
    global user, call_py
    try:
        user = Client(
            "voice-user",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING,
        )
        call_py = PyTgCalls(user)

        await user.start()
        logger.info("User client started")

        loaded = 0
        async for _ in user.get_dialogs(limit=300):
            loaded += 1
        logger.info("Preloaded dialogs: %s", loaded)

        await call_py.start()
        logger.info("PyTgCalls started")

        me = await user.get_me()
        logger.info("User logged in as: %s", me.first_name)

        VOICE_READY.set()
    except Exception:
        logger.exception("voice_boot failed")


def run_voice_loop() -> None:
    asyncio.set_event_loop(VOICE_LOOP)
    VOICE_LOOP.create_task(voice_boot())
    VOICE_LOOP.run_forever()


def schedule_coro(coro: Any):
    return asyncio.run_coroutine_threadsafe(coro, VOICE_LOOP)

# -----------------------------
# Flask routes
# -----------------------------
@app.get("/")
def home():
    return "Webhook Library Music Bot is running"


@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "mode": "webhook-library",
            "webhook_url": FULL_WEBHOOK_URL,
            "voice_ready": VOICE_READY.is_set(),
        }
    )


@app.get("/setup-webhook")
def manual_setup_webhook():
    ok = set_webhook_once()
    return jsonify({"ok": ok, "webhook_url": FULL_WEBHOOK_URL})


@app.get("/webhook-info")
def webhook_info():
    try:
        r = tg_get("getWebhookInfo")
        return app.response_class(response=r.text, status=r.status_code, mimetype="application/json")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.post(WEBHOOK_PATH)
def telegram_webhook():
    data = request.get_json(silent=True) or {}
    logger.info("Incoming update: %s", str(data)[:3000])
    msg = data.get("message") or data.get("edited_message")
    if not msg:
        return jsonify({"ok": True, "ignored": "no-message"})

    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    chat_type = chat.get("type")
    message_id = msg.get("message_id")
    text = msg.get("text", "")
    if not chat_id:
        return jsonify({"ok": True, "ignored": "no-chat-id"})

    cmd, arg_text = parse_command(text)

    if cmd == "start":
        send_message(
            chat_id,
            "Webhook Library Music Bot is alive.\n\n"
            "Commands:\n"
            "/start\n"
            "/ping\n"
            "/addsong <name>\n"
            "/listsongs\n"
            "/searchsong <keyword>\n"
            "/delsong <name>\n"
            "/play <name>\n"
            "/stop\n"
            "/nowplaying\n\n"
            "How to use:\n"
            "1. Send or forward an audio/video file to the bot in private chat\n"
            "2. Reply to that file with /addsong <name>\n"
            "3. In your group, start voice chat and use /play <name>\n\n"
            "Group playback commands work for admins only.",
            reply_to_message_id=message_id,
        )
        return jsonify({"ok": True})

    if cmd == "ping":
        send_message(chat_id, "pong", reply_to_message_id=message_id)
        return jsonify({"ok": True})

    if chat_type == "private":
        if cmd == "addsong":
            if not arg_text:
                send_message(chat_id, "Usage:\n/addsong <name>", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            reply_msg = msg.get("reply_to_message")
            media, media_type = extract_replied_media(reply_msg)
            if not media:
                send_message(
                    chat_id,
                    "Reply to an audio, voice, video, or audio/video document with /addsong <name>.",
                    reply_to_message_id=message_id,
                )
                return jsonify({"ok": True})
            song_name = arg_text.strip()
            file_id = media.get("file_id")
            original_name = media.get("file_name")
            mime_type = media.get("mime_type")
            added_by = (msg.get("from") or {}).get("id")
            add_song_to_db(song_name, file_id, original_name, mime_type, added_by)
            send_message(
                chat_id,
                f"Saved successfully.\nName: {normalize_name(song_name)}\nType: {media_type}",
                reply_to_message_id=message_id,
            )
            return jsonify({"ok": True})

        if cmd == "listsongs":
            rows = list_songs(limit=100)
            if not rows:
                send_message(chat_id, "No songs saved yet.", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            text_out = "Saved songs:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
            for part in split_long_text(text_out):
                send_message(chat_id, part)
            return jsonify({"ok": True})

        if cmd == "searchsong":
            if not arg_text:
                send_message(chat_id, "Usage:\n/searchsong <keyword>", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            rows = search_songs(arg_text, limit=20)
            if not rows:
                send_message(chat_id, "No matching songs found.", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            text_out = "Search results:\n\n" + "\n".join(f"- {row['name']}" for row in rows)
            send_message(chat_id, text_out, reply_to_message_id=message_id)
            return jsonify({"ok": True})

        if cmd == "delsong":
            if not arg_text:
                send_message(chat_id, "Usage:\n/delsong <name>", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            ok = delete_song(arg_text)
            if ok:
                send_message(chat_id, f"Deleted: {normalize_name(arg_text)}", reply_to_message_id=message_id)
            else:
                send_message(chat_id, "Song not found.", reply_to_message_id=message_id)
            return jsonify({"ok": True})

    if chat_type in ("group", "supergroup"):
        if cmd in {"play", "stop", "nowplaying"}:
            from_user = msg.get("from") or {}
            user_id = from_user.get("id")
            if not user_id or not is_group_admin(chat_id, user_id):
                send_message(chat_id, "Only group admins can use this command.", reply_to_message_id=message_id)
                return jsonify({"ok": True})

        if cmd == "nowplaying":
            info = ACTIVE_STREAMS.get(chat_id)
            if not info:
                send_message(chat_id, "Nothing is playing right now.", reply_to_message_id=message_id)
            else:
                send_message(chat_id, f"Now playing: {info['name']}", reply_to_message_id=message_id)
            return jsonify({"ok": True})

        if cmd == "play":
            if not arg_text:
                send_message(chat_id, "Usage:\n/play <saved song name>", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            if not VOICE_READY.is_set():
                send_message(chat_id, "Voice engine is not ready yet. Try again in a few seconds.", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            status_message_id = send_message(chat_id, "Queued...", reply_to_message_id=message_id)
            if status_message_id is None:
                send_message(chat_id, "Failed to create status message.", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            schedule_coro(play_saved_song(chat_id, arg_text, chat_id, status_message_id))
            return jsonify({"ok": True})

        if cmd == "stop":
            if not VOICE_READY.is_set():
                send_message(chat_id, "Voice engine is not ready yet.", reply_to_message_id=message_id)
                return jsonify({"ok": True})
            schedule_coro(stop_current_stream(chat_id))
            send_message(chat_id, "Stopped the stream.", reply_to_message_id=message_id)
            return jsonify({"ok": True})

    if chat_type == "private" and text and not cmd:
        send_message(chat_id, f"got: {text}", reply_to_message_id=message_id)
        return jsonify({"ok": True})

    return jsonify({"ok": True, "ignored": True})


if __name__ == "__main__":
    init_db()
    threading.Thread(target=run_voice_loop, daemon=True).start()
    threading.Thread(target=setup_webhook_with_retry, daemon=True).start()
    logger.info("Starting Flask on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)