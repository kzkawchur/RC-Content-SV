import os
import asyncio
import sys
import time
import math
import subprocess

# --- Auto install requests if missing ---
try:
    import requests
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
    import requests

from flask import Flask
import threading
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- Web Server ---
app = Flask(__name__)
@app.route('/')
def health_check(): return "✅ Bot is running perfectly!", 200
def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
threading.Thread(target=run_web_server, daemon=True).start()

# --- Async Setup ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try: loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)

# --- Configuration ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "") 
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")       
TG_LIMIT = 1 * 1024 * 1024 * 1024
TERA_LIMIT = 450 * 1024 * 1024

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- Helpers ---
def humanbytes(size):
    if not size: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total
        completed = math.floor(percentage / 5)
        bar = "[{0}{1}{2}]".format("=" * completed, ">" if completed < 20 else "", "." * (20 - completed - 1 if completed < 20 else 0))
        speed = current / diff if diff > 0 else 0
        tmp = f"**Status:** {ud_type}\n**Progress:** {round(percentage, 2)}%\n`{bar}`\n**Size:** {humanbytes(current)} / {humanbytes(total)}\n**Speed:** {humanbytes(speed)}/s"
        try: await message.edit(text=tmp)
        except: pass

async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except: return False

# --- COMMANDS (Fix for /start, /help, /about) ---
@bot.on_message(filters.command(["start", "help", "about"]) & filters.private)
async def commands(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text("❌ Join our channel first!", reply_markup=InlineKeyboardMarkup(btn))
    
    cmd = message.command[0]
    if cmd == "start":
        await message.reply_text(f"Welcome **{message.from_user.first_name}**! 👋\nSend me a Telegram Restricted Link or Terabox Link.")
    elif cmd == "help":
        await message.reply_text("❓ **How to use:**\n1. Join channel.\n2. Send original Terabox link (not short links).\n3. Or send Telegram restricted link.")
    elif cmd == "about":
        await message.reply_text("🤖 **Restricted & Terabox Saver Bot**\nVersion: 3.0 (Stable)")

# --- LINK HANDLER ---
@bot.on_message(filters.text & filters.private & ~filters.command(["start", "help", "about"]))
async def handle_link(client, message):
    if not await check_fsub(client, message): return
    
    link = message.text.strip()
    status_msg = await message.reply_text("⏳ **Processing...**")

    # Terabox Support
    tera_domains = ["terabox.com", "teraboxapp.com", "1024tera.com", "terasharelink.com", "terabox.app"]
    if any(domain in link for domain in tera_domains):
        try:
            shorturl = link.split('/')[-1].split('?')[0]
            api_url = f"https://terabox-dl.qtcloud.workers.dev/api/get-info?shorturl={shorturl}"
            response = requests.get(api_url).json()
            
            if not response.get("list"):
                return await status_msg.edit("❌ **Invalid Link!** Please send the original Terabox link, not a short link.")
            
            file_info = response["list"][0]
            file_name, dl_link, file_size = file_info["filename"], file_info["download_link"]["url"], int(file_info["size"])

            if file_size > TERA_LIMIT:
                return await status_msg.edit(f"❌ Limit Exceeded: {humanbytes(file_size)}")

            await status_msg.edit(f"📥 **Downloading:** `{file_name}`")
            file_path = f"downloads/{file_name}"
            if not os.path.exists("downloads"): os.makedirs("downloads")
            
            with requests.get(dl_link, stream=True) as r:
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): f.write(chunk)

            await status_msg.edit("📤 **Uploading...**")
            await client.send_document(message.chat.id, document=file_path, caption=f"**File:** `{file_name}`\n\n{CUSTOM_CAPTION}")
            os.remove(file_path)
            await status_msg.delete()
        except Exception as e:
            await status_msg.edit(f"❌ Error: {str(e)}")
        return

    # Telegram Link Support
    if "t.me/" in link:
        try:
            if "t.me/c/" in link:
                parts = link.split("/")
                chat_id, msg_id = int("-100" + parts[parts.index("c") + 1]), int(parts[-1].split("?")[0])
            else:
                parts = link.split("/")
                chat_id, msg_id = parts[-2], int(parts[-1].split("?")[0])

            if not userbot.is_connected: await userbot.start()
            target_msg = await userbot.get_messages(chat_id, msg_id)
            
            if target_msg.media:
                media = target_msg.document or target_msg.video or target_msg.audio or target_msg.photo
                if getattr(media, 'file_size', 0) > TG_LIMIT: return await status_msg.edit("❌ Over 1GB!")
                
                start_time = time.time()
                file_path = await userbot.download_media(target_msg, progress=progress_bar, progress_args=("📥 Downloading...", status_msg, start_time))
                await client.send_document(message.chat.id, document=file_path, caption=f"{target_msg.caption or ''}\n\n{CUSTOM_CAPTION}")
                if os.path.exists(file_path): os.remove(file_path)
                await status_msg.delete()
            else:
                await client.send_message(message.chat.id, f"{target_msg.text}\n\n{CUSTOM_CAPTION}")
                await status_msg.delete()
        except Exception as e: await status_msg.edit(f"❌ Error: {str(e)}")
        return

    await status_msg.edit("❌ Unsupported Link! Please send original Terabox or TG link.")

async def main_runner():
    await userbot.start(); await bot.start()
    print("✅ Bot is online!"); await idle()

if __name__ == "__main__":
    loop.run_until_complete(main_runner())
