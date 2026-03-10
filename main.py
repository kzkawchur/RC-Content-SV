import os
import asyncio
import sys
import time
import math
from datetime import datetime
from flask import Flask
import threading
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- 1. Web Server for Render & UptimeRobot ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ Bot is running perfectly!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_web_server, daemon=True).start()

# --- 2. Async Loop Handling ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# --- 3. Configuration ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "")
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")

# Limit configuration (1 GB)
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024 

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
        remaining = 20 - completed
        bar = "[{0}{1}{2}]".format("=" * completed, ">" if completed < 20 else "", "." * (remaining - 1 if completed < 20 and remaining > 0 else remaining))
        speed = current / diff if diff > 0 else 0
        tmp = f"**Status:** {ud_type}\n**Progress:** {round(percentage, 2)}%\n`{bar}`\n**Size:** {humanbytes(current)} / {humanbytes(total)}\n**Speed:** {humanbytes(speed)}/s"
        try:
            await message.edit(text=tmp)
        except: pass

# --- Stylish Auto Status Update ---
async def auto_status():
    last_msg = None
    while True:
        try:
            if FORCE_SUB_CHANNEL:
                if last_msg:
                    try: await last_msg.delete()
                    except: pass
                
                current_time = datetime.now().strftime("%H:%M:%S")
                status_text = (
                    "╔══════════════════╗\n"
                    "      🛡 **BOT MONITORING**\n"
                    "╚══════════════════╝\n\n"
                    "🔹 **System:** `Operational` ✅\n"
                    "🔹 **Server:** `Online` 📡\n"
                    "🔹 **Limit:** `1.0 GB Per File` ⚠️\n\n"
                    "🕒 **Last Check:** `{}`\n"
                    "🚀 **Speed:** `Maximized`"
                ).format(current_time)

                last_msg = await bot.send_message(f"@{FORCE_SUB_CHANNEL}", status_text)
        except Exception as e:
            print(f"Auto status error: {e}")
        await asyncio.sleep(600)

async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant: return False
    except: return True

# --- Commands ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(f"Hello **{message.from_user.first_name}**!\n\n❌ **Access Denied!** Join our updates channel to use this bot.", reply_markup=InlineKeyboardMarkup(btn))
    await message.reply_text(f"Welcome **{message.from_user.first_name}**! 👋\nSend me the restricted link. (Max: 1GB)")

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    await message.reply_text("❓ **How to use:**\n1. Join our channel.\n2. Copy restricted link.\n3. Paste here.\n\n⚠️ **Limit:** 1GB per file.")

# --- Main Logic with Size Limit ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(client, message): return
    link = message.text.strip()
    if not "t.me/" in link: return await message.reply_text("❌ Invalid link.")

    status_msg = await message.reply_text("⏳ **Analyzing link...**")

    try:
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        if not userbot.is_connected: await userbot.start()
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        # --- File Size Check ---
        file_size = 0
        if target_msg.document: file_size = target_msg.document.file_size
        elif target_msg.video: file_size = target_msg.video.file_size
        elif target_msg.audio: file_size = target_msg.audio.file_size
        elif target_msg.photo: file_size = 0 # Photos are always small

        if file_size > MAX_FILE_SIZE:
            return await status_msg.edit(f"❌ **File Too Large!**\n\n**Your File:** {humanbytes(file_size)}\n**Limit:** 1.00 GB\n\n_Reason: Server safety._")

        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
            return

        if target_msg.media:
            start_time = time.time()
            file_path = await userbot.download_media(target_msg, progress=progress_bar, progress_args=("📥 **Downloading...**", status_msg, start_time))
            
            final_caption = f"{target_msg.caption if target_msg.caption else ''}\n\n{CUSTOM_CAPTION}"
            start_time = time.time()
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 **Uploading Video...**", status_msg, start_time))
                elif target_msg.audio:
                    await client.send_audio(message.chat.id, audio=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 **Uploading Audio...**", status_msg, start_time))
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 **Uploading File...**", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path): os.remove(file_path)
            await status_msg.delete()
        else:
            await status_msg.edit("❌ No content found.")

    except FloodWait as e: await status_msg.edit(f"⚠️ Wait {e.value} seconds.")
    except Exception as e: await status_msg.edit(f"❌ **Error:** {str(e)}")

async def main_runner():
    await userbot.start()
    await bot.start()
    asyncio.create_task(auto_status())
    await idle()

if __name__ == "__main__":
    loop.run_until_complete(main_runner())
