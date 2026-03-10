import os
import asyncio
import sys
import time
import math
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
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024 # 1GB Limit in Bytes

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- NEW: Progress Bar Helpers ---
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
        bar = "[{0}{1}{2}]".format(
            "=" * completed,
            ">" if completed < 20 else "",
            "." * (remaining - 1 if completed < 20 and remaining > 0 else remaining)
        )
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
                
                # More Stylish Status Template
                status_text = (
                    "✨ **「 BOT STATUS UPDATE 」** ✨\n\n"
                    "👤 **Bot Name:** Restricted Saver\n"
                    "🛰 **Server:** Cloud Service\n"
                    "⚡ **Status:** Active & Stable\n"
                    "📥 **Download Limit:** 1.0 GB\n"
                    f"⏰ **Last Check:** `{time.strftime('%H:%M:%S')} (UTC)`\n\n"
                    "💎 _Bot is working fine, send your links!_"
                )
                
                last_msg = await bot.send_message(f"@{FORCE_SUB_CHANNEL}", status_text)
        except Exception as e:
            print(f"Auto status error: {e}")
        await asyncio.sleep(600) # 10 Minutes

async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant: return False
    except: return True

@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(f"Hello **{message.from_user.first_name}**!\n\n❌ **Access Denied!** Join our channel to use this bot.", reply_markup=InlineKeyboardMarkup(btn))
    await message.reply_text(f"Welcome **{message.from_user.first_name}**! 👋\n\nI can download restricted content. Send me any link.\n\n/help - Usage Guide")

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    await message.reply_text("❓ **How to use:**\n\n1️⃣ Join our channel.\n2️⃣ Copy the link of the restricted post.\n3️⃣ Paste it here.\n\n⚠️ **Limit:** Maximum 1GB per file.")

@bot.on_message(filters.command("about") & filters.private)
async def about_cmd(client, message):
    await message.reply_text("🤖 **Bot:** Restricted Content Saver\n🚀 **Version:** 2.5 (Stable)\n📡 **Server:** Cloud")

# --- Main Logic: Link Processing with 1GB Limit ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(client, message): return
    link = message.text.strip()
    if not "t.me/" in link: return await message.reply_text("❌ Not a valid Telegram link.")

    status_msg = await message.reply_text("⏳ **Processing...**")
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
        
        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
            return

        if target_msg.media:
            # --- 1GB Limit Check Logic ---
            media = target_msg.document or target_msg.video or target_msg.audio or target_msg.voice or target_msg.photo
            file_size = getattr(media, 'file_size', 0)
            
            if file_size > MAX_FILE_SIZE:
                return await status_msg.edit(f"❌ **File Rejected!**\n\nYour file is **{humanbytes(file_size)}**. I can only download files up to **1 GB** to prevent server crash.")

            start_time = time.time()
            file_path = await userbot.download_media(target_msg, progress=progress_bar, progress_args=("📥 **Downloading...**", status_msg, start_time))
            
            final_caption = f"{target_msg.caption or ''}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else (target_msg.caption or "")
            start_time = time.time()
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 **Uploading...**", status_msg, start_time))
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 **Uploading...**", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path): os.remove(file_path)
            await status_msg.delete()
        else:
            await status_msg.edit("❌ No content found.")
    except Exception as e:
        await status_msg.edit(f"❌ **Error:** {str(e)}")

async def main_runner():
    await userbot.start()
    await bot.start()
    asyncio.create_task(auto_status())
    print("✅ Bot is online with 1GB Limit!")
    await idle()

if __name__ == "__main__":
    try: loop.run_until_complete(main_runner())
    except KeyboardInterrupt: pass
