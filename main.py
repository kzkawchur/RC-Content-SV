import os
import asyncio
import sys
import time
import math
import shutil
from flask import Flask
import threading
from pyrogram import Client, filters, idle
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- 1. Web Server for Render & UptimeRobot ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ Premium Bot is running flawlessly!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_web_server, daemon=True).start()

# --- 2. Async Loop Handling ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try: loop = asyncio.get_event_loop()
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
MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024 # 1GB Limit

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- 4. NEW: Auto Storage Cleanup System 🧹 ---
def cleanup_storage():
    """Starts with a fresh storage to prevent server crash."""
    if os.path.exists("downloads"):
        shutil.rmtree("downloads")
        print("🗑️ Previous cache cleared!")
    os.makedirs("downloads", exist_ok=True)

# --- 5. Helpers & Progress Bar ---
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
        bar = "[{0}{1}{2}]".format("█" * completed, "", "▒" * remaining) # Premium Box Style Bar
        speed = current / diff if diff > 0 else 0
        
        tmp = (
            f"**{ud_type}**\n\n"
            f"📊 **Progress:** `{round(percentage, 2)}%`\n"
            f"🚀 `{bar}`\n\n"
            f"📁 **Size:** `{humanbytes(current)} / {humanbytes(total)}`\n"
            f"⚡ **Speed:** `{humanbytes(speed)}/s`"
        )
        try: await message.edit(text=tmp)
        except: pass

# --- 6. Stylish Auto Status ---
async def auto_status():
    last_msg = None
    while True:
        try:
            if FORCE_SUB_CHANNEL:
                if last_msg:
                    try: await last_msg.delete()
                    except: pass
                status_text = (
                    "✨ **「 SERVER SYSTEM STATUS 」** ✨\n\n"
                    "🤖 **Bot:** `Premium Restricted Saver`\n"
                    "🟢 **Node:** `Active & Operational`\n"
                    "🛡️ **Features:** `Telegram Restricted  Content Download `\n"
                    "📦 **Capacity:** `1.0 GB per Task`\n"
                    f"⏱️ **Last Sync:** `{time.strftime('%H:%M:%S')} (UTC)`\n\n"
                    "💎 _Systems online. Ready for your links!_ @RestrictedLink_Bot"
                )
                last_msg = await bot.send_message(f"@{FORCE_SUB_CHANNEL}", status_text)
        except: pass
        await asyncio.sleep(600)

async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant: return False
    except: return True

# --- 7. NEW: Global Queue System 🚦 ---
task_queue = asyncio.Queue()

async def process_worker():
    """Background worker that processes one link at a time."""
    while True:
        client, message, link, status_msg = await task_queue.get()
        try:
            await execute_download(client, message, link, status_msg)
        except Exception as e:
            await status_msg.edit(f"❌ **Task Failed:** `{str(e)}`")
        finally:
            task_queue.task_done()

async def execute_download(client, message, link, status_msg):
    """Core download/upload logic separated for the Queue."""
    await status_msg.edit("🔍 **Task Started! Analyzing link...**")
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
            media = target_msg.document or target_msg.video or target_msg.audio or target_msg.voice or target_msg.photo
            file_size = getattr(media, 'file_size', 0)
            
            if file_size > MAX_FILE_SIZE:
                return await status_msg.edit(f"⛔ **System Alert!**\n\n**File Size:** `{humanbytes(file_size)}`\n⚠️ _Limit exceeded! Maximum allowed size is 1 GB._")

            start_time = time.time()
            file_path = await userbot.download_media(
                target_msg, 
                progress=progress_bar, 
                progress_args=("📥 DOWNLOADING CONTENT...", status_msg, start_time)
            )
            
            final_caption = f"{target_msg.caption or ''}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else (target_msg.caption or "")
            start_time = time.time()
            
            await status_msg.edit("🔄 **Preparing to upload...**")
            
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 UPLOADING VIDEO...", status_msg, start_time))
                else:
                    await client.send_document(message.chat.id, document=file_path, caption=final_caption, progress=progress_bar, progress_args=("📤 UPLOADING FILE...", status_msg, start_time))
            finally:
                if file_path and os.path.exists(file_path): 
                    os.remove(file_path) # Clean up specific file immediately
            
            await status_msg.delete()
            await message.reply_text("✅ **Task Completed Successfully!**\n_File has been delivered._", quote=True)
        else:
            await status_msg.edit("⚠️ **Notice:** No extractable content found in this link.")
    except Exception as e:
        await status_msg.edit(f"❌ **Error Encountered:** `{str(e)}`")

# --- 8. Commands & UI ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Authorization Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text("🛑 **Access Restricted!**\n\nPlease join our official channel to authenticate your access.", reply_markup=InlineKeyboardMarkup(btn))
    
    welcome_text = (
        f"⚡ **Welcome, {message.from_user.first_name}!** ⚡\n\n"
        "I am an advanced Restricted Content Saver.\n"
        "Simply forward me a link from any restricted channel or group, and I'll securely extract it for you.\n\n"
        "💡 _Type /help for usage instructions._"
    )
    await message.reply_text(welcome_text)

@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    help_text = (
        "🛠 **System Guide:**\n\n"
        "**1.** Ensure you have joined our channel.\n"
        "**2.** Copy the link of the restricted post.\n"
        "**3.** Paste the link here.\n"
        "**4.** Wait in the queue if the server is busy.\n\n"
        "⚖️ **Constraints:** Max `1.0 GB` per operation."
    )
    await message.reply_text(help_text)

@bot.on_message(filters.command("about") & filters.private)
async def about_cmd(client, message):
    await message.reply_text("🤖 **Identity:** Premium Content Extractor\n⚙️ **Core:** Pyrogram V2\n🛡️ **Features:** Smart Queue, Auto-Wipe")

@bot.on_message(filters.text & filters.private & ~filters.command(["start", "help", "about"]))
async def handle_link(client, message):
    if not await check_fsub(client, message): return
    link = message.text.strip()
    if not "t.me/" in link: 
        return await message.reply_text("⚠️ **Invalid Input:** Please provide a valid Telegram link.")

    # Queue logic applied here!
    position = task_queue.qsize() + 1
    
    queue_text = (
        "📝 **Task Queued!**\n\n"
        f"📍 **Your Position:** `{position}`\n"
        "⏳ _Please hold on, your processing will begin automatically when your turn arrives._"
    )
    status_msg = await message.reply_text(queue_text)
    
    # Put task in background queue
    await task_queue.put((client, message, link, status_msg))

# --- Main Initialization ---
async def main_runner():
    cleanup_storage() # Cleans old junk on startup
    await userbot.start()
    await bot.start()
    
    # Start background tasks
    asyncio.create_task(auto_status())
    asyncio.create_task(process_worker()) # Starts the Queue worker
    
    print("✅ Premium System Online: Queue & Cleanup Active!")
    await idle()

if __name__ == "__main__":
    try: loop.run_until_complete(main_runner())
    except KeyboardInterrupt: pass
