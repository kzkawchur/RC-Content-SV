import os
import asyncio
import sys
import time
import math
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, UserNotParticipant
from motor.motor_asyncio import AsyncIOMotorClient

# --- 1. Web Server (For Render Uptime) ---
app = Flask(__name__)
@app.route('/')
def health_check(): return "✅ Bot is Online!", 200
def run_web_server():
    port = int(os.environ.get("PORT", 10000)) # Render uses 10000 by default
    app.run(host='0.0.0.0', port=port)
threading.Thread(target=run_web_server, daemon=True).start()

# --- 2. Config ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
MONGO_URL = os.environ.get("MONGO_URL", "")
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "")
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")

# --- 3. Database ---
db_client = AsyncIOMotorClient(MONGO_URL)
db = db_client.content_saver_bot
users_col = db.users

# --- 4. Clients ---
bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- 5. Progress Bar Utility ---
async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff if diff > 0 else 0
        progress = "[{0}{1}] \n**Progress**: {2}%\n".format(
            ''.join(["▰" for i in range(math.floor(percentage / 5))]),
            ''.join(["▱" for i in range(20 - math.floor(percentage / 5))]),
            round(percentage, 2))
        tmp = progress + f"**Status**: {ud_type}\n**Size**: {humanbytes(current)} / {humanbytes(total)}"
        try:
            await message.edit(text=tmp)
        except: pass

def humanbytes(size):
    if not size: return "0 B"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024: return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

# --- 6. Helpers ---
async def check_fsub(message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await bot.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant: return False
    except: return True

# --- 7. Commands ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    user_id = message.from_user.id
    try: await users_col.update_one({"user_id": user_id}, {"$set": {"user_id": user_id}}, upsert=True)
    except: pass
    if not await check_fsub(message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text("❌ Please join our channel to use the bot!", reply_markup=InlineKeyboardMarkup(btn))
    await message.reply_text(f"Welcome **{message.from_user.first_name}**! I am active. Send me a link.")

@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(client, message):
    count = await users_col.count_documents({})
    await message.reply_text(f"📊 **Total Users:** {count}")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID) & filters.reply)
async def broadcast_cmd(client, message):
    msg = await message.reply_text("📢 Starting broadcast...")
    users = users_col.find({})
    count = 0
    async for user in users:
        try:
            await message.reply_to_message.copy(user['user_id'])
            count += 1
            await asyncio.sleep(0.3)
        except: pass
    await msg.edit(f"✅ Sent to {count} users.")

# --- 8. Main Logic ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(message): return
    link = message.text.strip()
    if "t.me/" not in link: return
    status_msg = await message.reply_text("🔎 Checking...")
    try:
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        target_msg = await userbot.get_messages(chat_id, msg_id)
        if target_msg.media:
            await status_msg.edit("📥 Downloading...")
            start_time = time.time()
            file_path = await userbot.download_media(target_msg, progress=progress_bar, progress_args=("Downloading...", status_msg, start_time))
            await status_msg.edit("📤 Uploading...")
            start_time = time.time()
            cap = f"{target_msg.caption if target_msg.caption else ''}\n\n{CUSTOM_CAPTION}"
            if target_msg.video:
                await client.send_video(message.chat.id, video=file_path, caption=cap, progress=progress_bar, progress_args=("Uploading...", status_msg, start_time))
            else:
                await client.send_document(message.chat.id, document=file_path, caption=cap, progress=progress_bar, progress_args=("Uploading...", status_msg, start_time))
            if os.path.exists(file_path): os.remove(file_path)
            await status_msg.delete()
        elif target_msg.text:
            await client.send_message(message.chat.id, text=f"{target_msg.text}\n\n{CUSTOM_CAPTION}")
            await status_msg.delete()
    except Exception as e: await status_msg.edit(f"❌ Error: {str(e)}")

# --- 9. Execution ---
async def main():
    await userbot.start()
    await bot.start()
    print("✅ All Systems Go!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main()) # fixed the loop error here
