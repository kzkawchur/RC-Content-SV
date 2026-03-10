import os
import asyncio
import sys
import time
import math
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant
from motor.motor_asyncio import AsyncIOMotorClient

# --- 1. Web Server for UptimeRobot ---
app = Flask(__name__)
@app.route('/')
def health_check(): return "✅ Pro Bot is Running!", 200
def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
threading.Thread(target=run_web_server, daemon=True).start()

# --- 2. Configuration ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
MONGO_URL = os.environ.get("MONGO_URL", "") # MongoDB Connection String
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "")
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")

# --- 3. Database Setup ---
db_client = AsyncIOMotorClient(MONGO_URL)
db = db_client.content_saver_bot
users_col = db.users

# --- 4. Clients Setup ---
bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- 5. Progress Bar Utility ---
async def progress_bar(current, total, ud_type, message, start_time):
    now = time.time()
    diff = now - start_time
    if round(diff % 4.00) == 0 or current == total:
        percentage = current * 100 / total
        speed = current / diff
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000
        estimated_total_time = elapsed_time + time_to_completion

        elapsed_time = TimeFormatter(milliseconds=elapsed_time)
        estimated_total_time = TimeFormatter(milliseconds=estimated_total_time)

        progress = "[{0}{1}] \n**Progress**: {2}%\n".format(
            ''.join(["▰" for i in range(math.floor(percentage / 5))]),
            ''.join(["▱" for i in range(20 - math.floor(percentage / 5))]),
            round(percentage, 2))

        tmp = progress + "**Status**: {0}\n**Done**: {1} / {2}\n**Speed**: {3}/s\n**ETA**: {4}\n".format(
            ud_type,
            humanbytes(current),
            humanbytes(total),
            humanbytes(speed),
            estimated_total_time if estimated_total_time != '' else "0 s"
        )
        try:
            await message.edit(text=tmp)
        except:
            pass

def humanbytes(size):
    if not size: return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

def TimeFormatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
        ((str(hours) + "h, ") if hours else "") + \
        ((str(minutes) + "m, ") if minutes else "") + \
        ((str(seconds) + "s, ") if seconds else "")
    return tmp[:-2]

# --- 6. Helper: Force Subscribe ---
async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL: return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant: return False
    except: return True

# --- 7. Admin Commands ---
@bot.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats(client, message):
    count = await users_col.count_documents({})
    await message.reply_text(f"📊 **Total Users:** {count}")

@bot.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast(client, message):
    if not message.reply_to_message:
        return await message.reply_text("Reply to a message to broadcast.")
    
    msg = await message.reply_text("📢 Broadcasting started...")
    users = users_col.find({})
    done = 0
    failed = 0
    async for user in users:
        try:
            await message.reply_to_message.copy(user['user_id'])
            done += 1
        except:
            failed += 1
    await msg.edit(f"✅ **Broadcast Completed!**\n\nSuccess: {done}\nFailed: {failed}")

# --- 8. User Commands ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    # Add user to DB
    user_id = message.from_user.id
    if not await users_col.find_one({"user_id": user_id}):
        await users_col.insert_one({"user_id": user_id})

    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text("❌ Join our channel to use the bot!", reply_markup=InlineKeyboardMarkup(btn))
    
    await message.reply_text(f"Hello **{message.from_user.first_name}**! Send me any restricted link.")

# --- 9. Main Processing Logic ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(client, message):
        return await message.reply_text("❌ Join channel first!")

    link = message.text.strip()
    if "t.me/" not in link: return
    
    status_msg = await message.reply_text("⏳ Processing...")
    
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
        
        if target_msg.media:
            start_time = time.time()
            file_path = await userbot.download_media(
                target_msg, 
                progress=progress_bar, 
                progress_args=("📥 **Downloading...**", status_msg, start_time)
            )
            
            await status_msg.edit("📤 **Uploading... Please wait.**")
            start_time = time.time()
            
            # Caption
            cap = f"{target_msg.caption if target_msg.caption else ''}\n\n{CUSTOM_CAPTION}"
            
            if target_msg.photo:
                await client.send_photo(message.chat.id, photo=file_path, caption=cap)
            elif target_msg.video:
                await client.send_video(message.chat.id, video=file_path, caption=cap, progress=progress_bar, progress_args=("📤 **Uploading...**", status_msg, start_time))
            else:
                await client.send_document(message.chat.id, document=file_path, caption=cap, progress=progress_bar, progress_args=("📤 **Uploading...**", status_msg, start_time))
            
            if os.path.exists(file_path): os.remove(file_path)
            await status_msg.delete()
        elif target_msg.text:
            await client.send_message(message.chat.id, text=f"{target_msg.text}\n\n{CUSTOM_CAPTION}")
            await status_msg.delete()
            
    except Exception as e:
        await status_msg.edit(f"❌ **Error:** {str(e)}")

print("✅ Pro Bot Started!")
bot.run()
