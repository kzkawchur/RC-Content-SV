import os
import asyncio
import sys
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- 1. Web Server for Render & UptimeRobot ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ Bot is running perfectly!", 200

def run_web_server():
    # Render binds to port 8080 by default
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# Running Flask in a separate thread
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

# --- 3. Configuration (From Environment Variables) ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

# Extra Features
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "") # Example: MyChannel (without @)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")       # Example: 🚀 Join @MyChannel

# Clients Setup
bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- Helper: Force Subscribe Checker ---
async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL:  
        return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant:
        return False
    except Exception:
        return True

# --- Command: /start ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            f"Hello **{message.from_user.first_name}**!\n\n"
            "❌ **Access Denied!** You must join our updates channel to use this bot.\n\n"
            "Join using the button below and send `/start` again.",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    await message.reply_text(
        f"Welcome **{message.from_user.first_name}**! 👋\n\n"
        "I can download restricted content from any Telegram channel (Video, Photo, Audio, Document, or Text).\n\n"
        "**How to use:**\n"
        "Just send me the link of the restricted message.\n\n"
        "**Commands:**\n"
        "/help - See usage guide\n"
        "/about - Bot information"
    )

# --- Command: /help ---
@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    help_text = (
        "❓ **How to use this bot:**\n\n"
        "1️⃣ Ensure you have joined our required channel.\n"
        "2️⃣ Go to any Restricted Channel/Group.\n"
        "3️⃣ Copy the link of the post you want to save.\n"
        "4️⃣ Paste the link here and wait for the upload.\n\n"
        "⚠️ **Note:** For private channels, I can only help if my Admin is already a member of that channel."
    )
    await message.reply_text(help_text)

# --- Command: /about ---
@bot.on_message(filters.command("about") & filters.private)
async def about_cmd(client, message):
    about_text = (
        "🤖 **Bot:** Restricted Content Saver\n"
        "🚀 **Version:** 2.5 (English Edition)\n"
        "🛠 **Platform:** Pyrogram\n"
        "📡 **Server:** Render Cloud\n\n"
        "Designed to help you save media that is restricted from forwarding."
    )
    await message.reply_text(about_text)

# --- Main Logic: Link Processing ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    # Check F-Sub again before processing
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            "❌ Please join our channel first to unlock the bot features!",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    link = message.text.strip()
    if not "t.me/" in link:
        return await message.reply_text("❌ This is not a valid Telegram link.")

    status_msg = await message.reply_text("⏳ **Processing your link... Please wait.**")

    try:
        # Parsing the link
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        # Start Userbot if not connected
        if not userbot.is_connected:
            await userbot.start()

        # Get the message via Userbot
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        # Handle Text-Only Messages
        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
            return

        # Handle All Media Types
        if target_msg.media:
            await status_msg.edit("📥 **Downloading content...** (Restricted Mode)")
            file_path = await userbot.download_media(target_msg)
            
            await status_msg.edit("📤 **Download complete! Uploading to you...**")
            
            # Caption Logic
            original_caption = target_msg.caption if target_msg.caption else ""
            final_caption = f"{original_caption}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else original_caption

            # Send media based on type
            try:
                if target_msg.photo:
                    await client.send_photo(message.chat.id, photo=file_path, caption=final_caption)
                elif target_msg.video:
                    await client.send_video(message.chat.id, video=file_path, caption=final_caption)
                elif target_msg.audio:
                    await client.send_audio(message.chat.id, audio=file_path, caption=final_caption)
                elif target_msg.voice:
                    await client.send_voice(message.chat.id, voice=file_path, caption=final_caption)
                elif target_msg.animation:
                    await client.send_animation(message.chat.id, animation=file_path, caption=final_caption)
                else:
                     await client.send_document(message.chat.id, document=file_path, caption=final_caption)
            finally:
                # Cleanup: Delete file from server after upload
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
            
            await status_msg.delete()
        else:
            await status_msg.edit("❌ No content found in this link.")

    except FloodWait as e:
        await status_msg.edit(f"⚠️ **Telegram Limit!** Please wait {e.value} seconds.")
    except Exception as e:
        await status_msg.edit(f"❌ **Error:** {str(e)}\n\n*Make sure my admin ID is a member of that channel.*")

# Start the Bot
print("✅ Bot is successfully active and running!")
bot.run()
