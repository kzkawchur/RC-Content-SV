import os
import asyncio
import sys
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- 1. Web Server for UptimeRobot ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ Bot is alive and running!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_web_server, daemon=True).start()

# --- 2. Python Loop Issue Fix ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# --- 3. Configuration Variables ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "") # e.g., my_channel (without @)
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")       # e.g., Join @MyChannel

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- Force Subscribe Checker ---
async def check_fsub(client, message):
    if not FORCE_SUB_CHANNEL:  
        return True
    try:
        await client.get_chat_member(FORCE_SUB_CHANNEL, message.from_user.id)
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        print(f"FSub Error: {e}")
        return True

# --- /start Command ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            f"Hello **{message.from_user.first_name}**!\n\n"
            "❌ You must join our channel to use this bot.\n"
            "Please join using the button below and send `/start` again.",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    await message.reply_text(
        f"Welcome **{message.from_user.first_name}**! 👋\n\n"
        "I am a powerful bot that can extract content from restricted channels.\n\n"
        "**How to use:**\n"
        "1. Send me the link of the message (Video, Photo, Audio, Document, or Text).\n"
        "2. If it's a private channel, make sure my underlying account is a member there."
    )

# --- Help Command ---
@bot.on_message(filters.command("help") & filters.private)
async def help_cmd(client, message):
    help_text = (
        "❓ **How to use this bot:**\n\n"
        "1️⃣ Join our required channel first.\n"
        "2️⃣ Go to any Restricted Channel/Group.\n"
        "3️⃣ Copy the link of the Video or File.\n"
        "4️⃣ Paste the link here and wait for the upload.\n\n"
        "⚠️ **Note:** For private channels, I can only help if my Admin is already a member there."
    )
    await message.reply_text(help_text)

# --- About Command ---
@bot.on_message(filters.command("about") & filters.private)
async def about_cmd(client, message):
    about_text = (
        "🤖 **Bot Name:** Restricted Content Saver\n"
        "🚀 **Version:** 2.0 (High Speed)\n"
        "🛠 **Framework:** Pyrogram\n"
        "📡 **Server:** Render (Cloud)\n\n"
        "Specialized in saving restricted media from Telegram."
    )
    await message.reply_text(about_text)

# --- Link Processing (All Media Types) ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 Join Our Channel", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            "❌ You haven't joined our channel yet! Please join to use the bot.",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    link = message.text.strip()
    if not "t.me/" in link:
        return await message.reply_text("❌ Invalid Telegram link. Please send a valid link.")

    status_msg = await message.reply_text("⏳ Processing your link... Please wait.")

    try:
        # Extract chat_id and msg_id
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        if not userbot.is_connected:
            await userbot.start()

        # Fetch the message
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        # Handling Text-Only Messages
        if target_msg.text and not target_msg.media:
            final_text = f"{target_msg.text}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else target_msg.text
            await client.send_message(message.chat.id, text=final_text)
            await status_msg.delete()
            return

        # Handling Media Files (Video, Photo, Audio, Document, Animation)
        if target_msg.media:
            await status_msg.edit("📥 Downloading file (Restricted Mode)...")
            file_path = await userbot.download_media(target_msg)
            
            await status_msg.edit("📤 Download complete! Uploading to you...")
            
            # Setup Caption
            original_caption = target_msg.caption if target_msg.caption else ""
            final_caption = f"{original_caption}\n\n{CUSTOM_CAPTION}" if CUSTOM_CAPTION else original_caption

            # Upload based on media type
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
                # Always delete the file from the server to save space, even if upload fails
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
            
            await status_msg.delete()
        else:
            await status_msg.edit("❌ No supported media or text found in this link.")

    except FloodWait as e:
        await status_msg.edit(f"⚠️ Telegram limitation! Please wait {e.value} seconds.")
    except Exception as e:
        await status_msg.edit(f"❌ Error: {str(e)}\n\nMake sure the account is a member of the chat.")

print("✅ Bot is successfully running!")
bot.run()
