import os
import asyncio
import sys
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, RPCError, UserNotParticipant

# --- ১. ওয়েব সার্ভার (UptimeRobot-এর জন্য) ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ বট সচল আছে!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_web_server, daemon=True).start()

# --- ২. লুপ ইস্যু ফিক্স ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# --- ৩. কনফিগারেশন ভেরিয়েবল ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

# নতুন ফিচারগুলোর ভেরিয়েবল:
FORCE_SUB_CHANNEL = os.environ.get("FORCE_SUB_CHANNEL", "") # উদা: my_channel ( @ ছাড়া )
CUSTOM_CAPTION = os.environ.get("CUSTOM_CAPTION", "")       # উদা: 🌟 জয়েন করুন @MyChannel

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# --- Force Subscribe চেক করার ফাংশন ---
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

# --- /start কমান্ড ---
@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 আমাদের চ্যানেলে জয়েন করুন", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            f"হ্যালো **{message.from_user.first_name}**!\n\n"
            "❌ বটটি ব্যবহার করতে হলে আপনাকে প্রথমে আমাদের টেলিগ্রাম চ্যানেলে জয়েন করতে হবে।\n"
            "নিচের বাটনে ক্লিক করে জয়েন করুন এবং আবার `/start` দিন।",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    await message.reply_text(
        f"স্বাগতম **{message.from_user.first_name}**!\n\n"
        "আমি রেস্ট্রিক্টেড ভিডিও ডাউনলোড করতে পারি।\n"
        "**কীভাবে ব্যবহার করবেন:**\n"
        "১. আমাকে ভিডিওর লিংকটি পাঠান।\n"
        "২. চ্যানেলটি প্রাইভেট হলে আপনার আইডিটি সেখানে জয়েন থাকতে হবে।"
    )

# --- লিংক প্রসেসিং এবং কাস্টম ক্যাপশন ---
@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    if not await check_fsub(client, message):
        btn = [[InlineKeyboardButton("📢 আমাদের চ্যানেলে জয়েন করুন", url=f"https://t.me/{FORCE_SUB_CHANNEL}")]]
        return await message.reply_text(
            "❌ আপনি এখনো আমাদের চ্যানেলে জয়েন করেননি! বটটি ব্যবহার করতে প্রথমে জয়েন করুন।",
            reply_markup=InlineKeyboardMarkup(btn)
        )

    link = message.text.strip()
    if not "t.me/" in link:
        return await message.reply_text("❌ এটি কোনো সঠিক টেলিগ্রাম লিংক নয়।")

    status_msg = await message.reply_text("⏳ প্রসেসিং হচ্ছে... একটু অপেক্ষা করুন।")

    try:
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

        await status_msg.edit("📥 ফাইলটি ডাউনলোড করছি (রেস্ট্রিক্টেড মোড)...")
        
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        if target_msg.media:
            file_path = await userbot.download_media(target_msg)
            await status_msg.edit("📤 ডাউনলোড শেষ! এখন আপনাকে পাঠানো হচ্ছে...")
            
            # --- কাস্টম ক্যাপশন (Custom Caption) লজিক ---
            original_caption = target_msg.caption if target_msg.caption else ""
            
            if CUSTOM_CAPTION:
                # যদি কাস্টম ক্যাপশন দেওয়া থাকে, তবে সেটি অরিজিনাল ক্যাপশনের নিচে যোগ হবে
                final_caption = f"{original_caption}\n\n{CUSTOM_CAPTION}" if original_caption else CUSTOM_CAPTION
            else:
                final_caption = original_caption if original_caption else "আপনার ডাউনলোড করা ফাইল।"

            # ভিডিও বা ফাইল হিসেবে পাঠানো
            if target_msg.video:
                await client.send_video(message.chat.id, video=file_path, caption=final_caption)
            elif target_msg.document:
                await client.send_document(message.chat.id, document=file_path, caption=final_caption)
            else:
                 await client.send_document(message.chat.id, document=file_path, caption=final_caption)
            
            if os.path.exists(file_path):
                os.remove(file_path)
            await status_msg.delete()
        else:
            await status_msg.edit("❌ এই লিংকে কোনো মিডিয়া ফাইল পাওয়া যায়নি।")

    except FloodWait as e:
        await status_msg.edit(f"⚠️ টেলিগ্রাম লিমিট! {e.value} সেকেন্ড অপেক্ষা করুন।")
    except Exception as e:
        await status_msg.edit(f"❌ এরর: {str(e)}\n\nনিশ্চিত করুন যে আপনার আইডিটি ওই চ্যানেলের মেম্বার।")

print("✅ বট সফলভাবে চালু হয়েছে!")
bot.run()
