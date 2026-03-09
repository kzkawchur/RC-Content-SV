import os
import asyncio
import sys
from flask import Flask
import threading
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError

# --- ১. Render-এর জন্য ওয়েব সার্ভার (UptimeRobot-এর জন্য) ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "✅ বট সচল আছে এবং কাজ করছে!", 200

def run_web_server():
    # Render সাধারণত ৮MD বা ৮০৮০ পোর্ট খুঁজে
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ওয়েব সার্ভারটি আলাদা থ্রেডে চালানো যাতে বটের কাজে বাধা না দেয়
threading.Thread(target=run_web_server, daemon=True).start()

# --- ২. পাইথন লুপ ইস্যু ফিক্স ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# --- ৩. কনফিগারেশন (Render/Railway Variables থেকে নিবে) ---
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

# ক্লায়েন্ট সেটআপ
bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

@bot.on_message(filters.command("start") & filters.private)
async def start(client, message):
    await message.reply_text(
        f"স্বাগতম **{message.from_user.first_name}**!\n\n"
        "আমি রেস্ট্রিক্টেড ভিডিও ডাউনলোড করতে পারি।\n"
        "**কীভাবে ব্যবহার করবেন:**\n"
        "১. আমাকে ভিডিওর লিংকটি পাঠান।\n"
        "২. যদি চ্যানেলটি প্রাইভেট হয়, তবে আপনার আইডিটি জয়েন থাকতে হবে।"
    )

@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    link = message.text.strip()
    
    if not "t.me/" in link:
        return await message.reply_text("❌ এটি কোনো সঠিক টেলিগ্রাম লিংক নয়।")

    status_msg = await message.reply_text("⏳ প্রসেসিং হচ্ছে... একটু অপেক্ষা করুন।")

    try:
        # লিংক থেকে ডাটা বের করা
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        # ইউজারবট কানেকশন চেক
        if not userbot.is_connected:
            await userbot.start()

        await status_msg.edit("📥 ফাইলটি ডাউনলোড করছি (রেস্ট্রিক্টেড মোড)...")
        
        # মেসেজটি খুঁজে বের করা
        target_msg = await userbot.get_messages(chat_id, msg_id)
        
        if target_msg.media:
            # ফাইল ডাউনলোড করা
            file_path = await userbot.download_media(target_msg)
            await status_msg.edit("📤 ডাউনলোড শেষ! এখন আপনাকে পাঠানো হচ্ছে...")
            
            # ভিডিও বা ফাইল হিসেবে পাঠানো
            if target_msg.video:
                await client.send_video(
                    chat_id=message.chat.id,
                    video=file_path,
                    caption=target_msg.caption or "আপনার ভিডিও।"
                )
            elif target_msg.document:
                await client.send_document(
                    chat_id=message.chat.id,
                    document=file_path,
                    caption=target_msg.caption or "আপনার ফাইল।"
                )
            else:
                 await client.send_document(
                    chat_id=message.chat.id,
                    document=file_path,
                    caption=target_msg.caption or "মিডিয়া ফাইল।"
                )
            
            # সার্ভার থেকে ফাইল মুছে ফেলা
            if os.path.exists(file_path):
                os.remove(file_path)
            await status_msg.delete()
        else:
            await status_msg.edit("❌ এই লিংকে কোনো মিডিয়া ফাইল পাওয়া যায়নি।")

    except FloodWait as e:
        await status_msg.edit(f"⚠️ টেলিগ্রাম লিমিট! {e.value} সেকেন্ড অপেক্ষা করুন।")
    except Exception as e:
        await status_msg.edit(f"❌ এরর: {str(e)}\n\nনিশ্চিত করুন যে আপনার আইডিটি ওই চ্যানেলের মেম্বার।")

# ৪. বট রান করা
print("✅ বট সফলভাবে চালু হয়েছে!")
bot.run()
