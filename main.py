import os
import asyncio
import sys
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError

# --- পাইথন ৩.১০+ এর জন্য লুপ ইস্যু ফিক্স ---
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEvent_loop_policy())
else:
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

# --- কনফিগারেশন (Railway বা Render Variables থেকে নিবে) ---
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
        f"হ্যালো **{message.from_user.first_name}**!\n\n"
        "আমি রেস্ট্রিক্টেড চ্যানেলের ভিডিও ডাউনলোড করে দিতে পারি।\n"
        "**লিংক দিন:** `https://t.me/channel_name/123` ফরম্যাটে।"
    )

@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    link = message.text.strip()
    
    if not "t.me/" in link:
        return await message.reply_text("❌ এটি কোনো সঠিক টেলিগ্রাম লিংক নয়।")

    status_msg = await message.reply_text("⏳ যাচাই করছি... একটু অপেক্ষা করুন।")

    try:
        # লিংক থেকে চ্যাট আইডি ও মেসেজ আইডি বের করা
        if "t.me/c/" in link:
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        async with userbot:
            await status_msg.edit("📥 ফাইলটি ডাউনলোড করছি (রেস্ট্রিক্টেড মোড)...")
            
            # মেসেজটি খুঁজে বের করা
            target_msg = await userbot.get_messages(chat_id, msg_id)
            
            if target_msg.media:
                # ফাইলটি সার্ভারে ডাউনলোড করা
                file_path = await userbot.download_media(target_msg)
                
                await status_msg.edit("📤 ডাউনলোড শেষ! এখন আপনাকে পাঠানো হচ্ছে...")
                
                # সরাসরি ভিডিও বা ডকুমেন্ট হিসেবে পাঠানো
                if target_msg.video:
                    await client.send_video(
                        chat_id=message.chat.id,
                        video=file_path,
                        caption=target_msg.caption or "আপনার ভিডিও।"
                    )
                else:
                    await client.send_document(
                        chat_id=message.chat.id,
                        document=file_path,
                        caption=target_msg.caption or "আপনার ফাইল।"
                    )
                
                # সার্ভারের মেমোরি খালি করতে ফাইলটি ডিলিট করা
                if os.path.exists(file_path):
                    os.remove(file_path)
                    
                await status_msg.delete()
            else:
                await status_msg.edit("❌ এই লিংকে কোনো মিডিয়া ফাইল (ভিডিও/ডকুমেন্ট) নেই।")

    except FloodWait as e:
        await status_msg.edit(f"⚠️ টেলিগ্রাম লিমিট! {e.value} সেকেন্ড পর আবার চেষ্টা করুন।")
    except Exception as e:
        await status_msg.edit(f"❌ এরর: {str(e)}\n\nনিশ্চিত করুন যে আপনার আইডিটি ওই চ্যানেলের মেম্বার।")

print("✅ বট সফলভাবে চালু হয়েছে!")
bot.run()
