import os
import asyncio
from pyrogram import Client, filters
from pyrogram.errors import FloodWait, RPCError

# --- কনফিগারেশন (Railway Variables থেকে অটোমেটিক নিয়ে নেবে) ---
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
        "আমি রেস্ট্রিক্টেড চ্যানেলের ভিডিও বা ফাইল সেভ করতে পারি।\n"
        "**কীভাবে ব্যবহার করবেন:**\n"
        "১. আমাকে ভিডিওর লিংক পাঠান।\n"
        "২. যদি চ্যানেলটি প্রাইভেট হয়, তবে আমার ইউজারবট আইডিটি ওই চ্যানেলে জয়েন থাকতে হবে।"
    )

@bot.on_message(filters.text & filters.private)
async def handle_link(client, message):
    link = message.text.strip()
    
    if not "t.me/" in link:
        return await message.reply_text("❌ এটি কোনো সঠিক টেলিগ্রাম লিংক নয়।")

    status_msg = await message.reply_text("⏳ প্রসেসিং হচ্ছে... একটু অপেক্ষা করুন।")

    try:
        # লিংক থেকে ডাটা বের করার উন্নত লজিক
        if "t.me/c/" in link:
            # প্রাইভেট চ্যানেলের ক্ষেত্রে (যেমন: t.me/c/12345/678)
            parts = link.split("/")
            chat_id = int("-100" + parts[parts.index("c") + 1])
            msg_id = int(parts[-1].split("?")[0])
        else:
            # পাবলিক চ্যানেলের ক্ষেত্রে (যেমন: t.me/channel_username/123)
            parts = link.split("/")
            chat_id = parts[-2]
            msg_id = int(parts[-1].split("?")[0])

        async with userbot:
            await status_msg.edit("📥 ফাইলটি সংগ্রহ করছি...")
            
            # ভিডিও/ফাইলটি কপি করে সরাসরি ইউজারের কাছে পাঠানো
            await userbot.copy_message(
                chat_id=message.chat.id,
                from_chat_id=chat_id,
                message_id=msg_id
            )
            
        await status_msg.delete()

    except FloodWait as e:
        await status_msg.edit(f"⚠️ টেলিগ্রাম থেকে লিমিট করা হয়েছে। {e.value} সেকেন্ড অপেক্ষা করুন।")
    except Exception as e:
        await status_msg.edit(f"❌ এরর: {str(e)}\n\nনিশ্চিত করুন যে আপনার আইডিটি ওই চ্যানেলের মেম্বার।")

print("✅ বট চালু হয়েছে!")
bot.run()
