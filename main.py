import asyncio
import os
import signal
from flask import Flask
from threading import Thread
from pyrogram import Client, filters
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioPiped
from yt_dlp import YoutubeDL

# --- Flask Server (Render Health Check) ---
web_app = Flask(__name__)

@web_app.route('/')
def home():
    return "Bot is Alive!"

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port)

# --- Fetch Credentials from Render Environment ---
# Render-এর ড্যাশবোর্ডে আপনি যে নামে সেভ করেছেন সেই নামগুলো এখানে দিন
API_ID = int(os.environ.get("API_ID"))
API_HASH = os.environ.get("API_HASH")
SESSION_STRING = os.environ.get("SESSION_STRING")
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# --- Clients Setup ---
app = Client(
    "music_user", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    session_string=SESSION_STRING
)
bot = Client(
    "music_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN
)
call_py = PyTgCalls(app)

# --- YoutubeDL Settings ---
YDL_OPTS = {
    "format": "bestaudio/best",
    "quiet": True,
    "no_warnings": True,
    "cookiefile": "cookies.txt", 
    "nocheckcertificate": True,
    "geo_bypass": True,
}

def get_audio_url(query):
    with YoutubeDL(YDL_OPTS) as ydl:
        if not query.startswith("http"):
            query = f"ytsearch:{query}"
        info = ydl.extract_info(query, download=False)
        if 'entries' in info:
            return info['entries'][0]['url']
        return info['url']

# --- Commands ---
@bot.on_message(filters.command("play") & filters.group)
async def play(client, message):
    if len(message.command) < 2:
        return await message.reply("গানের নাম বা লিঙ্ক দিন। যেমন: /play o priya tui kothay")
    
    query = message.text.split(None, 1)[1]
    m = await message.reply("প্রসেসিং হচ্ছে... ⏳")
    
    try:
        audio_url = get_audio_url(query)
        await call_py.join_group_call(
            message.chat.id,
            AudioPiped(audio_url)
        )
        await m.edit(f"🎶 এখন বাজছে: **{query}**")
    except Exception as e:
        await m.edit(f"❌ এরর: `{str(e)}`")

@bot.on_message(filters.command("stop") & filters.group)
async def stop(client, message):
    try:
        await call_py.leave_group_call(message.chat.id)
        await message.reply("⏹ গান বন্ধ করা হয়েছে।")
    except:
        pass

# --- Execution ---
async def main():
    Thread(target=run_web).start()
    await bot.start()
    await app.start()
    await call_py.start()
    print("Bot is successfully running using Environment Variables!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
