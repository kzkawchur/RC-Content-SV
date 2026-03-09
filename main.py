import os
import asyncio
import sys
from pyrogram import Client, filters
from flask import Flask
import threading

# --- Render-এর জন্য ছোট একটি ওয়েব সার্ভার ---
app = Flask(__name__)

@app.route('/')
def health_check():
    return "Bot is Running!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ওয়েব সার্ভারটি আলাদা থ্রেডে চালানো
threading.Thread(target=run_web_server, daemon=True).start()

# --- আপনার আগের বটের বাকি কোড এখান থেকে শুরু ---
from pyrogram.errors import FloodWait, RPCError

# কনফিগারেশন
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")

bot = Client("my_saver_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
userbot = Client("userbot_helper", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)

# ... (বাকি সব হ্যান্ডলার এবং লজিক আগের মতোই থাকবে) ...

print("✅ বট এবং ওয়েব সার্ভার চালু হয়েছে!")
bot.run()
