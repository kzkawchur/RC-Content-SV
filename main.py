import asyncio, html, logging, os, random, re, sqlite3, threading, time
from collections import defaultdict, deque
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional

import colorsys, edge_tts, requests
from flask import Flask, jsonify
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps
from telegram import (BotCommand, InlineKeyboardButton, InlineKeyboardMarkup,
    Message, Update, BotCommandScopeDefault, BotCommandScopeAllPrivateChats,
    BotCommandScopeAllGroupChats, BotCommandScopeAllChatAdministrators)
from telegram.constants import ChatAction, ChatMemberStatus, ParseMode
from telegram.ext import (Application, ApplicationBuilder, CallbackQueryHandler,
    ChatMemberHandler, CommandHandler, ContextTypes, MessageHandler, filters)
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("MayaWelcomeBot")

BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
PORT = int(os.environ.get("PORT","8080"))
DB_PATH = os.environ.get("DB_PATH","maya_welcome_bot.db")
TMP_DIR = Path(os.environ.get("TMP_DIR","/tmp/maya_welcome_bot"))
TMP_DIR.mkdir(parents=True, exist_ok=True)

BOT_NAME = os.environ.get("BOT_NAME","Maya")
TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME","Asia/Dhaka")
SUPPORT_GROUP_NAME = os.environ.get("SUPPORT_GROUP_NAME","Support Group")
SUPPORT_GROUP_URL = os.environ.get("SUPPORT_GROUP_URL","").strip()

VOICE_NAME_BN = os.environ.get("VOICE_NAME_BN","bn-BD-NabanitaNeural")
VOICE_NAME_EN = os.environ.get("VOICE_NAME_EN","en-US-JennyNeural")
VOICE_RATE = os.environ.get("VOICE_RATE","-2%")
VOICE_PITCH = os.environ.get("VOICE_PITCH","+0Hz")
VOICE_VOLUME = os.environ.get("VOICE_VOLUME","+0%")

WELCOME_DELETE_AFTER = int(os.environ.get("WELCOME_DELETE_AFTER","90"))
JOIN_COOLDOWN_SECONDS = int(os.environ.get("JOIN_COOLDOWN_SECONDS","10"))
REJOIN_IGNORE_SECONDS = int(os.environ.get("REJOIN_IGNORE_SECONDS","300"))
HOURLY_INTERVAL_SECONDS = int(os.environ.get("HOURLY_INTERVAL_SECONDS","3600"))
AI_HOURLY_ENABLED = os.environ.get("AI_HOURLY_ENABLED","true").strip().lower()=="true"
GROQ_API_KEY = os.environ.get("GROQ_API_KEY","").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL","llama-3.3-70b-versatile").strip()
GROQ_TIMEOUT_SECONDS = int(os.environ.get("GROQ_TIMEOUT_SECONDS","20"))
AI_BATCH_SIZE = int(os.environ.get("AI_BATCH_SIZE","8"))
AI_MAX_TEXT_LEN = int(os.environ.get("AI_MAX_TEXT_LEN","140"))
WELCOME_QUEUE_MIN_SECONDS = int(os.environ.get("WELCOME_QUEUE_MIN_SECONDS","20"))
WELCOME_QUEUE_MAX_SECONDS = int(os.environ.get("WELCOME_QUEUE_MAX_SECONDS","30"))
KEYWORD_REPLY_ENABLED_DEFAULT = os.environ.get("KEYWORD_REPLY_ENABLED_DEFAULT","true").strip().lower()=="true"
KEYWORD_COOLDOWN_SECONDS = int(os.environ.get("KEYWORD_COOLDOWN_SECONDS","900"))
KEYWORD_USER_COOLDOWN_SECONDS = int(os.environ.get("KEYWORD_USER_COOLDOWN_SECONDS","600"))
KEYWORD_REPLY_CHANCE = float(os.environ.get("KEYWORD_REPLY_CHANCE","0.55"))
HUMAN_DELAY_ENABLED = os.environ.get("HUMAN_DELAY_ENABLED","true").strip().lower()=="true"
FESTIVAL_MODE_DEFAULT = os.environ.get("FESTIVAL_MODE_DEFAULT","true").strip().lower()=="true"
EID_FITR_DATE = os.environ.get("EID_FITR_DATE","").strip()
EID_ADHA_DATE = os.environ.get("EID_ADHA_DATE","").strip()
COUNTDOWN_NOTIFY_WINDOW_DAYS = int(os.environ.get("COUNTDOWN_NOTIFY_WINDOW_DAYS","7"))
SUPER_ADMINS = {int(x.strip()) for x in os.environ.get("SUPER_ADMINS","").split(",") if x.strip().isdigit()}

GROQ_API_KEYS_RAW = os.environ.get("GROQ_API_KEYS","").strip()
GROQ_API_KEYS = [k.strip() for k in GROQ_API_KEYS_RAW.split(",") if k.strip()]
if not GROQ_API_KEYS and GROQ_API_KEY:
    GROQ_API_KEYS = [GROQ_API_KEY]

NAGER_COUNTRY_CODE = (os.environ.get("NAGER_COUNTRY_CODE","BD").strip() or "BD").upper()
ALADHAN_COUNTRY = os.environ.get("ALADHAN_COUNTRY","Bangladesh").strip() or "Bangladesh"
ALADHAN_CITY = os.environ.get("ALADHAN_CITY","Dhaka").strip() or "Dhaka"
FRIDAY_SPECIAL_HOUR = int(os.environ.get("FRIDAY_SPECIAL_HOUR","20"))
MONDAY_SPECIAL_HOUR = int(os.environ.get("MONDAY_SPECIAL_HOUR","9"))
SPECIAL_EVENT_DELETE_AFTER = int(os.environ.get("SPECIAL_EVENT_DELETE_AFTER","0"))

API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"
URLISH_RE = re.compile(r"(https?://|www\.|t\.me/|\+[\w\-]{8,})", re.I)

recent_hourly_by_chat: dict[int, deque] = defaultdict(lambda: deque(maxlen=10))
recent_welcome_keys: dict[str, float] = {}
chat_join_history: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
LAST_GROQ_STATUS: dict = {"configured": bool(GROQ_API_KEYS),"last_ok": None,"last_error": "No check yet","last_checked_at": None,"key_count": len(GROQ_API_KEYS)}
AI_BATCH_CACHE: dict = {}
HOURLY_MOODS = ["peaceful","motivating","classy","cozy","soft","energetic"]
pending_join_members: dict[int, dict] = defaultdict(dict)
pending_join_titles: dict[int, str] = {}
pending_join_tasks: dict[int, asyncio.Task] = {}
keyword_last_chat_at: dict[int, float] = {}
keyword_last_user_at: dict = {}
FALLBACK_CACHE: dict = {}
GROQ_KEY_POINTER = 0
NAGER_YEAR_CACHE: dict = {}
ALADHAN_DAY_CACHE: dict = {}
DAILY_EVENT_MARK_CACHE: dict = {}
group_taste_memory: dict[int, deque] = defaultdict(lambda: deque(maxlen=40))

THEME_NAMES = list(dict.fromkeys([
    "gold","neon","soft-pink","royal-blue","night-glow","lavender","pearl","emerald","ruby","sapphire",
    "sunrise","sunset","moonlight","aurora","rose-gold","midnight","ocean","sky","mint","coral",
    "champagne","violet","crystal","plum","ice-blue","amber","pastel","galaxy","velvet","blush",
    "candy","steel","opal","forest","dream","bronze","silver","dusk","dawn","lotus",
    "mist","flame","sand","berry","wave","gloss","noir","halo","frost","petal",
    "crystal-morning","soft-bloom","velvet-night",
]))
AURA_CORE_THEMES = ["moonlight","rose-gold","velvet-night","crystal-morning","soft-bloom"]
AURA_PHASE_THEMES = {"morning":"crystal-morning","day":"soft-bloom","evening":"rose-gold","night":"moonlight"}
AURA_ALIAS_MAP = {"crystal-morning":"crystal","soft-bloom":"petal","velvet-night":"velvet"}
AURA_PERSONAS = {
    "moonlight":{"footer_bn":["Moonlight hush by Maya","নরম চাঁদের আলোয় Maya","চুপচাপ কোমলতা — Maya"],"footer_en":["Moonlight hush by Maya","Soft moonlit calm by Maya","A quiet glow by Maya"],"hourly_bn":["রাতের মতো নরম থাকুক সময়টা।","চারপাশে একটু শান্তি থাকুক।","মুহূর্তটা যেন মৃদু আর স্থির থাকে।"],"hourly_en":["May the moment stay quiet and gentle.","A little moonlit calm for this group.","Let the mood stay soft and steady."]},
    "rose-gold":{"footer_bn":["Rose Gold glow by Maya","নরম সোনালি আভা — Maya","মোলায়েম আভা — Maya"],"footer_en":["Rose Gold glow by Maya","A warm glow by Maya","Soft golden warmth by Maya"],"hourly_bn":["সময়টা থাকুক উষ্ণ আর মার্জিত।","আজকের vibe হোক একটু উজ্জ্বল আর সুন্দর।","নরম আভায় ভালো থাকুক সবাই।"],"hourly_en":["May the vibe stay warm and elegant.","A soft golden glow for this group.","Wishing everyone a brighter, warmer moment."]},
    "velvet-night":{"footer_bn":["Velvet Night by Maya","গভীর নরম রাত — Maya","গাঢ় শান্তি — Maya"],"footer_en":["Velvet Night by Maya","Deep calm by Maya","A velvet hush by Maya"],"hourly_bn":["গভীর শান্তিতে কাটুক এই সময়টা।","কিছু মুহূর্ত শুধু নীরবতার জন্য থাক।","স্থির আর নরম থাকুক আজকের mood।"],"hourly_en":["Let the mood stay deep, calm, and velvet-soft.","A little deeper calm for everyone here.","May the night feel steady and gentle."]},
    "crystal-morning":{"footer_bn":["Crystal Morning by Maya","স্বচ্ছ সকালের আলো — Maya","উজ্জ্বল কোমল সকাল — Maya"],"footer_en":["Crystal Morning by Maya","Clear morning light by Maya","A bright soft morning by Maya"],"hourly_bn":["সকালের আলোয় মনটাও স্বচ্ছ থাকুক।","উজ্জ্বল শুরু হোক দিনের।","সকালটা হোক হালকা আর নির্মল।"],"hourly_en":["May the morning feel clear and bright.","A fresh beginning for the day.","Let the morning stay light and crystal-soft."]},
    "soft-bloom":{"footer_bn":["Soft Bloom by Maya","নরম ফুলেল ছোঁয়া — Maya","মোলায়েম প্রস্ফুটন — Maya"],"footer_en":["Soft Bloom by Maya","A gentle bloom by Maya","Soft bloom and warmth by Maya"],"hourly_bn":["দিনটা নরম প্রস্ফুটনের মতো কাটুক।","মনটা আজ একটু হালকা থাকুক।","শান্ত উষ্ণতায় ভরে থাকুক সময়টা।"],"hourly_en":["May the day bloom gently.","A soft little moment for everyone here.","Let the hours feel warm and lightly blooming."]},
}
PHASE_LOCKED_MOODS = {"morning":["motivating","soft","peaceful"],"day":["motivating","classy","energetic","soft"],"evening":["cozy","classy","peaceful","soft"],"night":["peaceful","cozy","soft","classy"]}

BN_PHASE_OPENERS = {
    "morning":["🌼 শুভ সকাল সবাইকে।","☀️ সকালের সুন্দর শুভেচ্ছা রইল।","✨ নতুন সকাল মানেই নতুন আলো।","💛 সকালটা হোক কোমল আর সুন্দর।","🌸 মিষ্টি এক সকালের শুভেচ্ছা।","🍃 আজকের সকালটা শান্ত হোক।","🕊️ ভালো একটি সকাল সবার জন্য।","🌤️ আলো ভরা সকাল তোমাদের জন্য।","🌺 সকালের নরম মায়া ছড়িয়ে থাকুক।","💫 আজকের সকালটা হোক আশাবাদী।","🌷 সকালের প্রশান্তি সবার হৃদয়ে থাকুক।","🌞 দিনের শুরুটা হোক সুন্দর।","🍀 শান্ত, পরিষ্কার, সুন্দর এক সকাল।","🌼 ভালো অনুভূতির একটা সকাল রইল।","💐 এই সকালটা হোক হাসিমাখা।"],
    "day":["🌷 দিনের শুভেচ্ছা সবাইকে।","💫 দিনটা যেন সুন্দর কাটে।","🌸 একটু হাসো, একটু ভালো থাকো।","🍀 আজকের দিনটা হোক দারুণ।","🌞 উষ্ণ দিনের শুভেচ্ছা রইল।","✨ নরম এক দিনের শুভেচ্ছা।","🌺 সবার জন্য সুন্দর দিনের বার্তা।","💐 ভালো থাকুক এই group-এর সবাই।","🕊️ দিনের মাঝেও শান্তি থাকুক।","🌿 দিনটা হোক সহজ আর সুন্দর।","🌻 আজকের সময়টা হোক ইতিবাচক।","💛 এই দিনে থাকুক মমতা।","🍃 স্বস্তির একটি দিন সবার জন্য।","🌸 ছোট্ট একটু উষ্ণতা ছড়িয়ে দিই।","✨ ভালো vibe থাকুক চারদিকে।"],
    "evening":["🌙 শুভ সন্ধ্যা সবাইকে।","✨ সন্ধ্যার নরম শুভেচ্ছা রইল।","🌆 আজকের সন্ধ্যাটা হোক মিষ্টি।","💜 শান্ত এক সন্ধ্যার শুভেচ্ছা।","🕯️ সন্ধ্যার আলোয় ভালোবাসা রইল।","🌃 নরম সন্ধ্যার শুভেচ্ছা সবাইকে।","🍂 সন্ধ্যাটা হোক আরামদায়ক।","💫 ক্লান্তি ভুলে একটু ভালো থাকো।","🌸 সন্ধ্যার ছোঁয়ায় মন শান্ত থাকুক।","🌷 এই সন্ধ্যা হোক মোলায়েম আর সুন্দর।","💐 সবার জন্য নরম এক সন্ধ্যা।","🌟 আলো-আঁধারির শুভেচ্ছা রইল।","🍃 দিনশেষের সময়টা হোক শান্ত।","🕊️ শান্ত সন্ধ্যার বার্তা সবার জন্য।","✨ আজকের সন্ধ্যা হোক স্বস্তিদায়ক।"],
    "night":["🌌 শুভ রাত্রি সবাইকে।","⭐ রাতের শান্ত শুভেচ্ছা রইল।","💙 আজকের রাতটা হোক শান্ত।","🌙 মিষ্টি এক রাতের শুভেচ্ছা।","🕊️ নীরব রাতের কোমল শুভেচ্ছা।","✨ রাতের শেষে ভালো থেকো সবাই।","🌠 আরামদায়ক একটি রাত কামনা করি।","💫 সবার জন্য শান্ত রাতের বার্তা।","🌸 শান্ত ঘুমের শুভেচ্ছা রইল।","🍀 আজকের রাতটা হোক নির্ভার।","💐 নরম এক শুভ রাত্রি সবার জন্য।","🌌 মায়াময় রাতের শুভেচ্ছা রইল।","💛 আজ রাতেও মনটা থাকুক হালকা।","🌷 শান্তির একটি রাত সবার জন্য।","✨ রাতটা হোক আরাম আর স্বস্তিতে ভরা।"],
}
BN_MIDDLES=["এই group-এর সবার জন্য অনেক শুভকামনা।","একটু হাসো, একটু স্বস্তিতে থাকো।","নিজের মনটাকে আজ একটু হালকা রাখো।","আশা করি সময়টা তোমাদের ভালো কাটছে।","সবাই যেন সুন্দর আর নিরাপদে থাকো।","দিনের ভিড়ে মনটাও যেন সুন্দর থাকে।","মনে রাখো, শান্ত থাকাও একধরনের শক্তি।","আজও ভালো কিছুর অপেক্ষা থাকুক।","সুন্দর কথা, সুন্দর মন—দুটোই জরুরি।","ভালো vibes ছড়িয়ে দাও চারদিকে।","নিজেকে একটু যত্নে রাখো।","ক্লান্তি থাকলেও মনটা নরম থাকুক।","সবার জীবনে একটু করে আলো থাকুক।","ভালো থাকার ছোট্ট কারণও অনেক মূল্যবান।","আজও মনের ভেতর শান্তি থাকুক।","নিজের প্রতি কোমল থেকো।","সুন্দর অনুভূতির জন্য বড় কারণ লাগে না।","স্বস্তির একটু সময় সবাই পাক।","এই group-এ ভালো vibe সবসময় থাকুক।","নরম, সুন্দর, ভদ্র energy ছড়িয়ে থাকুক।"]
BN_ENDINGS=["🌷 ভালো থাকো সবাই।","💫 সুন্দর থাকো সবাই।","🌼 হাসিখুশি থাকো সবাই।","💙 শান্তিতে থাকো সবাই।","✨ হৃদয়টা নরম আর সুন্দর থাকুক।","🕊️ মনটা হোক হালকা আর শান্ত।","🌸 তোমাদের সবার জন্য রইল শুভেচ্ছা।","🍀 সুন্দর সময় কাটুক সবার।","💐 শান্তি থাকুক চারপাশে।","🌙 মনটা থাকুক প্রশান্ত।"]
EN_PHASE_OPENERS = {
    "morning":["🌼 Good morning everyone.","☀️ A gentle morning hello to all of you.","✨ Wishing this group a soft and beautiful morning.","💛 Hope your morning feels light and peaceful.","🌸 Sending warm morning wishes to everyone.","🍃 May this morning begin softly for you all.","🕊️ A calm and lovely morning to this group.","🌤️ Bright morning wishes to everyone here.","🌺 A graceful morning note for everyone.","💫 Hope today begins with a little peace.","🌷 Wishing you all a warm morning.","🌞 A clear and kind morning to this group.","🍀 May this morning feel easy and bright.","💐 A sweet little morning message for all.","✨ Gentle morning vibes to everyone here."],
    "day":["🌷 Hope everyone is having a good day.","💫 Sending warm daytime vibes to this group.","🌸 A little beautiful message for your day.","🍀 Wishing everyone a smooth and lovely day.","🌞 Daytime wishes to all of you.","✨ Hope today feels a little softer and brighter.","🌺 Sending kindness across the group today.","💐 A warm little note for everyone here.","🕊️ Wishing everyone calm energy today.","🌿 May the day stay gentle and kind.","🌻 A soft little daytime greeting for all.","💛 Hope the day brings something lovely.","🍃 Sending fresh and peaceful energy.","🌸 Warm thoughts for everyone in this group.","✨ May your day keep flowing beautifully."],
    "evening":["🌙 Good evening everyone.","✨ Sending peaceful evening wishes to this group.","🌆 Hope your evening feels calm and gentle.","💜 A soft evening hello to all of you.","🕯️ Wishing everyone a lovely evening.","🌃 Evening warmth to this beautiful group.","🍂 Hope the evening brings a little peace.","💫 A gentle evening message for everyone here.","🌸 Let the evening feel soft and easy.","🌷 Sending warm evening comfort to all.","💐 A calm evening note for this group.","🌟 May your evening feel graceful and light.","🍃 Rest a little and breathe gently.","🕊️ A peaceful evening vibe to everyone.","✨ Wishing you all a beautiful sunset mood."],
    "night":["🌌 Good night everyone.","⭐ Sending calm night wishes to all of you.","💙 Hope your night feels peaceful and restful.","🌙 A soft night message for this group.","🕊️ Wishing everyone a gentle and quiet night.","✨ End the day with a little peace.","🌠 Warm night wishes to everyone here.","💫 A peaceful close to the day for all of you.","🌸 A soft good night to this lovely group.","🍀 May your night feel light and easy.","💐 Wishing comfort and calm to everyone.","🌌 Let the night wrap you in peace.","💛 A gentle good night to all.","🌷 Wishing you all a restful night.","✨ May your mind feel settled tonight."],
}
EN_MIDDLES=["Wishing this group a little more peace and softness.","Hope your heart feels a little lighter today.","Take a small moment to breathe and smile.","May your day carry a little extra kindness.","Sending good energy to everyone here.","Hope things feel a bit easier and brighter.","A small warm message can change a day.","Keep your heart gentle and your mind steady.","You all deserve a peaceful moment today.","May this group stay kind, calm, and warm.","Let this be a reminder to slow down softly.","A little grace can brighten any hour.","Hope your thoughts feel clear and calm.","Sending a soft note of comfort to everyone.","Wishing each of you a beautiful little pause.","Peaceful vibes can make a big difference.","May something good find you today.","Keep your energy warm and elegant.","Gentle moments are worth holding onto.","You are allowed to move through the day softly."]
EN_ENDINGS=["🌷 Stay well, everyone.","💫 Stay beautiful, everyone.","🌼 Wishing you comfort and peace.","💙 Take care, everyone.","✨ Keep your vibe soft and bright.","🕊️ May your mind feel calm.","🌸 Warm wishes to all of you.","🍀 Hope the rest of your time feels lovely.","💐 Sending light and warmth to all.","🌙 Wishing you a peaceful heart."]
BN_MOOD_MIDDLES={"peaceful":["মনটা আজ একটু শান্ত আর নরম থাকুক।","শান্তির একটু ছোঁয়া থাকুক সবার ভেতর।","আজকের সময়টা হোক মোলায়েম আর স্বস্তির।"],"motivating":["আজও ভালো কিছুর জন্য এগিয়ে যাও।","ছোট্ট করে হলেও এগিয়ে থাকো।","মনোবলটা ধরে রাখো, ভালো কিছু অপেক্ষায় আছে।"],"classy":["ভদ্রতা আর সৌন্দর্য একসাথেই থাকুক।","নরম, পরিপাটি আর সুন্দর energy থাকুক চারদিকে।","আজকের vibe হোক classy আর refined।"],"cozy":["স্বস্তির ছোট্ট একটা কোণ খুঁজে নাও আজ।","মনটাকে একটু আরাম দাও।","আরামদায়ক, উষ্ণ একটা অনুভূতি থাকুক।"],"soft":["কথা আর মন—দুটোই থাকুক কোমল।","আজ একটু নরম থেকো নিজের প্রতিও।","হালকা, শান্ত, মিষ্টি একটা সময় কাটুক।"],"energetic":["আজকের সময়টা হোক প্রাণবন্ত।","ভালো vibe নিয়ে এগিয়ে যাও সবাই।","চারদিকে থাকুক চনমনে একটা অনুভূতি।"]}
EN_MOOD_MIDDLES={"peaceful":["May your mind feel a little calmer today.","A softer and quieter vibe for everyone here.","Let this hour feel gentle and peaceful."],"motivating":["Keep moving forward with quiet confidence.","A little progress still matters today.","Hold your energy steady and keep going."],"classy":["May the vibe stay elegant and refined.","A graceful little note for this lovely group.","Let the mood stay polished and warm."],"cozy":["Hope this hour feels warm and comforting.","Take a small cozy pause for yourself.","Wishing everyone a softer, warmer moment."],"soft":["Keep your words and heart gentle today.","May this moment feel light and tender.","A soft little reminder to breathe and smile."],"energetic":["Hope this hour feels bright and alive.","Sending a lively and positive mood to everyone.","Keep the energy fresh and uplifting."]}
KEYWORD_REPLIES={"bn":{"salam":["ওয়ালাইকুমুস সালাম 🌷 সবাই ভালো থাকুন।","ওয়ালাইকুমুস সালাম ✨ সবার জন্য শুভেচ্ছা।","ওয়ালাইকুমুস সালাম 🌸 শান্তি থাকুক সবার মাঝে।"],"hello":["হ্যালো সবাই 🌼 সুন্দর সময় কাটুক।","সবাইকে মিষ্টি শুভেচ্ছা ✨","হাই সবাই 🌷 group-এ ভালো vibe থাকুক।"],"night":["শুভ রাত্রি 🌙 শান্তিতে থাকুন সবাই।","মিষ্টি এক রাত কাটুক সবার 💙","রাতটা হোক শান্ত আর স্বস্তির 🌌"]},"en":{"salam":["Wa alaikum assalam 🌷 warm wishes to everyone.","Wa alaikum assalam ✨ peace to everyone here.","Wa alaikum assalam 🌸 wishing the group calm and warmth."],"hello":["Hello everyone 🌼 hope you're all doing well.","Hi everyone ✨ warm little wishes to the group.","Hey everyone 🌷 hope the vibe stays lovely here."],"night":["Good night 🌙 wishing everyone a peaceful rest.","Have a calm and gentle night 💙","Wishing the group a soft night ahead 🌌"]}}
PHASE_BLOCKLIST={"bn":{"morning":("রাত","রাত্রি","শুভ রাত্রি","শুভ সন্ধ্যা"),"day":("শুভ সকাল","সকালের","ভোর","রাত","রাত্রি","শুভ সন্ধ্যা"),"evening":("শুভ সকাল","সকালের","ভোর","শুভ রাত্রি","রাতের"),"night":("শুভ সকাল","সকালের","ভোর","দুপুর","বিকাল","শুভ সন্ধ্যা")},"en":{"morning":("good night","night","evening"),"day":("good morning","morning","good night","night","evening"),"evening":("good morning","morning","good night","night"),"night":("good morning","morning","afternoon","daytime","good evening","evening")}}
WEAK_GENERIC_PHRASES={"bn":{"উজ্জ্বল থাকুন","সুখী থাকুন","শুভ সকাল","শুভ সন্ধ্যা","শুভ রাত্রি","ভালো থাকুন","আশা সবুজ থাকুক","সুরেলা দিন কাটুক"},"en":{"stay bright","stay happy","good morning","good evening","good night","stay well","be happy"}}
TEXTS = {
    "bn":{"start_private":["আমি {bot} 🌸\n\nCommands:\n/ping\n/myid\n/support\n/aistatus\n/broadcast <text>\n\nGroup-এ add করলে auto কাজ শুরু করব। Admin /lang, /voice, /deleteservice, /hourly ব্যবহার করতে পারবে।","{bot} ready 🌷\n\nAdmin /lang bn বা /lang en, /hourly on/off, /voice on/off দিতে পারবে।"],"start_group":["{bot} ready for this group 🌸\nPremium welcome, voice আর hourly text handle করব।","{bot} এই group-এ ready 🌷"],"only_group_admin":["Only group admins can use this command.","এই command শুধু group admin ব্যবহার করতে পারবে।"],"lang_usage":["Usage:\n/lang bn\n/lang en"],"lang_set_bn":["ঠিক আছে, এখন থেকে আমি বাংলায় কথা বলব।","Language changed to বাংলা."],"lang_set_en":["Okay, I will speak in English now.","Language changed to English."],"voice_usage":["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],"voice_set":["Voice welcome: {value}","ঠিক আছে, voice welcome এখন {value}।"],"deleteservice_usage":["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],"deleteservice_set":["Delete service message: {value}"],"hourly_usage":["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],"hourly_set":["Hourly text: {value}"],"hourly_now":["এখনই একটি premium hourly message পাঠালাম।"],"welcome_saved":["Custom welcome text save হয়ে গেছে।"],"welcome_reset":["Custom welcome reset করা হয়েছে।"],"status":["Bot: {bot}\nLanguage: {lang_name}\nVoice: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTimezone: {tz}\nPhase: {phase}"],"aistatus":["Groq configured: {configured}\nAI hourly: {enabled}\nLast check: {checked}\nResult: {result}\nModel: {model}"],"broadcast_owner_only":["Broadcast is owner-only."],"broadcast_none":["No groups found."],"broadcast_start":["Broadcast started to {count} groups..."],"broadcast_done":["Broadcast done.\nSuccess: {ok}\nFailed: {fail}"],"welcome_voice_caption":["🎤 {bot} welcome voice"],"ping":["pong | {tz} | {time}"],"myid":["Your user ID: {user_id}"],"support":["Support: {support}"],"burst_compact":["🌸 {name}, তোমাকে {group} এ স্বাগতম।","✨ {name}, {group} এ উষ্ণ স্বাগতম।","💫 {name}, {group} এ তোমাকে আন্তরিক শুভেচ্ছা।","🌷 {name}, {group} এ তোমাকে পেয়ে groupটা আরও সুন্দর।"],"setvoice_usage":["Usage:\n/setvoice bd\n/setvoice in\n\nCurrent: {current}"],"setvoice_set":["Voice changed to: {value}"],"analytics":["Welcomes: {welcomes}\nHourly: {hourly}\nAI: {ai}\nFallback: {fallback}\nLast welcome: {welcome}\nVoice: {voice}"]},
    "en":{"start_private":["I am {bot} 🌸\n\nOnce added to a group I work automatically. Admins can use /lang, /voice, /deleteservice, /hourly.","{bot} is ready 🌷"],"start_group":["{bot} is ready for this group 🌸","{bot} is now ready in this group 🌷"],"only_group_admin":["Only group admins can use this command."],"lang_usage":["Usage:\n/lang bn\n/lang en"],"lang_set_bn":["Language changed to Bangla."],"lang_set_en":["Language changed to English."],"voice_usage":["Usage:\n/voice on\n/voice off\n\nCurrent: {current}"],"voice_set":["Voice welcome: {value}"],"deleteservice_usage":["Usage:\n/deleteservice on\n/deleteservice off\n\nCurrent: {current}"],"deleteservice_set":["Delete service message: {value}"],"hourly_usage":["Usage:\n/hourly on\n/hourly off\n/hourly now\n\nCurrent: {current}"],"hourly_set":["Hourly text: {value}"],"hourly_now":["I just sent a premium hourly message."],"welcome_saved":["Custom welcome text saved successfully."],"welcome_reset":["Custom welcome has been reset."],"status":["Bot: {bot}\nLanguage: {lang_name}\nVoice: {voice}\nDelete service: {delete_service}\nHourly: {hourly}\nTimezone: {tz}\nPhase: {phase}"],"aistatus":["Groq configured: {configured}\nAI hourly: {enabled}\nLast check: {checked}\nResult: {result}\nModel: {model}"],"broadcast_owner_only":["Broadcast is owner-only."],"broadcast_none":["No groups found."],"broadcast_start":["Broadcast started to {count} groups..."],"broadcast_done":["Broadcast done.\nSuccess: {ok}\nFailed: {fail}"],"welcome_voice_caption":["🎤 {bot} welcome voice"],"ping":["pong | {tz} | {time}"],"myid":["Your user ID: {user_id}"],"support":["Support: {support}"],"burst_compact":["🌸 {name}, welcome to {group}.","✨ {name}, warm welcome to {group}.","💫 {name}, a heartfelt welcome to {group}.","🌷 {name}, glad to see you in {group}."],"setvoice_usage":["Usage:\n/setvoice bd\n/setvoice in\n\nCurrent: {current}"],"setvoice_set":["Voice changed to: {value}"],"analytics":["Welcomes: {welcomes}\nHourly: {hourly}\nAI: {ai}\nFallback: {fallback}\nLast welcome: {welcome}\nVoice: {voice}"]},
}

def t(lang,key,**kw):
    lang=lang if lang in TEXTS else "bn"
    arr=TEXTS[lang].get(key) or TEXTS["bn"].get(key) or [key]
    return random.choice(arr).format(bot=BOT_NAME,support=support_text(),**kw)

flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return f"<h2>🌸 {BOT_NAME} is running!</h2><p>Status: Online</p>", 200

@flask_app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "bot": BOT_NAME,
        "groq_configured": bool(GROQ_API_KEYS),
        "ai_hourly_enabled": AI_HOURLY_ENABLED,
        "version": "v10",
    })

@flask_app.get("/ping")
def flask_ping():
    return "pong", 200

def run_flask():
    # Log the public URL so it appears in Render logs
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if render_url:
        logger.info("🌐 Public URL: %s", render_url)
        logger.info("🔗 Health URL: %s/health", render_url)
        logger.info("📌 Use this URL in UptimeRobot to keep bot alive: %s/ping", render_url)
    else:
        logger.info("🌐 Flask starting on http://0.0.0.0:%s", PORT)
        logger.info("📌 Set RENDER_EXTERNAL_URL env var for public URL logging")
    flask_app.run(host="0.0.0.0", port=PORT, threaded=True)

def self_ping_loop():
    """Pings own /ping endpoint every 10 minutes to prevent Render spin-down."""
    import time as _time
    _time.sleep(60)  # Wait for Flask to start
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if not render_url:
        logger.info("Self-ping disabled (RENDER_EXTERNAL_URL not set)")
        return
    ping_url = f"{render_url}/ping"
    logger.info("🔄 Self-ping loop started → %s", ping_url)
    while True:
        try:
            resp = requests.get(ping_url, timeout=10)
            logger.info("🏓 Self-ping OK (%d)", resp.status_code)
        except Exception as e:
            logger.warning("🏓 Self-ping failed: %s", e)
        _time.sleep(600)  # Every 10 minutes

def tg_post(method,payload):
    try:
        resp=requests.post(f"{API_BASE}/{method}",json=payload,timeout=30)
        data=resp.json()
        logger.info("Telegram POST %s -> %s",method,str(data)[:500])
        return data
    except Exception:
        logger.exception("tg_post failed: %s",method)
        return {"ok":False}

def delete_webhook(): tg_post("deleteWebhook",{"drop_pending_updates":False})

def db_connect():
    conn=sqlite3.connect(DB_PATH,check_same_thread=False)
    conn.row_factory=sqlite3.Row
    return conn

def init_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS groups (chat_id INTEGER PRIMARY KEY,title TEXT,enabled INTEGER NOT NULL DEFAULT 1,language TEXT NOT NULL DEFAULT 'bn',custom_welcome TEXT,voice_enabled INTEGER NOT NULL DEFAULT 1,delete_service INTEGER NOT NULL DEFAULT 1,hourly_enabled INTEGER NOT NULL DEFAULT 1,voice_choice TEXT NOT NULL DEFAULT 'bd',total_welcome_sent INTEGER NOT NULL DEFAULT 0,total_hourly_sent INTEGER NOT NULL DEFAULT 0,last_ai_success_at INTEGER NOT NULL DEFAULT 0,last_fallback_used_at INTEGER NOT NULL DEFAULT 0,last_welcome_at INTEGER NOT NULL DEFAULT 0,last_milestone_sent INTEGER NOT NULL DEFAULT 0,welcome_style TEXT NOT NULL DEFAULT 'auto',footer_text TEXT NOT NULL DEFAULT '',last_primary_msg_id INTEGER,last_voice_msg_id INTEGER,last_hourly_at INTEGER NOT NULL DEFAULT 0,updated_at INTEGER NOT NULL DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS join_memory (chat_id INTEGER NOT NULL,user_id INTEGER NOT NULL,joined_at INTEGER NOT NULL,PRIMARY KEY (chat_id,user_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS ai_generated (id INTEGER PRIMARY KEY AUTOINCREMENT,lang TEXT NOT NULL,phase TEXT NOT NULL,source TEXT NOT NULL,text TEXT NOT NULL,created_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS failure_logs (id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,chat_id INTEGER,title TEXT,error TEXT NOT NULL,created_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS countdowns (chat_id INTEGER PRIMARY KEY,title TEXT NOT NULL,target_ts INTEGER NOT NULL,event_type TEXT NOT NULL DEFAULT 'event',last_sent_day TEXT NOT NULL DEFAULT '',created_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS rps_games (game_id TEXT PRIMARY KEY,chat_id INTEGER NOT NULL,message_id INTEGER,creator_id INTEGER NOT NULL,creator_name TEXT NOT NULL,player1_id INTEGER NOT NULL,player1_name TEXT NOT NULL,player2_id INTEGER,player2_name TEXT,mode TEXT NOT NULL,p1_choice TEXT,p2_choice TEXT,status TEXT NOT NULL,winner TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scheduled_events (chat_id INTEGER NOT NULL,event_kind TEXT NOT NULL,title TEXT NOT NULL,target_ts INTEGER NOT NULL,last_sent_day TEXT NOT NULL DEFAULT '',created_at INTEGER NOT NULL,PRIMARY KEY (chat_id,event_kind))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_event_marks (chat_id INTEGER NOT NULL,event_key TEXT NOT NULL,day_key TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY (chat_id,event_key,day_key))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS sent_text_history (id INTEGER PRIMARY KEY AUTOINCREMENT,chat_id INTEGER NOT NULL,kind TEXT NOT NULL,text_norm TEXT NOT NULL,signature TEXT NOT NULL,created_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS xo_games (game_id TEXT PRIMARY KEY,chat_id INTEGER NOT NULL,message_id INTEGER,creator_id INTEGER NOT NULL,creator_name TEXT NOT NULL,player_x_id INTEGER NOT NULL,player_x_name TEXT NOT NULL,player_o_id INTEGER,player_o_name TEXT,mode TEXT NOT NULL,board TEXT NOT NULL,turn TEXT NOT NULL,status TEXT NOT NULL,winner TEXT,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS luckybox_rounds (game_id TEXT PRIMARY KEY,chat_id INTEGER NOT NULL,message_id INTEGER,creator_id INTEGER NOT NULL,creator_name TEXT NOT NULL,status TEXT NOT NULL,winning_box INTEGER NOT NULL,winner_id INTEGER,winner_name TEXT,total_boxes INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS luckybox_plays (id INTEGER PRIMARY KEY AUTOINCREMENT,game_id TEXT NOT NULL,user_id INTEGER NOT NULL,user_name TEXT NOT NULL,box_index INTEGER NOT NULL,result_kind TEXT NOT NULL,result_text TEXT NOT NULL,created_at INTEGER NOT NULL,UNIQUE(game_id,user_id),UNIQUE(game_id,box_index))""")
        existing_cols={row[1] for row in conn.execute("PRAGMA table_info(groups)").fetchall()}
        for col,ddl in {"voice_choice":"TEXT NOT NULL DEFAULT 'bd'","total_welcome_sent":"INTEGER NOT NULL DEFAULT 0","total_hourly_sent":"INTEGER NOT NULL DEFAULT 0","last_ai_success_at":"INTEGER NOT NULL DEFAULT 0","last_fallback_used_at":"INTEGER NOT NULL DEFAULT 0","last_welcome_at":"INTEGER NOT NULL DEFAULT 0","last_milestone_sent":"INTEGER NOT NULL DEFAULT 0","welcome_style":"TEXT NOT NULL DEFAULT 'auto'","footer_text":"TEXT NOT NULL DEFAULT ''","hourly_delete_after":"INTEGER NOT NULL DEFAULT 0","festival_mode":"INTEGER NOT NULL DEFAULT 1","keyword_replies_enabled":"INTEGER NOT NULL DEFAULT 1","mood_index":"INTEGER NOT NULL DEFAULT 0","last_presence_at":"INTEGER NOT NULL DEFAULT 0","message_taste":"TEXT NOT NULL DEFAULT 'auto'","variant_cursor":"INTEGER NOT NULL DEFAULT 0","msg_min_len":"INTEGER NOT NULL DEFAULT 0","msg_max_len":"INTEGER NOT NULL DEFAULT 0","msg_limit_action":"TEXT NOT NULL DEFAULT 'delete'"}.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE groups ADD COLUMN {col} {ddl}")
        conn.commit()

def ensure_group(chat_id,title):
    now_ts=int(time.time())
    with db_connect() as conn:
        conn.execute("INSERT INTO groups (chat_id,title,enabled,updated_at,last_hourly_at) VALUES (?,?,1,?,0) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title,updated_at=excluded.updated_at",(chat_id,title or "",now_ts))
        conn.commit()

def get_group(chat_id):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE chat_id=?",(chat_id,)).fetchone()

def get_group_lang(chat_id):
    row=get_group(chat_id)
    lang=((row["language"] if row else "bn") or "bn").strip().lower()
    return lang if lang in {"bn","en"} else "bn"

def set_group_value(chat_id,field,value):
    allowed={"title","language","custom_welcome","voice_enabled","delete_service","hourly_enabled","enabled","voice_choice","total_welcome_sent","total_hourly_sent","last_ai_success_at","last_fallback_used_at","last_welcome_at","last_milestone_sent","welcome_style","footer_text","hourly_delete_after","festival_mode","keyword_replies_enabled","mood_index","last_primary_msg_id","last_voice_msg_id","last_hourly_at","updated_at","last_presence_at","message_taste","variant_cursor","msg_min_len","msg_max_len","msg_limit_action"}
    if field not in allowed: raise ValueError("Invalid field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field}=? WHERE chat_id=?",(value,chat_id))
        conn.commit()

def increment_group_counter(chat_id,field,amount=1):
    if field not in {"total_welcome_sent","total_hourly_sent"}: raise ValueError("Invalid counter field")
    with db_connect() as conn:
        conn.execute(f"UPDATE groups SET {field}=COALESCE({field},0)+?,updated_at=? WHERE chat_id=?",(amount,int(time.time()),chat_id))
        conn.commit()

def get_enabled_groups_for_hourly():
    now_ts=int(time.time())
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE enabled=1 AND hourly_enabled=1 AND (?-last_hourly_at)>=? ORDER BY updated_at DESC",(now_ts,HOURLY_INTERVAL_SECONDS)).fetchall()

def get_all_enabled_groups():
    with db_connect() as conn:
        return [int(r["chat_id"]) for r in conn.execute("SELECT chat_id FROM groups WHERE enabled=1").fetchall()]

def get_all_enabled_group_rows():
    with db_connect() as conn:
        return conn.execute("SELECT * FROM groups WHERE enabled=1").fetchall()

def get_last_join_time(chat_id,user_id):
    with db_connect() as conn:
        row=conn.execute("SELECT joined_at FROM join_memory WHERE chat_id=? AND user_id=?",(chat_id,user_id)).fetchone()
        return int(row["joined_at"]) if row else 0

def save_join_time(chat_id,user_id):
    with db_connect() as conn:
        conn.execute("INSERT INTO join_memory (chat_id,user_id,joined_at) VALUES (?,?,?) ON CONFLICT(chat_id,user_id) DO UPDATE SET joined_at=excluded.joined_at",(chat_id,user_id,int(time.time())))
        conn.commit()

def save_generated_text(lang,phase,source,text):
    with db_connect() as conn:
        conn.execute("INSERT INTO ai_generated (lang,phase,source,text,created_at) VALUES (?,?,?,?,?)",(lang,phase,source,text[:300],int(time.time())))
        conn.commit()

def record_failure(kind,chat_id,title,error):
    with db_connect() as conn:
        conn.execute("INSERT INTO failure_logs (kind,chat_id,title,error,created_at) VALUES (?,?,?,?,?)",(kind[:32],chat_id,(title or "")[:120],(error or "")[:500],int(time.time())))
        conn.commit()

def count_known_groups():
    with db_connect() as conn:
        return int(conn.execute("SELECT COUNT(*) c FROM groups").fetchone()["c"] or 0)

def get_active_groups(limit=20):
    with db_connect() as conn:
        return conn.execute("SELECT chat_id,title,updated_at FROM groups WHERE enabled=1 ORDER BY updated_at DESC LIMIT ?",(limit,)).fetchall()

def get_recent_failed_groups(limit=15):
    with db_connect() as conn:
        return conn.execute("SELECT chat_id,title,MAX(created_at) AS last_time,COUNT(*) AS fail_count FROM failure_logs WHERE kind IN ('send_message','send_photo','send_voice','broadcast') GROUP BY chat_id,title ORDER BY last_time DESC LIMIT ?",(limit,)).fetchall()

def get_recent_ai_errors(limit=10):
    with db_connect() as conn:
        return conn.execute("SELECT error,created_at FROM failure_logs WHERE kind='ai' ORDER BY created_at DESC LIMIT ?",(limit,)).fetchall()

def get_countdown(chat_id):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM countdowns WHERE chat_id=?",(chat_id,)).fetchone()

def set_countdown(chat_id,title,target_ts,event_type):
    with db_connect() as conn:
        conn.execute("INSERT INTO countdowns (chat_id,title,target_ts,event_type,last_sent_day,created_at) VALUES (?,?,?,?,'',?) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title,target_ts=excluded.target_ts,event_type=excluded.event_type,created_at=excluded.created_at",(chat_id,title[:80],int(target_ts),event_type[:24],int(time.time())))
        conn.commit()

def clear_countdown(chat_id):
    with db_connect() as conn:
        conn.execute("DELETE FROM countdowns WHERE chat_id=?",(chat_id,))
        conn.commit()

def update_countdown_last_sent_day(chat_id,day_key):
    with db_connect() as conn:
        conn.execute("UPDATE countdowns SET last_sent_day=? WHERE chat_id=?",(day_key,chat_id))
        conn.commit()

def set_scheduled_event(chat_id,event_kind,title,target_ts):
    with db_connect() as conn:
        conn.execute("INSERT INTO scheduled_events (chat_id,event_kind,title,target_ts,last_sent_day,created_at) VALUES (?,?,?,?,'',?) ON CONFLICT(chat_id,event_kind) DO UPDATE SET title=excluded.title,target_ts=excluded.target_ts,last_sent_day='',created_at=excluded.created_at",(chat_id,event_kind,title[:90],target_ts,int(time.time())))
        conn.commit()

def get_scheduled_event(chat_id,event_kind):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM scheduled_events WHERE chat_id=? AND event_kind=?",(chat_id,event_kind)).fetchone()

def clear_scheduled_event(chat_id,event_kind):
    with db_connect() as conn:
        conn.execute("DELETE FROM scheduled_events WHERE chat_id=? AND event_kind=?",(chat_id,event_kind))
        conn.commit()

def mark_daily_event_sent(chat_id,event_key,day_key):
    with db_connect() as conn:
        conn.execute("INSERT OR IGNORE INTO daily_event_marks (chat_id,event_key,day_key,created_at) VALUES (?,?,?,?)",(chat_id,event_key,day_key,int(time.time())))
        conn.commit()
    DAILY_EVENT_MARK_CACHE[(chat_id,event_key,day_key)]=time.time()

def was_daily_event_sent(chat_id,event_key,day_key):
    if (chat_id,event_key,day_key) in DAILY_EVENT_MARK_CACHE: return True
    with db_connect() as conn:
        return bool(conn.execute("SELECT 1 FROM daily_event_marks WHERE chat_id=? AND event_key=? AND day_key=?",(chat_id,event_key,day_key)).fetchone())

def cleanup_daily_marks():
    now_ts = int(time.time())
    with db_connect() as conn:
        # Daily event marks older than 60 days
        conn.execute("DELETE FROM daily_event_marks WHERE created_at<?", (now_ts - 86400*60,))
        # Sent text history older than 7 days (prevent unbounded growth)
        conn.execute("DELETE FROM sent_text_history WHERE created_at<?", (now_ts - 86400*7,))
        # Failure logs older than 30 days
        conn.execute("DELETE FROM failure_logs WHERE created_at<?", (now_ts - 86400*30,))
        # AI generated older than 14 days
        conn.execute("DELETE FROM ai_generated WHERE created_at<?", (now_ts - 86400*14,))
        conn.commit()

def normalize_history_text(text):
    s=re.sub(r"\s+"," ",(text or "")).strip().lower()
    return s[:260]

def structure_signature(text):
    raw=normalize_history_text(text)
    if not raw: return "empty"
    words=raw.split()
    lb="s" if len(words)<=6 else "m" if len(words)<=13 else "l"
    sb=str(max(1,len([x for x in re.split(r"[\.!?।]+",raw) if x.strip()])))
    starter="other"
    for name,pat in [("greet",r"^(শুভ|hello|hi|good|warm|soft|gentle|calm|peaceful)\b"),("wish",r"^(আশা|wishing|hope|may|আজ|today|এই)\b"),("group",r"^(এই group|this group|সবাইকে|everyone)\b")]:
        if re.search(pat,raw,re.I): starter=name; break
    emoji="e1" if re.match(r"^[^\w\s]",text or "") else "e0"
    punct="q" if "?" in raw else "x" if "!" in raw else "d"
    return f"{starter}|{lb}|{sb}|{emoji}|{punct}"

def was_recent_duplicate_text(chat_id,kind,text,lookback_days=3):
    since=int(time.time())-(lookback_days*86400)
    text_norm=normalize_history_text(text)
    sig=structure_signature(text)
    with db_connect() as conn:
        return bool(conn.execute("SELECT 1 FROM sent_text_history WHERE chat_id=? AND kind=? AND created_at>=? AND (text_norm=? OR signature=?) LIMIT 1",(chat_id,kind,since,text_norm,sig)).fetchone())

def record_sent_history(chat_id,kind,text):
    text_norm=normalize_history_text(text)
    if not text_norm: return
    with db_connect() as conn:
        conn.execute("INSERT INTO sent_text_history (chat_id,kind,text_norm,signature,created_at) VALUES (?,?,?,?,?)",(chat_id,kind[:24],text_norm,structure_signature(text),int(time.time())))
        conn.commit()

def format_ts(ts):
    if not ts: return "Never"
    return datetime.fromtimestamp(int(ts),ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d %I:%M:%S %p")

def support_text():
    if SUPPORT_GROUP_URL and SUPPORT_GROUP_NAME: return f"{SUPPORT_GROUP_NAME} | {SUPPORT_GROUP_URL}"
    return SUPPORT_GROUP_URL or SUPPORT_GROUP_NAME

def local_now(): return datetime.now(ZoneInfo(TIMEZONE_NAME))

def phase_now():
    h=local_now().hour
    if 5<=h<12: return "morning"
    if 12<=h<17: return "day"
    if 17<=h<20: return "evening"
    return "night"

def clean_name(name):
    if not name: return "বন্ধু"
    return name.replace("\n"," ").strip()[:40]

def ascii_name(name):
    s=(name or "").encode("ascii","ignore").decode().strip()
    return s[:24] if s else "FRIEND"

def shorten_name(name):
    name=clean_name(name)
    return name.split()[0][:18] if name else "বন্ধু"

def voice_name_variant(full_name,lang):
    options=[full_name,shorten_name(full_name)]
    return random.choice([x for x in options if x] or [full_name])

def recent_key(chat_id,user_id): return f"{chat_id}:{user_id}"

def is_recent_duplicate(chat_id,user_id):
    key=recent_key(chat_id,user_id)
    now_ts=time.time()
    prev=recent_welcome_keys.get(key,0)
    recent_welcome_keys[key]=now_ts
    return now_ts-prev<12

def is_join_burst(chat_id):
    now_ts=time.time()
    hist=chat_join_history[chat_id]
    hist.append(now_ts)
    while hist and now_ts-hist[0]>25: hist.popleft()
    return len(hist)>=4

def is_super_admin(user_id): return bool(user_id and user_id in SUPER_ADMINS)

def is_linkish_message(msg):
    text=(msg.text or msg.caption or "").strip()
    if not text: return False
    if URLISH_RE.search(text): return True
    for ent in list(msg.entities or [])+list(msg.caption_entities or []):
        try:
            if "url" in str(ent.type).lower() or "text_link" in str(ent.type).lower(): return True
        except: continue
    return bool(getattr(msg,"forward_origin",None) or getattr(msg,"forward_date",None))

def parse_duration_to_seconds(value: str) -> int:
    v = value.strip().lower()
    if v in {"off", "0", "0m", "0h", "0d", "none"}: return 0
    if v.endswith("s") and v[:-1].isdigit(): return int(v[:-1])
    if v.endswith("m") and v[:-1].isdigit(): return int(v[:-1]) * 60
    if v.endswith("h") and v[:-1].isdigit(): return int(v[:-1]) * 3600
    if v.endswith("d") and v[:-1].isdigit(): return int(v[:-1]) * 86400
    if v.isdigit(): return int(v)
    raise ValueError(f"Invalid duration '{value}'. Use: 30s, 5m, 1h, 1d, or off")

def parse_countdown_input(raw):
    text=raw.strip()
    if "|" not in text: raise ValueError("Use format: YYYY-MM-DD HH:MM | Event title")
    left,right=[x.strip() for x in text.split("|",1)]
    dt=datetime.strptime(left,"%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(TIMEZONE_NAME))
    return int(dt.timestamp()),right[:80]

def mark_presence(chat_id):
    try: set_group_value(chat_id,"last_presence_at",int(time.time()))
    except: pass

def get_presence_gap(chat_id):
    row=get_group(chat_id)
    if not row: return 999999
    last_ts=int(row["last_presence_at"] or 0)
    return 999999 if not last_ts else max(0,int(time.time())-last_ts)

def presence_tier(chat_id):
    gap=get_presence_gap(chat_id)
    if gap>=4*3600: return "rich"
    if gap>=3600: return "warm"
    if gap>=900: return "normal"
    return "short"

def current_voice_choice(chat_id):
    row=get_group(chat_id)
    choice=((row["voice_choice"] if row and row["voice_choice"] else "bd") or "bd").strip().lower()
    return choice if choice in {"bd","in"} else "bd"

def selected_voice_name(lang,chat_id=None):
    if lang=="en": return VOICE_NAME_EN
    return "bn-IN-TanishaaNeural" if (current_voice_choice(chat_id or 0) if chat_id else "bd")=="in" else "bn-BD-NabanitaNeural"

def current_welcome_style(chat_id):
    row=get_group(chat_id)
    value=((row["welcome_style"] if row and row["welcome_style"] else "auto") or "auto").strip().lower()
    return value if value in {"auto","random"} or value in THEME_NAMES else "auto"

def current_footer_text(chat_id):
    row=get_group(chat_id)
    return ((row["footer_text"] if row and row["footer_text"] else "") or "").strip()[:80]

def current_hourly_delete_after(chat_id):
    row=get_group(chat_id)
    return int(row["hourly_delete_after"] or 0) if row else 0

def current_festival_mode(chat_id):
    row=get_group(chat_id)
    return bool(int(row["festival_mode"] or 1)) if row else FESTIVAL_MODE_DEFAULT

def current_keyword_mode(chat_id):
    row=get_group(chat_id)
    return bool(int(row["keyword_replies_enabled"] or 1)) if row else KEYWORD_REPLY_ENABLED_DEFAULT

def current_mood_index(chat_id):
    row=get_group(chat_id)
    return int(row["mood_index"] or 0) if row else 0

def next_hourly_mood(chat_id):
    phase=phase_now()
    allowed=PHASE_LOCKED_MOODS.get(phase,HOURLY_MOODS)
    idx=current_mood_index(chat_id)
    mood=allowed[idx%len(allowed)]
    set_group_value(chat_id,"mood_index",(idx+1)%max(1,len(allowed)))
    return mood

def peek_hourly_mood(chat_id):
    phase=phase_now()
    allowed=PHASE_LOCKED_MOODS.get(phase,HOURLY_MOODS)
    return allowed[current_mood_index(chat_id)%len(allowed)]

def list_theme_names_text(): return ", ".join(THEME_NAMES)

def detect_text_taste(text,title=""):
    sample=f"{title or ''} {(text or '')}".lower()
    if URLISH_RE.search(sample) or "http" in sample or "t.me" in sample: return "minimal"
    if any(x in sample for x in ["official","academy","study","crypto","news","family","community","team"]): return "classy"
    if any(x in sample for x in ["💗","💕","🌸","✨","dear","sweet","cute","gentle","soft","calm"]): return "soft"
    return "balanced"

def current_message_taste(chat_id,title=""):
    row=get_group(chat_id)
    if row and (row["message_taste"] or "").strip().lower() not in {"","auto"}:
        val=row["message_taste"].strip().lower()
        if val in {"minimal","classy","soft","balanced"}: return val
    votes=list(group_taste_memory[chat_id])+[detect_text_taste("",title)]
    if not votes: return "balanced"
    counts={k:votes.count(k) for k in {"minimal","classy","soft","balanced"}}
    return max(counts.items(),key=lambda kv:kv[1])[0]

def filter_pool_by_taste(chat_id,pool):
    row=get_group(chat_id)
    taste=current_message_taste(chat_id,row["title"] if row else "")
    out=[]
    for text in pool:
        emoji_count=sum(1 for ch in text if ord(ch)>10000)
        length=len(text)
        if taste=="minimal" and (length>95 or emoji_count>2): continue
        if taste=="classy" and emoji_count>3: continue
        if taste=="soft" and length<24: continue
        out.append(text)
    return out or pool

def resolve_aura_theme(style,phase):
    style=(style or "auto").strip().lower()
    if style in {"","auto"}: return AURA_PHASE_THEMES.get(phase,"soft-bloom")
    if style=="random": return random.choice(AURA_CORE_THEMES)
    return style

def current_effective_aura(chat_id,phase=None):
    return resolve_aura_theme(current_welcome_style(chat_id),phase or phase_now())

def aura_footer_for(chat_id,lang,phase=None):
    persona=AURA_PERSONAS.get(current_effective_aura(chat_id,phase),AURA_PERSONAS["soft-bloom"])
    return random.choice(persona["footer_en"] if lang=="en" else persona["footer_bn"])

def aura_hourly_phrase(chat_id,lang,phase=None):
    persona=AURA_PERSONAS.get(current_effective_aura(chat_id,phase),AURA_PERSONAS["soft-bloom"])
    return random.choice(persona["hourly_en"] if lang=="en" else persona["hourly_bn"])

def theme_palette(style,phase):
    style=resolve_aura_theme(style,phase)
    handcrafted={"moonlight":((34,44,79),(99,121,196),(236,239,255),(191,206,255)),"rose-gold":((138,80,106),(243,171,164),(255,240,228),(255,214,196)),"velvet-night":((21,16,36),(87,64,124),(234,228,255),(189,166,255)),"crystal-morning":((111,177,231),(240,246,255),(255,255,255),(212,233,255)),"soft-bloom":((246,187,205),(255,233,240),(255,252,254),(255,217,230))}
    if style in handcrafted:
        c1,c2,glow,accent=handcrafted[style]
        return c1,c2,glow,accent,style
    base=AURA_ALIAS_MAP.get(style,style)
    if base=="random": base=random.choice(AURA_CORE_THEMES)
    seed=sum(ord(c) for c in base)%360
    sat=0.55+(sum(ord(c) for c in base[::-1])%20)/100
    val1=0.24 if phase=="night" else 0.58
    val2=0.86 if phase in {"morning","day"} else 0.72
    def hsv(h,s,v):
        r,g,b=colorsys.hsv_to_rgb((h%360)/360.0,max(0,min(1,s)),max(0,min(1,v)))
        return (int(r*255),int(g*255),int(b*255))
    return hsv(seed,sat,val1),hsv(seed+38,min(1,sat+0.12),val2),hsv(seed+18,0.22,1.0),hsv(seed+10,0.45,0.98),style

def effective_style_footer(chat_id,style,footer):
    phase=phase_now()
    resolved_style=resolve_aura_theme(style,phase)
    resolved_footer=footer.strip() if footer else aura_footer_for(chat_id,get_group_lang(chat_id),phase)
    festival=current_festival() if current_festival_mode(chat_id) else None
    if festival:
        resolved_style=festival.get("theme") or resolved_style
        fest_name=festival["name_bn"] if get_group_lang(chat_id)=="bn" else festival["name_en"]
        if not footer: resolved_footer=f"{fest_name} | {aura_footer_for(chat_id,get_group_lang(chat_id),phase)}"
    return resolved_style,resolved_footer[:80],festival

def fetch_nager_holidays(year):
    if year in NAGER_YEAR_CACHE: return NAGER_YEAR_CACHE[year]
    try:
        resp=requests.get(f"https://date.nager.at/api/v3/PublicHolidays/{year}/{NAGER_COUNTRY_CODE}",timeout=15)
        data=resp.json()
        if isinstance(data,list):
            NAGER_YEAR_CACHE[year]=data
            return data
    except: pass
    NAGER_YEAR_CACHE[year]=[]
    return []

def fetch_aladhan_today():
    today=local_now().strftime("%d-%m-%Y")
    if today in ALADHAN_DAY_CACHE: return ALADHAN_DAY_CACHE[today]
    result={}
    for url,params in [("https://api.aladhan.com/v1/gToH",{"date":today}),("https://api.aladhan.com/v1/gToHCalendar",{"month":local_now().strftime("%m"),"year":local_now().strftime("%Y"),"adjustment":0})]:
        try:
            resp=requests.get(url,params=params,timeout=15)
            data=resp.json()
            if url.endswith("/gToH") and isinstance(data,dict) and data.get("data"):
                result=data["data"]; break
            if url.endswith("/gToHCalendar") and isinstance(data,dict) and data.get("data"):
                day=int(local_now().strftime("%d")); cal=data["data"]
                if 1<=day<=len(cal): result=cal[day-1]; break
        except: continue
    ALADHAN_DAY_CACHE[today]=result or {}
    return ALADHAN_DAY_CACHE[today]

def _map_nager_today():
    now=local_now(); today=now.strftime("%Y-%m-%d")
    for item in fetch_nager_holidays(now.year):
        if item.get("date")!=today: continue
        combined=f"{(item.get('name') or '')} {(item.get('localName') or '')}".lower()
        if "new year" in combined: return {"key":"new_year","name_bn":"নতুন বছর","name_en":"New Year","theme":"crystal"}
        if "independence" in combined: return {"key":"independence","name_bn":"স্বাধীনতা দিবস","name_en":"Independence Day","theme":"royal-blue"}
        if "victory" in combined: return {"key":"victory","name_bn":"বিজয় দিবস","name_en":"Victory Day","theme":"emerald"}
        if any(x in combined for x in ["bengali","boishakh","pohela","pahela"]): return {"key":"pohela_boishakh","name_bn":"পহেলা বৈশাখ","name_en":"Pohela Boishakh","theme":"flame"}
    return None

def _map_hijri_today():
    data=fetch_aladhan_today()
    hijri={}
    if isinstance(data,dict): hijri=data.get("hijri") or data.get("data",{}).get("hijri") or {}
    month=str((hijri.get("month") or {}).get("number") or "")
    day=str(hijri.get("day") or "")
    if month=="10" and day=="1": return {"key":"eid_fitr","name_bn":"ঈদ মোবারক","name_en":"Eid Mubarak","theme":"gold"}
    if month=="12" and day=="10": return {"key":"eid_adha","name_bn":"ঈদ মোবারক","name_en":"Eid Mubarak","theme":"emerald"}
    return None

def current_festival():
    static={"01-01":{"key":"new_year","name_bn":"নতুন বছর","name_en":"New Year","theme":"crystal"},"03-26":{"key":"independence","name_bn":"স্বাধীনতা দিবস","name_en":"Independence Day","theme":"royal-blue"},"04-14":{"key":"pohela_boishakh","name_bn":"পহেলা বৈশাখ","name_en":"Pohela Boishakh","theme":"flame"},"12-16":{"key":"victory","name_bn":"বিজয় দিবস","name_en":"Victory Day","theme":"emerald"}}
    return _map_nager_today() or _map_hijri_today() or static.get(local_now().strftime("%m-%d"))

def festival_hourly_prefix(lang):
    fest=current_festival()
    if not fest: return ""
    return fest["name_bn"] if lang=="bn" else fest["name_en"]

def normalize_hourly_text(text):
    text=re.sub(r"\s+"," ",text).strip(" -•*\t\r\n")
    if text and text[-1] not in ".!?।":
        text+="।" if re.search(r"[ঀ-৾]",text) else "."
    return text

def is_valid_hourly_text(line, lang, phase):
    raw = line.strip()
    if not raw: return False
    ll = raw.lower()
    if len(raw) < 18 or len(raw) > AI_MAX_TEXT_LEN: return False
    # Reject lines that are just emoji
    if re.fullmatch(r"[𐀀-􏿿\s☀-➿]+", raw): return False
    # Reject lines with excessive repetition
    words = ll.split()
    if len(words) > 3 and len(set(words)) < len(words) * 0.4: return False
    if raw in WEAK_GENERIC_PHRASES.get(lang,set()): return False
    if any(b in ll for b in ["18+","sex","sexy","dating","kiss","adult","nude","xxx","porn"]): return False
    if any(t.lower() in ll for t in PHASE_BLOCKLIST.get(lang,{}).get(phase,())): return False
    if re.fullmatch(r"[\W_]*(শুভ সকাল|শুভ সন্ধ্যা|শুভ রাত্রি|good morning|good evening|good night)[\W_]*",ll): return False
    return True

def sanitize_ai_lines(text,lang,phase):
    lines=[]
    for raw in text.splitlines():
        line=re.sub(r"^[\-\*\d\.\)\s]+","",raw.strip())
        line=normalize_hourly_text(line)
        if is_valid_hourly_text(line,lang,phase): lines.append(line)
    seen=set(); uniq=[]
    for x in lines:
        if x not in seen: seen.add(x); uniq.append(x)
    return uniq

def build_fallback_messages(lang,phase,mood="soft",festival_key=""):
    key=(lang,phase,mood,festival_key)
    if key in FALLBACK_CACHE: return FALLBACK_CACHE[key]
    result=[]
    if lang=="en":
        mood_bank=EN_MOOD_MIDDLES.get(mood,EN_MOOD_MIDDLES["soft"])
        for a in EN_PHASE_OPENERS[phase]:
            for b in EN_MIDDLES+mood_bank:
                for c in EN_ENDINGS:
                    text=normalize_hourly_text(f"{a} {b} {c}".strip())
                    if festival_key and len(text)<AI_MAX_TEXT_LEN-24: text=normalize_hourly_text(f"{festival_hourly_prefix(lang)} vibes — {text}")
                    if is_valid_hourly_text(text,lang,phase): result.append(text)
    else:
        mood_bank=BN_MOOD_MIDDLES.get(mood,BN_MOOD_MIDDLES["soft"])
        for a in BN_PHASE_OPENERS[phase]:
            for b in BN_MIDDLES+mood_bank:
                for c in BN_ENDINGS:
                    text=normalize_hourly_text(f"{a} {b} {c}".strip())
                    if festival_key and len(text)<AI_MAX_TEXT_LEN-22: text=normalize_hourly_text(f"{festival_hourly_prefix(lang)} এর শুভ vibes। {text}")
                    if is_valid_hourly_text(text,lang,phase): result.append(text)
    seen=set(); uniq=[]
    for x in result:
        if x not in seen: seen.add(x); uniq.append(x)
    random.shuffle(uniq)
    FALLBACK_CACHE[key] = uniq
    # Cap cache size to prevent unbounded memory growth
    if len(FALLBACK_CACHE) > 2000:
        oldest = list(FALLBACK_CACHE.keys())[:500]
        for k in oldest:
            FALLBACK_CACHE.pop(k, None)
    return uniq

def groq_candidate_keys():
    global GROQ_KEY_POINTER
    if not GROQ_API_KEYS: return []
    start=GROQ_KEY_POINTER%len(GROQ_API_KEYS)
    ordered=GROQ_API_KEYS[start:]+GROQ_API_KEYS[:start]
    GROQ_KEY_POINTER=(GROQ_KEY_POINTER+1)%max(1,len(GROQ_API_KEYS))
    return ordered

# Keys that are rate-limited: key -> reset_timestamp
_GROQ_RATE_LIMITED: dict[str, float] = {}

def _groq_is_rate_limited(key: str) -> bool:
    reset_ts = _GROQ_RATE_LIMITED.get(key, 0)
    if reset_ts and time.time() < reset_ts:
        return True
    _GROQ_RATE_LIMITED.pop(key, None)
    return False

def _groq_mark_rate_limited(key: str, error_msg: str):
    """Parse wait time from Groq rate limit error and mark key as limited."""
    wait = 1200  # default 20 min
    m = re.search(r"try again in (\d+)m(\d+)", str(error_msg))
    if m:
        wait = int(m.group(1)) * 60 + int(m.group(2)) + 30
    else:
        m2 = re.search(r"try again in (\d+)s", str(error_msg))
        if m2:
            wait = int(m2.group(1)) + 10
    _GROQ_RATE_LIMITED[key] = time.time() + wait
    logger.warning("Groq key rate-limited, cooling for %ds: %s...", wait, key[:12])

def _groq_chat_request(payload):
    last_error = None
    candidates = groq_candidate_keys()
    for idx, key in enumerate(candidates, start=1):
        if _groq_is_rate_limited(key):
            last_error = f"key {idx} rate-limited"
            continue
        try:
            resp = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=payload,
                timeout=GROQ_TIMEOUT_SECONDS,
            )
            data = resp.json()
            if isinstance(data, dict) and data.get("choices"):
                LAST_GROQ_STATUS["last_key_index"] = idx
                return data
            # Check for rate limit in error response
            err_msg = str(data.get("error", {}).get("message", ""))
            if "rate_limit" in err_msg or "Rate limit" in err_msg:
                _groq_mark_rate_limited(key, err_msg)
            last_error = data
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(str(last_error)[:500] if last_error is not None else "No Groq key available")

def _update_groq_status(ok,message):
    LAST_GROQ_STATUS.update({"configured":bool(GROQ_API_KEYS),"last_ok":ok,"last_error":message,"last_checked_at":local_now().strftime("%Y-%m-%d %I:%M:%S %p")})

def groq_live_check():
    if not GROQ_API_KEYS: _update_groq_status(False,"No key configured"); return False,"No key configured"
    try:
        data=_groq_chat_request({"model":GROQ_MODEL,"messages":[{"role":"user","content":"Reply with just OK"}],"max_tokens":8,"temperature":0})
        content=(data["choices"][0]["message"]["content"] or "").strip()
        _update_groq_status(True,f"Live OK via key #{LAST_GROQ_STATUS.get('last_key_index',1)}")
        return True,content[:80] or "Empty"
    except Exception as e:
        _update_groq_status(False,f"Live check failed: {e}")
        record_failure("ai",None,"",f"Live check failed: {e}")
        return False,str(e)[:120]

def groq_generate_batch(lang,phase,mood="soft",festival_key=""):
    if not AI_HOURLY_ENABLED or not GROQ_API_KEYS:
        _update_groq_status(False,"Groq disabled or API key missing"); return []
    pl={"bn":{"morning":"সকাল","day":"দিন বা দুপুর","evening":"সন্ধ্যা","night":"রাত"},"en":{"morning":"morning","day":"daytime or afternoon","evening":"evening","night":"night"}}
    fn=f"- lightly reflect a festive mood for {festival_hourly_prefix(lang)}\n" if festival_key else ""
    prompt=(f"Write {AI_BATCH_SIZE} short premium Telegram group hourly messages in {'Bengali' if lang=='bn' else 'English'}.\nCurrent time phase: {pl['bn' if lang=='bn' else 'en'][phase]}.\nCurrent mood: {mood}.\nRules:\n- warm, elegant, premium, tasteful, group-safe\n- non-sexual, non-romantic, non-political, non-religious\n- no flirting, no hashtags\n- each line complete and natural\n- do NOT mention the wrong time phase\n{fn}- keep each between 18 and {AI_MAX_TEXT_LEN} characters\n- each line different, avoid robotic phrases\nReturn only the messages, one per line.")
    try:
        data=_groq_chat_request({"model":GROQ_MODEL,"messages":[{"role":"system","content":"You write tasteful, premium, natural Telegram group texts. Never mismatch time-of-day greetings."},{"role":"user","content":prompt}],"temperature":0.9,"max_tokens":280})
        content=data["choices"][0]["message"]["content"]
        lines=sanitize_ai_lines(content,lang,phase)
        if lines:
            _update_groq_status(True,f"OK | {len(lines)} lines | {mood}")
            logger.info("Groq success | lang=%s phase=%s mood=%s count=%s",lang,phase,mood,len(lines))
            return lines
        _update_groq_status(False,"Groq returned empty/filtered text")
        record_failure("ai",None,"","Groq returned empty/filtered text")
        return []
    except Exception as e:
        _update_groq_status(False,f"Failed: {e}")
        record_failure("ai",None,"",str(e))
        logger.exception("Groq failed | lang=%s phase=%s mood=%s",lang,phase,mood)
        return []

def get_batch_pool(lang,phase,mood="soft",festival_key=""):
    key=(lang,phase,mood,festival_key)
    cached=AI_BATCH_CACHE.get(key)
    now_ts=time.time()
    if cached and now_ts-cached["created_at"]<900 and cached.get("texts"):
        return cached["texts"],cached["source"]
    ai_lines=groq_generate_batch(lang,phase,mood=mood,festival_key=festival_key)
    if ai_lines: source="ai"; texts=ai_lines
    else: source="fallback"; texts=build_fallback_messages(lang,phase,mood=mood,festival_key=festival_key)
    AI_BATCH_CACHE[key]={"texts":texts,"source":source,"created_at":now_ts}
    for line in texts[:12]:
        try: save_generated_text(lang,phase,source,line)
        except: pass
    return texts,source

def variantize_message_text(chat_id,lang,text,kind="hourly"):
    row=get_group(chat_id)
    taste=current_message_taste(chat_id,row["title"] if row else "")
    tier=presence_tier(chat_id)
    aura=current_effective_aura(chat_id,phase_now())
    base=normalize_hourly_text(text)
    variants=[base]
    if lang=="en":
        if tier in {"rich","warm"}: variants.append(f"{base} Wishing everyone a beautiful moment ahead.")
        if taste=="classy": variants.append(f"{base} May the mood stay elegant and steady.")
        elif taste=="soft": variants.append(f"{base} Hope the heart feels a little softer today.")
        elif taste=="minimal": variants.append(base.replace(" everyone","").strip())
    else:
        if tier in {"rich","warm"}: variants.append(f"{base} এই group-এর সবার জন্য রইল কোমল শুভেচ্ছা।")
        if taste=="classy": variants.append(f"{base} আজকের সময়টা হোক স্থির, সুন্দর আর মার্জিত।")
        elif taste=="soft": variants.append(f"{base} মনটা আজ একটু নরম আর হালকা থাকুক।")
        elif taste=="minimal": variants.append(base.replace("সবাইকে","").strip())
    if kind in {"hourly","welcome"}:
        aura_line=aura_hourly_phrase(chat_id,lang,phase_now())
        variants.append(f"{aura_line} {base}" if aura=="crystal-morning" else f"{base} {aura_line}")
    cleaned=[]; seen=set()
    for v in variants:
        v=normalize_hourly_text(v)
        if v and len(v)<=AI_MAX_TEXT_LEN and v not in seen: seen.add(v); cleaned.append(v)
    return cleaned or [base]

def pick_hourly_message(chat_id,lang,phase,pool):
    candidates=[normalize_hourly_text(x) for x in pool if is_valid_hourly_text(normalize_hourly_text(x),lang,phase)]
    if not candidates:
        candidates=[normalize_hourly_text(x) for x in build_fallback_messages(lang,phase,mood=peek_hourly_mood(chat_id),festival_key=(current_festival() or {}).get("key","")) if is_valid_hourly_text(normalize_hourly_text(x),lang,phase)]
    candidates=filter_pool_by_taste(chat_id,candidates)
    expanded=[]
    for c in candidates: expanded.extend(variantize_message_text(chat_id,lang,c,kind="hourly"))
    final_pool=[cand for cand in expanded if not was_recent_duplicate_text(chat_id,"hourly",cand,lookback_days=3)]
    if not final_pool: final_pool=expanded or candidates
    recent=recent_hourly_by_chat[chat_id]
    recent_sigs={structure_signature(y) for y in recent}
    choices=[x for x in final_pool if x not in recent and structure_signature(x) not in recent_sigs]
    if not choices: choices=final_pool
    text=random.choice(choices)
    recent.append(text)
    record_sent_history(chat_id,"hourly",text)
    return text

def pick_font(size,bold=False):
    candidates=["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf","/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"]
    for path in candidates:
        try: return ImageFont.truetype(path,size=size)
        except: continue
    return ImageFont.load_default()

# ─── Ultra Premium Welcome Card v3 ───────────────────────────────────────────
def build_cover_bytes(first_name, group_title, lang, style="auto", footer="", profile_bytes=None, member_count=None):
    """Ultra premium welcome card — cinematic layered design."""
    import math as _math
    W, H = 1280, 720
    phase = phase_now()
    c1, c2, glow, accent, resolved_style = theme_palette(style, phase)

    # ── Cinematic gradient base ────────────────────────────────────────────────
    base = Image.new("RGB", (W, H), c1)
    bd   = ImageDraw.Draw(base)
    for y in range(H):
        t  = y / max(1, H - 1)
        t2 = t * t * (3 - 2 * t)  # smoothstep
        r  = int(c1[0]*(1-t2) + c2[0]*t2)
        g  = int(c1[1]*(1-t2) + c2[1]*t2)
        b  = int(c1[2]*(1-t2) + c2[2]*t2)
        bd.line((0, y, W, y), fill=(r, g, b))

    # ── Radial glow from top-right ────────────────────────────────────────────
    glow_layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    gld = ImageDraw.Draw(glow_layer)
    cx, cy = W - 80, 80
    for radius in range(500, 0, -12):
        alpha = int(28 * (radius / 500) ** 2.2)
        gld.ellipse((cx-radius, cy-radius, cx+radius, cy+radius),
                    fill=(*glow[:3], alpha))
    glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(30))
    base = Image.alpha_composite(base.convert("RGBA"), glow_layer).convert("RGB")

    # ── Bokeh particles ────────────────────────────────────────────────────────
    bokeh = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    bkd   = ImageDraw.Draw(bokeh)
    particles = [
        (90, 90, 120, 22), (1150, 60, 90, 18), (200, 600, 110, 16),
        (1050, 600, 130, 20), (600, 30, 80, 14), (400, 680, 95, 17),
        (1180, 360, 75, 13), (50, 400, 85, 15),
    ]
    for px, py, pr, pa in particles:
        for dr in range(pr, 0, -5):
            a = int(pa * (dr/pr)**2)
            bkd.ellipse((px-dr, py-dr, px+dr, py+dr), fill=(*accent[:3], a))
    bokeh = bokeh.filter(ImageFilter.GaussianBlur(18))
    base  = Image.alpha_composite(base.convert("RGBA"), bokeh).convert("RGB")

    # ── Deep shadow for card ───────────────────────────────────────────────────
    shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle(
        (72, 72, W-72, H-72), radius=56, fill=(0, 0, 0, 130))
    shadow = shadow.filter(ImageFilter.GaussianBlur(28))
    base   = Image.alpha_composite(base.convert("RGBA"), shadow).convert("RGB")

    # ── Main card glass panel ──────────────────────────────────────────────────
    card = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    cd   = ImageDraw.Draw(card)
    # Primary dark card
    cd.rounded_rectangle((84, 84, W-84, H-84), radius=52, fill=(6, 10, 24, 248))
    # Subtle top-edge highlight
    cd.rounded_rectangle((84, 84, W-84, 86+H//20), radius=52, fill=(*glow[:3], 8))
    # Inner glow border
    cd.rounded_rectangle((88, 88, W-88, H-88), radius=50,
                          outline=(*accent[:3], 45), width=1)
    base = Image.alpha_composite(base.convert("RGBA"), card).convert("RGB")
    draw = ImageDraw.Draw(base)

    # ── Left accent bar (thick gradient stripe) ───────────────────────────────
    for i in range(8):
        alpha = int(255 * (1 - i/8))
        x = 104 + i
        draw.rounded_rectangle((x, 108, x+1, H-108), radius=1,
                                fill=(*accent[:3], alpha))
    draw.rounded_rectangle((112, 108, 118, H-108), radius=3,
                            fill=(*accent[:3], 255))

    # ── Diagonal decorative lines (subtle) ────────────────────────────────────
    deco = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    dd   = ImageDraw.Draw(deco)
    for i, x_off in enumerate(range(830, 1200, 38)):
        dd.line((x_off, 84, x_off+120, H-84), fill=(*glow[:3], 6))
    deco = deco.filter(ImageFilter.GaussianBlur(1))
    base = Image.alpha_composite(base.convert("RGBA"), deco).convert("RGB")
    draw = ImageDraw.Draw(base)

    # ── Right side subtle panel ────────────────────────────────────────────────
    rp = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ImageDraw.Draw(rp).rounded_rectangle(
        (820, 90, W-90, H-90), radius=40, fill=(*glow[:3], 4))
    base = Image.alpha_composite(base.convert("RGBA"), rp).convert("RGB")
    draw = ImageDraw.Draw(base)

    # ── Phase + theme badges ───────────────────────────────────────────────────
    phase_icon = {"morning":"☀", "day":"✦", "evening":"◑", "night":"★"}.get(phase,"✦")
    tf_badge = pick_font(17, True)
    for idx, (badge_txt, bx) in enumerate([
        (f"{phase_icon} {phase.upper()}", 852),
        (resolved_style.upper()[:10], 1042),
    ]):
        bw = 176 if idx == 0 else 132
        bl = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(bl).rounded_rectangle(
            (bx, 106, bx+bw, 136), radius=13, fill=(*accent[:3], 35))
        base = Image.alpha_composite(base.convert("RGBA"), bl).convert("RGB")
        draw = ImageDraw.Draw(base)
        draw.rounded_rectangle((bx, 106, bx+bw, 136), radius=13,
                                outline=(*accent[:3], 90), width=1)
        draw.text((bx+10, 114), badge_txt[:14], fill=(*glow[:3], 220), font=tf_badge)

    # ── Fonts ──────────────────────────────────────────────────────────────────
    tf_sub   = pick_font(20, False)    # "WELCOME" label
    tf_name  = pick_font(82, True)     # Member name
    tf_group = pick_font(30, False)    # Group name
    tf_bot   = pick_font(21, True)     # Bot name
    tf_foot  = pick_font(18, False)    # Footer
    tf_cnt   = pick_font(20, True)     # Member count

    LEFT = 138

    # ── "WELCOME" label with letter spacing simulation ─────────────────────────
    welcome_str = "W E L C O M E"
    # shadow
    draw.text((LEFT+2, 156), welcome_str, fill=(0,0,0,90), font=tf_sub)
    draw.text((LEFT, 154), welcome_str, fill=(*glow[:3], 200), font=tf_sub)

    # ── Accent divider ─────────────────────────────────────────────────────────
    for seg_x, seg_w, seg_alpha in [(LEFT, 240, 255), (LEFT+250, 80, 140), (LEFT+340, 40, 70)]:
        draw.rounded_rectangle((seg_x, 184, seg_x+seg_w, 188),
                                radius=2, fill=(*accent[:3], seg_alpha))

    # ── Member name ────────────────────────────────────────────────────────────
    name_str = ascii_name(first_name).upper()[:14]
    # Double-draw for pseudo-glow
    draw.text((LEFT+2, 198), name_str, fill=(0,0,0,80), font=tf_name)
    draw.text((LEFT, 196), name_str, fill=(252, 228, 158), font=tf_name)

    # ── "to" + group name ─────────────────────────────────────────────────────
    group_str = ascii_name(group_title or "GROUP").upper()
    draw.text((LEFT, 306), "to", fill=(*glow[:3], 150), font=tf_sub)
    draw.text((LEFT, 330), group_str[:24], fill=(215, 228, 255), font=tf_group)

    # ── Dot separator ─────────────────────────────────────────────────────────
    for di in range(5):
        dot_x = LEFT + di * 22
        draw.ellipse((dot_x, 376, dot_x+8, 384),
                     fill=(*accent[:3], 180-di*30))

    # ── Bot name ──────────────────────────────────────────────────────────────
    draw.text((LEFT, 396), BOT_NAME.upper(), fill=(*glow[:3], 240), font=tf_bot)

    # ── Member count badge ────────────────────────────────────────────────────
    if member_count:
        cnt_str = f"✦ {int(member_count):,} members"
        cb = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        cbd = ImageDraw.Draw(cb)
        cbd.rounded_rectangle((LEFT, 428, LEFT+240, 456),
                               radius=12, fill=(*accent[:3], 28))
        base = Image.alpha_composite(base.convert("RGBA"), cb).convert("RGB")
        draw = ImageDraw.Draw(base)
        draw.rounded_rectangle((LEFT, 428, LEFT+240, 456),
                                radius=12, outline=(*accent[:3], 80), width=1)
        draw.text((LEFT+12, 435), cnt_str, fill=(*glow[:3], 200), font=tf_cnt)

    # ── Footer ────────────────────────────────────────────────────────────────
    foot_txt = (footer.strip()[:55] if footer else f"Powered by {BOT_NAME}")
    draw.text((LEFT, H-108), foot_txt, fill=(*glow[:3], 100), font=tf_foot)

    # ── Bottom accent segments ─────────────────────────────────────────────────
    for si, (sw, sa) in enumerate([(260, 200), (120, 120), (60, 60)]):
        sx = LEFT + si * (280 if si == 0 else (si == 1 and 266 or 398))
        sx = LEFT + [0, 266, 392][si]
        bl2 = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(bl2).rounded_rectangle(
            (sx, H-90, sx+sw, H-84), radius=3, fill=(*accent[:3], sa))
        base = Image.alpha_composite(base.convert("RGBA"), bl2).convert("RGB")
        draw = ImageDraw.Draw(base)

    # ── Avatar (right side) ────────────────────────────────────────────────────
    AV_SZ = 252
    AV_X, AV_Y = 862, 198

    if profile_bytes:
        try:
            avatar = ImageOps.fit(
                Image.open(BytesIO(profile_bytes)).convert("RGB"),
                (AV_SZ, AV_SZ)
            )
            # Layered glow rings
            for ring_r, ring_alpha, ring_blur in [
                (AV_SZ//2+28, 18, 6),
                (AV_SZ//2+16, 45, 3),
                (AV_SZ//2+6,  90, 1),
            ]:
                rl = Image.new("RGBA", (W, H), (0, 0, 0, 0))
                cx_r = AV_X + AV_SZ//2
                cy_r = AV_Y + AV_SZ//2
                ImageDraw.Draw(rl).ellipse(
                    (cx_r-ring_r, cy_r-ring_r, cx_r+ring_r, cy_r+ring_r),
                    fill=(*accent[:3], ring_alpha)
                )
                rl = rl.filter(ImageFilter.GaussianBlur(ring_blur))
                base = Image.alpha_composite(base.convert("RGBA"), rl).convert("RGB")
                draw = ImageDraw.Draw(base)

            # Circular mask + accent border
            mask  = Image.new("L", (AV_SZ, AV_SZ), 0)
            ImageDraw.Draw(mask).ellipse((0, 0, AV_SZ, AV_SZ), fill=255)

            ring_sz = AV_SZ + 10
            ring_img = Image.new("RGBA", (ring_sz, ring_sz), (0, 0, 0, 0))
            ImageDraw.Draw(ring_img).ellipse(
                (0, 0, ring_sz-1, ring_sz-1), fill=(*accent[:3], 255))
            ring_img.paste(avatar, (5, 5), mask)

            ring_mask = Image.new("L", ring_img.size, 0)
            ImageDraw.Draw(ring_mask).ellipse(
                (0, 0, ring_sz-1, ring_sz-1), fill=255)
            base.paste(ring_img.convert("RGB"), (AV_X-5, AV_Y-5), ring_mask)
            draw = ImageDraw.Draw(base)
        except Exception:
            pass
    else:
        # Elegant monogram circle
        cx_m = AV_X + AV_SZ//2
        cy_m = AV_Y + AV_SZ//2
        mono_r = AV_SZ//2
        for mr, ma in [(mono_r+20, 15), (mono_r+10, 35), (mono_r, 55)]:
            ml = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            ImageDraw.Draw(ml).ellipse(
                (cx_m-mr, cy_m-mr, cx_m+mr, cy_m+mr), fill=(*accent[:3], ma))
            base = Image.alpha_composite(base.convert("RGBA"), ml).convert("RGB")
            draw = ImageDraw.Draw(base)
        # Border
        draw.ellipse((cx_m-mono_r, cy_m-mono_r, cx_m+mono_r, cy_m+mono_r),
                     outline=(*accent[:3], 180), width=2)
        # Initial letter
        mono_ch = ascii_name(first_name)[:1].upper() or "W"
        tf_mono = pick_font(96, True)
        draw.text((cx_m-32, cy_m-56), mono_ch, fill=(*glow[:3], 220), font=tf_mono)

    bio = BytesIO()
    base.save(bio, format="PNG", optimize=True)
    bio.name = "welcome.png"
    bio.seek(0)
    return bio

def build_milestone_card_bytes(group_title,count):
    width,height=1280,720
    img=Image.new("RGB",(width,height),(26,22,45))
    draw=ImageDraw.Draw(img)
    for y in range(height):
        blend=y/max(1,height-1)
        draw.line((0,y,width,y),fill=(int(26*(1-blend)+104*blend),int(22*(1-blend)+55*blend),int(45*(1-blend)+186*blend)))
    draw.rounded_rectangle((90,90,1190,630),radius=48,fill=(12,16,31))
    draw.rounded_rectangle((120,120,1140,600),radius=36,outline=(255,214,122),width=6)
    draw.text((160,160),"MILESTONE",fill=(255,244,214),font=pick_font(70,True))
    draw.text((160,290),str(count),fill=(255,214,122),font=pick_font(126,True))
    draw.text((160,465),f"MEMBERS • {ascii_name(group_title or 'GROUP').upper()}",fill=(220,232,255),font=pick_font(38,False))
    draw.text((160,540),BOT_NAME.upper(),fill=(176,255,223),font=pick_font(38,False))
    bio=BytesIO(); img.save(bio,format="PNG"); bio.name="milestone.png"; bio.seek(0)
    return bio

def build_combined_welcome_card_bytes(group_title,lang,names_text,style="auto",footer=""):
    width,height=1280,720
    phase=phase_now()
    c1,c2,glow,accent,resolved_style=theme_palette(style,phase)
    img=Image.new("RGB",(width,height),c1)
    draw=ImageDraw.Draw(img)
    for y in range(height):
        blend=y/max(1,height-1)
        draw.line((0,y,width,y),fill=(int(c1[0]*(1-blend)+c2[0]*blend),int(c1[1]*(1-blend)+c2[1]*blend),int(c1[2]*(1-blend)+c2[2]*blend)))
    draw.rounded_rectangle((90,90,1190,630),radius=48,fill=(12,16,31))
    draw.rounded_rectangle((120,120,1140,600),radius=36,outline=accent,width=6)
    ff=pick_font(24,True)
    draw.text((160,155),"WELCOME CREW",fill=glow,font=pick_font(64,True))
    draw.text((160,280),ascii_name(group_title or "GROUP").upper(),fill=(255,226,170),font=pick_font(42,True))
    draw.text((160,380),names_text[:120],fill=(222,233,255),font=pick_font(30,False))
    draw.text((160,530),(footer or f"Powered by {BOT_NAME}")[:60],fill=(214,229,255),font=ff)
    draw.rounded_rectangle((905,140,1110,184),radius=20,fill=(255,255,255))
    draw.text((930,150),resolved_style.upper()[:12],fill=(38,52,87),font=ff)
    bio=BytesIO(); img.save(bio,format="PNG"); bio.name="welcome_burst.png"; bio.seek(0)
    return bio

def build_countdown_card_bytes(group_title,event_title,days_left,hours_left,lang):
    width,height=1280,720; phase=phase_now()
    c1,c2,glow,accent,_=theme_palette("halo",phase)
    img=Image.new("RGB",(width,height),c1); draw=ImageDraw.Draw(img)
    for y in range(height):
        blend=y/max(1,height-1)
        draw.line((0,y,width,y),fill=(int(c1[0]*(1-blend)+c2[0]*blend),int(c1[1]*(1-blend)+c2[1]*blend),int(c1[2]*(1-blend)+c2[2]*blend)))
    draw.rounded_rectangle((90,90,1190,630),radius=48,fill=(12,16,31))
    draw.rounded_rectangle((120,120,1140,600),radius=36,outline=accent,width=6)
    draw.text((160,150),"COUNTDOWN",fill=glow,font=pick_font(60,True))
    draw.text((160,255),ascii_name(group_title or "GROUP").upper(),fill=(255,226,170),font=pick_font(42,True))
    draw.text((160,330),(event_title or "EVENT")[:44],fill=(222,233,255),font=pick_font(28,False))
    draw.text((160,410),f"{days_left}D  {hours_left}H",fill=(255,248,190),font=pick_font(104,True))
    draw.text((160,550),"Special event reminder" if lang=="en" else "বিশেষ ইভেন্টের কাউন্টডাউন",fill=(214,229,255),font=pick_font(24,True))
    bio=BytesIO(); img.save(bio,format="PNG"); bio.name="countdown.png"; bio.seek(0)
    return bio

async def make_voice_file(text,voice_name,path):
    communicate=edge_tts.Communicate(text=text,voice=voice_name,rate=VOICE_RATE,pitch=VOICE_PITCH,volume=VOICE_VOLUME)
    await communicate.save(str(path))

def aura_story_variants(lang,mention_name,safe_group,phase,aura):
    if lang=="en":
        stories={"morning":[f"🌼 Morning light brushed gently across {safe_group}, and right then {mention_name} arrived. Welcome.",f"☀️ A crystal-soft morning opened over {safe_group}; {mention_name} stepped in with it. Welcome.",f"✨ The day began quietly in {safe_group}, and {mention_name} became part of that first glow."],"day":[f"🌸 The day was already warm in {safe_group}, then {mention_name} arrived and made it feel fuller.",f"✨ A soft bloom settled over {safe_group}, and {mention_name} arrived right inside that gentle moment.",f"💫 In the middle of the day, {safe_group} gained a little more warmth with {mention_name}."],"evening":[f"🌙 Evening gold touched {safe_group}, and then {mention_name} arrived. A graceful welcome.",f"🌆 As the light softened around {safe_group}, {mention_name} stepped into the scene. Welcome.",f"✨ The evening grew warmer the moment {mention_name} joined {safe_group}."],"night":[f"🌌 A quiet moonlit hush rested over {safe_group}, and {mention_name} arrived into it. Welcome.",f"💙 Night had already gone soft in {safe_group}; then {mention_name} came in, calm and welcome.",f"⭐ Under a velvet night, {safe_group} quietly opened its door to {mention_name}."]}
    else:
        stories={"morning":[f"🌼 সকালের আলো ধীরে ধীরে {safe_group} ছুঁয়ে গেল, আর ঠিক তখনই এসে পৌঁছালে {mention_name}। স্বাগতম।",f"☀️ স্বচ্ছ এক সকাল {safe_group} জুড়ে খুলে গেল, আর সেই আলোয় এসে পড়লে {mention_name}।",f"✨ দিনের প্রথম নরম উজ্জ্বলতায় {safe_group} তোমাকে আপন করে নিল, {mention_name}।"],"day":[f"🌸 দিনের নরম উষ্ণতার ভেতর {safe_group} আরও সুন্দর হলো, কারণ এলে {mention_name}।",f"✨ দুপুরের হালকা আলোয় {safe_group} যেন একটু বেশি প্রস্ফুটিত হলো, {mention_name} তোমাকে পেয়ে।",f"💫 সময়টা চলছিল নিজের ছন্দে, তারপর {mention_name} এসে {safe_group}-কে আরেকটু মোলায়েম করে দিল।"],"evening":[f"🌙 সন্ধ্যার সোনালি আভা নেমেছিল {safe_group} জুড়ে, আর ঠিক তখনই এলে {mention_name}।",f"🌆 নরম সন্ধ্যার আলোয় {safe_group} তোমাকে গ্রহণ করল খুব শান্তভাবে, {mention_name}।",f"✨ আজকের সন্ধ্যাটায় {safe_group} একটু বেশি মায়াময় লাগছে, কারণ এলে {mention_name}।"],"night":[f"🌌 চুপচাপ চাঁদের আলোয় ভেজা {safe_group}-এ এসে পৌঁছালে {mention_name}। স্বাগতম।",f"💙 রাতের নরম নীরবতায় {safe_group} তোমাকে খুব শান্তভাবে গ্রহণ করল, {mention_name}।",f"⭐ মখমলি এক রাতের ভেতর {safe_group}-এ এলে {mention_name}, আর মুহূর্তটা হয়ে উঠল আরও কোমল।"]}
    return stories[phase]

def build_text_styles(lang,mention_name,safe_group,phase,chat_id=0):
    aura=current_effective_aura(chat_id,phase) if chat_id else resolve_aura_theme("auto",phase)
    story=aura_story_variants(lang,mention_name,safe_group,phase,aura)
    if lang=="en":
        elegant={"morning":[f"🌼 Good morning {mention_name}. A graceful welcome to {safe_group}.",f"✨ Morning opened softly in {safe_group}; welcome, {mention_name}.",f"☀️ {mention_name}, a crystal-bright welcome to {safe_group}."],"day":[f"🌸 Welcome {mention_name}. {safe_group} feels warmer with you here.",f"✨ A soft-bloom welcome to {mention_name} in {safe_group}.",f"💫 Delighted to have you here, {mention_name}. Welcome to {safe_group}."],"evening":[f"🌙 Good evening {mention_name}. A rose-gold welcome to {safe_group}.",f"✨ The evening feels softer with you in {safe_group}, {mention_name}.",f"🌆 Warm evening wishes and welcome, {mention_name}."],"night":[f"🌌 Good night {mention_name}. A moonlit welcome to {safe_group}.",f"💙 A calm velvet welcome to {mention_name} in {safe_group}.",f"⭐ Quiet warmth and a gentle welcome to you, {mention_name}."]}
    else:
        elegant={"morning":[f"🌼 শুভ সকাল {mention_name}। {safe_group} এ তোমাকে আন্তরিক স্বাগতম।",f"✨ সকালের স্বচ্ছ আলোয় তোমাকে স্বাগতম, {mention_name}।",f"☀️ {mention_name}, {safe_group} আজ তোমাকে পেয়ে আরও উজ্জ্বল।"],"day":[f"🌸 স্বাগতম {mention_name}। {safe_group} এ তোমাকে পেয়ে ভালো লাগছে।",f"✨ {safe_group} আজ একটু বেশি কোমল লাগছে, কারণ এলে {mention_name}।",f"💫 নরম এক শুভেচ্ছা রইল তোমার জন্য, {mention_name}।"],"evening":[f"🌙 শুভ সন্ধ্যা {mention_name}। {safe_group} এ তোমাকে স্বাগতম।",f"✨ সন্ধ্যার সোনালি আভায় তোমাকে স্বাগতম, {mention_name}।",f"🌆 {mention_name}, {safe_group} এ আজকের সন্ধ্যাটায় তোমাকে পেয়ে ভালো লাগছে।"],"night":[f"🌌 শুভ রাত্রি {mention_name}। {safe_group} এ তোমাকে স্বাগতম।",f"💙 চাঁদের নরম আলোয় তোমার জন্য রইল এক শান্ত স্বাগতম, {mention_name}।",f"⭐ রাতের মখমলি নীরবতায় তোমাকে স্বাগত, {mention_name}।"]}
    seen,out=set(),[]
    for x in story+elegant[phase]:
        x=x.strip()
        if x and x not in seen: seen.add(x); out.append(x)
    return out

def welcome_texts(lang,mention_name,first_name,group_title,custom_text,chat_id=0):
    phase=phase_now()
    safe_group=group_title or ("our group" if lang=="en" else "আমাদের গ্রুপ")
    aura=current_effective_aura(chat_id,phase) if chat_id else resolve_aura_theme("auto",phase)
    if custom_text:
        text=custom_text.replace("{name}",mention_name).replace("{group}",safe_group).replace("{phase}",phase)
    else:
        pool=build_text_styles(lang,mention_name,safe_group,phase,chat_id=chat_id)
        candidates=[x for x in pool if not was_recent_duplicate_text(chat_id,"welcome",x,2)] if chat_id else pool
        text=random.choice(candidates or pool)
    if lang=="en":
        vb={"moonlight":[f"Hello {first_name}. A quiet moonlit welcome to {safe_group}.",f"{first_name}, welcome softly into {safe_group}."],"rose-gold":[f"Hello {first_name}. A warm glowing welcome to {safe_group}.",f"{first_name}, welcome with a little golden warmth to {safe_group}."],"velvet-night":[f"Hello {first_name}. A deep calm welcome to {safe_group}.",f"{first_name}, welcome gently into this velvet night mood in {safe_group}."],"crystal-morning":[f"Hello {first_name}. A crystal-bright morning welcome to {safe_group}.",f"{first_name}, welcome into the clear morning light of {safe_group}."],"soft-bloom":[f"Hello {first_name}. A soft blooming welcome to {safe_group}.",f"{first_name}, welcome into the gentle warmth of {safe_group}."]}
    else:
        vb={"moonlight":[f"হ্যালো {first_name}। চাঁদের নরম আলোয় {safe_group} এ তোমাকে স্বাগতম।",f"{first_name}, {safe_group} এ তোমার জন্য রইল এক শান্ত স্বাগতম।"],"rose-gold":[f"হ্যালো {first_name}। সোনালি নরম আভায় {safe_group} এ তোমাকে স্বাগতম।",f"{first_name}, {safe_group} এ তোমার জন্য রইল উষ্ণ আর সুন্দর এক স্বাগতম।"],"velvet-night":[f"হ্যালো {first_name}। মখমলি নরম রাতের ভেতর {safe_group} এ তোমাকে স্বাগতম।",f"{first_name}, শান্ত গভীরতায় ভরা {safe_group} তোমাকে আপন করে নিল।"],"crystal-morning":[f"হ্যালো {first_name}। স্বচ্ছ সকালের আলোয় {safe_group} এ তোমাকে স্বাগতম।",f"{first_name}, উজ্জ্বল কোমল এক সকাল থেকে তোমার জন্য রইল শুভেচ্ছা।"],"soft-bloom":[f"হ্যালো {first_name}। নরম প্রস্ফুটনের মতো {safe_group} এ তোমাকে স্বাগতম।",f"{first_name}, {safe_group} তোমাকে খুব কোমল এক শুভেচ্ছায় গ্রহণ করল।"]}
    voice=random.choice(vb.get(aura,vb["soft-bloom"]))
    return text,voice

def personalize_voice_text(voice_text,first_name,lang,chat_id=0):
    variant=voice_name_variant(first_name,lang)
    aura=current_effective_aura(chat_id,phase_now()) if chat_id else resolve_aura_theme("auto",phase_now())
    if lang=="en":
        pb={"moonlight":[f"Hello {variant}. ",f"{variant}, softly now. "],"rose-gold":[f"Hello {variant}. ",f"{variant}, warmly. "],"velvet-night":[f"{variant}, gently. ",f"Hello {variant}. "],"crystal-morning":[f"Hello {variant}. ",f"{variant}, bright morning. "],"soft-bloom":[f"Hello {variant}. ",f"{variant}, softly. "]}
    else:
        pb={"moonlight":[f"হ্যালো {variant}। ",f"{variant}, নরম করে বলি। "],"rose-gold":[f"হ্যালো {variant}। ",f"{variant}, উষ্ণ শুভেচ্ছা। "],"velvet-night":[f"{variant}, শান্তভাবে শোনো। ",f"হ্যালো {variant}। "],"crystal-morning":[f"হ্যালো {variant}। ",f"{variant}, উজ্জ্বল সকাল। "],"soft-bloom":[f"হ্যালো {variant}। ",f"{variant}, কোমল শুভেচ্ছা। "]}
    prefix=random.choice(pb.get(aura,pb["soft-bloom"]))
    if voice_text.lower().startswith(("hello","hi","হ্যালো",first_name.lower())): return voice_text
    return f"{prefix}{voice_text}"

def schedule_http_delete(chat_id,message_id,delay):
    def _w():
        try: time.sleep(max(1,delay)); tg_post("deleteMessage",{"chat_id":chat_id,"message_id":message_id})
        except: pass
    threading.Thread(target=_w,daemon=True).start()

async def schedule_delete(bot,chat_id,message_id,delay):
    try: await asyncio.sleep(delay); await bot.delete_message(chat_id=chat_id,message_id=message_id)
    except: pass

def http_humanize(chat_id,action="typing",kind="auto"):
    try: tg_post("sendChatAction",{"chat_id":chat_id,"action":action})
    except: pass
    if HUMAN_DELAY_ENABLED: time.sleep(random.uniform(0.4,1.0) if kind=="auto" else random.choice([1.5,3.0]))

async def bot_humanize(bot,chat_id,action="typing",kind="reply"):
    gap=get_presence_gap(chat_id)
    try: await bot.send_chat_action(chat_id=chat_id,action=action)
    except: pass
    if not HUMAN_DELAY_ENABLED: return
    if gap>=4*3600: delay=random.uniform(2.4,4.2) if kind=="reply" else random.uniform(1.8,3.1)
    elif gap>=1800: delay=random.uniform(1.6,2.8) if kind=="reply" else random.uniform(1.2,2.1)
    elif gap>=600: delay=random.uniform(1.0,1.9) if kind=="reply" else random.uniform(0.9,1.5)
    else: delay=random.uniform(0.6,1.2) if kind=="reply" else random.uniform(0.5,1.0)
    if delay>2.2:
        await asyncio.sleep(delay/2)
        try: await bot.send_chat_action(chat_id=chat_id,action=action)
        except: pass
        await asyncio.sleep(delay/2)
    else: await asyncio.sleep(delay)

async def human_delay_and_action(context,update,action="typing"):
    chat=update.effective_chat if update else None
    if chat: await bot_humanize(context.bot,chat.id,action=action,kind="reply")

async def send_photo_with_retry(bot,**kwargs):
    last_error=None; chat_id=kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id: await bot_humanize(bot,chat_id,ChatAction.UPLOAD_PHOTO,"photo")
            photo=kwargs.get("photo")
            if attempt>0 and hasattr(photo,"seek"): photo.seek(0)
            result=await bot.send_photo(**kwargs)
            if chat_id:
                mark_presence(chat_id)
                caption=kwargs.get("caption") or ""
                if caption: record_sent_history(chat_id,"photo_caption",re.sub(r"<[^>]+>","",caption))
            return result
        except Exception as e:
            last_error=e
            if attempt==0: await asyncio.sleep(1)
    record_failure("send_photo", kwargs.get("chat_id"), "", str(last_error)[:300])
    raise last_error

async def send_voice_with_retry(bot,**kwargs):
    last_error=None; chat_id=kwargs.get("chat_id")
    for attempt in range(2):
        try:
            if chat_id: await bot_humanize(bot,chat_id,ChatAction.UPLOAD_VOICE,"voice")
            result=await bot.send_voice(**kwargs)
            if chat_id: mark_presence(chat_id)
            return result
        except Exception as e:
            last_error=e
            if attempt==0: await asyncio.sleep(1)
    record_failure("send_voice",kwargs.get("chat_id"),"",str(last_error))
    raise last_error

async def send_text_with_retry(bot, **kwargs):
    last_error = None
    chat_id = kwargs.get("chat_id")
    for attempt in range(3):
        try:
            if chat_id and attempt == 0:
                await bot_humanize(bot, chat_id, ChatAction.TYPING, "reply")
            kw = dict(kwargs)
            if attempt > 0:
                kw.pop("parse_mode", None)
                kw.pop("disable_web_page_preview", None)
            result = await bot.send_message(**kw)
            if chat_id:
                mark_presence(chat_id)
                txt = kw.get("text") or ""
                if txt:
                    record_sent_history(chat_id, "text", re.sub(r"<[^>]+>", "", txt))
            return result
        except Exception as e:
            last_error = e
            err_str = str(e).lower()
            # Flood wait: respect Telegram's backoff
            if "flood" in err_str or "retry" in err_str:
                import re as _re
                m = _re.search(r"retry.{0,10}(\d+)", err_str)
                wait = int(m.group(1)) if m else 30
                logger.warning("Flood wait %ds on send_text to %s", wait, chat_id)
                await asyncio.sleep(min(wait, 60))
            elif attempt < 2:
                await asyncio.sleep(1.5 * (attempt + 1))
    record_failure("send_message", kwargs.get("chat_id"), "", str(last_error)[:300])
    raise last_error

async def copy_message_with_retry(bot,**kwargs):
    last_error=None
    for attempt in range(2):
        try: return await bot.copy_message(**kwargs)
        except Exception as e:
            last_error=e
            if attempt==0: await asyncio.sleep(1)
    record_failure("copy_message",kwargs.get("chat_id"),"",str(last_error))
    raise last_error

def send_message_http_full(chat_id,text):
    http_humanize(chat_id,"typing","auto")
    data=tg_post("sendMessage",{"chat_id":chat_id,"text":text,"disable_web_page_preview":True})
    ok=bool(data.get("ok")); mid=data.get("result",{}).get("message_id") if ok else None
    if ok: mark_presence(chat_id); record_sent_history(chat_id,"http_text",text); return ok,mid
    data2=tg_post("sendMessage",{"chat_id":chat_id,"text":text})
    ok2=bool(data2.get("ok")); mid2=data2.get("result",{}).get("message_id") if ok2 else None
    if ok2: mark_presence(chat_id); record_sent_history(chat_id,"http_text",text); return ok2,mid2
    record_failure("send_message",chat_id,"",str(data2 or data)[:400])
    return False,None

def next_milestone(member_count,last_sent):
    for m in (100,500,1000):
        if member_count>=m and m>last_sent: return m
    return 0

def build_combined_names(members):
    names=[clean_name(m.first_name) for m in members[:5]]
    if len(members)>5: names.append(f"+{len(members)-5}")
    return ", ".join(names)

def build_burst_text(lang,title,members):
    group=title or ("our group" if lang=="en" else "আমাদের গ্রুপ")
    names=build_combined_names(members)
    if lang=="en": return f"✨ A warm welcome to {group}!\nNew members: {names}"
    return f"✨ {group} এ আন্তরিক স্বাগতম!\nনতুন সদস্যরা: {names}"

def guess_broadcast_action(msg):
    if not msg: return ChatAction.TYPING
    if getattr(msg,"photo",None): return ChatAction.UPLOAD_PHOTO
    if getattr(msg,"video",None): return ChatAction.UPLOAD_VIDEO
    if getattr(msg,"voice",None) or getattr(msg,"audio",None): return ChatAction.UPLOAD_VOICE
    if getattr(msg,"document",None): return ChatAction.UPLOAD_DOCUMENT
    return ChatAction.TYPING

async def fetch_profile_photo_bytes(bot,user_id):
    try:
        photos=await bot.get_user_profile_photos(user_id,limit=1)
        if not photos or getattr(photos,"total_count",0)<1: return None
        file=await bot.get_file(photos.photos[0][-1].file_id)
        return bytes(await file.download_as_bytearray())
    except: return None

def cleanup_old_temp_files(max_age_seconds=1800):
    now_ts = time.time()
    cleaned = 0
    try:
        for p in TMP_DIR.iterdir():
            try:
                if p.is_file() and now_ts - p.stat().st_mtime > max_age_seconds:
                    p.unlink(missing_ok=True)
                    cleaned += 1
            except Exception:
                pass
    except Exception:
        pass
    if cleaned:
        logger.info("Cleaned %d temp files", cleaned)

def cleanup_loop():
    logger.info("Cleanup loop started")
    _cycle = 0
    while True:
        try:
            cleanup_old_temp_files()
            _cycle += 1
            # Every 6 cycles (1 hour) also clean DB tables
            if _cycle % 6 == 0:
                try:
                    cleanup_daily_marks()
                    logger.info("DB cleanup done")
                except Exception:
                    logger.exception("DB cleanup failed")
        except Exception:
            logger.exception("cleanup_loop failed")
        time.sleep(600)

async def require_group_admin(update,context):
    chat=update.effective_chat; user=update.effective_user
    if not chat or chat.type not in {"group","supergroup"}:
        await update.effective_message.reply_text("Use this command in group."); return False
    ensure_group(chat.id,chat.title or "")
    if not user: return False
    member=await context.bot.get_chat_member(chat.id,user.id)
    if member.status not in {ChatMemberStatus.ADMINISTRATOR,ChatMemberStatus.OWNER}:
        await update.effective_message.reply_text(t(get_group_lang(chat.id),"only_group_admin")); return False
    return True

async def require_owner_private(update):
    user=update.effective_user; chat=update.effective_chat
    if not user or not chat or chat.type!="private" or not is_super_admin(user.id):
        await update.effective_message.reply_text("Only bot owners can use this command in private chat."); return False
    return True

async def delete_previous_welcome(context,chat_id):
    row=get_group(chat_id)
    if not row: return
    for mid in (row["last_primary_msg_id"],row["last_voice_msg_id"]):
        if mid:
            try: await context.bot.delete_message(chat_id=chat_id,message_id=int(mid))
            except: pass

async def maybe_send_milestone(context,chat_id,title,lang):
    try:
        row=get_group(chat_id)
        if not row: return
        member_count=await context.bot.get_chat_member_count(chat_id)
        milestone=next_milestone(member_count,int(row["last_milestone_sent"] or 0))
        if not milestone: return
        card=build_milestone_card_bytes(title or "GROUP",milestone)
        msg=await send_photo_with_retry(context.bot,chat_id=chat_id,photo=card,caption=f"🎉 {milestone} members milestone!")
        set_group_value(chat_id,"last_milestone_sent",milestone)
        asyncio.create_task(schedule_delete(context.bot,chat_id,msg.message_id,WELCOME_DELETE_AFTER+30))
    except: logger.exception("Milestone send failed in chat %s",chat_id)

# ═══════════════════════════════════════════════════════════════════════════════
# SMART ENGINE v3 — AI Welcome · Anti-Raid · Extended Keywords · Groq v2
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Anti-Raid System ─────────────────────────────────────────────────────────
raid_join_window: dict[int, deque] = defaultdict(lambda: deque(maxlen=30))
raid_alert_sent:  dict[int, float] = {}
RAID_THRESHOLD = 7
RAID_WINDOW    = 20
RAID_COOLDOWN  = 120

def is_raid_detected(chat_id: int) -> bool:
    now = time.time()
    w = raid_join_window[chat_id]
    w.append(now)
    while w and now - w[0] > RAID_WINDOW:
        w.popleft()
    return len(w) >= RAID_THRESHOLD

def raid_cooldown_active(chat_id: int) -> bool:
    return (time.time() - raid_alert_sent.get(chat_id, 0)) < RAID_COOLDOWN

def mark_raid_alerted(chat_id: int):
    raid_alert_sent[chat_id] = time.time()

RAID_WARN_BN = [
    "⚠️ অনেক নতুন সদস্য একসাথে যোগ দিচ্ছে। Admin-রা সতর্ক থাকুন।",
    "🚨 অস্বাভাবিক join activity ধরা পড়েছে। Admin-রা চেক করুন।",
    "🛡️ দ্রুত join হচ্ছে — group admin-রা একটু নজর রাখুন।",
]
RAID_WARN_EN = [
    "⚠️ Unusual join activity detected. Admins, please stay alert.",
    "🚨 Multiple rapid joins spotted. Group admins — heads up!",
    "🛡️ Fast joins detected — admins, please check the group.",
]

async def handle_raid_check(bot, chat_id: int, lang: str):
    if is_raid_detected(chat_id) and not raid_cooldown_active(chat_id):
        mark_raid_alerted(chat_id)
        warn = random.choice(RAID_WARN_EN if lang == "en" else RAID_WARN_BN)
        try:
            await bot.send_message(chat_id=chat_id, text=warn)
        except Exception:
            pass
        logger.warning("Raid detected in chat %s", chat_id)

# ─── Extended Smart Keyword System ────────────────────────────────────────────
BIRTHDAY_PATTERNS  = [r"\bbirthday\b",r"\bhbd\b",r"\bhappy bday\b",r"জন্মদিন",r"শুভ জন্মদিন",r"জন্মদিনের শুভেচ্ছা"]
CONGRATS_PATTERNS  = [r"\bcongrats?\b",r"\bwell done\b",r"\bbravo\b",r"অভিনন্দন",r"শুভকামনা",r"মাশাআল্লাহ",r"\bwon\b",r"\bpassed\b",r"পাস করেছ"]
SAD_PATTERNS       = [r"\bsad\b",r"\bcrying\b",r"\bcry\b",r"\bdepressed\b",r"মন খারাপ",r"কষ্ট লাগছে",r"কাঁদছি",r"দুঃখ"]
STRESS_PATTERNS    = [r"\bstressed?\b",r"\btired\b",r"\bexhausted\b",r"\bworried\b",r"\bexam\b",r"ক্লান্ত",r"টেনশন",r"পরীক্ষা",r"চিন্তা করছি"]
FOOD_PATTERNS      = [r"\bfood\b",r"\blunch\b",r"\bdinner\b",r"\bbreakfast\b",r"\bkhabar\b",r"খাবার",r"ভাত খাব",r"বিরিয়ানি",r"ইফতার",r"রান্না"]
LOVE_PATTERNS      = [r"\blove\b",r"\blovely\b",r"\bbeautiful\b",r"সুন্দর",r"ভালো লাগছে",r"\bawesome\b",r"\bamazing\b"]
MORNING_GREET      = [r"^শুভ সকাল$",r"^good morning$",r"^gm$",r"^subah$"]
NIGHT_GREET        = [r"^শুভ রাত্রি$",r"^good night$",r"^gn$",r"^রাত্রি$"]

BIRTHDAY_R_BN = ["🎂 জন্মদিনের উষ্ণ শুভেচ্ছা! ✨ আজকের দিনটা হোক অসাধারণ।","🎉 শুভ জন্মদিন! 🌟 এই বিশেষ দিনে অনেক ভালো থাকুন।","🎈 জন্মদিনে অনেক দোয়া ও শুভেচ্ছা রইল।💫 সুন্দর একটা বছর কাটুক।","💝 আজকের এই বিশেষ দিনে group-এর পক্ষ থেকে জন্মদিনের শুভেচ্ছা! 🌸"]
BIRTHDAY_R_EN = ["🎂 Happy Birthday! ✨ Wishing you an incredible day!","🎉 Many happy returns! 🌟 Hope today feels truly special.","🎈 Warmest birthday wishes! 💫 May this year be your best yet.","💝 The whole group celebrates with you today! Happy Birthday! 🌸"]

CONGRATS_R_BN = ["🎊 অভিনন্দন! তোমার সাফল্যে আমরাও আনন্দিত। ✨","🏆 অসাধারণ খবর! group-এর পক্ষ থেকে শুভকামনা ও অভিনন্দন।","💫 বাহ! অনেক অনেক অভিনন্দন। ভালো থাকো।","🌟 তোমার এই সাফল্য আমাদেরও গর্বিত করে। অভিনন্দন!"]
CONGRATS_R_EN = ["🎊 Congratulations! We're all so proud of you! ✨","🏆 Amazing news! The whole group celebrates your success.","💫 Well deserved! Warm congratulations from everyone here.","🌟 Your achievement makes us all proud. Congrats!"]

SAD_R_BN = ["🌷 মন খারাপ থাকলে বলো। এই group সবসময় তোমার পাশে।","💙 কষ্টের সময়টা কেটে যাবে। শান্ত থাকো, ভালো হবে ইনশাআল্লাহ।","🕊️ একটু খারাপ লাগলেও ঠিক হয়ে যাবে। আমরা আছি।","🌸 ভেঙে পড়ো না। এই কঠিন সময়টাও কেটে যাবে।"]
SAD_R_EN = ["🌷 Hey, it's okay. This group is here with you always. 💙","🕊️ Tough times pass. Hang in there — better days are coming.","💫 You're not alone here. Take it easy and breathe.","🌸 It's okay not to be okay sometimes. We're with you."]

STRESS_R_BN = ["📚 একটু বিশ্রাম নাও। তুমি পারবে, শান্ত থাকো।","💪 একটু কঠিন লাগলেও হাল ছেড়ো না। এগিয়ে যাও।","🍵 থামো, শ্বাস নাও। এক ধাপ এক ধাপ করে এগোলেই হবে।","🌿 চাপ নেওয়াটা স্বাভাবিক। কিন্তু মনে রেখো, তুমি যথেষ্ট সক্ষম।"]
STRESS_R_EN = ["📚 Take a short break — you've absolutely got this! 💪","🍵 Breathe. One step at a time. You'll get through it.","🌷 It's okay to feel overwhelmed. Rest and reset — you can do it.","🌿 The pressure is real, but so is your strength. Keep going."]

FOOD_R_BN = ["🍽️ খাবারের কথায় মেজাজ ভালো হয়ে যায়! 😄 মজা করে খাও।","🥘 বিরিয়ানি হলে আরও ভালো হতো! 😋 যা-ই হোক, ভালো খাও।","🍜 খেয়ে নাও, বাকি কাজ পরে হবে। 😄","🌶️ খাওয়ার আলোচনায় group জমে ওঠে! ভালো খাও সবাই।"]
FOOD_R_EN = ["🍽️ Food talk = instant mood boost! 😄 Enjoy your meal!","🥘 Now I'm hungry too! 😋 Eat well, eat happy!","🍜 Good food, good mood. Savor every bite! 😄","🌶️ Nothing unites a group like food talk! Enjoy! 🍽️"]

LOVE_R_BN = ["🌸 সুন্দর অনুভূতি ভাগ করে নেওয়াটা দারুণ। group-কে সুন্দর রাখো।","✨ এই ইতিবাচক energy group-এ ছড়িয়ে পড়ুক।","💫 ভালো লাগার মুহূর্তগুলো মূল্যবান। ধরে রাখো।"]
LOVE_R_EN = ["🌸 Sharing good vibes makes the group beautiful. Keep it up!","✨ That positive energy is contagious — love it!","💫 Good moments like these are worth savoring."]

MORNING_R_BN = ["🌅 শুভ সকাল! দিনটা সুন্দর হোক সবার।","☀️ সকালের নরম আলোয় সবাইকে শুভেচ্ছা!","🌤️ নতুন দিনের শুভ সূচনা হোক সবার।"]
MORNING_R_EN = ["🌅 Good morning! Wishing everyone a beautiful day.","☀️ Morning greetings to this wonderful group!","🌤️ A fresh start to a great day — good morning!"]

NIGHT_R_BN = ["🌙 শুভ রাত্রি! আরামদায়ক ঘুম হোক।","⭐ রাতের শান্তিতে সবাই ভালো থাকুন।","💤 আজকের দিনটা সুন্দর ছিল। রাতটাও হোক তেমন।"]
NIGHT_R_EN = ["🌙 Good night! Rest well, everyone.","⭐ Peaceful night to this lovely group.","💤 Sweet dreams! See everyone tomorrow."]

SMART_KW_CHECKS = [
    ("birthday",  BIRTHDAY_PATTERNS),
    ("congrats",  CONGRATS_PATTERNS),
    ("sad",       SAD_PATTERNS),
    ("stress",    STRESS_PATTERNS),
    ("food",      FOOD_PATTERNS),
    ("love",      LOVE_PATTERNS),
    ("morning",   MORNING_GREET),
    ("night",     NIGHT_GREET),
]
SMART_KW_REPLIES: dict[str, tuple] = {
    "birthday": (BIRTHDAY_R_BN, BIRTHDAY_R_EN),
    "congrats":  (CONGRATS_R_BN, CONGRATS_R_EN),
    "sad":       (SAD_R_BN, SAD_R_EN),
    "stress":    (STRESS_R_BN, STRESS_R_EN),
    "food":      (FOOD_R_BN, FOOD_R_EN),
    "love":      (LOVE_R_BN, LOVE_R_EN),
    "morning":   (MORNING_R_BN, MORNING_R_EN),
    "night":     (NIGHT_R_BN, NIGHT_R_EN),
}
SMART_KW_COOLDOWNS = {
    "birthday": 86400,  "congrats": 3600, "sad": 1800,
    "stress": 1800, "food": 900, "love": 1200,
    "morning": 3600, "night": 3600,
}
SMART_KW_CHANCES = {
    "birthday": 0.95, "congrats": 0.90, "sad": 0.85,
    "stress": 0.75, "food": 0.55, "love": 0.45,
    "morning": 0.70, "night": 0.70,
}
smart_kw_chat_at: dict[int, dict[str, float]] = defaultdict(dict)

def smart_keyword_match(text: str) -> str | None:
    lowered = re.sub(r"\s+", " ", (text or "").lower().strip())
    if not lowered or URLISH_RE.search(lowered) or len(lowered) > 120:
        return None
    for key, patterns in SMART_KW_CHECKS:
        for p in patterns:
            if re.search(p, lowered, re.I):
                return key
    return None

def smart_kw_allowed(chat_id: int, key: str) -> bool:
    now = time.time()
    cooldown = SMART_KW_COOLDOWNS.get(key, 1800)
    last = smart_kw_chat_at[chat_id].get(key, 0)
    if now - last >= cooldown:
        smart_kw_chat_at[chat_id][key] = now
        return True
    return False

def smart_kw_reply(lang: str, key: str) -> str | None:
    if key not in SMART_KW_REPLIES:
        return None
    pool_bn, pool_en = SMART_KW_REPLIES[key]
    return random.choice(pool_en if lang == "en" else pool_bn)

# ─── AI Welcome (Groq-powered, cached, graceful fallback) ────────────────────
_AI_WELCOME_CACHE: dict[tuple, str] = {}
_AI_WELCOME_TS:    dict[tuple, float] = {}
_AI_WELCOME_TTL = 1800  # 30 min cache

def groq_generate_welcome(lang: str, first_name: str, group_title: str, phase: str) -> str | None:
    # Disabled: saves Groq tokens; template system handles welcome
    return None
    if not GROQ_API_KEYS:
        return None
    key = (lang, first_name[:10], (group_title or "")[:20], phase)
    now = time.time()
    if key in _AI_WELCOME_CACHE and now - _AI_WELCOME_TS.get(key, 0) < _AI_WELCOME_TTL:
        return _AI_WELCOME_CACHE[key]

    safe_name  = first_name[:20].strip()
    safe_group = (group_title or ("our group" if lang == "en" else "আমাদের গ্রুপ"))[:30].strip()
    ph_en = {"morning":"morning","day":"afternoon","evening":"evening","night":"night"}.get(phase,"day")
    ph_bn = {"morning":"সকাল","day":"দিন","evening":"সন্ধ্যা","night":"রাত"}.get(phase,"দিন")

    if lang == "en":
        prompt = (
            f"Write ONE short, warm, premium welcome message in English for a Telegram group.\n"
            f"New member: {safe_name} | Group: {safe_group} | Time: {ph_en}\n"
            f"Rules: 1-2 sentences, under 130 chars, warm & elegant, mention the member's name "
            f"naturally, match time-of-day ({ph_en}), max 2 emojis, no hashtags, no AI/bot mentions, "
            f"sound like a genuine human host, avoid clichés like 'have a great day'.\n"
            f"Return ONLY the welcome message, nothing else."
        )
    else:
        prompt = (
            f"একটি Telegram group-এর জন্য একটি উষ্ণ, মার্জিত বাংলা স্বাগত বার্তা লেখো।\n"
            f"নতুন সদস্য: {safe_name} | Group: {safe_group} | সময়: {ph_bn}\n"
            f"নিয়ম: ১-২ বাক্য, ১৩০ অক্ষরের মধ্যে, উষ্ণ ও মার্জিত, নামটি স্বাভাবিকভাবে উল্লেখ করো, "
            f"সময়ের ({ph_bn}) সাথে মিলিয়ে লেখো, সর্বোচ্চ ২টা emoji, hashtag নয়, "
            f"AI/bot উল্লেখ নয়, মানবিক host-এর মতো লেখো, cliché পরিহার করো।\n"
            f"শুধু স্বাগত বার্তাটি দাও।"
        )
    try:
        data = _groq_chat_request({
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content":
                 "You write premium, tasteful, emotionally intelligent Telegram group welcome messages. "
                 "Every message feels genuinely human, fresh, and perfectly timed. Never sound robotic."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.93,
            "max_tokens": 90,
        })
        raw = (data["choices"][0]["message"]["content"] or "").strip()
        raw = re.sub(r'^["\']|["\']$', "", raw).strip()
        if len(raw) < 10 or len(raw) > 220:
            return None
        _AI_WELCOME_CACHE[key] = raw
        _AI_WELCOME_TS[key] = now
        logger.info("AI welcome OK | lang=%s name=%s", lang, safe_name)
        return raw
    except Exception as e:
        logger.warning("AI welcome failed: %s", e)
        return None

def smart_welcome_text(lang: str, mention_name: str, first_name: str,
                       group_title: str, custom_text: str | None, chat_id: int = 0) -> tuple[str, str]:
    phase = phase_now()
    safe_group = group_title or ("our group" if lang == "en" else "আমাদের গ্রুপ")
    if custom_text:
        text = custom_text.replace("{name}", mention_name).replace("{group}", safe_group).replace("{phase}", phase)
        voice = (f"Hello {first_name}, welcome to {safe_group}." if lang == "en"
                 else f"{first_name}, তোমাকে {safe_group} এ স্বাগতম।")
        return text, voice

    # Try Groq AI welcome
    ai_raw = None
    try:
        ai_raw = groq_generate_welcome(lang, first_name, group_title or "", phase)
    except Exception:
        pass

    if ai_raw and len(ai_raw) > 10:
        if first_name in ai_raw:
            ai_text = ai_raw.replace(first_name, mention_name, 1)
        else:
            ai_text = f"{mention_name} — {ai_raw}"
        voice = personalize_voice_text(
            f"Hello {first_name}, welcome to {safe_group}." if lang == "en"
            else f"হ্যালো {first_name}, {safe_group} এ তোমাকে স্বাগতম।",
            first_name, lang, chat_id
        )
        return ai_text, voice

    # Template fallback
    return welcome_texts(lang, mention_name, first_name, group_title, None, chat_id)

# ─── Groq Hourly v2 (Advanced, context-aware prompts) ────────────────────────
_GROQ_SYSTEM_V2 = (
    "You are an expert Telegram community manager writing premium, human-feeling group messages. "
    "Your messages are warm, emotionally intelligent, culturally sensitive, and never repetitive. "
    "You write as if you genuinely care about the group members. "
    "You avoid clichés, never over-use emojis, and make every single message feel fresh and authentic. "
    "You match the tone precisely to the time of day and mood requested."
)

def groq_generate_batch_v2(lang: str, phase: str, mood: str = "soft", festival_key: str = "") -> list[str]:
    if not AI_HOURLY_ENABLED or not GROQ_API_KEYS:
        _update_groq_status(False, "Groq disabled or API key missing")
        return []

    ph_map = {
        "bn": {"morning":"সকাল","day":"দিন বা দুপুর","evening":"সন্ধ্যা","night":"রাত"},
        "en": {"morning":"morning","day":"daytime or afternoon","evening":"evening","night":"night"},
    }
    mood_desc_en = {
        "peaceful":   "calm, serene, mindful — like a quiet Sunday morning",
        "motivating": "uplifting, forward-looking, quietly encouraging",
        "classy":     "elegant, refined, sophisticated — like a luxury brand",
        "cozy":       "warm, intimate, comforting — like a cup of tea",
        "soft":       "gentle, tender, delicate — like a warm whisper",
        "energetic":  "lively, vibrant, enthusiastic — like a morning run",
    }
    mood_desc_bn = {
        "peaceful":   "শান্ত, প্রশান্তিময় — যেন একটি শান্ত রবিবারের সকাল",
        "motivating": "অনুপ্রেরণামূলক, উৎসাহজনক — সামনে এগিয়ে যাওয়ার আহ্বান",
        "classy":     "মার্জিত, পরিশীলিত — যেন একটি luxury brand-এর ভাষা",
        "cozy":       "আরামদায়ক, উষ্ণ — যেন এক কাপ গরম চা",
        "soft":       "কোমল, মৃদু — যেন একটি উষ্ণ ফিসফিসানি",
        "energetic":  "প্রাণবন্ত, উদ্যমী — যেন ভোরের হাঁটা",
    }
    festival_note = (f"- Subtly reflect a festive mood for {festival_hourly_prefix(lang)}, "
                     f"but keep it tasteful and inclusive\n") if festival_key else ""

    if lang == "en":
        ph = ph_map["en"][phase]
        md = mood_desc_en.get(mood, "warm and elegant")
        prompt = (
            f"Write {AI_BATCH_SIZE} unique, premium Telegram group hourly messages in English.\n"
            f"Time of day: {ph}  |  Emotional tone: {md}\n\n"
            f"Non-negotiable rules:\n"
            f"- Each message: 22–{AI_MAX_TEXT_LEN} characters, one complete and meaningful thought\n"
            f"- Must authentically match {ph} — never use wrong-time greetings\n"
            f"- Warm, human, and elegant — never robotic, never generic\n"
            f"- Max 2 emojis per message — use them purposefully, not decoratively\n"
            f"- No hashtags, no romantic content, no politics, no religion\n"
            f"- Never use: 'have a great day', 'stay blessed', 'good vibes only', or similar clichés\n"
            f"- Each of the {AI_BATCH_SIZE} messages must have a DIFFERENT opening word and sentence structure\n"
            f"- Write as a thoughtful human host who genuinely cares, not as a bot\n"
            f"- Messages should feel like they come from wisdom, not from a template\n"
            f"{festival_note}"
            f"\nReturn ONLY the {AI_BATCH_SIZE} messages, one per line, no numbers or bullets."
        )
    else:
        ph = ph_map["bn"][phase]
        md = mood_desc_bn.get(mood, "উষ্ণ ও মার্জিত")
        prompt = (
            f"বাংলায় {AI_BATCH_SIZE}টি অনন্য, প্রিমিয়াম Telegram group hourly বার্তা লেখো।\n"
            f"সময়: {ph}  |  মনোভাব: {md}\n\n"
            f"অলঙ্ঘনীয় নিয়ম:\n"
            f"- প্রতিটি বার্তা: ২২–{AI_MAX_TEXT_LEN} অক্ষর, একটি সম্পূর্ণ ও অর্থবহ ভাব\n"
            f"- অবশ্যই {ph}-এর সাথে মানানসই হবে — ভুল সময়ের শুভেচ্ছা নয়\n"
            f"- উষ্ণ, মানবিক, মার্জিত — robotic বা generic নয়\n"
            f"- প্রতিটি বার্তায় সর্বোচ্চ ২টা emoji — উদ্দেশ্যমূলকভাবে ব্যবহার করো\n"
            f"- hashtag নয়, romantic নয়, রাজনৈতিক নয়, ধর্মীয় নয়\n"
            f"- 'ভালো থাকুন', 'সুখী থাকুন', 'আল্লাহ ভালো রাখুন' — এই ধরনের cliché পরিহার\n"
            f"- প্রতিটি বার্তার প্রথম শব্দ ও বাক্যের গঠন আলাদা হতে হবে\n"
            f"- একজন চিন্তাশীল, যত্নশীল মানুষের মতো লেখো — bot-এর মতো নয়\n"
            f"- বার্তাগুলো যেন জ্ঞান ও অনুভূতি থেকে আসে, template থেকে নয়\n"
            f"{festival_note}"
            f"\nশুধু {AI_BATCH_SIZE}টি বার্তা দাও, একটি করে লাইনে, কোনো নম্বর বা bullet নয়।"
        )
    try:
        data = _groq_chat_request({
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": _GROQ_SYSTEM_V2},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.88,
            "max_tokens": 300,
        })
        content = data["choices"][0]["message"]["content"]
        lines = sanitize_ai_lines(content, lang, phase)
        if lines:
            _update_groq_status(True, f"v2 OK | {len(lines)} lines | {mood}")
            logger.info("Groq v2 | lang=%s phase=%s mood=%s count=%s", lang, phase, mood, len(lines))
            return lines
        _update_groq_status(False, "Groq v2 empty/filtered")
        return []
    except Exception as e:
        _update_groq_status(False, f"v2 fail: {e}")
        record_failure("ai", None, "", str(e))
        logger.exception("Groq v2 fail | lang=%s phase=%s", lang, phase)
        return []

def get_batch_pool_v2(lang: str, phase: str, mood: str = "soft", festival_key: str = ""):
    key = (lang, phase, mood, festival_key, "v2")
    now_ts = time.time()
    # Purge stale cache entries (older than 2 hours)
    stale = [k for k, v in AI_BATCH_CACHE.items() if now_ts - v.get("created_at", 0) > 7200]
    for sk in stale:
        AI_BATCH_CACHE.pop(sk, None)
    cached = AI_BATCH_CACHE.get(key)
    if cached and now_ts - cached["created_at"] < 900 and cached.get("texts"):
        return cached["texts"], cached["source"]
    # v2 first → v1 fallback → static fallback
    ai_lines = groq_generate_batch_v2(lang, phase, mood=mood, festival_key=festival_key)
    if not ai_lines:
        ai_lines = groq_generate_batch(lang, phase, mood=mood, festival_key=festival_key)
    if ai_lines:
        source, texts = "ai", ai_lines
    else:
        source, texts = "fallback", build_fallback_messages(lang, phase, mood=mood, festival_key=festival_key)
    AI_BATCH_CACHE[key] = {"texts": texts, "source": source, "created_at": now_ts}
    for line in texts[:12]:
        try: save_generated_text(lang, phase, source, line)
        except: pass
    return texts, source

# ─── Improved Hourly Loop (equal for all groups, no skip) ────────────────────
def hourly_loop():
    logger.info("Hourly Loop started")
    while True:
        try:
            due_rows = get_enabled_groups_for_hourly()
            if due_rows:
                phase = phase_now()
                prepared = {}
                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    lang = get_group_lang(chat_id)
                    mood = next_hourly_mood(chat_id)
                    festival_key = (current_festival() or {}).get("key", "") if current_festival_mode(chat_id) else ""
                    prepared[chat_id] = (lang, mood, festival_key)

                pools = {}
                pool_source = {}
                for lang, mood, fk in {(l, m, f) for l, m, f in prepared.values()}:
                    texts, source = get_batch_pool_v2(lang, phase, mood, fk)
                    pools[(lang, mood, fk)] = texts
                    pool_source[(lang, mood, fk)] = source

                for row in due_rows:
                    chat_id = int(row["chat_id"])
                    if chat_id not in prepared:
                        continue
                    lang, mood, fk = prepared[chat_id]
                    msg = pick_hourly_message(chat_id, lang, phase, pools[(lang, mood, fk)])
                    ok, mid = send_message_http_full(chat_id, msg)
                    if ok:
                        set_group_value(chat_id, "last_hourly_at", int(time.time()))
                        increment_group_counter(chat_id, "total_hourly_sent")
                        if pool_source.get((lang, mood, fk)) == "ai":
                            set_group_value(chat_id, "last_ai_success_at", int(time.time()))
                        else:
                            set_group_value(chat_id, "last_fallback_used_at", int(time.time()))
                        ca = current_hourly_delete_after(chat_id)
                        if ca > 0 and mid:
                            schedule_http_delete(chat_id, mid, ca)
                        try: maybe_send_countdown_reminder(chat_id, row["title"] or "")
                        except: pass
                        logger.info("Hourly sent to %s | mood=%s", chat_id, mood)
                    else:
                        logger.warning("Hourly failed to %s", chat_id)

            for row in get_all_enabled_group_rows():
                try: maybe_send_scheduled_specials(row)
                except: logger.exception("Special scheduler failed in %s", row["chat_id"])

            cleanup_daily_marks()
        except Exception:
            logger.exception("hourly_loop failed")
        time.sleep(60)

# ─── Smart track_group ────────────────────────────────────────────────────────
async def track_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in {"group", "supergroup"}:
        ensure_group(chat.id, chat.title or "")
        msg = update.effective_message
        if msg and not (msg.from_user and msg.from_user.is_bot):
            body = (msg.text or msg.caption or "")[:300]
            group_taste_memory[chat.id].append(detect_text_taste(body, chat.title or ""))

# ─── Smart on_status ──────────────────────────────────────────────────────────
async def on_keyword_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg = update.effective_message
    user = update.effective_user
    if not chat or chat.type not in {"group","supergroup"} or not msg or not user or user.is_bot:
        return
    ensure_group(chat.id, chat.title or "")

    text = msg.text or ""

    if not current_keyword_mode(chat.id) or is_linkish_message(msg):
        return

    # 1. Base keywords (salam / hello / night)
    matched = keyword_reply_match(text)
    if matched:
        now_ts = time.time()
        cd_ok  = now_ts - keyword_last_chat_at.get(chat.id, 0) >= KEYWORD_COOLDOWN_SECONDS
        ucd_ok = now_ts - keyword_last_user_at.get((chat.id, user.id), 0) >= KEYWORD_USER_COOLDOWN_SECONDS
        if cd_ok and ucd_ok and random.random() <= KEYWORD_REPLY_CHANCE:
            lang = get_group_lang(chat.id)
            options = [x for x in keyword_reply_variants(lang, matched, chat.id)
                       if not was_recent_duplicate_text(chat.id, "keyword", x, 2)]
            if not options:
                options = keyword_reply_variants(lang, matched, chat.id)
            reply_text = random.choice(options)
            keyword_last_chat_at[chat.id] = now_ts
            keyword_last_user_at[(chat.id, user.id)] = now_ts
            try:
                await bot_humanize(context.bot, chat.id, ChatAction.TYPING, "reply")
                await msg.reply_text(reply_text)
                mark_presence(chat.id)
                record_sent_history(chat.id, "keyword", reply_text)
            except Exception:
                logger.exception("Base keyword reply failed in %s", chat.id)
        return

    # 2. Smart extended keywords (birthday, congrats, sad, stress, food, love, morning, night)
    smart_key = smart_keyword_match(text)
    if smart_key:
        chance = SMART_KW_CHANCES.get(smart_key, 0.55)
        if smart_kw_allowed(chat.id, smart_key) and random.random() < chance:
            lang = get_group_lang(chat.id)
            reply = smart_kw_reply(lang, smart_key)
            if reply:
                try:
                    await asyncio.sleep(random.uniform(1.5, 3.5))
                    await msg.reply_text(reply)
                    mark_presence(chat.id)
                    record_sent_history(chat.id, "smart_kw", reply)
                except Exception:
                    logger.exception("Smart keyword reply failed in %s", chat.id)

# ─── Improved on_new_chat_members (with raid check) ──────────────────────────
async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type not in {"group","supergroup"} or not update.effective_message:
        return
    ensure_group(chat.id, chat.title or "")
    group = get_group(chat.id)
    if int(group["delete_service"]) == 1:
        try: await update.effective_message.delete()
        except: pass
    members = [m for m in (update.effective_message.new_chat_members or []) if not m.is_bot]
    if not members:
        return
    lang = get_group_lang(chat.id)
    asyncio.create_task(handle_raid_check(context.bot, chat.id, lang))
    for member in members:
        raid_join_window[chat.id].append(time.time())
        chat_join_history[chat.id].append(time.time())
        await queue_join_welcome(context.application, chat.id, chat.title or "", member)

# ─── Improved maybe_welcome (AI-powered, smart, equal for all groups) ─────────
async def maybe_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, title: str, user):
    ensure_group(chat_id, title or "")
    group = get_group(chat_id)
    if not group or int(group["enabled"]) != 1 or user.is_bot:
        return
    if is_recent_duplicate(chat_id, user.id):
        return
    if time.time() - get_last_join_time(chat_id, user.id) < REJOIN_IGNORE_SECONDS:
        return

    lang = get_group_lang(chat_id)
    first_name = clean_name(user.first_name)
    mention_name = user.mention_html(first_name)
    save_join_time(chat_id, user.id)

    # Burst mode
    if is_join_burst(chat_id):
        try:
            compact = t(lang, "burst_compact",
                        name=mention_name,
                        group=(title or ("our group" if lang == "en" else "আমাদের গ্রুপ")))
            variants = variantize_message_text(chat_id, lang, compact, kind="welcome")
            compact = next((v for v in variants
                            if not was_recent_duplicate_text(chat_id, "welcome", v, 2)), variants[0])
            msg = await send_text_with_retry(context.bot, chat_id=chat_id,
                                             text=compact, parse_mode=ParseMode.HTML)
            record_sent_history(chat_id, "welcome", compact)
            set_group_value(chat_id, "last_primary_msg_id", msg.message_id)
            increment_group_counter(chat_id, "total_welcome_sent")
            set_group_value(chat_id, "last_welcome_at", int(time.time()))
            asyncio.create_task(schedule_delete(context.bot, chat_id, msg.message_id, WELCOME_DELETE_AFTER))
            await maybe_send_milestone(context, chat_id, title or "", lang)
        except Exception:
            logger.exception("Burst welcome failed in %s", chat_id)
        return

    # Smart welcome (AI-powered with graceful fallback)
    text_welcome, voice_text = smart_welcome_text(
        lang, mention_name, first_name, title or "", group["custom_welcome"], chat_id=chat_id
    )
    variants = variantize_message_text(chat_id, lang, text_welcome, kind="welcome")
    text_welcome = next(
        (v for v in variants if not was_recent_duplicate_text(chat_id, "welcome", v, 2)),
        variants[0]
    )
    voice_text = personalize_voice_text(voice_text, first_name, lang, chat_id=chat_id)
    await delete_previous_welcome(context, chat_id)

    primary = None
    voice_msg = None
    voice_path = TMP_DIR / f"welcome_{chat_id}_{user.id}_{int(time.time())}.mp3"
    try:
        style = current_welcome_style(chat_id)
        footer = current_footer_text(chat_id)
        style, footer, festival = effective_style_footer(chat_id, style, footer)
        if festival and len(text_welcome) < 900:
            fest_name = festival["name_bn"] if lang == "bn" else festival["name_en"]
            text_welcome = f"{text_welcome}\n\n✨ {fest_name}"
        member_count = None
        try: member_count = await context.bot.get_chat_member_count(chat_id)
        except: pass
        profile_bytes = await fetch_profile_photo_bytes(context.bot, user.id)
        cover = build_cover_bytes(first_name, title or "GROUP", lang,
                                  style=style, footer=footer,
                                  profile_bytes=profile_bytes, member_count=member_count)
        try:
            primary = await send_photo_with_retry(context.bot, chat_id=chat_id,
                                                  photo=cover, caption=text_welcome,
                                                  parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Photo welcome failed in %s, falling back to text", chat_id)
            primary = await send_text_with_retry(context.bot, chat_id=chat_id,
                                                 text=re.sub(r"<[^>]+>","", text_welcome))

        if int(group["voice_enabled"]) == 1 and primary:
            try:
                await make_voice_file(voice_text, selected_voice_name(lang, chat_id), voice_path)
                voice_msg = await send_voice_with_retry(
                    context.bot, chat_id=chat_id,
                    voice=voice_path.read_bytes(),
                    caption=t(lang, "welcome_voice_caption")
                )
            except Exception:
                logger.exception("Voice welcome failed in %s", chat_id)

        set_group_value(chat_id, "last_primary_msg_id", primary.message_id if primary else None)
        set_group_value(chat_id, "last_voice_msg_id", voice_msg.message_id if voice_msg else None)
        set_group_value(chat_id, "updated_at", int(time.time()))
        set_group_value(chat_id, "last_welcome_at", int(time.time()))
        increment_group_counter(chat_id, "total_welcome_sent")
        record_sent_history(chat_id, "welcome", re.sub(r"<[^>]+>","",text_welcome))
        if primary:
            asyncio.create_task(schedule_delete(context.bot, chat_id, primary.message_id, WELCOME_DELETE_AFTER))
        if voice_msg:
            asyncio.create_task(schedule_delete(context.bot, chat_id, voice_msg.message_id, WELCOME_DELETE_AFTER))
        await maybe_send_milestone(context, chat_id, title or "", lang)
    except Exception:
        logger.exception("Welcome failed in chat %s for user %s", chat_id, user.id)
    finally:
        if voice_path.exists():
            try: voice_path.unlink()
            except: pass

async def flush_join_queue(application,chat_id):
    pending_join_tasks.pop(chat_id,None)
    title=pending_join_titles.pop(chat_id,"")
    members=list(pending_join_members.pop(chat_id,{}).values())
    if not members: return
    ctx=type("QueueContext",(),{"bot":application.bot,"application":application})()
    lang=get_group_lang(chat_id)
    if len(members)>=2:
        try:
            style=current_welcome_style(chat_id); footer=current_footer_text(chat_id)
            style,footer,festival=effective_style_footer(chat_id,style,footer)
            names_text=build_combined_names(members)
            card=build_combined_welcome_card_bytes(title or "GROUP",lang,names_text,style=style,footer=footer)
            caption=build_burst_text(lang,title or "",members)
            if festival:
                fest_name=festival["name_bn"] if lang=="bn" else festival["name_en"]
                caption=f"{caption}\n\n✨ {fest_name}"
            msg=await send_photo_with_retry(application.bot,chat_id=chat_id,photo=card,caption=caption)
            set_group_value(chat_id,"last_primary_msg_id",msg.message_id)
            set_group_value(chat_id,"last_voice_msg_id",None)
            increment_group_counter(chat_id,"total_welcome_sent",amount=len(members))
            set_group_value(chat_id,"last_welcome_at",int(time.time()))
            asyncio.create_task(schedule_delete(application.bot,chat_id,msg.message_id,WELCOME_DELETE_AFTER))
            await maybe_send_milestone(ctx,chat_id,title or "",lang)
            return
        except: logger.exception("Queued combined welcome failed in %s",chat_id)
    for member in members[:1]: await maybe_welcome(ctx,chat_id,title or "",member)

async def queue_join_welcome(application,chat_id,title,user):
    if user.is_bot: return
    if is_recent_duplicate(chat_id,user.id): return
    pending_join_members[chat_id][user.id]=user
    pending_join_titles[chat_id]=title or ""
    if chat_id not in pending_join_tasks or pending_join_tasks[chat_id].done():
        async def _runner():
            await asyncio.sleep(random.randint(WELCOME_QUEUE_MIN_SECONDS,WELCOME_QUEUE_MAX_SECONDS))
            await flush_join_queue(application,chat_id)
        pending_join_tasks[chat_id]=asyncio.create_task(_runner())

def maybe_send_countdown_reminder(chat_id,title):
    row=get_countdown(chat_id)
    if not row: return
    now=local_now(); today_key=now.strftime("%Y-%m-%d")
    if row["last_sent_day"]==today_key: return
    diff=int(row["target_ts"])-int(now.timestamp())
    if diff<=0 or diff>COUNTDOWN_NOTIFY_WINDOW_DAYS*86400: return
    text=f"⏳ {row['title']}\n{diff//86400} days {(diff%86400)//3600} hours left"
    ok,_=send_message_http_full(chat_id,text)
    if ok: update_countdown_last_sent_day(chat_id,today_key)

def _special_lines(lang,key):
    bank={"bn":{"monday":["🌟 নতুন সপ্তাহটা সুন্দরভাবে শুরু হোক। মনোযোগ, শান্তি আর সাফল্য থাকুক সবার সাথে।","💼 সোমবার মানেই নতুন শুরু। আজকের দিনটা হোক গুছানো আর ফলপ্রসূ।","✨ সপ্তাহের শুরুতে এই group-এর সবার জন্য রইল উজ্জ্বল শুভেচ্ছা।"],"friday":["🕌 জুমার দিনটা হোক শান্ত, সুন্দর আর বরকতময়। এই group-এর সবার জন্য শুভেচ্ছা।","🌙 শুক্রবারের কোমল শুভেচ্ছা। আজকের দিনটা হোক প্রশান্তি ভরা।","💙 জুমার দিনের মিষ্টি শুভেচ্ছা রইল। ভালো থাকুন সবাই।"],"exam":["📘 আজ {title}। শান্ত থাকুন, মনোযোগ ধরে রাখুন, আর নিজের সেরাটা দিন।","📝 আজ {title}। আত্মবিশ্বাস রাখুন — ইনশাআল্লাহ ভালো হবে।","🎯 {title} আজ। মনটা শান্ত রেখে সুন্দরভাবে এগিয়ে যান।"]},"en":{"monday":["🌟 A fresh week begins today. Wishing everyone focus, calm, and a strong start.","💼 Monday is a new beginning. Hope the day feels organized and productive.","✨ Warm wishes to this group for a bright and graceful week ahead."],"friday":["🌙 Soft Friday wishes to everyone. Hope the day feels peaceful and kind.","💙 Wishing this group a calm, gentle, and beautiful Friday.","✨ May your Friday carry a little more peace and comfort."],"exam":["📘 {title} is today. Stay calm, focused, and do your best.","📝 It's {title} today. Trust yourself and move forward with confidence.","🎯 {title} is today. Keep your mind steady and your heart calm."]}}
    return bank["en" if lang=="en" else "bn"][key]

def maybe_weekly_special_text(lang):
    now=local_now()
    if now.weekday()==0 and now.hour>=MONDAY_SPECIAL_HOUR: return "weekly_monday",random.choice(_special_lines(lang,"monday"))
    if now.weekday()==4 and now.hour>=FRIDAY_SPECIAL_HOUR: return "weekly_friday",random.choice(_special_lines(lang,"friday"))
    return None

def maybe_send_scheduled_specials(chat_row):
    chat_id=int(chat_row["chat_id"]); lang=get_group_lang(chat_id)
    today_key=local_now().strftime("%Y-%m-%d")
    weekly=maybe_weekly_special_text(lang)
    if weekly:
        event_key,text=weekly
        if not was_daily_event_sent(chat_id,event_key,today_key):
            ok,mid=send_message_http_full(chat_id,text)
            if ok:
                mark_daily_event_sent(chat_id,event_key,today_key)
                if SPECIAL_EVENT_DELETE_AFTER>0 and mid: schedule_http_delete(chat_id,mid,SPECIAL_EVENT_DELETE_AFTER)
    exam_row=get_scheduled_event(chat_id,"exam")
    if exam_row:
        target=int(exam_row["target_ts"]); now_ts=int(local_now().timestamp())
        event_day=datetime.fromtimestamp(target,ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d")
        if event_day==today_key and exam_row["last_sent_day"]!=today_key and now_ts>=target:
            text=random.choice(_special_lines(lang,"exam")).format(title=exam_row["title"])
            try:
                ok,mid=send_message_http_full(chat_id,text)
                if ok and SPECIAL_EVENT_DELETE_AFTER>0 and mid: schedule_http_delete(chat_id,mid,SPECIAL_EVENT_DELETE_AFTER)
            except: pass
            with db_connect() as conn:
                conn.execute("UPDATE scheduled_events SET last_sent_day=? WHERE chat_id=? AND event_kind='exam'",(today_key,chat_id))
                conn.commit()

def keyword_reply_match(text):
    lowered=re.sub(r"\s+"," ",(text or "").strip().lower())
    if not lowered or URLISH_RE.search(lowered): return None
    cleaned=re.sub(r"\s+"," ",re.sub(r"[^\w\s\u0980-\u09ff]"," ",lowered)).strip()
    if len(cleaned)>60: return None
    checks=[("salam",[r"\bassalamu alaikum\b",r"\bassalamualaikum\b",r"^আসসালামু আলাইকুম$"]),("hello",[r"\bhello everyone\b",r"\bhi everyone\b",r"\bhey everyone\b",r"^হ্যালো সবাই$",r"^হাই সবাই$"]),("night",[r"\bgood night\b",r"^gn$",r"^শুভ রাত্রি$",r"^গুড নাইট$"])]
    for key,patterns in checks:
        for p in patterns:
            if re.search(p,cleaned,re.I): return key
    return None

def keyword_reply_variants(lang,matched,chat_id):
    base=KEYWORD_REPLIES["en" if lang=="en" else "bn"][matched]
    out=[]
    for item in base: out.extend(variantize_message_text(chat_id,lang,item,kind="keyword"))
    seen=[]; used=set()
    for x in out:
        if x not in used: seen.append(x); used.add(x)
    return seen or base

# ═══════════════════════════════════════════════════════════════════════════════
# PREMIUM SYSTEM UPGRADES — Leaderboard Fix · Owner Group Browser ·
# Premium Welcome Card · Upgraded Status · Smart Admin Panel
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Premium Text Renderers ───────────────────────────────────────────────────
def _bar(value: int, max_val: int, length: int = 10) -> str:
    """Render a text progress bar."""
    filled = round((value / max(1, max_val)) * length)
    return "█" * filled + "░" * (length - filled)

def _medal(rank: int) -> str:
    return {0:"🥇", 1:"🥈", 2:"🥉"}.get(rank, f"{rank+1}.")

def _fmt_num(n: int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)

# ─── Fixed Leaderboard (visible to all) ───────────────────────────────────────
async def on_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg  = update.effective_message
    if not chat or not msg:
        return
    if chat.type not in {"group", "supergroup"}:
        await msg.reply_text("🏆 Use /leaderboard inside a group.")
        return
    rows = get_chat_leaderboard(chat.id, limit=15)
    if not rows:
        await msg.reply_text(
            "🏆 <b>Leaderboard</b>\n━━━━━━━━━━━━━━━━━━\n"
            "<i>No game data yet!\n\nPlay /rps, /xo, /luckybox or /tod to appear here.</i>",
            parse_mode=ParseMode.HTML
        )
        return
    max_score = max((r["total_score"] or 0) for r in rows) or 1
    lines = [
        "🏆 <b>Game Leaderboard</b>",
        f"<i>{html.escape(chat.title or 'This Group')}</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for i, r in enumerate(rows):
        score = r["total_score"] or 0
        rps_w = r["rps_wins"] or 0
        xo_w  = r["xo_wins"] or 0
        bar   = _bar(score, max_score, 8)
        medal = _medal(i)
        details = []
        if rps_w: details.append(f"🎮 RPS:{rps_w}")
        if xo_w:  details.append(f"⭕ XO:{xo_w}")
        detail_str = "  ".join(details) if details else "—"
        lines.append(
            f"{medal} <b>{html.escape(r['user_name'])}</b>\n"
            f"   {bar} <b>{score} pts</b>\n"
            f"   {detail_str}"
        )
        if i < len(rows) - 1:
            lines.append("")
    lines += [
        "━━━━━━━━━━━━━━━━━━",
        "<i>🎯 RPS/XO Win = 2pts</i>",
    ]
    try:
        await context.bot.send_chat_action(chat_id=chat.id, action=ChatAction.TYPING)
    except:
        pass
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── Owner Group Browser ──────────────────────────────────────────────────────
_browse_page: dict[int, int] = {}   # owner_user_id -> current page index
_BROWSE_PER_PAGE = 5

def _get_group_detail(chat_id: int) -> str:
    """Rich single-group summary for owner browsing."""
    row = get_group(chat_id)
    if not row:
        return f"<b>Chat ID:</b> <code>{chat_id}</code>\n<i>No data found.</i>"
    lang     = (row["language"] or "bn").upper()
    enabled  = "✅ Active" if int(row["enabled"] or 1) == 1 else "❌ Disabled"
    voice    = "🔊 On" if int(row["voice_enabled"] or 1) == 1 else "🔇 Off"
    hourly   = "⏰ On" if int(row["hourly_enabled"] or 1) == 1 else "⏸ Off"
    del_svc  = "🗑 On" if int(row["delete_service"] or 1) == 1 else "💬 Off"
    welcomes = _fmt_num(int(row["total_welcome_sent"] or 0))
    hourly_c = _fmt_num(int(row["total_hourly_sent"] or 0))
    last_w   = format_ts(int(row["last_welcome_at"] or 0))
    last_h   = format_ts(int(row["last_hourly_at"] or 0))
    last_ai  = format_ts(int(row["last_ai_success_at"] or 0))
    style    = row["welcome_style"] or "auto"
    footer   = row["footer_text"] or "—"
    custom_w = "✅ Set" if row["custom_welcome"] else "❌ Not set"
    return (
        f"<b>📌 {html.escape(row['title'] or 'Untitled')}</b>\n"
        f"<code>{chat_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Status:    {enabled}\n"
        f"Language:  {lang}\n"
        f"Voice:     {voice}\n"
        f"Hourly:    {hourly}\n"
        f"Del-svc:   {del_svc}\n"
        f"Style:     {style}\n"
        f"Footer:    {html.escape(footer[:30])}\n"
        f"Custom W:  {custom_w}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👋 Welcomes:  {welcomes}\n"
        f"📨 Hourly:    {hourly_c}\n"
        f"🕐 Last welcome: {last_w}\n"
        f"🕐 Last hourly:  {last_h}\n"
        f"🤖 Last AI:      {last_ai}"
    )

def _browse_markup(page: int, total_pages: int, chat_id: int) -> InlineKeyboardMarkup:
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gb|nav|{page-1}"))
    nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="gb|noop|0"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"gb|nav|{page+1}"))
    rows = [nav]
    rows.append([
        InlineKeyboardButton("📊 Stats",    callback_data=f"gb|detail|{chat_id}"),
        InlineKeyboardButton("📢 Msg",      callback_data=f"gb|msg|{chat_id}"),
        InlineKeyboardButton("🔄 Refresh",  callback_data=f"gb|nav|{page}"),
    ])
    rows.append([InlineKeyboardButton("❌ Close", callback_data="gb|close|0")])
    return InlineKeyboardMarkup(rows)

def _browse_page_text(page: int, groups: list) -> tuple[str, int]:
    total_pages = max(1, (len(groups) + _BROWSE_PER_PAGE - 1) // _BROWSE_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * _BROWSE_PER_PAGE
    slice_ = groups[start: start + _BROWSE_PER_PAGE]
    lines = [
        f"🗂 <b>Group Browser</b>  Page {page+1}/{total_pages}",
        f"<i>Total: {len(groups)} groups</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for i, row in enumerate(slice_, start=1):
        chat_id  = int(row["chat_id"])
        title    = html.escape(row["title"] or "Untitled")
        enabled  = "✅" if int(row["enabled"] or 1) == 1 else "❌"
        lang     = (row["language"] or "bn").upper()
        welcomes = _fmt_num(int(row["total_welcome_sent"] or 0))
        last     = format_ts(int(row["updated_at"] or 0))
        lines.append(
            f"{start+i}. {enabled} <b>{title}</b>\n"
            f"   <code>{chat_id}</code>  [{lang}]  👋 {welcomes}\n"
            f"   🕐 {last}"
        )
        if i < len(slice_):
            lines.append("")
    # First group_id on this page for detail button
    first_id = int(slice_[0]["chat_id"]) if slice_ else 0
    return "\n".join(lines), page, total_pages, first_id

async def on_groupbrowser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only command to browse all groups."""
    if not await require_owner_private(update):
        return
    user = update.effective_user
    with db_connect() as conn:
        groups = conn.execute(
            "SELECT * FROM groups ORDER BY updated_at DESC"
        ).fetchall()
    if not groups:
        await update.effective_message.reply_text("No groups found.")
        return
    page = 0
    text, page, total_pages, first_id = _browse_page_text(page, groups)
    markup = _browse_markup(page, total_pages, first_id)
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
    except:
        pass
    await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

async def on_groupbrowser_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    if not query or not user or not query.data:
        return
    if not is_super_admin(user.id):
        await query.answer("Owner only.", True)
        return
    try:
        _, action, value = query.data.split("|", 2)
    except:
        await query.answer()
        return
    if action == "noop":
        await query.answer()
        return
    if action == "close":
        await query.answer("Closed.")
        try: await query.message.delete()
        except: pass
        return
    if action == "nav":
        page = int(value)
        with db_connect() as conn:
            groups = conn.execute("SELECT * FROM groups ORDER BY updated_at DESC").fetchall()
        if not groups:
            await query.answer("No groups."); return
        text, page, total_pages, first_id = _browse_page_text(page, groups)
        markup = _browse_markup(page, total_pages, first_id)
        await query.answer()
        try:
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        except:
            pass
        return
    if action == "detail":
        chat_id = int(value)
        detail  = _get_group_detail(chat_id)
        markup  = InlineKeyboardMarkup([[
            InlineKeyboardButton("◀️ Back", callback_data="gb|nav|0"),
            InlineKeyboardButton("❌ Close", callback_data="gb|close|0"),
        ]])
        await query.answer()
        try:
            await query.edit_message_text(detail, reply_markup=markup, parse_mode=ParseMode.HTML)
        except:
            pass
        return
    if action == "msg":
        chat_id = int(value)
        context.user_data["broadcast_target"] = chat_id
        await query.answer(f"Send your message to chat {chat_id} — use /broadcastone <text>", show_alert=True)
        return
    await query.answer()

async def on_broadcastone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner: send a single message to one specific group."""
    if not update.effective_user or not is_super_admin(update.effective_user.id):
        return
    if not context.args:
        await update.effective_message.reply_text("Usage: /broadcastone <chat_id> <text>")
        return
    try:
        target_id = int(context.args[0])
        text = " ".join(context.args[1:]).strip()
    except:
        await update.effective_message.reply_text("Usage: /broadcastone <chat_id> <text>")
        return
    if not text:
        await update.effective_message.reply_text("No message text provided.")
        return
    try:
        await context.bot.send_message(chat_id=target_id, text=text)
        await update.effective_message.reply_text(f"✅ Sent to {target_id}")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Failed: {e}")

# ─── Premium /status (upgraded look) ─────────────────────────────────────────
async def on_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat  = update.effective_chat
    group = get_group(chat.id)
    lang  = get_group_lang(chat.id)

    voice_on    = int(group["voice_enabled"]  or 1) == 1
    del_svc_on  = int(group["delete_service"] or 1) == 1
    hourly_on   = int(group["hourly_enabled"] or 1) == 1
    fest_on     = current_festival_mode(chat.id)
    kw_on       = current_keyword_mode(chat.id)
    mood        = peek_hourly_mood(chat.id)
    aura        = current_effective_aura(chat.id, phase_now())
    welcomes    = _fmt_num(int(group["total_welcome_sent"] or 0))
    hourly_cnt  = _fmt_num(int(group["total_hourly_sent"] or 0))
    ai_time     = format_ts(int(group["last_ai_success_at"] or 0))
    last_w      = format_ts(int(group["last_welcome_at"] or 0))
    lang_name   = "🇧🇩 Bangla" if lang == "bn" else "🇬🇧 English"
    phase       = phase_now()
    phase_icons = {"morning":"🌅","day":"☀️","evening":"🌆","night":"🌙"}
    ph_icon     = phase_icons.get(phase, "🕐")

    on  = "✅"
    off = "❌"

    lines = [
        f"⚙️ <b>Bot Status</b>",
        f"<i>{html.escape(chat.title or '')}</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        f"🌐 Language:      {lang_name}",
        f"{ph_icon} Phase:         {phase.capitalize()}",
        f"🎨 Aura theme:    {aura}",
        f"🎭 Mood wheel:    {mood}",
        "",
        "── Features ──────────────",
        f"{on if voice_on else off} Voice welcome",
        f"{on if del_svc_on else off} Delete service msgs",
        f"{on if hourly_on else off} Hourly messages",
        f"{on if fest_on else off} Festival mode",
        f"{on if kw_on else off} Keyword replies",
        "",
        "── Msg Limits ────────────",
    ]
    _mn, _mx, _ = get_msg_limits(chat.id)
    if _mn > 0 or _mx > 0:
        _ms = f"min={_mn}c  max={_mx}c" if _mn and _mx else (f"min={_mn}c" if _mn else f"max={_mx}c")
        lines.append(f"📏 {_ms}")
    else:
        lines.append("📏 <i>Not set</i>")
    lines += [
        "",
        "── Stats ─────────────────",
        f"👋 Welcomes sent: <b>{welcomes}</b>",
        f"📨 Hourly sent:   <b>{hourly_cnt}</b>",
        f"🕐 Last welcome:  {last_w}",
        f"🤖 Last AI:       {ai_time}",
        "",
        "━━━━━━━━━━━━━━━━━━",
        f"<i>Bot: {BOT_NAME}  •  TZ: {TIMEZONE_NAME}</i>",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── Premium /analytics (upgraded look) ──────────────────────────────────────
async def on_analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat  = update.effective_chat
    lang  = get_group_lang(chat.id)
    row   = get_group(chat.id)

    welcomes  = int(row["total_welcome_sent"]  or 0)
    hourly_s  = int(row["total_hourly_sent"]   or 0)
    ai_ok     = format_ts(int(row["last_ai_success_at"]    or 0))
    fb_used   = format_ts(int(row["last_fallback_used_at"] or 0))
    last_w    = format_ts(int(row["last_welcome_at"]       or 0))
    voice_ch  = "🇧🇩 BD Female" if current_voice_choice(chat.id) == "bd" else "🇮🇳 IN Female"
    aura      = current_effective_aura(chat.id, phase_now())
    style     = current_welcome_style(chat.id)
    footer    = current_footer_text(chat.id) or "—"
    custom_w  = "✅ Custom set" if row["custom_welcome"] else "❌ Default"
    milestone = _fmt_num(int(row["last_milestone_sent"] or 0))

    max_ref   = max(welcomes, hourly_s, 1)
    w_bar     = _bar(welcomes, max_ref, 10)
    h_bar     = _bar(hourly_s, max_ref, 10)

    lines = [
        f"📊 <b>Analytics</b>",
        f"<i>{html.escape(chat.title or '')}</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        f"👋 Welcomes:  {w_bar} <b>{_fmt_num(welcomes)}</b>",
        f"📨 Hourly:    {h_bar} <b>{_fmt_num(hourly_s)}</b>",
        f"🏅 Milestone: <b>{milestone}</b>",
        "",
        "── Welcome Config ─────────",
        f"🎨 Style:   {style}",
        f"🖼 Aura:    {aura}",
        f"📝 Footer:  {html.escape(footer[:30])}",
        f"✏️ Custom:  {custom_w}",
        f"🎙 Voice:   {voice_ch}",
        "",
        "── AI Performance ─────────",
        f"✅ Last AI success:   {ai_ok}",
        f"📦 Last fallback:     {fb_used}",
        f"🕐 Last welcome:      {last_w}",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── Premium /aistatus (upgraded look) ────────────────────────────────────────
async def on_ai_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    # Owner-only command
    if not user or not is_super_admin(user.id):
        await update.effective_message.reply_text("🔒 This command is restricted to the bot owner.")
        return
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text("🔍 Checking Groq AI status...")
    ok, result = await asyncio.to_thread(groq_live_check)
    checked    = LAST_GROQ_STATUS["last_checked_at"] or "Never"
    key_count  = len(GROQ_API_KEYS)
    model      = GROQ_MODEL
    status_icon = "🟢" if ok else "🔴"
    ai_enabled  = "✅ Enabled" if AI_HOURLY_ENABLED else "❌ Disabled"
    configured  = f"✅ {key_count} key(s)" if GROQ_API_KEYS else "❌ No keys"
    lines = [
        f"🤖 <b>AI Engine Status</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"🔑 API Keys:    {configured}",
        f"📡 Model:       <code>{model}</code>",
        f"🔄 AI Hourly:   {ai_enabled}",
        "",
        f"── Last Live Check ────────",
        f"{status_icon} Status:  {'OK' if ok else 'FAILED'}",
        f"📋 Result:  {html.escape(str(result)[:80])}",
        f"🕐 Checked: {checked}",
    ]
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── Premium /ping ─────────────────────────────────────────────────────────────
async def on_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    lang = get_group_lang(update.effective_chat.id) if update.effective_chat and update.effective_chat.type in {"group","supergroup"} else "bn"
    now  = local_now()
    phase = phase_now()
    phase_icons = {"morning":"🌅","day":"☀️","evening":"🌆","night":"🌙"}
    ph = phase_icons.get(phase, "🕐")
    uptime_hint = random.choice([
        "All systems running smoothly.",
        "Ready and responsive.",
        "Fully operational.",
        "Online and healthy.",
    ])
    text = (
        f"🏓 <b>Pong!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Bot:    <b>{BOT_NAME}</b>\n"
        f"🕐 Time:   <b>{now.strftime('%I:%M:%S %p')}</b>\n"
        f"📅 Date:   {now.strftime('%d %b %Y')}\n"
        f"{ph} Phase:  {phase.capitalize()}\n"
        f"🌍 TZ:     {TIMEZONE_NAME}\n\n"
        f"<i>{uptime_hint}</i>"
    )
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ─── Premium /start ────────────────────────────────────────────────────────────
async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    await human_delay_and_action(context, update)
    chat = update.effective_chat
    if chat and chat.type in {"group","supergroup"}:
        lang = get_group_lang(chat.id)
        lang_g = get_group_lang(chat.id)
        phase_g = phase_now()
        greet = {"morning":"☀️ Good morning","day":"🌤️ Hello","evening":"🌆 Good evening","night":"🌙 Good night"}.get(phase_g,"✨ Hello")
        text = (
            f"{greet}, <b>{html.escape(chat.title or 'everyone')}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"✨ <b>{BOT_NAME}</b> is active here!\n\n"
            f"🎨 Premium welcomes · 🎙 Voice · 📨 Hourly\n"
            f"🛡 Moderation · 🤖 AI replies · 🎮 Games\n\n"
            f"<b>Quick commands:</b>\n"
            f"📋 /status  📊 /analytics  👤 /profile\n"
            f"🎮 /rps · /xo · /luckybox · /tod\n"
            f"🏆 /leaderboard  👥 /top  📏 /rules\n\n"
            f"<i>Admins: /lang · /voice · /hourly · /setmsglimit</i>"
        )
    else:
        text = (
            f"✨ <b>{BOT_NAME}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Premium Telegram Group Bot\n\n"
            f"<b>Admin Commands:</b>\n"
            f"/lang · /voice · /hourly · /setwelcome\n"
            f"/welcomestyle · /setfooter · /deleteservice\n"
            f"/setcountdown · /setexamday · /hourlyclean\n\n"
            f"<b>Info Commands:</b>\n"
            f"/status · /analytics · /aistatus\n"
            f"/ping · /myid · /support\n\n"
            f"<b>Games:</b>\n"
            f"/rps · /xo · /luckybox · /tod · /leaderboard\n\n"
            f"<b>Owner:</b>\n"
            f"/groupbrowser · /broadcastone\n"
            f"/groupcount · /activegroups · /broadcast"
        )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ─── Premium /support ─────────────────────────────────────────────────────────
async def on_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await track_group(update, context)
    await human_delay_and_action(context, update)
    st = support_text()
    text = (
        f"💬 <b>Support & Help</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📌 {html.escape(st)}\n\n"
        f"<i>Have a question, suggestion, or issue?\n"
        f"Reach out anytime — we're happy to help.</i>"
    )
    await update.effective_message.reply_text(text, parse_mode=ParseMode.HTML)

# ─── Premium /myid ────────────────────────────────────────────────────────────
async def on_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id if update.effective_user else 0
    name = html.escape(clean_name(update.effective_user.first_name if update.effective_user else ""))
    text = (
        f"🪪 <b>Your Identity</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: <b>{name}</b>\n"
        f"🔢 User ID: <code>{uid}</code>\n"
        f"{'👑 Bot Owner' if is_super_admin(uid) else ''}"
    )
    await human_delay_and_action(context, update)
    await update.effective_message.reply_text(text.strip(), parse_mode=ParseMode.HTML)

async def on_setvoice(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id); current=current_voice_choice(chat.id)
    if not context.args: await update.effective_message.reply_text(t(lang,"setvoice_usage",current=current)); return
    value=context.args[0].strip().lower()
    if value not in {"bd","in"}: await update.effective_message.reply_text(t(lang,"setvoice_usage",current=current)); return
    set_group_value(chat.id,"voice_choice",value)
    await update.effective_message.reply_text(t(lang,"setvoice_set",value="Bangladesh female" if value=="bd" else "India Bengali female"))

async def on_festivalmode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    lang = get_group_lang(chat.id)
    if not context.args:
        current = "ON" if current_festival_mode(chat.id) else "OFF"
        await update.effective_message.reply_text(
            f"🎉 Festival Mode: <b>{current}</b>\n\nUsage:\n/festivalmode on\n/festivalmode off\n\n"
            f"<i>When ON, special themes and messages are sent on Eid, Boishakh, Independence Day, Victory Day etc.</i>",
            parse_mode=ParseMode.HTML)
        return
    val = context.args[0].strip().lower()
    if val not in {"on","off"}:
        await update.effective_message.reply_text("Usage: /festivalmode on or /festivalmode off")
        return
    set_group_value(chat.id, "festival_mode", 1 if val == "on" else 0)
    icon = "🎉" if val == "on" else "⏸"
    await update.effective_message.reply_text(
        f"{icon} Festival Mode set to <b>{val.upper()}</b>.", parse_mode=ParseMode.HTML)


# ─── Message Length Limit System ───────────────────────────────────────────────
def get_msg_limits(chat_id: int) -> tuple[int, int, str]:
    """Returns (min_len, max_len, action). 0 = disabled."""
    row = get_group(chat_id)
    if not row:
        return 0, 0, "delete"
    mn  = int(row["msg_min_len"] or 0)
    mx  = int(row["msg_max_len"] or 0)
    act = (row["msg_limit_action"] or "delete").strip().lower()
    return mn, mx, act

async def on_setmsglimit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setmsglimit <min> <max>
    /setmsglimit off
    min=0 means no min limit, max=0 means no max limit
    """
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    msg  = update.effective_message
    args = context.args or []

    if not args:
        mn, mx, act = get_msg_limits(chat.id)
        min_s = f"{mn} chars" if mn > 0 else "No min"
        max_s = f"{mx} chars" if mx > 0 else "No max"
        await msg.reply_text(
            f"📏 <b>Message Length Limit</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Min: <b>{min_s}</b>\n"
            f"Max: <b>{max_s}</b>\n"
            f"Action: <b>{act.upper()}</b>\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/setmsglimit 10 500</code> — min 10, max 500 chars\n"
            f"<code>/setmsglimit 0 200</code>  — max 200 only (no min)\n"
            f"<code>/setmsglimit 5 0</code>   — min 5 only (no max)\n"
            f"<code>/setmsglimit off</code>    — disable\n\n"
            f"<i>Messages outside the limit are auto-deleted.</i>",
            parse_mode=ParseMode.HTML
        )
        return

    if args[0].strip().lower() in {"off", "disable", "0 0"}:
        set_group_value(chat.id, "msg_min_len", 0)
        set_group_value(chat.id, "msg_max_len", 0)
        await msg.reply_text("📏 Message length limit <b>disabled</b>.", parse_mode=ParseMode.HTML)
        return

    if len(args) < 2:
        await msg.reply_text(
            "Usage: <code>/setmsglimit &lt;min&gt; &lt;max&gt;</code>\n"
            "Example: <code>/setmsglimit 10 500</code>\n"
            "Use 0 to skip that limit.",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        mn = max(0, int(args[0]))
        mx = max(0, int(args[1]))
    except ValueError:
        await msg.reply_text("❌ Invalid numbers. Use: /setmsglimit 10 500")
        return

    if mn > 0 and mx > 0 and mn >= mx:
        await msg.reply_text("❌ Min must be less than max.")
        return
    if mx > 4096:
        await msg.reply_text("❌ Max cannot exceed Telegram's 4096 char limit.")
        return

    set_group_value(chat.id, "msg_min_len", mn)
    set_group_value(chat.id, "msg_max_len", mx)

    min_s = f"<b>{mn}</b> chars" if mn > 0 else "<i>no min</i>"
    max_s = f"<b>{mx}</b> chars" if mx > 0 else "<i>no max</i>"
    await msg.reply_text(
        f"📏 <b>Message Length Limit Set</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Min length: {min_s}\n"
        f"Max length: {max_s}\n\n"
        f"<i>Messages outside this range will be auto-deleted.</i>",
        parse_mode=ParseMode.HTML
    )

async def check_msg_length_limit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Returns True if message was deleted (caller should stop processing).
    Checks message length against group limits and auto-deletes if violated.
    """
    chat = update.effective_chat
    msg  = update.effective_message
    user = update.effective_user
    if not chat or not msg or not user or user.is_bot:
        return False
    if chat.type not in {"group", "supergroup"}:
        return False

    mn, mx, _ = get_msg_limits(chat.id)
    if mn == 0 and mx == 0:
        return False  # No limits set

    text = msg.text or msg.caption or ""
    if not text:
        return False  # Don't police non-text messages

    # Admins are exempt from length limits
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
            return False
    except Exception:
        return False

    length = len(text)
    violated = False
    reason   = ""

    if mn > 0 and length < mn:
        violated = True
        reason   = f"too short ({length}/{mn} chars min)"
    elif mx > 0 and length > mx:
        violated = True
        reason   = f"too long ({length}/{mx} chars max)"

    if not violated:
        return False

    # Delete the message
    try:
        await msg.delete()
    except Exception:
        return False  # Can't delete, skip warning too

    # Send a temporary warning
    try:
        uname = html.escape(clean_name(user.first_name or "Member"))
        lang  = get_group_lang(chat.id)
        if lang == "bn":
            if "short" in reason:
                warn_text = f"⚠️ {user.mention_html(uname)}, তোমার message খুব ছোট! কমপক্ষে <b>{mn}</b> character লিখতে হবে।"
            else:
                warn_text = f"⚠️ {user.mention_html(uname)}, তোমার message খুব বড়! সর্বোচ্চ <b>{mx}</b> character লেখা যাবে।"
        else:
            if "short" in reason:
                warn_text = f"⚠️ {user.mention_html(uname)}, message too short! Minimum <b>{mn}</b> characters required."
            else:
                warn_text = f"⚠️ {user.mention_html(uname)}, message too long! Maximum <b>{mx}</b> characters allowed."
        notice = await context.bot.send_message(
            chat_id=chat.id,
            text=warn_text,
            parse_mode=ParseMode.HTML
        )
        # Auto-delete the warning after 8 seconds
        asyncio.create_task(schedule_delete(context.bot, chat.id, notice.message_id, 8))
    except Exception:
        pass

    return True

async def on_keywordmode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    if not context.args:
        current = "ON" if current_keyword_mode(chat.id) else "OFF"
        await update.effective_message.reply_text(
            f"💬 Keyword Replies: <b>{current}</b>\n\nUsage:\n/keywordmode on\n/keywordmode off\n\n"
            f"<i>Controls auto-replies to salam, birthday, good morning etc.</i>",
            parse_mode=ParseMode.HTML)
        return
    val = context.args[0].strip().lower()
    if val not in {"on","off"}:
        await update.effective_message.reply_text("Usage: /keywordmode on or /keywordmode off")
        return
    set_group_value(chat.id, "keyword_replies_enabled", 1 if val == "on" else 0)
    icon = "💬" if val == "on" else "🔇"
    await update.effective_message.reply_text(
        f"{icon} Keyword Replies set to <b>{val.upper()}</b>.", parse_mode=ParseMode.HTML)

async def on_welcomestyle(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat
    if not context.args:
        await update.effective_message.reply_text(f"Current: {current_welcome_style(chat.id)}\n\nUse:\n/welcomestyle list\n/welcomestyle random\n/welcomestyle gold"); return
    value=context.args[0].strip().lower()
    if value=="list": await update.effective_message.reply_text("Available themes:\n"+list_theme_names_text()); return
    if value not in {"auto","random"} and value not in THEME_NAMES: await update.effective_message.reply_text("Invalid theme.\nUse /welcomestyle list"); return
    set_group_value(chat.id,"welcome_style",value)
    await update.effective_message.reply_text(f"Welcome style set to: {value}")

async def on_setfooter(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; raw=update.effective_message.text or ""; parts=raw.split(" ",1)
    if len(parts)<2 or not parts[1].strip(): await update.effective_message.reply_text("Usage:\n/setfooter Powered by Maya"); return
    footer=parts[1].strip()[:60]; set_group_value(chat.id,"footer_text",footer)
    await update.effective_message.reply_text(f"Footer set to:\n{footer}")

async def on_lang(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id)
    if not context.args: await update.effective_message.reply_text(t(lang,"lang_usage")); return
    new_lang=context.args[0].strip().lower()
    if new_lang not in {"bn","en"}: await update.effective_message.reply_text(t(lang,"lang_usage")); return
    set_group_value(chat.id,"language",new_lang)
    await update.effective_message.reply_text(t(new_lang,"lang_set_en" if new_lang=="en" else "lang_set_bn"))

async def on_voice(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id); group=get_group(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang,"voice_usage",current="ON" if int(group["voice_enabled"])==1 else "OFF")); return
    value=context.args[0].strip().lower()
    if value not in {"on","off"}:
        await update.effective_message.reply_text(t(lang,"voice_usage",current="ON" if int(group["voice_enabled"])==1 else "OFF")); return
    set_group_value(chat.id,"voice_enabled",1 if value=="on" else 0)
    await update.effective_message.reply_text(t(lang,"voice_set",value=value.upper()))

async def on_delete_service(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id); group=get_group(chat.id)
    if not context.args:
        await update.effective_message.reply_text(t(lang,"deleteservice_usage",current="ON" if int(group["delete_service"])==1 else "OFF")); return
    value=context.args[0].strip().lower()
    if value not in {"on","off"}:
        await update.effective_message.reply_text(t(lang,"deleteservice_usage",current="ON" if int(group["delete_service"])==1 else "OFF")); return
    set_group_value(chat.id,"delete_service",1 if value=="on" else 0)
    await update.effective_message.reply_text(t(lang,"deleteservice_set",value=value.upper()))

async def on_hourly(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id); group=get_group(chat.id)
    if not context.args:
        current="ON" if int(group["hourly_enabled"])==1 else "OFF"
        mood=peek_hourly_mood(chat.id); ca=current_hourly_delete_after(chat.id)
        cl="OFF" if ca<=0 else f"{ca//60}m" if ca<3600 else f"{ca//3600}h"
        await human_delay_and_action(context,update)
        await update.effective_message.reply_text(f"{t(lang,'hourly_usage',current=current)}\nMood wheel: {mood}\nAuto-clean: {cl}"); return
    value=context.args[0].strip().lower()
    if value=="now":
        phase=phase_now(); mood=next_hourly_mood(chat.id)
        festival_key=(current_festival() or {}).get("key","")
        pool,source=await asyncio.to_thread(get_batch_pool_v2,lang,phase,mood,festival_key)
        msg=pick_hourly_message(chat.id,lang,phase,pool)
        await human_delay_and_action(context,update)
        sent=await send_text_with_retry(context.bot,chat_id=chat.id,text=msg)
        ca=current_hourly_delete_after(chat.id)
        if ca>0: asyncio.create_task(schedule_delete(context.bot,chat.id,sent.message_id,ca))
        set_group_value(chat.id,"last_hourly_at",int(time.time()))
        increment_group_counter(chat.id,"total_hourly_sent")
        if source=="ai": set_group_value(chat.id,"last_ai_success_at",int(time.time()))
        else: set_group_value(chat.id,"last_fallback_used_at",int(time.time()))
        await human_delay_and_action(context,update)
        await update.effective_message.reply_text(f"{t(lang,'hourly_now')}\nMood: {mood}"); return
    if value not in {"on","off"}:
        await human_delay_and_action(context,update)
        await update.effective_message.reply_text(t(lang,"hourly_usage",current="ON" if int(group["hourly_enabled"])==1 else "OFF")); return
    set_group_value(chat.id,"hourly_enabled",1 if value=="on" else 0)
    if value=="on": set_group_value(chat.id,"last_hourly_at",0)
    await human_delay_and_action(context,update)
    await update.effective_message.reply_text(t(lang,"hourly_set",value=value.upper()))

async def on_setwelcome(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; lang=get_group_lang(chat.id); raw=update.effective_message.text or ""; parts=raw.split(" ",1)
    if len(parts)<2 or not parts[1].strip():
        await update.effective_message.reply_text("Usage:\n/setwelcome your text\n\nPlaceholders:\n{name} {group} {phase}"); return
    set_group_value(chat.id,"custom_welcome",parts[1].strip()[:600])
    await update.effective_message.reply_text(t(lang,"welcome_saved"))

async def on_resetwelcome(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat; set_group_value(chat.id,"custom_welcome",None)
    await update.effective_message.reply_text(t(get_group_lang(chat.id),"welcome_reset"))

async def on_testwelcome(update,context):
    await track_group(update,context)
    chat=update.effective_chat; user=update.effective_user
    if chat and user and chat.type in {"group","supergroup"}: await maybe_welcome(context,chat.id,chat.title or "",user)

async def on_hourlyclean(update,context):
    if not await require_group_admin(update,context): return
    chat=update.effective_chat
    if not context.args:
        ca=current_hourly_delete_after(chat.id)
        label="OFF" if ca<=0 else f"{ca//60}m" if ca<3600 else f"{ca//3600}h"
        await human_delay_and_action(context,update)
        await update.effective_message.reply_text(f"Usage:\n/hourlyclean off\n/hourlyclean 30m\n/hourlyclean 1h\n\nCurrent: {label}"); return
    try:
        seconds=parse_duration_to_seconds(context.args[0]); set_group_value(chat.id,"hourly_delete_after",seconds)
        label="OFF" if seconds<=0 else f"{seconds//60}m" if seconds<3600 else f"{seconds//3600}h"
        await human_delay_and_action(context,update); await update.effective_message.reply_text(f"Hourly auto-clean set to {label}.")
    except:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Use /hourlyclean off, /hourlyclean 30m or /hourlyclean 1h")

async def on_setcountdown(update,context):
    if not await require_group_admin(update,context): return
    raw=(update.effective_message.text or "").split(" ",1)
    if len(raw)<2:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Usage:\n/setcountdown YYYY-MM-DD HH:MM | Event title"); return
    try:
        target_ts,title=parse_countdown_input(raw[1]); set_countdown(update.effective_chat.id,title,target_ts,"event")
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Countdown saved successfully.")
    except Exception as e:
        await human_delay_and_action(context,update); await update.effective_message.reply_text(str(e))

async def on_showcountdown(update,context):
    chat=update.effective_chat
    if not chat or chat.type not in {"group","supergroup"}:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Use /countdown in group."); return
    row=get_countdown(chat.id)
    if not row:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("No countdown set for this group."); return
    diff=max(0,int(row["target_ts"])-int(time.time()))
    lang=get_group_lang(chat.id)
    card=build_countdown_card_bytes(chat.title or "GROUP",row["title"],diff//86400,(diff%86400)//3600,lang)
    await human_delay_and_action(context,update)
    await send_photo_with_retry(context.bot,chat_id=chat.id,photo=card,caption=f"{row['title']}\n{diff//86400} days {(diff%86400)//3600} hours left")

async def on_clearcountdown(update,context):
    if not await require_group_admin(update,context): return
    clear_countdown(update.effective_chat.id)
    await human_delay_and_action(context,update); await update.effective_message.reply_text("Countdown cleared.")

async def on_setexamday(update,context):
    if not await require_group_admin(update,context): return
    raw=(update.effective_message.text or "").split(" ",1)
    if len(raw)<2:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Usage:\n/setexamday YYYY-MM-DD HH:MM | Exam title"); return
    try:
        target_ts,title=parse_countdown_input(raw[1]); set_scheduled_event(update.effective_chat.id,"exam",title,target_ts)
        await human_delay_and_action(context,update); await update.effective_message.reply_text("Exam day reminder saved successfully.")
    except Exception as e:
        await human_delay_and_action(context,update); await update.effective_message.reply_text(str(e))

async def on_examday(update,context):
    if not await require_group_admin(update,context): return
    row=get_scheduled_event(update.effective_chat.id,"exam")
    if not row:
        await human_delay_and_action(context,update); await update.effective_message.reply_text("No exam day reminder set."); return
    dt=datetime.fromtimestamp(int(row["target_ts"]),ZoneInfo(TIMEZONE_NAME)).strftime("%Y-%m-%d %I:%M %p")
    await human_delay_and_action(context,update); await update.effective_message.reply_text(f"Exam reminder:\n{row['title']}\n{dt}")

async def on_clearexamday(update,context):
    if not await require_group_admin(update,context): return
    clear_scheduled_event(update.effective_chat.id,"exam")
    await human_delay_and_action(context,update); await update.effective_message.reply_text("Exam day reminder cleared.")

async def on_groupcount(update,context):
    if not await require_owner_private(update): return
    await update.effective_message.reply_text(f"Known groups: {count_known_groups()}\nEnabled groups: {len(get_all_enabled_groups())}")

async def on_activegroups(update,context):
    if not await require_owner_private(update): return
    rows=get_active_groups(20)
    if not rows: await update.effective_message.reply_text("No active groups found."); return
    lines=["Recent active groups:"]+[f"- {r['title'] or 'Untitled'} | {r['chat_id']} | {format_ts(int(r['updated_at'] or 0))}" for r in rows]
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_failedgroups(update,context):
    if not await require_owner_private(update): return
    rows=get_recent_failed_groups(15)
    if not rows: await update.effective_message.reply_text("No failed groups recorded."); return
    lines=["Recent failed groups:"]+[f"- {r['title'] or 'Untitled'} | {r['chat_id']} | fails={r['fail_count']} | last={format_ts(int(r['last_time'] or 0))}" for r in rows]
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_lastaierrors(update,context):
    if not await require_owner_private(update): return
    rows=get_recent_ai_errors(10)
    if not rows: await update.effective_message.reply_text("No recent AI errors."); return
    lines=["Recent AI errors:"]+[f"- {format_ts(int(r['created_at'] or 0))} | {r['error']}" for r in rows]
    await update.effective_message.reply_text("\n".join(lines)[:3900])

async def on_broadcast(update,context):
    if not update.effective_user or not is_super_admin(update.effective_user.id):
        await update.effective_message.reply_text(t("en","broadcast_owner_only")); return
    msg=update.effective_message; reply=msg.reply_to_message if msg else None
    raw=msg.text or ""; parts=raw.split(" ",1); arg_text=parts[1].strip() if len(parts)>1 and parts[1].strip() else ""
    if not reply and not arg_text:
        await msg.reply_text("Usage:\n/broadcast your message\n\nOr reply to any message and send:\n/broadcast"); return
    groups=get_all_enabled_groups()
    if not groups: await msg.reply_text(t("en","broadcast_none")); return
    ok_count=fail_count=0
    if reply:
        status=await msg.reply_text(f"Broadcasting to {len(groups)} groups...")
        for gid in groups:
            try:
                await bot_humanize(context.bot,gid,action=guess_broadcast_action(reply),kind="reply")
                await copy_message_with_retry(context.bot,chat_id=gid,from_chat_id=reply.chat_id,message_id=reply.message_id)
                ok_count+=1
            except Exception as e: record_failure("broadcast",gid,"",f"broadcast_copy: {e}"); fail_count+=1
        await status.edit_text(f"Broadcast done.\nMode: copy\nSuccess: {ok_count}\nFailed: {fail_count}"); return
    status=await msg.reply_text(t("en","broadcast_start",count=len(groups)))
    for gid in groups:
        try:
            await send_text_with_retry(context.bot, chat_id=gid, text=arg_text)
            ok_count += 1
            await asyncio.sleep(0.05)  # gentle rate limiting between sends
        except Exception as e:
            record_failure("broadcast", gid, "", f"broadcast_text: {e}")
            fail_count += 1
    await status.edit_text(t("en","broadcast_done",ok=ok_count,fail=fail_count))

async def on_chat_member(update, context):
    cmu = update.chat_member
    if not cmu: return
    chat = cmu.chat
    if chat.type not in {"group", "supergroup"}: return
    ensure_group(chat.id, chat.title or "")
    if cmu.new_chat_member.user.is_bot: return
    old_s = cmu.old_chat_member.status
    new_s = cmu.new_chat_member.status
    if (old_s in {ChatMemberStatus.LEFT, ChatMemberStatus.BANNED} and
            new_s in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}):
        lang = get_group_lang(chat.id)
        asyncio.create_task(handle_raid_check(context.bot, chat.id, lang))
        chat_join_history[chat.id].append(time.time())
        raid_join_window[chat.id].append(time.time())
        await queue_join_welcome(context.application, chat.id, chat.title or "", cmu.new_chat_member.user)

# ─── Smart Message Handler (replaces on_keyword_message) ─────────────────────
# ─── ENHANCED GAME SYSTEM ─────────────────────────────────────────────────────

import asyncio, html, random, time, sqlite3
from typing import Optional

# ── DB helpers for games ──────────────────────────────────────────────────────
def init_games_db():
    with db_connect() as conn:
        # Coins ledger
        conn.execute("""CREATE TABLE IF NOT EXISTS lb_coins (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT NOT NULL DEFAULT '',
            coins INTEGER NOT NULL DEFAULT 100,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        # Player stats for Lucky Box
        conn.execute("""CREATE TABLE IF NOT EXISTS lb_stats (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT NOT NULL DEFAULT '',
            games INTEGER NOT NULL DEFAULT 0,
            jackpots INTEGER NOT NULL DEFAULT 0,
            traps INTEGER NOT NULL DEFAULT 0,
            total_won INTEGER NOT NULL DEFAULT 0,
            total_lost INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )""")
        # Shields per round
        conn.execute("""CREATE TABLE IF NOT EXISTS lb_shields (
            game_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (game_id, user_id)
        )""")
        # RPS: add challenge_target_id if missing
        existing = {r[1] for r in conn.execute("PRAGMA table_info(rps_games)").fetchall()}
        if "challenge_target_id" not in existing:
            conn.execute("ALTER TABLE rps_games ADD COLUMN challenge_target_id INTEGER")
        # XO: extra columns for multi-round
        existing_xo = {r[1] for r in conn.execute("PRAGMA table_info(xo_games)").fetchall()}
        for col, ddl in {
            "score_x": "INTEGER NOT NULL DEFAULT 0",
            "score_o": "INTEGER NOT NULL DEFAULT 0",
            "round_num": "INTEGER NOT NULL DEFAULT 1",
            "streak_x": "INTEGER NOT NULL DEFAULT 0",
            "streak_o": "INTEGER NOT NULL DEFAULT 0",
            "last_winner": "TEXT",
        }.items():
            if col not in existing_xo:
                conn.execute(f"ALTER TABLE xo_games ADD COLUMN {col} {ddl}")
        conn.commit()

# ── Coins ─────────────────────────────────────────────────────────────────────
def lb_get_coins(user_id: int) -> int:
    with db_connect() as conn:
        row = conn.execute("SELECT coins FROM lb_coins WHERE user_id=?", (user_id,)).fetchone()
        return int(row["coins"]) if row else 100

def lb_ensure_coins(user_id: int, user_name: str):
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO lb_coins (user_id,user_name,coins,created_at,updated_at) VALUES (?,?,100,?,?)",
            (user_id, user_name[:40], now, now)
        )
        conn.execute("UPDATE lb_coins SET user_name=?,updated_at=? WHERE user_id=?", (user_name[:40], now, user_id))
        conn.commit()

def lb_adjust_coins(user_id: int, user_name: str, delta: int) -> int:
    lb_ensure_coins(user_id, user_name)
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "UPDATE lb_coins SET coins=MAX(0,coins+?),updated_at=? WHERE user_id=?",
            (delta, now, user_id)
        )
        conn.commit()
    return lb_get_coins(user_id)

def lb_update_stats(user_id: int, user_name: str, jackpot=False, trap=False, won=0, lost=0):
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO lb_stats (user_id,user_name,games,jackpots,traps,total_won,total_lost,updated_at) VALUES (?,?,0,0,0,0,0,?)",
            (user_id, user_name[:40], now)
        )
        conn.execute(
            "UPDATE lb_stats SET user_name=?,games=games+1,jackpots=jackpots+?,traps=traps+?,total_won=total_won+?,total_lost=total_lost+?,updated_at=? WHERE user_id=?",
            (user_name[:40], 1 if jackpot else 0, 1 if trap else 0, won, lost, now, user_id)
        )
        conn.commit()

def lb_has_shield(game_id: str, user_id: int) -> bool:
    with db_connect() as conn:
        return bool(conn.execute("SELECT 1 FROM lb_shields WHERE game_id=? AND user_id=?", (game_id, user_id)).fetchone())

def lb_grant_shield(game_id: str, user_id: int):
    with db_connect() as conn:
        conn.execute("INSERT OR IGNORE INTO lb_shields (game_id,user_id) VALUES (?,?)", (game_id, user_id))
        conn.commit()

def lb_consume_shield(game_id: str, user_id: int):
    with db_connect() as conn:
        conn.execute("DELETE FROM lb_shields WHERE game_id=? AND user_id=?", (game_id, user_id))
        conn.commit()

# ── RPS helpers ───────────────────────────────────────────────────────────────
def rps_make_id(): return f"rps{int(time.time()*1000)}{random.randint(100,999)}"
def rps_now(): return int(time.time())

def rps_create_game(chat_id: int, creator_id: int, creator_name: str, mode: str, challenge_target_id: int = 0) -> str:
    game_id = rps_make_id()
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO rps_games
               (game_id,chat_id,message_id,creator_id,creator_name,
                player1_id,player1_name,player2_id,player2_name,
                mode,p1_choice,p2_choice,status,winner,created_at,updated_at,challenge_target_id)
               VALUES (?,?,NULL,?,?,?,?,?,?,?,NULL,NULL,?,NULL,?,?,?)""",
            (game_id, chat_id, creator_id, creator_name,
             creator_id, creator_name,
             0 if mode == "bot" else None,
             BOT_NAME if mode == "bot" else None,
             mode,
             "choosing" if mode == "bot" else "waiting",
             rps_now(), rps_now(), challenge_target_id or 0)
        )
        conn.commit()
    return game_id

def rps_get_game(game_id: str):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM rps_games WHERE game_id=?", (game_id,)).fetchone()

def rps_set_message_id(game_id: str, message_id: int):
    with db_connect() as conn:
        conn.execute("UPDATE rps_games SET message_id=?,updated_at=? WHERE game_id=?", (message_id, rps_now(), game_id))
        conn.commit()

def rps_update_player2(game_id: str, user_id: int, user_name: str):
    with db_connect() as conn:
        conn.execute(
            "UPDATE rps_games SET player2_id=?,player2_name=?,status='choosing',updated_at=? WHERE game_id=?",
            (user_id, user_name, rps_now(), game_id)
        )
        conn.commit()

def rps_save_state(game_id: str, p1_choice, p2_choice, status: str, winner=None):
    with db_connect() as conn:
        conn.execute(
            "UPDATE rps_games SET p1_choice=?,p2_choice=?,status=?,winner=?,updated_at=? WHERE game_id=?",
            (p1_choice, p2_choice, status, winner, rps_now(), game_id)
        )
        conn.commit()

def rps_delete_game(game_id: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM rps_games WHERE game_id=?", (game_id,))
        conn.commit()

def rps_choice_emoji(choice: Optional[str]) -> str:
    return {"rock": "🪨", "paper": "📄", "scissors": "✂️"}.get((choice or "").lower(), "❓")

def rps_choice_label(choice: Optional[str]) -> str:
    return {"rock": "🪨 Rock", "paper": "📄 Paper", "scissors": "✂️ Scissors"}.get((choice or "").lower(), "—")

def rps_determine_winner(p1: str, p2: str) -> str:
    if p1 == p2: return "draw"
    return "p1" if (p1, p2) in {("rock","scissors"),("paper","rock"),("scissors","paper")} else "p2"

RPS_RESULT_LINES = {
    "win_rock":    ["Crushed it with a Rock! 💪","Stone cold win! 🪨","Rock solid victory!"],
    "win_paper":   ["Wrapped up the win! 📄","Smooth paper play, nice! ✨","Outplayed with Paper!"],
    "win_scissors":["Sharp as ever! ✂️","Snip snap, you win! 🎉","Clean cut victory!"],
    "lose_rock":   ["Your Rock got wrapped 😅","Paper beats Rock, try again!","Tough break, keep going!"],
    "lose_paper":  ["Paper got cut! ✂️","Scissors beats Paper, so close!","Almost had it!"],
    "lose_scissors":["Your Scissors got crushed! 🪨","Rock beats Scissors, next round!","So close!"],
    "draw":        ["Dead even! 🤝 Neither budges.","It's a tie — destiny calls for a rematch!","Perfectly matched!","Great minds pick alike 🤯"],
    "bot_win":     ["Maya wins this round! 🤖✨","Maya's too sharp today 🎯","The bot strikes back! 🤖"],
    "player_win":  ["You beat Maya! 🎉 Well played!","Human wins! The bot bows 🙇","Victory over the machine! 🏆"],
}

def rps_result_line(key: str) -> str:
    return random.choice(RPS_RESULT_LINES.get(key, ["Good game!"]))

def rps_render_text(game, note: str = "", phase: str = "normal") -> str:
    mode_text = "🤖 vs Bot" if game["mode"] == "bot" else "👥 PvP"
    p1 = html.escape(game["player1_name"] or "Player 1")
    p2 = html.escape(game["player2_name"] or (BOT_NAME if game["mode"] == "bot" else "Waiting..."))
    status = game["status"]

    lines = [f"🎮 <b>Rock Paper Scissors</b>  <i>{mode_text}</i>", "━━━━━━━━━━━━━━━━━━"]

    if phase == "choosing":
        lines += [
            f"👤 {p1}  •  {'✅ Locked' if game['p1_choice'] else '⌛ Choosing...'}",
            f"🤝 {p2}  •  {'✅ Locked' if game['p2_choice'] else '⌛ Choosing...'}",
            "", "🔒 <i>Choices hidden until both lock in</i>",
        ]
    elif phase == "revealing":
        lines += [
            f"👤 {p1}  •  🎴 Revealing...",
            f"🤝 {p2}  •  🎴 Revealing...",
        ]
    elif phase == "result":
        c1 = rps_choice_label(game["p1_choice"])
        c2 = rps_choice_label(game["p2_choice"])
        lines += [
            f"👤 {p1}  •  {c1}",
            f"🤝 {p2}  •  {c2}",
            "",
        ]
        if game["winner"] == "draw":
            lines.append(f"⚖️ <b>Draw!</b>  {rps_result_line('draw')}")
        elif game["winner"] == "p1":
            lines.append(f"🏆 <b>{p1} wins!</b>")
            key = f"win_{game['p1_choice']}" if game["mode"] != "bot" else "player_win"
            lines.append(f"<i>{rps_result_line(key)}</i>")
        elif game["winner"] == "p2":
            lines.append(f"🏆 <b>{p2} wins!</b>")
            key = "bot_win" if game["mode"] == "bot" else f"win_{game['p2_choice']}"
            lines.append(f"<i>{rps_result_line(key)}</i>")
    elif status == "waiting":
        target_id = int(game["challenge_target_id"] or 0)
        if target_id:
            lines += [f"👤 {p1}  <i>challenged someone</i>", "", "⏳ Waiting for the challenged player to join..."]
        else:
            lines += [f"👤 {p1}  ✅ Ready", f"🤝 {p2}", "", "⏳ Open challenge — anyone can join!"]
    else:
        lines += [
            f"👤 {p1}  •  {'✅ Locked' if game['p1_choice'] else '⌛ Choosing...'}",
            f"🤝 {p2}  •  {'✅ Locked' if game['p2_choice'] else '⌛ Choosing...'}",
        ]

    if note:
        lines += ["", f"<i>{html.escape(note)}</i>"]
    return "\n".join(lines)

def rps_markup(game) -> InlineKeyboardMarkup:
    gid = game["game_id"]
    status = game["status"]
    if status == "waiting":
        btns = [[InlineKeyboardButton("✅ Join", callback_data=f"rps|{gid}|join|0"),
                 InlineKeyboardButton("❌ Cancel", callback_data=f"rps|{gid}|cancel|0")]]
    elif status == "choosing":
        btns = [
            [InlineKeyboardButton("🪨 Rock",     callback_data=f"rps|{gid}|pick|rock")],
            [InlineKeyboardButton("📄 Paper",    callback_data=f"rps|{gid}|pick|paper")],
            [InlineKeyboardButton("✂️ Scissors", callback_data=f"rps|{gid}|pick|scissors")],
            [InlineKeyboardButton("🚫 Forfeit",  callback_data=f"rps|{gid}|close|0")],
        ]
    elif status == "done":
        btns = [[InlineKeyboardButton("🔁 Rematch", callback_data=f"rps|{gid}|rematch|0"),
                 InlineKeyboardButton("🗑 Close",   callback_data=f"rps|{gid}|close|0")]]
    else:
        btns = [[InlineKeyboardButton("🗑 Close", callback_data=f"rps|{gid}|close|0")]]
    return InlineKeyboardMarkup(btns)

async def rps_safe_answer(query, text: str = "", alert: bool = False):
    try: await query.answer(text=text[:180] if text else None, show_alert=alert)
    except: pass

async def rps_edit(query, game, note: str = "", phase: str = "normal"):
    try:
        await query.edit_message_text(
            rps_render_text(game, note, phase),
            reply_markup=rps_markup(game),
            parse_mode=ParseMode.HTML,
        )
    except: pass

async def on_rps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user: return

    mode = "pvp"
    challenge_target_id = 0

    args = context.args or []
    arg0 = args[0].strip().lower() if args else ""

    if arg0 == "bot":
        mode = "bot"
    elif msg.reply_to_message and msg.reply_to_message.from_user and not msg.reply_to_message.from_user.is_bot:
        challenge_target_id = msg.reply_to_message.from_user.id
        mode = "pvp"
    elif arg0.startswith("@"):
        # Store username challenge - we can't resolve ID here, open challenge
        mode = "pvp"

    creator_name = clean_name(user.full_name or user.first_name or "Player")
    game_id = rps_create_game(chat.id, user.id, creator_name, mode, challenge_target_id)
    game = rps_get_game(game_id)

    if mode == "bot":
        note = "Choose your move and Maya will reveal hers instantly!"
    elif challenge_target_id:
        target = msg.reply_to_message.from_user
        note = f"Challenge sent to {html.escape(clean_name(target.full_name or target.first_name))}!"
    else:
        note = "Open challenge — first to join gets to play!"

    try: await context.bot.send_chat_action(chat_id=chat.id, action=ChatAction.TYPING)
    except: pass

    sent = await msg.reply_text(
        rps_render_text(game, note, "waiting" if mode == "pvp" else "choosing"),
        reply_markup=rps_markup(game),
        parse_mode=ParseMode.HTML,
    )
    rps_set_message_id(game_id, sent.message_id)

async def on_rps_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user or not query.data: return

    try:
        _, game_id, action, value = query.data.split("|", 3)
    except:
        await rps_safe_answer(query, "Invalid action.", True); return

    game = rps_get_game(game_id)
    if not game:
        await rps_safe_answer(query, "Game not found or expired.", True); return

    p1_id = int(game["player1_id"])
    p2_id = int(game["player2_id"] or 0)
    creator_id = int(game["creator_id"])
    uid = int(user.id)
    player_ids = {p1_id}
    if p2_id: player_ids.add(p2_id)
    target_id = int(game["challenge_target_id"] or 0)

    if action == "join":
        if game["status"] != "waiting":
            await rps_safe_answer(query, "This game already started.", True); return
        if uid == creator_id:
            await rps_safe_answer(query, "You created this challenge — wait for an opponent.", True); return
        if target_id and uid != target_id:
            await rps_safe_answer(query, "This challenge is not for you.", True); return
        uname = clean_name(user.full_name or user.first_name or "Player")
        rps_update_player2(game_id, uid, uname)
        game = rps_get_game(game_id)
        await rps_safe_answer(query, "Joined! Choose your move.")
        await rps_edit(query, game, "Both players choose your move! Choices stay hidden.", "choosing")
        return

    if action == "cancel":
        if uid != creator_id:
            await rps_safe_answer(query, "Only the creator can cancel.", True); return
        rps_save_state(game_id, None, None, "cancelled")
        await rps_safe_answer(query, "Challenge cancelled.")
        try: await query.edit_message_text("🎮 <b>Rock Paper Scissors</b>\n\n<i>Challenge cancelled.</i>", parse_mode=ParseMode.HTML)
        except: pass
        rps_delete_game(game_id); return

    if action == "close":
        if uid not in player_ids and uid != creator_id:
            await rps_safe_answer(query, "Only players can close this game.", True); return
        rps_save_state(game_id, game["p1_choice"], game["p2_choice"], "cancelled")
        await rps_safe_answer(query, "Game closed.")
        try: await query.edit_message_text("🎮 <b>Rock Paper Scissors</b>\n\n<i>Game closed.</i>", parse_mode=ParseMode.HTML)
        except: pass
        rps_delete_game(game_id); return

    if action == "rematch":
        if uid not in player_ids:
            await rps_safe_answer(query, "Only players can request a rematch.", True); return
        if game["mode"] == "pvp" and p2_id == 0:
            await rps_safe_answer(query, "No second player to rematch with.", True); return
        # Properly reset: clear choices, reset status
        rps_save_state(game_id, None, None, "choosing", None)
        game = rps_get_game(game_id)
        await rps_safe_answer(query, "Rematch! Choose your move.")
        await rps_edit(query, game, "Rematch started — choose your move!", "choosing")
        return

    if action == "pick":
        if game["status"] != "choosing":
            await rps_safe_answer(query, "This round is not active.", True); return
        if value not in {"rock", "paper", "scissors"}:
            await rps_safe_answer(query, "Invalid move.", True); return
        if uid not in player_ids:
            await rps_safe_answer(query, "You are not a player in this game.", True); return

        p1_choice = game["p1_choice"]
        p2_choice = game["p2_choice"]

        if uid == p1_id:
            if p1_choice:
                await rps_safe_answer(query, "You already locked your choice!", True); return
            p1_choice = value
        elif uid == p2_id:
            if p2_choice:
                await rps_safe_answer(query, "You already locked your choice!", True); return
            p2_choice = value
        else:
            await rps_safe_answer(query, "You are not a player in this game.", True); return

        if game["mode"] == "bot":
            p2_choice = random.choice(["rock", "paper", "scissors"])

        # Both picked
        if p1_choice and p2_choice:
            await rps_safe_answer(query, "🔒 Choice locked!")
            rps_save_state(game_id, p1_choice, p2_choice, "revealing")
            game = rps_get_game(game_id)

            # Suspense: show locked state
            await rps_edit(query, game, "Both choices locked... 🔒", "choosing")
            await asyncio.sleep(1.2)

            # Suspense: revealing
            await rps_edit(query, game, "Revealing... 🎴", "revealing")
            await asyncio.sleep(1.0)

            # Final result
            winner = rps_determine_winner(p1_choice, p2_choice)
            rps_save_state(game_id, p1_choice, p2_choice, "done", winner)
            game = rps_get_game(game_id)
            await rps_edit(query, game, "", "result")
        else:
            # One picked
            await rps_safe_answer(query, "✅ Choice locked! Waiting for opponent...")
            rps_save_state(game_id, p1_choice, p2_choice, "choosing")
            game = rps_get_game(game_id)
            await rps_edit(query, game, "One player locked in, waiting for the other...", "choosing")


# ─── XO / Tic-Tac-Toe (Enhanced Multi-Round) ─────────────────────────────────
XO_EMPTY = "▫️"
XO_WIN_LINES = ((0,1,2),(3,4,5),(6,7,8),(0,3,6),(1,4,7),(2,5,8),(0,4,8),(2,4,6))

def xo_now(): return int(time.time())
def xo_make_id(): return f"xo{xo_now()}{random.randint(1000,9999)}"

def xo_create_game(chat_id: int, creator_id: int, creator_name: str, mode: str) -> str:
    game_id = xo_make_id()
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO xo_games
               (game_id,chat_id,message_id,creator_id,creator_name,
                player_x_id,player_x_name,player_o_id,player_o_name,
                mode,board,turn,status,winner,
                score_x,score_o,round_num,streak_x,streak_o,last_winner,
                created_at,updated_at)
               VALUES (?,?,NULL,?,?,?,?,NULL,NULL,?,?,?,?,NULL,0,0,1,0,0,NULL,?,?)""",
            (game_id, chat_id, creator_id, creator_name,
             creator_id, creator_name,
             mode, " " * 9, "X",
             "waiting" if mode == "pvp" else "active",
             xo_now(), xo_now())
        )
        if mode == "bot":
            conn.execute(
                "UPDATE xo_games SET player_o_id=?,player_o_name=?,updated_at=? WHERE game_id=?",
                (0, BOT_NAME, xo_now(), game_id)
            )
        conn.commit()
    return game_id

def xo_get_game(game_id: str):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM xo_games WHERE game_id=?", (game_id,)).fetchone()

def xo_set_message_id(game_id: str, message_id: int):
    with db_connect() as conn:
        conn.execute("UPDATE xo_games SET message_id=?,updated_at=? WHERE game_id=?", (message_id, xo_now(), game_id))
        conn.commit()

def xo_update_players(game_id: str, player_o_id: int, player_o_name: str):
    with db_connect() as conn:
        conn.execute(
            "UPDATE xo_games SET player_o_id=?,player_o_name=?,status='active',updated_at=? WHERE game_id=?",
            (player_o_id, player_o_name, xo_now(), game_id)
        )
        conn.commit()

def xo_save_state(game_id: str, board: str, turn: str, status: str, winner=None,
                  score_x=None, score_o=None, round_num=None,
                  streak_x=None, streak_o=None, last_winner=None):
    with db_connect() as conn:
        game = conn.execute("SELECT * FROM xo_games WHERE game_id=?", (game_id,)).fetchone()
        if not game: return
        conn.execute(
            """UPDATE xo_games SET
               board=?,turn=?,status=?,winner=?,
               score_x=COALESCE(?,score_x),score_o=COALESCE(?,score_o),
               round_num=COALESCE(?,round_num),
               streak_x=COALESCE(?,streak_x),streak_o=COALESCE(?,streak_o),
               last_winner=COALESCE(?,last_winner),
               updated_at=?
               WHERE game_id=?""",
            (board, turn, status, winner,
             score_x, score_o, round_num,
             streak_x, streak_o, last_winner,
             xo_now(), game_id)
        )
        conn.commit()

def xo_delete_game(game_id: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM xo_games WHERE game_id=?", (game_id,))
        conn.commit()

def xo_player_symbol(game, user_id: int) -> Optional[str]:
    if int(game["player_x_id"]) == int(user_id): return "X"
    if game["player_o_id"] is not None and int(game["player_o_id"]) == int(user_id): return "O"
    return None

def xo_display_cell(ch: str) -> str:
    return "❌" if ch == "X" else "⭕" if ch == "O" else XO_EMPTY

def xo_check_winner(board: str) -> Optional[str]:
    for a, b, c in XO_WIN_LINES:
        if board[a] != " " and board[a] == board[b] == board[c]:
            return board[a]
    return None

def xo_is_draw(board: str) -> bool:
    return " " not in board and not xo_check_winner(board)

def xo_apply_move(board: str, idx: int, symbol: str) -> str:
    return board[:idx] + symbol + board[idx + 1:]

def xo_minimax(board: str, depth: int, is_max: bool, alpha: float, beta: float) -> int:
    w = xo_check_winner(board)
    if w == "O": return 10 - depth
    if w == "X": return depth - 10
    if " " not in board: return 0
    if depth >= 6: return 0
    free = [i for i, c in enumerate(board) if c == " "]
    if is_max:
        best = -100
        for idx in free:
            val = xo_minimax(xo_apply_move(board, idx, "O"), depth + 1, False, alpha, beta)
            best = max(best, val)
            alpha = max(alpha, best)
            if beta <= alpha: break
        return best
    else:
        best = 100
        for idx in free:
            val = xo_minimax(xo_apply_move(board, idx, "X"), depth + 1, True, alpha, beta)
            best = min(best, val)
            beta = min(beta, best)
            if beta <= alpha: break
        return best

def xo_best_bot_move(board: str) -> int:
    free = [i for i, c in enumerate(board) if c == " "]
    if not free: return -1
    # Win/block in 1
    for sym in ("O", "X"):
        for idx in free:
            if xo_check_winner(xo_apply_move(board, idx, sym)) == sym:
                return idx
    # Minimax for smarter play
    best_val, best_idx = -100, free[0]
    for idx in free:
        val = xo_minimax(xo_apply_move(board, idx, "O"), 0, False, -100, 100)
        if val > best_val:
            best_val, best_idx = val, idx
    return best_idx

def xo_streak_title(streak: int) -> str:
    if streak >= 5: return "🔥🔥 LEGEND STREAK"
    if streak >= 4: return "⚡ Unstoppable"
    if streak >= 3: return "🔥 On Fire"
    return ""

XO_WIN_LINES_DISPLAY = ["Row 1","Row 2","Row 3","Col 1","Col 2","Col 3","Diagonal ↘","Diagonal ↙"]

XO_WIN_QUIPS = [
    "Classic domination! 🏆",
    "They never saw it coming! 👀",
    "Flawless victory! ✨",
    "Chess grandmaster energy! ♟️",
    "The board bows to you! 🎯",
    "Strategic perfection! 🧠",
]
XO_DRAW_QUIPS = [
    "An elegant stalemate! 🤝",
    "Neither budges — perfectly balanced! ⚖️",
    "Great minds, same moves! 🤯",
    "The board calls it even! 🌐",
    "Destiny demands a rematch! 🔁",
]
XO_BOT_WIN_QUIPS = ["Maya is unbeatable today! 🤖","The bot strikes back! 🎯","Machine precision wins again! ⚙️"]
XO_PLAYER_BEATS_BOT = ["You outsmarted Maya! 🧠","Human intelligence prevails! 🏆","Maya got schooled! 😅"]

def xo_board_markup(game) -> InlineKeyboardMarkup:
    board = game["board"]
    rows = []
    for r in range(3):
        btns = []
        for c in range(3):
            idx = r * 3 + c
            label = xo_display_cell(board[idx])
            data = f"xo|{game['game_id']}|tap|{idx}"
            btns.append(InlineKeyboardButton(label, callback_data=data))
        rows.append(btns)
    status = game["status"]
    if status == "waiting":
        rows.append([
            InlineKeyboardButton("🤝 Join", callback_data=f"xo|{game['game_id']}|join|0"),
            InlineKeyboardButton("✖ Cancel", callback_data=f"xo|{game['game_id']}|cancel|0"),
        ])
    elif status == "active":
        rows.append([
            InlineKeyboardButton("🗑 End Game", callback_data=f"xo|{game['game_id']}|close|0"),
        ])
    else:  # done or cancelled
        rows.append([
            InlineKeyboardButton("🔄 Next Round", callback_data=f"xo|{game['game_id']}|rematch|0"),
            InlineKeyboardButton("🗑 End", callback_data=f"xo|{game['game_id']}|close|0"),
        ])
    return InlineKeyboardMarkup(rows)

def xo_render_text(game, note: str = "") -> str:
    x_name = html.escape(game["player_x_name"] or "Player X")
    o_name = html.escape(game["player_o_name"] or (BOT_NAME if game["mode"] == "bot" else "Waiting..."))
    score_x = int(game["score_x"] or 0)
    score_o = int(game["score_o"] or 0)
    round_num = int(game["round_num"] or 1)
    streak_x = int(game["streak_x"] or 0)
    streak_o = int(game["streak_o"] or 0)
    status = game["status"]

    title = xo_streak_title(max(streak_x, streak_o))
    header = f"🎮 <b>X-O</b>  Round {round_num}"
    if title:
        header += f"  {title}"

    score_line = f"❌ {x_name}: <b>{score_x}</b>  vs  ⭕ {o_name}: <b>{score_o}</b>"
    lines = [header, "━━━━━━━━━━━━━━━━━━", score_line, ""]

    board = game["board"]
    rows_disp = []
    for r in range(3):
        row_str = " ".join(xo_display_cell(board[r*3+c]) for c in range(3))
        rows_disp.append(row_str)
    lines.extend(rows_disp)
    lines.append("")

    if status == "waiting":
        lines.append(f"❌ {x_name}  ✅ Ready")
        lines.append(f"⭕ <i>Waiting for opponent...</i>")
    elif status == "active":
        turn_icon = "❌" if game["turn"] == "X" else "⭕"
        turn_name = x_name if game["turn"] == "X" else o_name
        lines.append(f"🎯 Turn: {turn_icon} <b>{turn_name}</b>")
    elif status == "done":
        winner = game["winner"]
        if winner == "draw":
            lines.append(f"⚖️ <b>Draw!</b>  <i>{random.choice(XO_DRAW_QUIPS)}</i>")
        elif winner == "X":
            q = random.choice(XO_PLAYER_BEATS_BOT if game["mode"] == "bot" else XO_WIN_QUIPS)
            lines.append(f"🏆 <b>{x_name} wins!</b>  <i>{q}</i>")
            if streak_x >= 3:
                lines.append(f"🔥 Win streak: {streak_x}x!")
        elif winner == "O":
            q = random.choice(XO_BOT_WIN_QUIPS if game["mode"] == "bot" else XO_WIN_QUIPS)
            lines.append(f"🏆 <b>{o_name} wins!</b>  <i>{q}</i>")
            if streak_o >= 3:
                lines.append(f"🔥 Win streak: {streak_o}x!")

    if note:
        lines += ["", f"<i>{html.escape(note)}</i>"]
    return "\n".join(lines)

async def xo_safe_answer(query, text: str = "", alert: bool = False):
    try: await query.answer(text=text, show_alert=alert)
    except: pass

async def xo_edit(query, game, note: str = ""):
    try:
        await query.edit_message_text(
            xo_render_text(game, note),
            reply_markup=xo_board_markup(game),
            parse_mode=ParseMode.HTML,
        )
    except: pass

async def on_xo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user: return
    mode = "bot" if (context.args and context.args[0].strip().lower() == "bot") else "pvp"
    creator_name = clean_name(user.full_name or user.first_name or "Player")
    game_id = xo_create_game(chat.id, user.id, creator_name, mode)
    game = xo_get_game(game_id)
    try: await context.bot.send_chat_action(chat_id=chat.id, action=ChatAction.TYPING)
    except: pass
    note = "You play ❌ X vs Maya ⭕ O. Your move!" if mode == "bot" else "PvP match — tap Join to play!"
    sent = await msg.reply_text(xo_render_text(game, note), reply_markup=xo_board_markup(game), parse_mode=ParseMode.HTML)
    xo_set_message_id(game_id, sent.message_id)

async def on_xo_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user or not query.data: return

    try:
        _, game_id, action, value = query.data.split("|", 3)
    except:
        await xo_safe_answer(query, "Invalid action.", True); return

    game = xo_get_game(game_id)
    if not game:
        await xo_safe_answer(query, "Game not found or expired.", True); return

    uid = int(user.id)
    x_id = int(game["player_x_id"])
    o_id = int(game["player_o_id"] or 0)
    creator_id = int(game["creator_id"])
    player_ids = {x_id}
    if o_id: player_ids.add(o_id)

    if action == "join":
        if game["status"] != "waiting":
            await xo_safe_answer(query, "This game already started.", True); return
        if uid == creator_id:
            await xo_safe_answer(query, "Wait for an opponent to join.", True); return
        uname = clean_name(user.full_name or user.first_name or "Player")
        xo_update_players(game_id, uid, uname)
        game = xo_get_game(game_id)
        await xo_safe_answer(query, "Joined! You play ⭕ O. ❌ X goes first.")
        await xo_edit(query, game, "Game on! ❌ X goes first.")
        return

    if action == "cancel":
        if uid != creator_id:
            await xo_safe_answer(query, "Only the creator can cancel.", True); return
        xo_save_state(game_id, game["board"], game["turn"], "cancelled")
        await xo_safe_answer(query, "Cancelled.")
        try: await query.edit_message_text("🎮 <b>X-O</b>\n\n<i>Game cancelled.</i>", parse_mode=ParseMode.HTML)
        except: pass
        xo_delete_game(game_id); return

    if action == "close":
        if uid not in player_ids and uid != creator_id:
            await xo_safe_answer(query, "Only players can end this game.", True); return
        await xo_safe_answer(query, "Game ended.")
        sx = int(game["score_x"] or 0); so = int(game["score_o"] or 0)
        xname = html.escape(game["player_x_name"] or "X")
        oname = html.escape(game["player_o_name"] or "O")
        try:
            await query.edit_message_text(
                f"🎮 <b>X-O — Final Score</b>\n━━━━━━━━━\n❌ {xname}: <b>{sx}</b>\n⭕ {oname}: <b>{so}</b>\n\n<i>Game over! GG 🤝</i>",
                parse_mode=ParseMode.HTML
            )
        except: pass
        xo_delete_game(game_id); return

    if action == "rematch":
        if uid not in player_ids:
            await xo_safe_answer(query, "Only players can start the next round.", True); return
        if game["mode"] == "pvp" and o_id == 0:
            await xo_safe_answer(query, "No second player for a rematch.", True); return
        new_round = int(game["round_num"] or 1) + 1
        # Alternate who goes first based on round
        new_turn = "X" if new_round % 2 == 1 else "O"
        xo_save_state(game_id, " " * 9, new_turn, "active", None,
                      round_num=new_round, last_winner=None)
        game = xo_get_game(game_id)
        await xo_safe_answer(query, f"Round {new_round} started!")
        turn_name = html.escape(game["player_x_name"] if new_turn == "X" else game["player_o_name"] or BOT_NAME)
        await xo_edit(query, game, f"Round {new_round}! {turn_name} goes first.")
        return

    if action != "tap":
        await xo_safe_answer(query); return

    if game["status"] != "active":
        await xo_safe_answer(query, "This game is not active.", True); return

    symbol = xo_player_symbol(game, uid)
    if not symbol:
        await xo_safe_answer(query, "You are not a player in this game.", True); return
    if symbol != game["turn"]:
        await xo_safe_answer(query, "Not your turn!", True); return

    idx = int(value)
    board = game["board"]
    if idx < 0 or idx > 8 or board[idx] != " ":
        await xo_safe_answer(query, "That cell is already taken!", True); return

    board = xo_apply_move(board, idx, symbol)
    winner_sym = xo_check_winner(board)

    if winner_sym:
        is_x_win = winner_sym == "X"
        new_sx = int(game["score_x"] or 0) + (1 if is_x_win else 0)
        new_so = int(game["score_o"] or 0) + (0 if is_x_win else 1)
        last = game.get("last_winner") or ""
        new_sx_streak = int(game["streak_x"] or 0)
        new_so_streak = int(game["streak_o"] or 0)
        if is_x_win:
            new_sx_streak += 1
            new_so_streak = 0
        else:
            new_so_streak += 1
            new_sx_streak = 0
        xo_save_state(game_id, board, symbol, "done", winner_sym,
                      score_x=new_sx, score_o=new_so,
                      streak_x=new_sx_streak, streak_o=new_so_streak,
                      last_winner=winner_sym)
        game = xo_get_game(game_id)
        await xo_safe_answer(query)
        await xo_edit(query, game, "Tap 🔄 Next Round or 🗑 End.")
        return

    if xo_is_draw(board):
        xo_save_state(game_id, board, symbol, "done", "draw",
                      streak_x=0, streak_o=0, last_winner="draw")
        game = xo_get_game(game_id)
        await xo_safe_answer(query)
        await xo_edit(query, game, "Draw! Tap 🔄 Next Round or 🗑 End.")
        return

    next_turn = "O" if symbol == "X" else "X"
    xo_save_state(game_id, board, next_turn, "active")
    game = xo_get_game(game_id)

    # Bot move
    if game["mode"] == "bot" and next_turn == "O":
        bi = xo_best_bot_move(board)
        if bi >= 0:
            board = xo_apply_move(board, bi, "O")
            bw = xo_check_winner(board)
            if bw:
                new_so = int(game["score_o"] or 0) + 1
                new_so_str = int(game["streak_o"] or 0) + 1
                xo_save_state(game_id, board, "O", "done", "O",
                              score_o=new_so, streak_o=new_so_str, streak_x=0, last_winner="O")
            elif xo_is_draw(board):
                xo_save_state(game_id, board, "O", "done", "draw", streak_x=0, streak_o=0)
            else:
                xo_save_state(game_id, board, "X", "active")
        game = xo_get_game(game_id)

    await xo_safe_answer(query)
    await xo_edit(query, game, "")


# ─── Lucky Box (Enhanced with Coins + Rare Events) ────────────────────────────
LB_RESULTS = {
    "jackpot":      {"emoji":"🎉","label":"JACKPOT!",    "coins":+50,  "rare":False},
    "double":       {"emoji":"💰","label":"DOUBLE REWARD","coins":+30,  "rare":False},
    "empty":        {"emoji":"📭","label":"Empty Box",    "coins":-5,   "rare":False},
    "trap":         {"emoji":"💣","label":"TRAP!",        "coins":-20,  "rare":False},
    "clown":        {"emoji":"🤡","label":"Clown Box",    "coins":-10,  "rare":False},
    "steal":        {"emoji":"🦝","label":"Steal Chance", "coins":0,    "rare":False},
    "shield":       {"emoji":"🛡️","label":"Shield",       "coins":0,    "rare":False},
    "reroll":       {"emoji":"🎲","label":"Reroll Ticket","coins":0,    "rare":False},
    "golden":       {"emoji":"✨","label":"GOLDEN BOX!",  "coins":+200, "rare":True},
    "cursed":       {"emoji":"💀","label":"CURSED BOX!",  "coins":-100, "rare":True},
    "mythic":       {"emoji":"🌟","label":"MYTHIC BOX!!!", "coins":+500,"rare":True},
}

# Weighted pool (rare excluded from normal draw)
LB_NORMAL_POOL = [
    "jackpot","jackpot",
    "double","double",
    "empty","empty","empty","empty",
    "trap","trap",
    "clown",
    "steal",
    "shield",
    "reroll",
]

def lb_pick_result(rare_chance: float = 0.01) -> str:
    if random.random() < rare_chance:
        return random.choice(["golden","cursed","mythic"])
    return random.choice(LB_NORMAL_POOL)

LB_JACKPOT_LINES = ["🎉 Stars aligned for you! Jackpot!","💫 Fortune favors the bold — you win big!","🌟 The lucky box chose YOU!"]
LB_DOUBLE_LINES  = ["💰 Double the coins, double the joy!","🎯 Smart pick — double reward incoming!","✨ Two is better than one!"]
LB_EMPTY_LINES   = ["📭 Nothing but air... and broken dreams.","😶 The box was just... empty. Oops.","🌫️ Not every box shines today."]
LB_TRAP_LINES    = ["💣 BOOM! That was a trap!","😱 Oof, this one stings!","🔥 Trap activated — coins gone!"]
LB_CLOWN_LINES   = ["🤡 Honk honk! You found the clown box.","😅 The universe laughed with (at?) you.","🎪 Welcome to the circus — enjoy your loss!"]
LB_STEAL_LINES   = ["🦝 Steal! You swiped coins from the pot.","🕵️ Sneaky! Coins transferred to you.","🥷 You stole 15 coins from the last opener!"]
LB_SHIELD_LINES  = ["🛡️ You got a Shield! Protected from next trap or steal.","⚔️ Shield acquired — one free block incoming!","🛡️ Protected for your next misfortune!"]
LB_REROLL_LINES  = ["🎲 Reroll Ticket! Use /luckybox to reroll this round.","♻️ Second chance acquired — use it wisely!","🎰 Another spin awaits you!"]
LB_GOLDEN_LINES  = ["✨✨✨ GOLDEN BOX! Rare fortune! +200 coins!","🌟 The rarest of finds — Golden Box!","💛 GOLDEN!!! This happens once in a blue moon!"]
LB_CURSED_LINES  = ["💀 CURSED BOX! Dark energy drains your coins!","🌑 The cursed box strikes! -100 coins!","😈 Ancient curse activated — your wallet weeps!"]
LB_MYTHIC_LINES  = ["🌟🌟🌟 MYTHIC BOX!!! LEGENDARY +500 COINS!!!","⭐ THE MYTHIC BOX!! Once in a lifetime! +500!","🔱 MYTHIC POWER UNLOCKED! +500 coins!!"]

def lb_result_lines(kind: str) -> str:
    m = {
        "jackpot": LB_JACKPOT_LINES, "double": LB_DOUBLE_LINES,
        "empty": LB_EMPTY_LINES, "trap": LB_TRAP_LINES,
        "clown": LB_CLOWN_LINES, "steal": LB_STEAL_LINES,
        "shield": LB_SHIELD_LINES, "reroll": LB_REROLL_LINES,
        "golden": LB_GOLDEN_LINES, "cursed": LB_CURSED_LINES,
        "mythic": LB_MYTHIC_LINES,
    }
    return random.choice(m.get(kind, ["..."]))

def lb_now(): return int(time.time())
def lb_make_id(): return f"lb{lb_now()}{random.randint(1000,9999)}"

def lb_create_round(chat_id: int, creator_id: int, creator_name: str, total_boxes: int = 5) -> str:
    game_id = lb_make_id()
    winning_box = random.randint(0, max(0, total_boxes - 1))
    rare_event = random.random() < 0.01  # 1% ultra rare round
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO luckybox_rounds
               (game_id,chat_id,message_id,creator_id,creator_name,
                status,winning_box,winner_id,winner_name,total_boxes,created_at,updated_at)
               VALUES (?,?,NULL,?,?,'active',?,NULL,NULL,?,?,?)""",
            (game_id, chat_id, creator_id, creator_name,
             winning_box, total_boxes, lb_now(), lb_now())
        )
        conn.commit()
    return game_id

def lb_get_round(game_id: str):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM luckybox_rounds WHERE game_id=?", (game_id,)).fetchone()

def lb_set_message_id(game_id: str, message_id: int):
    with db_connect() as conn:
        conn.execute("UPDATE luckybox_rounds SET message_id=?,updated_at=? WHERE game_id=?", (message_id, lb_now(), game_id))
        conn.commit()

def lb_get_plays(game_id: str):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM luckybox_plays WHERE game_id=? ORDER BY id ASC", (game_id,)).fetchall()

def lb_user_play(game_id: str, user_id: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM luckybox_plays WHERE game_id=? AND user_id=?", (game_id, user_id)).fetchone()

def lb_box_play(game_id: str, box_index: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM luckybox_plays WHERE game_id=? AND box_index=?", (game_id, box_index)).fetchone()

def lb_record_play(game_id: str, user_id: int, user_name: str, box_index: int, result_kind: str, result_text: str):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO luckybox_plays (game_id,user_id,user_name,box_index,result_kind,result_text,created_at) VALUES (?,?,?,?,?,?,?)",
            (game_id, user_id, user_name, box_index, result_kind, result_text, lb_now())
        )
        conn.commit()

def lb_finish_round(game_id: str, winner_id: int, winner_name: str):
    with db_connect() as conn:
        conn.execute(
            "UPDATE luckybox_rounds SET status='done',winner_id=?,winner_name=?,updated_at=? WHERE game_id=?",
            (winner_id, winner_name, lb_now(), game_id)
        )
        conn.commit()

def lb_reset_round(game_id: str):
    rr = lb_get_round(game_id)
    if not rr: return
    winning_box = random.randint(0, max(0, int(rr["total_boxes"]) - 1))
    with db_connect() as conn:
        conn.execute("DELETE FROM luckybox_plays WHERE game_id=?", (game_id,))
        conn.execute("DELETE FROM lb_shields WHERE game_id=?", (game_id,))
        conn.execute(
            "UPDATE luckybox_rounds SET status='active',winning_box=?,winner_id=NULL,winner_name=NULL,updated_at=? WHERE game_id=?",
            (winning_box, lb_now(), game_id)
        )
        conn.commit()

def lb_delete_round(game_id: str):
    with db_connect() as conn:
        conn.execute("DELETE FROM luckybox_plays WHERE game_id=?", (game_id,))
        conn.execute("DELETE FROM lb_shields WHERE game_id=?", (game_id,))
        conn.execute("DELETE FROM luckybox_rounds WHERE game_id=?", (game_id,))
        conn.commit()

def lb_render_text(round_row, note: str = "") -> str:
    plays = lb_get_plays(round_row["game_id"])
    opened = len(plays)
    total = int(round_row["total_boxes"])
    lines = [
        "🎁 <b>Lucky Box</b>",
        f"Host: <b>{html.escape(round_row['creator_name'])}</b>  •  Opened: <b>{opened}/{total}</b>",
    ]
    status = round_row["status"]
    if status == "active":
        lines.append("<i>One lucky box hides the jackpot. Each player opens one box.</i>")
    elif status == "done":
        lines.append(f"🏆 <b>Winner: {html.escape(round_row['winner_name'] or 'Nobody')}</b>")
    elif status == "closed":
        lines.append("<i>Round closed.</i>")

    if note:
        lines += ["", f"{note}"]

    if plays:
        lines.append("")
        lines.append("<b>Recent opens:</b>")
        for row in plays[-5:]:
            info = LB_RESULTS.get(row["result_kind"], {})
            em = info.get("emoji","📦")
            lines.append(f"{em} <b>{html.escape(row['user_name'])}</b> → Box {int(row['box_index'])+1} — {info.get('label','?')}")
    return "\n".join(lines)

def lb_markup(round_row) -> InlineKeyboardMarkup:
    gid = round_row["game_id"]
    plays = {int(r["box_index"]): r for r in lb_get_plays(gid)}
    rows = []
    row = []
    for idx in range(int(round_row["total_boxes"])):
        if idx in plays:
            rk = plays[idx]["result_kind"]
            info = LB_RESULTS.get(rk, {})
            em = info.get("emoji", "📦")
            label = f"{em} {idx+1}"
            cb = f"lb|{gid}|noop|{idx}"
        else:
            label = f"🎁 {idx+1}"
            cb = f"lb|{gid}|pick|{idx}"
        row.append(InlineKeyboardButton(label, callback_data=cb))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("🔄 New Round", callback_data=f"lb|{gid}|reroll|0"),
        InlineKeyboardButton("📊 My Stats",  callback_data=f"lb|{gid}|stats|0"),
        InlineKeyboardButton("🗑 Close",     callback_data=f"lb|{gid}|close|0"),
    ])
    return InlineKeyboardMarkup(rows)

async def lb_safe_answer(query, text: str = "", alert: bool = False):
    try: await query.answer(text=text[:180] if text else None, show_alert=alert)
    except: pass

async def lb_edit(query, round_row, note: str = ""):
    try:
        await query.edit_message_text(
            lb_render_text(round_row, note),
            reply_markup=lb_markup(round_row),
            parse_mode=ParseMode.HTML,
        )
    except: pass

async def on_luckybox(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    chat = update.effective_chat
    user = update.effective_user
    if not msg or not chat or not user: return
    creator_name = clean_name(user.full_name or user.first_name or "Player")
    lb_ensure_coins(user.id, creator_name)
    game_id = lb_create_round(chat.id, user.id, creator_name, 5)
    round_row = lb_get_round(game_id)
    try: await context.bot.send_chat_action(chat_id=chat.id, action=ChatAction.TYPING)
    except: pass
    coins = lb_get_coins(user.id)
    sent = await msg.reply_text(
        lb_render_text(round_row, f"Your balance: <b>{coins} 🪙</b> — Pick a box!"),
        reply_markup=lb_markup(round_row),
        parse_mode=ParseMode.HTML,
    )
    lb_set_message_id(game_id, sent.message_id)

async def on_luckybox_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user or not query.data: return

    try:
        _, game_id, action, value = query.data.split("|", 3)
    except:
        await lb_safe_answer(query, "Invalid action.", True); return

    round_row = lb_get_round(game_id)
    if not round_row:
        await lb_safe_answer(query, "Lucky Box not found.", True); return

    uid = int(user.id)
    uname = clean_name(user.full_name or user.first_name or "Player")
    lb_ensure_coins(uid, uname)

    if action == "noop":
        await lb_safe_answer(query, "This box is already opened.", True); return

    if action == "stats":
        coins = lb_get_coins(uid)
        with db_connect() as conn:
            row = conn.execute("SELECT * FROM lb_stats WHERE user_id=?", (uid,)).fetchone()
        if row:
            txt = (f"📊 Your Lucky Box Stats\n"
                   f"🪙 Coins: {coins}\n"
                   f"🎮 Games: {row['games']}\n"
                   f"🎉 Jackpots: {row['jackpots']}\n"
                   f"💣 Traps: {row['traps']}\n"
                   f"📈 Total Won: {row['total_won']}  Lost: {row['total_lost']}")
        else:
            txt = f"📊 Your Stats\n🪙 Coins: {coins}\n<i>No games yet!</i>"
        await lb_safe_answer(query, txt, True); return

    if action == "close":
        if uid != int(round_row["creator_id"]):
            await lb_safe_answer(query, "Only the creator can close this round.", True); return
        with db_connect() as conn:
            conn.execute("UPDATE luckybox_rounds SET status='closed',updated_at=? WHERE game_id=?", (lb_now(), game_id))
            conn.commit()
        lb_delete_round(game_id)
        await lb_safe_answer(query, "Round closed.")
        try: await query.edit_message_text("🎁 <b>Lucky Box</b>\n\n<i>Round closed.</i>", parse_mode=ParseMode.HTML)
        except: pass
        return

    if action == "reroll":
        if uid != int(round_row["creator_id"]):
            await lb_safe_answer(query, "Only the creator can start a new round.", True); return
        lb_reset_round(game_id)
        round_row = lb_get_round(game_id)
        await lb_safe_answer(query, "New round started!")
        coins = lb_get_coins(uid)
        await lb_edit(query, round_row, f"Fresh boxes! Your balance: <b>{coins} 🪙</b>")
        return

    if action != "pick": return

    if round_row["status"] != "active":
        await lb_safe_answer(query, "This round is not active.", True); return
    if lb_user_play(game_id, uid):
        await lb_safe_answer(query, "You already opened a box this round!", True); return

    idx = int(value)
    if lb_box_play(game_id, idx):
        await lb_safe_answer(query, "That box is already taken!", True); return

    # Determine result
    winning_box = int(round_row["winning_box"])
    is_jackpot = (idx == winning_box)

    # Pick result kind
    if is_jackpot:
        # Jackpot box — rare chance for ultra event
        if random.random() < 0.02:
            kind = random.choice(["golden","mythic"])
        else:
            kind = "jackpot"
    else:
        # Non-jackpot box — pick random non-jackpot result
        kind = lb_pick_result(rare_chance=0.01)
        if kind == "jackpot":
            kind = "empty"  # fallback
        if kind in {"golden","mythic"} and not is_jackpot:
            kind = "double"  # safety

    info = LB_RESULTS.get(kind, {"coins": 0, "emoji": "📦", "label": "?"})
    result_line = lb_result_lines(kind)

    coins_delta = info["coins"]
    alert_text = f"{info['emoji']} {info['label']}\n{result_line}"

    # Special handling
    if kind == "shield":
        lb_grant_shield(game_id, uid)
        coins_delta = 0

    elif kind == "trap":
        if lb_has_shield(game_id, uid):
            lb_consume_shield(game_id, uid)
            coins_delta = 0
            alert_text = f"🛡️ Shield blocked the trap!\n{result_line}"
            kind = "trap_blocked"
        else:
            coins_delta = -20

    elif kind == "steal":
        # Steal 15 coins from the last opener (not self)
        plays = lb_get_plays(game_id)
        steal_target = None
        for p in reversed(plays):
            if int(p["user_id"]) != uid:
                steal_target = p
                break
        if steal_target:
            stolen = min(15, lb_get_coins(int(steal_target["user_id"])))
            lb_adjust_coins(int(steal_target["user_id"]), steal_target["user_name"], -stolen)
            coins_delta = stolen
            alert_text = f"🦝 You stole {stolen} coins from {html.escape(steal_target['user_name'])}!\n{result_line}"
        else:
            coins_delta = 5
            alert_text = f"🦝 Nobody to steal from — you found 5 coins instead!\n{result_line}"

    elif kind == "reroll":
        coins_delta = 0  # Just the ticket, no coin change

    # Apply coins
    if coins_delta != 0:
        new_coins = lb_adjust_coins(uid, uname, coins_delta)
        if coins_delta > 0:
            alert_text += f"\n\n🪙 +{coins_delta} coins  (Total: {new_coins})"
        else:
            alert_text += f"\n\n🪙 {coins_delta} coins  (Total: {new_coins})"
    else:
        new_coins = lb_get_coins(uid)
        alert_text += f"\n\n🪙 Balance: {new_coins}"

    lb_update_stats(uid, uname,
                    jackpot=kind in {"jackpot","golden","mythic"},
                    trap=kind in {"trap"},
                    won=max(0, coins_delta),
                    lost=max(0, -coins_delta))

    lb_record_play(game_id, uid, uname, idx, kind, result_line[:280])

    # Finish if jackpot
    if is_jackpot:
        lb_finish_round(game_id, uid, uname)

    round_row = lb_get_round(game_id)

    # For rare events add suspense
    if info.get("rare"):
        await lb_safe_answer(query)
        note_line = f"<b>⚠️ {uname} opened Box {idx+1}... Something rare happened!</b>"
        await lb_edit(query, round_row, note_line)
        await asyncio.sleep(1.5)
        note_line = f"<b>{info['emoji']} {info['label']}</b>\n<i>{result_line}</i>\n🪙 Balance: {new_coins}"
        await lb_edit(query, round_row, note_line)
        return

    await lb_safe_answer(query, alert_text, alert=True)
    note_str = f"<b>{uname}</b> opened Box {idx+1} — {info['emoji']} {info.get('label','')}"
    await lb_edit(query, round_row, note_str)

def init_extra_games_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS quiz_sessions (
            session_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            message_id INTEGER,
            creator_id INTEGER NOT NULL,
            creator_name TEXT NOT NULL,
            lang TEXT NOT NULL DEFAULT 'en',
            category TEXT NOT NULL DEFAULT 'mixed',
            q_index INTEGER NOT NULL DEFAULT 0,
            score_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'active',
            current_answer TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS quiz_answered (
            session_id TEXT NOT NULL,
            q_index INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (session_id, q_index, user_id)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS guess_games (
            game_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            message_id INTEGER,
            creator_id INTEGER NOT NULL,
            secret INTEGER NOT NULL,
            tries_left INTEGER NOT NULL DEFAULT 7,
            last_guess INTEGER,
            hint TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            winner_id INTEGER,
            winner_name TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS wordchain_games (
            game_id TEXT PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            message_id INTEGER,
            lang TEXT NOT NULL DEFAULT 'en',
            last_word TEXT,
            last_user_id INTEGER,
            last_user_name TEXT,
            used_words TEXT NOT NULL DEFAULT '[]',
            score_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'active',
            round_count INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS game_leaderboard (
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            user_name TEXT NOT NULL DEFAULT '',
            quiz_wins INTEGER NOT NULL DEFAULT 0,
            quiz_correct INTEGER NOT NULL DEFAULT 0,
            guess_wins INTEGER NOT NULL DEFAULT 0,
            wordchain_words INTEGER NOT NULL DEFAULT 0,
            rps_wins INTEGER NOT NULL DEFAULT 0,
            xo_wins INTEGER NOT NULL DEFAULT 0,
            coins INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (user_id, chat_id)
        )""")
        conn.commit()

def lb_update_leaderboard(user_id: int, chat_id: int, user_name: str, **kwargs):
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO game_leaderboard (user_id,chat_id,user_name,updated_at) VALUES (?,?,?,?)",
            (user_id, chat_id, user_name[:40], now)
        )
        conn.execute("UPDATE game_leaderboard SET user_name=?,updated_at=? WHERE user_id=? AND chat_id=?",
                     (user_name[:40], now, user_id, chat_id))
        for col, val in kwargs.items():
            allowed = {"quiz_wins","quiz_correct","guess_wins","wordchain_words","rps_wins","xo_wins","coins"}
            if col in allowed and isinstance(val, int):
                conn.execute(
                    f"UPDATE game_leaderboard SET {col}={col}+?,updated_at=? WHERE user_id=? AND chat_id=?",
                    (val, now, user_id, chat_id)
                )
        conn.commit()

def get_chat_leaderboard(chat_id: int, limit: int = 10):
    with db_connect() as conn:
        return conn.execute(
            """SELECT user_name,
               COALESCE(rps_wins,0) as rps_wins,
               COALESCE(xo_wins,0) as xo_wins,
               COALESCE(quiz_wins,0) as quiz_wins,
               (COALESCE(rps_wins,0)*2 + COALESCE(xo_wins,0)*2 + COALESCE(quiz_wins,0)) as total_score
               FROM game_leaderboard WHERE chat_id=?
               ORDER BY total_score DESC LIMIT ?""",
            (chat_id, limit)
        ).fetchall()

# ─── TRUTH OR DARE (/tod) ─────────────────────────────────────────────────────
TOD_TRUTHS_EN = [
    "What is the most embarrassing thing you've ever done?",
    "Have you ever cheated in a game?",
    "What's your biggest fear?",
    "Have you ever lied to get out of trouble?",
    "What's a secret you've never told anyone?",
    "Have you ever had a crush on someone in this group?",
    "What's the most childish thing you still do?",
    "What would you do with 1 million dollars?",
    "Have you ever blamed someone else for something you did?",
    "What's your biggest regret?",
    "What's the weirdest dream you've ever had?",
    "Have you ever stalked someone's social media for hours?",
    "What's the longest you've gone without showering?",
    "Have you ever pretended to be sick to avoid something?",
    "What's your most embarrassing moment at school or work?",
]
TOD_DARES_EN = [
    "Send a voice message saying 'I love you' to the last person you texted.",
    "Write 10 compliments about the person above you in this chat.",
    "Change your profile picture to a potato for 1 hour.",
    "Send the most embarrassing photo from your gallery.",
    "Write a 3-line poem about the group admin.",
    "Post your current screen time stats.",
    "Type everything in CAPS for the next 10 messages.",
    "Send a selfie right now without any filters.",
    "Say something nice about the last person who messaged in this group.",
    "Change your bio to 'I lost a dare game' for 30 minutes.",
    "Speak in rhymes for the next 5 minutes.",
    "Send a voice message of you singing for 10 seconds.",
    "Write the first 5 app names that appear on your phone screen.",
    "Respond to every message with an animal sound for 5 minutes.",
    "Tag 3 people in this group and say one nice thing about each.",
]
TOD_TRUTHS_BN = [
    "তুমি কি কখনো কোনো খেলায় চিটিং করেছ?",
    "তোমার সবচেয়ে বড় ভয় কোনটা?",
    "তুমি কি কখনো মিথ্যা বলে বিপদ এড়িয়েছ?",
    "এই group-এ কারো প্রতি কি কোনোদিন crush ছিল?",
    "তুমি কি এখনো ছোটবেলার কোনো অভ্যাস ধরে রেখেছ?",
    "তোমার সবচেয়ে বিব্রতকর মুহূর্ত কোনটা?",
    "যদি ১ কোটি টাকা পেতে, কী করতে?",
    "তুমি কি কখনো কারো social media ঘণ্টার পর ঘণ্টা ঘেঁটেছ?",
    "তোমার সবচেয়ে বড় অনুতাপ কী?",
    "সবচেয়ে অদ্ভুত স্বপ্ন কোনটা দেখেছিলে?",
    "তুমি কি কখনো অসুস্থতার ভান করে কিছু এড়িয়েছ?",
    "তোমার ফোনে সবচেয়ে বেশি কোন app চালাও?",
    "তোমার লুকানো কোনো talent আছে?",
    "তুমি কি কখনো কাউকে দোষ দিয়েছ নিজের কাজের জন্য?",
    "তোমার সবচেয়ে লজ্জাজনক মুহূর্ত কখন হয়েছিল?",
]
TOD_DARES_BN = [
    "সর্বশেষ যে মানুষকে message করেছিলে তাকে একটা voice message পাঠাও 'আমি তোমাকে ভালোবাসি' বলে।",
    "group-এর উপরের জনকে ১০টা compliment লেখো।",
    "১ ঘণ্টার জন্য profile picture আলু বানাও।",
    "গ্যালারি থেকে সবচেয়ে বিব্রতকর ছবিটা পাঠাও।",
    "group admin-কে নিয়ে ৩ লাইনের কবিতা লেখো।",
    "এখন screen time stats পোস্ট করো।",
    "পরের ১০টা message সব CAPS-এ টাইপ করো।",
    "এখনই কোনো filter ছাড়া selfie তোলো।",
    "শেষ যে message করেছে তার সম্পর্কে কিছু ভালো বলো।",
    "৩০ মিনিটের জন্য bio-তে 'Dare game-এ হেরেছি' লেখো।",
    "৫ মিনিট সব কথা ছড়ায় বলো।",
    "১০ সেকেন্ড গান গেয়ে voice message পাঠাও।",
    "ফোনের স্ক্রিনে যে ৫টা app দেখা যাচ্ছে সেগুলোর নাম লেখো।",
    "পরের ৫ মিনিট সব reply-এ প্রাণীর শব্দ ব্যবহার করো।",
    "group-এর ৩ জনকে tag করে প্রত্যেকের সম্পর্কে একটা ভালো কথা বলো।",
]

async def on_tod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message; chat = update.effective_chat; user = update.effective_user
    if not msg or not chat or not user: return
    lang = get_group_lang(chat.id) if chat.type in {"group","supergroup"} else "en"
    try: await context.bot.send_chat_action(chat_id=chat.id, action=ChatAction.TYPING)
    except: pass
    uname = html.escape(clean_name(user.full_name or user.first_name or "Player"))
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("😇 Truth", callback_data=f"tod|{lang}|truth|{user.id}"),
        InlineKeyboardButton("😈 Dare",  callback_data=f"tod|{lang}|dare|{user.id}"),
    ]])
    await msg.reply_text(
        f"🎭 <b>Truth or Dare</b>\n━━━━━━━━━━━━━━━━━━\n"
        f"<b>{uname}</b>, choose your fate!\n\n"
        f"😇 Truth — answer honestly\n😈 Dare — complete the challenge",
        reply_markup=markup, parse_mode=ParseMode.HTML)

async def on_tod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; user = update.effective_user
    if not query or not user or not query.data: return
    try: _, lang, kind, requester_id = query.data.split("|", 3)
    except: await query.answer("Invalid.", True); return
    try: await context.bot.send_chat_action(chat_id=query.message.chat_id, action=ChatAction.TYPING)
    except: pass
    uname = html.escape(clean_name(user.full_name or user.first_name or "Player"))
    if kind == "truth":
        pool = TOD_TRUTHS_EN if lang == "en" else TOD_TRUTHS_BN
        prompt = random.choice(pool)
        icon, label = "😇", "Truth"
    else:
        pool = TOD_DARES_EN if lang == "en" else TOD_DARES_BN
        prompt = random.choice(pool)
        icon, label = "😈", "Dare"
    replay_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔁 Another Truth", callback_data=f"tod|{lang}|truth|{user.id}"),
        InlineKeyboardButton("🔁 Another Dare",  callback_data=f"tod|{lang}|dare|{user.id}"),
    ]])
    await query.answer(f"{icon} {label} selected!")
    try:
        await query.edit_message_text(
            f"🎭 <b>Truth or Dare</b>  •  {icon} {label}\n━━━━━━━━━━━━━━━━━━\n"
            f"<b>{uname}</b>, your {label.lower()}:\n\n"
            f"<i>{html.escape(prompt)}</i>",
            reply_markup=replay_markup, parse_mode=ParseMode.HTML)
    except: pass

# ═══════════════════════════════════════════════════════════════════════════════
# MAYA ULTRA ENGINE — Smart Moderation · Economy · Daily Digest ·
# Group Health · Premium Cards · Smart Context · Member Profiles
# ═══════════════════════════════════════════════════════════════════════════════

import json as _json
import re as _re_ultra
from datetime import timedelta

# ─── Ultra DB Tables ──────────────────────────────────────────────────────────
def init_ultra_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS member_profiles (
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            user_name TEXT NOT NULL DEFAULT '',
            first_seen INTEGER NOT NULL,
            last_seen INTEGER NOT NULL,
            msg_count INTEGER NOT NULL DEFAULT 0,
            warn_count INTEGER NOT NULL DEFAULT 0,
            is_vip INTEGER NOT NULL DEFAULT 0,
            custom_title TEXT DEFAULT NULL,
            PRIMARY KEY (user_id, chat_id)
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS warn_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            admin_id INTEGER NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS autoreply_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            trigger_text TEXT NOT NULL,
            reply_text TEXT NOT NULL,
            match_type TEXT NOT NULL DEFAULT 'contains',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS group_health (
            chat_id INTEGER PRIMARY KEY,
            total_messages INTEGER NOT NULL DEFAULT 0,
            total_members_welcomed INTEGER NOT NULL DEFAULT 0,
            active_members_7d INTEGER NOT NULL DEFAULT 0,
            last_digest_sent INTEGER NOT NULL DEFAULT 0,
            health_score INTEGER NOT NULL DEFAULT 50,
            updated_at INTEGER NOT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS gift_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_user_id INTEGER NOT NULL,
            to_user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        )""")
        conn.commit()

# ─── Member Profile System ─────────────────────────────────────────────────────
def _now_ts(): return int(time.time())

def ensure_profile(user_id: int, chat_id: int, user_name: str):
    now = _now_ts()
    with db_connect() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO member_profiles
               (user_id,chat_id,user_name,first_seen,last_seen,msg_count,warn_count,is_vip)
               VALUES (?,?,?,?,?,0,0,0)""",
            (user_id, chat_id, user_name[:40], now, now)
        )
        conn.execute(
            "UPDATE member_profiles SET user_name=?,last_seen=?,msg_count=msg_count+1 WHERE user_id=? AND chat_id=?",
            (user_name[:40], now, user_id, chat_id)
        )
        conn.commit()

def get_profile(user_id: int, chat_id: int):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM member_profiles WHERE user_id=? AND chat_id=?",
            (user_id, chat_id)
        ).fetchone()

async def on_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg  = update.effective_message
    if not chat or not user or not msg: return
    # Check if replying to someone
    target = user
    if msg.reply_to_message and msg.reply_to_message.from_user and not msg.reply_to_message.from_user.is_bot:
        target = msg.reply_to_message.from_user
    tname = clean_name(target.full_name or target.first_name or "Member")
    is_group = chat.type in {"group","supergroup"}
    if not is_group:
        await msg.reply_text("📋 Use /profile inside a group!")
        return
    ensure_profile(target.id, chat.id, tname)
    row = get_profile(target.id, chat.id)
    lb_row = None
    with db_connect() as conn:
        lb_row = conn.execute(
            "SELECT * FROM game_leaderboard WHERE user_id=? AND chat_id=?",
            (target.id, chat.id)
        ).fetchone()
    msgs     = int(row["msg_count"] or 0)
    warns    = int(row["warn_count"] or 0)
    is_vip   = int(row["is_vip"] or 0)
    title    = row["custom_title"] or ("⭐ VIP Member" if is_vip else "")
    first_s  = format_ts(int(row["first_seen"] or 0))
    last_s   = format_ts(int(row["last_seen"] or 0))
    rps_w  = (lb_row["rps_wins"] if lb_row else 0) or 0
    xo_w   = (lb_row["xo_wins"] if lb_row else 0) or 0
    total_score = rps_w*2 + xo_w*2 + msgs//20
    rank_label = ("🏅 Bronze" if total_score < 20 else
                  "🥈 Silver" if total_score < 60 else
                  "🥇 Gold"   if total_score < 150 else
                  "💎 Diamond" if total_score < 400 else "👑 Legend")
    warn_bar = ("🟥"*warns + "⬜"*(3-min(warns,3))) if warns <= 3 else "🚨🚨🚨"
    game_bar = _bar(total_score, max(1, total_score+20), 8)
    lines = [
        f"👤 <b>{html.escape(tname)}</b>  {'⭐ VIP' if is_vip else ''}",
        f"<i>{html.escape(title)}</i>" if title else "",
        "━━━━━━━━━━━━━━━━━━",
        f"🏆 Rank:     <b>{rank_label}</b>",
        f"📈 Score:    {game_bar} <b>{total_score}</b>",
        f"💬 Messages: <b>{_fmt_num(msgs)}</b>",
        f"⚠️ Warns:    {warn_bar} ({warns}/3)",
        f"🎮 RPS wins: {rps_w}  •  ⭕ XO wins: {xo_w}",
        "",
        f"📅 First seen: {first_s}",
        f"🕐 Last seen:  {last_s}",
    ]
    await human_delay_and_action(context, update)
    await msg.reply_text("\n".join(l for l in lines if l), parse_mode=ParseMode.HTML)

async def on_warn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    msg  = update.effective_message
    admin = update.effective_user
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        await msg.reply_text("⚠️ Reply to a user's message to warn them.\nUsage: reply + /warn [reason]")
        return
    target = msg.reply_to_message.from_user
    if target.is_bot:
        await msg.reply_text("Can't warn a bot.")
        return
    if is_super_admin(target.id):
        await msg.reply_text("Can't warn the bot owner.")
        return
    reason = " ".join(context.args) if context.args else "No reason provided"
    tname = clean_name(target.full_name or target.first_name or "Member")
    ensure_profile(target.id, chat.id, tname)
    warn_count = add_warn(target.id, chat.id, admin.id, reason)
    await human_delay_and_action(context, update)
    if warn_count >= MAX_WARNS:
        # Auto-kick: ban then immediately unban = kick without permanent ban
        kicked = False
        kick_error = ""
        try:
            # Verify bot has kick permission
            bot_member = await context.bot.get_chat_member(chat.id, context.bot.id)
            if bot_member.status in {ChatMemberStatus.ADMINISTRATOR}:
                from datetime import datetime as _dt
                # revoke_messages=False to keep their messages
                await context.bot.ban_chat_member(
                    chat_id=chat.id,
                    user_id=target.id,
                    revoke_messages=False,
                )
                # Small delay then unban (makes it a kick, not permanent ban)
                await asyncio.sleep(0.5)
                await context.bot.unban_chat_member(
                    chat_id=chat.id,
                    user_id=target.id,
                    only_if_banned=True,
                )
                kicked = True
            else:
                kick_error = "Bot needs Admin rights to kick."
        except Exception as e:
            kick_error = str(e)[:120]
        if kicked:
            await msg.reply_text(
                f"🚨 <b>{html.escape(tname)}</b> has been kicked!\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Reason: 3 warnings reached\n"
                f"Last reason: {html.escape(reason)}",
                parse_mode=ParseMode.HTML
            )
        else:
            await msg.reply_text(
                f"⚠️ <b>{html.escape(tname)}</b> reached {warn_count}/{MAX_WARNS} warns.\n"
                f"Auto-kick failed: {html.escape(kick_error)}\n"
                f"<i>Please kick manually or give me Admin rights.</i>",
                parse_mode=ParseMode.HTML
            )
    else:
        warn_bar  = "🟥" * warn_count + "⬜" * (MAX_WARNS - warn_count)
        danger_txt = "🚨 One more warn = auto-kick!" if warn_count == MAX_WARNS - 1 else "⚠️ Be careful next time."
        admin_name = html.escape(clean_name(admin.first_name or "Admin"))
        await msg.reply_text(
            f"⚠️ <b>Warning Issued</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 User:   <b>{html.escape(tname)}</b>\n"
            f"👮 By:     {admin_name}\n"
            f"📋 Reason: {html.escape(reason)}\n"
            f"⚡ Warns:  {warn_bar} <b>{warn_count}/{MAX_WARNS}</b>\n\n"
            f"<i>{danger_txt}</i>",
            parse_mode=ParseMode.HTML
        )

async def on_unwarn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        await msg.reply_text("Reply to a user's message to clear their warns.")
        return
    target = msg.reply_to_message.from_user
    tname  = clean_name(target.full_name or target.first_name or "Member")
    clear_warns(target.id, update.effective_chat.id)
    await human_delay_and_action(context, update)
    await msg.reply_text(
        f"✅ All warnings cleared for <b>{html.escape(tname)}</b>.",
        parse_mode=ParseMode.HTML
    )

async def on_warns(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg  = update.effective_message
    chat = update.effective_chat
    if not msg or not chat: return
    target = update.effective_user
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
    tname  = clean_name(target.full_name or target.first_name or "Member")
    ensure_profile(target.id, chat.id, tname)
    warns  = get_warn_count(target.id, chat.id)
    bar    = "🟥"*warns + "⬜"*(MAX_WARNS-min(warns,MAX_WARNS))
    with db_connect() as conn:
        logs = conn.execute(
            "SELECT reason,created_at FROM warn_log WHERE user_id=? AND chat_id=? ORDER BY created_at DESC LIMIT 5",
            (target.id, chat.id)
        ).fetchall()
    lines = [
        f"⚠️ <b>Warn History: {html.escape(tname)}</b>",
        f"{bar} <b>{warns}/{MAX_WARNS}</b>",
        "━━━━━━━━━━━━━━━━━━",
    ]
    if logs:
        for log in logs:
            lines.append(f"• {html.escape(log['reason'][:60])} — {format_ts(int(log['created_at']))}")
    else:
        lines.append("<i>No warnings.</i>")
    await human_delay_and_action(context, update)
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── AutoReply System ─────────────────────────────────────────────────────────
_autoreply_cache: dict[int, list] = {}

def get_autoreply_rules(chat_id: int) -> list:
    if chat_id in _autoreply_cache:
        return _autoreply_cache[chat_id]
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM autoreply_rules WHERE chat_id=? AND enabled=1",
            (chat_id,)
        ).fetchall()
    _autoreply_cache[chat_id] = rows
    return rows

def invalidate_autoreply_cache(chat_id: int):
    _autoreply_cache.pop(chat_id, None)

def check_autoreply(chat_id: int, text: str) -> str | None:
    rules = get_autoreply_rules(chat_id)
    text_lower = text.lower().strip()
    for rule in rules:
        trigger = rule["trigger_text"].lower()
        match_type = rule["match_type"]
        if match_type == "exact" and text_lower == trigger:
            return rule["reply_text"]
        elif match_type == "startswith" and text_lower.startswith(trigger):
            return rule["reply_text"]
        elif match_type == "contains" and trigger in text_lower:
            return rule["reply_text"]
    return None

async def on_setreply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    msg  = update.effective_message
    chat = update.effective_chat
    raw  = (msg.text or "").split(" ", 1)
    if len(raw) < 2 or "|" not in raw[1]:
        await msg.reply_text(
            "🤖 <b>Set AutoReply</b>\n\nUsage:\n"
            "<code>/setreply trigger | reply text</code>\n\n"
            "Examples:\n"
            "<code>/setreply hello | Hi there! 👋</code>\n"
            "<code>/setreply link? | Check pinned message!</code>\n\n"
            "Match types: contains (default), exact, startswith\n"
            "<code>/setreply [exact] yes | Noted!</code>",
            parse_mode=ParseMode.HTML)
        return
    content = raw[1].strip()
    match_type = "contains"
    if content.startswith("[exact]"):
        match_type = "exact"
        content = content[7:].strip()
    elif content.startswith("[startswith]"):
        match_type = "startswith"
        content = content[12:].strip()
    parts = content.split("|", 1)
    trigger = parts[0].strip()[:80]
    reply   = parts[1].strip()[:500] if len(parts) > 1 else ""
    if not trigger or not reply:
        await msg.reply_text("Both trigger and reply text are required.")
        return
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO autoreply_rules (chat_id,trigger_text,reply_text,match_type,enabled,created_at) VALUES (?,?,?,?,1,?)",
            (chat.id, trigger, reply, match_type, _now_ts())
        )
        conn.commit()
    invalidate_autoreply_cache(chat.id)
    await msg.reply_text(
        f"✅ <b>AutoReply saved!</b>\n"
        f"Trigger: <code>{html.escape(trigger)}</code>\n"
        f"Match: {match_type}\n"
        f"Reply: {html.escape(reply[:60])}{'...' if len(reply)>60 else ''}",
        parse_mode=ParseMode.HTML
    )

async def on_listreplies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT id,trigger_text,match_type,enabled FROM autoreply_rules WHERE chat_id=? ORDER BY id",
            (chat.id,)
        ).fetchall()
    if not rows:
        await update.effective_message.reply_text("No autoreply rules set. Use /setreply to add one.")
        return
    lines = ["🤖 <b>AutoReply Rules</b>", "━━━━━━━━━━━━━━━━━━"]
    for r in rows[:20]:
        st = "✅" if r["enabled"] else "❌"
        lines.append(f"{st} <b>#{r['id']}</b> [{r['match_type']}] <code>{html.escape(r['trigger_text'])}</code>")
    lines.append("\nUse /delreply <id> to remove a rule.")
    await update.effective_message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def on_delreply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    if not context.args or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /delreply <id>  (get ID from /listreplies)")
        return
    rule_id = int(context.args[0])
    with db_connect() as conn:
        conn.execute("DELETE FROM autoreply_rules WHERE id=? AND chat_id=?", (rule_id, chat.id))
        conn.commit()
    invalidate_autoreply_cache(chat.id)
    await update.effective_message.reply_text(f"✅ AutoReply rule #{rule_id} removed.")

# ─── Smart Link Guard ─────────────────────────────────────────────────────────
_link_guard_enabled: dict[int, bool] = {}

def is_link_guard_on(chat_id: int) -> bool:
    if chat_id in _link_guard_enabled:
        return _link_guard_enabled[chat_id]
    row = get_group(chat_id)
    # Use a spare field; default off
    val = False
    _link_guard_enabled[chat_id] = val
    return val

async def on_linkguard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    if not context.args:
        status = "ON" if is_link_guard_on(chat.id) else "OFF"
        await update.effective_message.reply_text(
            f"🛡️ Link Guard: <b>{status}</b>\n\nUsage: /linkguard on  or  /linkguard off\n\n"
            f"<i>When ON, links from non-admins are automatically deleted with a warning.</i>",
            parse_mode=ParseMode.HTML)
        return
    val = context.args[0].strip().lower()
    if val not in {"on","off"}:
        await update.effective_message.reply_text("Usage: /linkguard on or /linkguard off")
        return
    _link_guard_enabled[chat.id] = (val == "on")
    await update.effective_message.reply_text(
        f"🛡️ Link Guard: <b>{val.upper()}</b>\n"
        f"<i>{'Links from non-admins will be deleted.' if val=='on' else 'Link protection disabled.'}</i>",
        parse_mode=ParseMode.HTML)

# ─── Group Health & Stats ──────────────────────────────────────────────────────
def update_group_health(chat_id: int, delta_msgs: int = 0):
    now = _now_ts()
    with db_connect() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO group_health (chat_id,total_messages,total_members_welcomed,
               active_members_7d,last_digest_sent,health_score,updated_at) VALUES (?,0,0,0,0,50,?)""",
            (chat_id, now)
        )
        conn.execute(
            "UPDATE group_health SET total_messages=total_messages+?,updated_at=? WHERE chat_id=?",
            (delta_msgs, now, chat_id)
        )
        conn.commit()

def get_group_health_row(chat_id: int):
    with db_connect() as conn:
        return conn.execute("SELECT * FROM group_health WHERE chat_id=?", (chat_id,)).fetchone()

async def on_groupstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    msg  = update.effective_message
    group_row = get_group(chat.id)
    health_row = get_group_health_row(chat.id)
    lang = get_group_lang(chat.id)
    total_msgs   = int(health_row["total_messages"] if health_row else 0)
    welcomes     = int(group_row["total_welcome_sent"] or 0)
    hourly_sent  = int(group_row["total_hourly_sent"] or 0)
    health_score = int(health_row["health_score"] if health_row else 50)
    # Health bar
    h_bar = _bar(health_score, 100, 10)
    # Top members
    with db_connect() as conn:
        top = conn.execute(
            "SELECT user_name, msg_count FROM member_profiles WHERE chat_id=? ORDER BY msg_count DESC LIMIT 5",
            (chat.id,)
        ).fetchall()
        total_members = conn.execute(
            "SELECT COUNT(*) c FROM member_profiles WHERE chat_id=?", (chat.id,)
        ).fetchone()["c"]
    lines = [
        f"📊 <b>Group Stats</b>",
        f"<i>{html.escape(chat.title or '')}</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        f"💬 Messages tracked: <b>{_fmt_num(total_msgs)}</b>",
        f"👥 Known members:    <b>{_fmt_num(total_members)}</b>",
        f"👋 Welcomes sent:    <b>{_fmt_num(welcomes)}</b>",
        f"📨 Hourly msgs:      <b>{_fmt_num(hourly_sent)}</b>",
        "",
        f"── Top Active Members ─",
    ]
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
    for i, row in enumerate(top):
        lines.append(f"{medals[i]} <b>{html.escape(row['user_name'])}</b> — {_fmt_num(int(row['msg_count']))} msgs")
    if not top:
        lines.append("<i>No message data yet.</i>")
    await human_delay_and_action(context, update)
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── /ask — AI Q&A powered by Groq ───────────────────────────────────────────
_ask_cooldowns: dict[int, float] = {}  # chat_id -> last ask time
ASK_COOLDOWN_SECONDS = 60

async def on_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg  = update.effective_message
    if not chat or not user or not msg: return
    if not GROQ_API_KEYS:
        await msg.reply_text("🤖 AI Q&A is not configured. No Groq API key found.")
        return
    question = " ".join(context.args).strip() if context.args else ""
    if not question and msg.reply_to_message:
        question = msg.reply_to_message.text or ""
    if not question:
        await msg.reply_text(
            "🧠 <b>Ask Maya AI</b>\n\nUsage: <code>/ask your question</code>\n\n"
            "Or reply to any message and use /ask",
            parse_mode=ParseMode.HTML)
        return
    if len(question) > 500:
        await msg.reply_text("Question too long. Max 500 characters.")
        return
    # Per-chat cooldown
    now = time.time()
    last = _ask_cooldowns.get(chat.id, 0)
    if now - last < ASK_COOLDOWN_SECONDS:
        wait = int(ASK_COOLDOWN_SECONDS - (now - last))
        mins = wait // 60
        secs = wait % 60
        time_str = f"{mins}m {secs}s" if mins else f"{secs}s"
        await msg.reply_text(
            f"⏳ <b>AI Cooldown</b>\n"
            f"Wait <b>{time_str}</b> before asking again.\n"
            f"<i>This prevents spam and saves API quota.</i>",
            parse_mode=ParseMode.HTML
        )
        return
    _ask_cooldowns[chat.id] = now
    lang = get_group_lang(chat.id) if chat.type in {"group","supergroup"} else "en"
    await human_delay_and_action(context, update)
    thinking = await msg.reply_text("🧠 <i>Thinking...</i>", parse_mode=ParseMode.HTML)
    try:
        sys_prompt = (
            "You are Maya, a helpful and warm Telegram group assistant. "
            "You give short, accurate, friendly answers. "
            "Max 150 words. No markdown, use plain text. "
            f"Reply in {'Bengali (Bangla)' if lang=='bn' else 'English'}."
        )
        data = _groq_chat_request({
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": question},
            ],
            "temperature": 0.7,
            "max_tokens": 200,
        })
        answer = (data["choices"][0]["message"]["content"] or "").strip()
        if not answer:
            answer = "I couldn't generate an answer. Please try again."
    except Exception as e:
        logger.warning("on_ask Groq failed: %s", e)
        answer = ("দুঃখিত, এই মুহূর্তে উত্তর দিতে পারছি না। পরে চেষ্টা করো।"
                  if lang=="bn" else "Sorry, I couldn't answer right now. Please try again.")
    uname = html.escape(clean_name(user.full_name or user.first_name or ""))
    try:
        await thinking.edit_text(
            f"🧠 <b>Maya AI</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"❓ <i>{html.escape(question[:100])}{'...' if len(question)>100 else ''}</i>\n\n"
            f"💡 {html.escape(answer)}",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        await msg.reply_text(
            f"🧠 <b>Maya AI</b>\n━━━━━━━━━━━━━━━━━━\n{html.escape(answer)}",
            parse_mode=ParseMode.HTML
        )

# ─── /translate — Instant Bangla ↔ English ────────────────────────────────────
_translate_cooldowns: dict[int, float] = {}

async def on_translate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg  = update.effective_message
    if not chat or not user or not msg: return
    if not GROQ_API_KEYS:
        await msg.reply_text("Translation requires Groq API key.")
        return
    text = " ".join(context.args).strip() if context.args else ""
    if not text and msg.reply_to_message:
        text = (msg.reply_to_message.text or "").strip()
    if not text:
        await msg.reply_text(
            "🌐 <b>Translate</b>\n\nUsage: <code>/tr your text here</code>\n"
            "Or reply to any message with /tr",
            parse_mode=ParseMode.HTML)
        return
    if len(text) > 400:
        await msg.reply_text("Text too long. Max 400 characters.")
        return
    now = time.time()
    if now - _translate_cooldowns.get(chat.id, 0) < 20:
        await msg.reply_text("⏳ Translate cooldown: 20s")
        return
    _translate_cooldowns[chat.id] = now
    # Detect language and translate to the other
    has_bangla = bool(_re_ultra.search(r"[\u0980-\u09FF]", text))
    if has_bangla:
        target_lang, direction = "English", "বাংলা → English"
    else:
        target_lang, direction = "Bangla (Bengali)", "English → বাংলা"
    thinking = await msg.reply_text("🌐 <i>Translating...</i>", parse_mode=ParseMode.HTML)
    try:
        data = _groq_chat_request({
            "model": GROQ_MODEL,
            "messages": [
                {"role": "system", "content": f"Translate the following text to {target_lang}. Return ONLY the translation, nothing else."},
                {"role": "user", "content": text},
            ],
            "temperature": 0.3,
            "max_tokens": 200,
        })
        result = (data["choices"][0]["message"]["content"] or "").strip()
    except Exception as e:
        logger.warning("translate failed: %s", e)
        result = "Translation failed. Please try again."
    try:
        await thinking.edit_text(
            f"🌐 <b>Translation</b>  <i>{direction}</i>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"<b>Original:</b> {html.escape(text[:100])}{'...' if len(text)>100 else ''}\n\n"
            f"<b>Translated:</b> {html.escape(result)}",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        await msg.reply_text(html.escape(result))

# ─── /top — Top members in group ──────────────────────────────────────────────
async def on_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg  = update.effective_message
    if not chat or not msg: return
    if chat.type not in {"group","supergroup"}:
        await msg.reply_text("Use /top inside a group!")
        return
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT user_name,msg_count FROM member_profiles WHERE chat_id=? ORDER BY msg_count DESC LIMIT 10",
            (chat.id,)
        ).fetchall()
    if not rows:
        await msg.reply_text("No activity tracked yet. Start chatting! 💬")
        return
    max_msgs = max(int(r["msg_count"]) for r in rows) or 1
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = [
        f"💬 <b>Most Active Members</b>",
        f"<i>{html.escape(chat.title or '')}</i>",
        "━━━━━━━━━━━━━━━━━━",
        "",
    ]
    for i, r in enumerate(rows):
        bar  = _bar(int(r["msg_count"]), max_msgs, 6)
        name = html.escape(r["user_name"] or "Unknown")
        lines.append(
            f"{medals[i]} <b>{name}</b>\n"
            f"   {bar} <b>{_fmt_num(int(r['msg_count']))}</b> messages"
        )
        if i < len(rows)-1: lines.append("")
    await human_delay_and_action(context, update)
    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

# ─── /rules — Group rules ──────────────────────────────────────────────────────
_rules_cache: dict[int, str] = {}

async def on_setrules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context): return
    chat = update.effective_chat
    raw  = (update.effective_message.text or "").split(" ", 1)
    if len(raw) < 2 or not raw[1].strip():
        await update.effective_message.reply_text(
            "Usage: /setrules Your rules here\n\nUse \\n for new lines.")
        return
    rules_text = raw[1].strip().replace("\\n", "\n")[:1000]
    set_group_value(chat.id, "custom_welcome", None)  # Don't touch welcome
    # Store in DB using footer_text-adjacent approach: use a separate key
    with db_connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO autoreply_rules (chat_id,trigger_text,reply_text,match_type,enabled,created_at) VALUES (?,?,?,?,1,?)",
            (chat.id, "__RULES__", rules_text, "exact", _now_ts())
        )
        conn.commit()
    _rules_cache[chat.id] = rules_text
    invalidate_autoreply_cache(chat.id)
    await update.effective_message.reply_text("✅ Group rules saved! Members can view with /rules")

async def on_rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    msg  = update.effective_message
    if not chat or not msg: return
    rules_text = _rules_cache.get(chat.id)
    if not rules_text:
        with db_connect() as conn:
            row = conn.execute(
                "SELECT reply_text FROM autoreply_rules WHERE chat_id=? AND trigger_text='__RULES__'",
                (chat.id,)
            ).fetchone()
        if row:
            rules_text = row["reply_text"]
            _rules_cache[chat.id] = rules_text
    if not rules_text:
        await msg.reply_text("📋 No rules set yet. Admins can use /setrules to set them.")
        return
    await human_delay_and_action(context, update)
    await msg.reply_text(
        f"📋 <b>Group Rules</b>\n"
        f"<i>{html.escape(chat.title or '')}</i>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"{html.escape(rules_text)}",
        parse_mode=ParseMode.HTML
    )

# ─── Smart Message Handler Integration ────────────────────────────────────────
async def handle_ultra_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Called from on_keyword_message to handle ultra features."""
    chat = update.effective_chat
    msg  = update.effective_message
    user = update.effective_user
    if not chat or not msg or not user or user.is_bot: return

    text = msg.text or ""

    # Track message in profile
    uname = clean_name(user.full_name or user.first_name or "Member")
    try:
        ensure_profile(user.id, chat.id, uname)
        update_group_health(chat.id, delta_msgs=1)
    except Exception:
        pass

    # Link guard check
    if is_link_guard_on(chat.id) and URLISH_RE.search(text):
        try:
            member = await context.bot.get_chat_member(chat.id, user.id)
            if member.status not in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}:
                await msg.delete()
                warn_count = add_warn(user.id, chat.id, 0, "Posted a link")
                lang = get_group_lang(chat.id)
                w_msg = (
                    f"🛡️ {user.mention_html(uname)}, links are not allowed here! ⚠️ ({warn_count}/{MAX_WARNS} warns)"
                )
                sent = await context.bot.send_message(chat.id, w_msg, parse_mode=ParseMode.HTML)
                asyncio.create_task(schedule_delete(context.bot, chat.id, sent.message_id, 15))
                return
        except Exception:
            pass

    # AutoReply check
    if text and not text.startswith("/"):
        reply_text = check_autoreply(chat.id, text)
        if reply_text:
            now = time.time()
            # Simple per-trigger cooldown (5 min)
            ck = f"ar_{chat.id}"
            if now - _ask_cooldowns.get(ck, 0) > 300:
                _ask_cooldowns[ck] = now
                try:
                    await asyncio.sleep(random.uniform(1.0, 2.5))
                    await msg.reply_text(reply_text)
                except Exception:
                    pass

# ═══════════════════════════════════════════════════════════════════════════════
# NEW FEATURES: Forward Button · Ban/Unban · Left Delete · Welcome v2
# ═══════════════════════════════════════════════════════════════════════════════

# ─── Forward Button System DB ─────────────────────────────────────────────────
_forward_tasks: dict[int, asyncio.Task] = {}   # chat_id -> running task
_forward_msg_id: dict[int, int] = {}           # chat_id -> last fwd message_id

def init_forward_db():
    with db_connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS forward_settings (
            chat_id INTEGER PRIMARY KEY,
            group_link TEXT NOT NULL DEFAULT '',
            group_title TEXT NOT NULL DEFAULT '',
            fwd_text TEXT NOT NULL DEFAULT '',
            fwd_interval INTEGER NOT NULL DEFAULT 300,
            enabled INTEGER NOT NULL DEFAULT 0,
            last_msg_id INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        conn.commit()

def get_forward_settings(chat_id: int):
    with db_connect() as conn:
        return conn.execute(
            "SELECT * FROM forward_settings WHERE chat_id=?", (chat_id,)
        ).fetchone()

def save_forward_settings(chat_id: int, group_link: str, group_title: str,
                           fwd_text: str, interval: int, enabled: int):
    now = int(time.time())
    with db_connect() as conn:
        conn.execute(
            """INSERT INTO forward_settings
               (chat_id,group_link,group_title,fwd_text,fwd_interval,enabled,last_msg_id,created_at,updated_at)
               VALUES (?,?,?,?,?,?,0,?,?)
               ON CONFLICT(chat_id) DO UPDATE SET
               group_link=excluded.group_link,group_title=excluded.group_title,
               fwd_text=excluded.fwd_text,fwd_interval=excluded.fwd_interval,
               enabled=excluded.enabled,updated_at=excluded.updated_at""",
            (chat_id, group_link[:200], group_title[:80], fwd_text[:500],
             interval, enabled, now, now)
        )
        conn.commit()

def set_forward_enabled(chat_id: int, enabled: int):
    with db_connect() as conn:
        conn.execute(
            "UPDATE forward_settings SET enabled=?,updated_at=? WHERE chat_id=?",
            (enabled, int(time.time()), chat_id)
        )
        conn.commit()

def set_forward_last_msg_id(chat_id: int, msg_id: int):
    with db_connect() as conn:
        conn.execute(
            "UPDATE forward_settings SET last_msg_id=?,updated_at=? WHERE chat_id=?",
            (msg_id, int(time.time()), chat_id)
        )
        conn.commit()

def _fwd_markup(link: str, count_str: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(f"📢 Forward ({count_str})", url=link)
    ]])

async def _run_forward_loop(bot, chat_id: int):
    """Runs in background: delete old msg, send new one, repeat every interval."""
    logger.info("Forward loop started for chat %s", chat_id)
    forward_count = 0
    while True:
        try:
            row = get_forward_settings(chat_id)
            if not row or not int(row["enabled"]):
                logger.info("Forward loop stopped for chat %s (disabled)", chat_id)
                break
            link     = row["group_link"]
            text     = row["fwd_text"] or "📢 Join our group!"
            interval = max(60, int(row["fwd_interval"] or 300))

            # Delete previous message
            old_mid = int(row["last_msg_id"] or 0)
            if old_mid:
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=old_mid)
                except Exception:
                    pass

            # Build count string (cycles 0→1→0 to show activity)
            count_str = f"{forward_count % 2}/{(forward_count+1) % 2}"
            forward_count += 1

            # Send new forward message
            sent = await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=_fwd_markup(link, count_str),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            set_forward_last_msg_id(chat_id, sent.message_id)
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Forward loop cancelled for chat %s", chat_id)
            break
        except Exception as e:
            logger.exception("Forward loop error in %s: %s", chat_id, e)
            await asyncio.sleep(60)

def _start_forward_task(bot, chat_id: int):
    """Start or restart forward loop task."""
    old = _forward_tasks.get(chat_id)
    if old and not old.done():
        old.cancel()
    task = asyncio.create_task(_run_forward_loop(bot, chat_id))
    _forward_tasks[chat_id] = task

def _stop_forward_task(chat_id: int):
    old = _forward_tasks.get(chat_id)
    if old and not old.done():
        old.cancel()
    _forward_tasks.pop(chat_id, None)

async def restore_forward_tasks(bot):
    """Called on bot startup to resume all active forward loops."""
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT chat_id FROM forward_settings WHERE enabled=1"
        ).fetchall()
    for row in rows:
        _start_forward_task(bot, int(row["chat_id"]))
    if rows:
        logger.info("Resumed %d forward loops", len(rows))

# ─── /setforward command ───────────────────────────────────────────────────────
async def on_setforward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /setforward <link> [text]
    Sets the group link and optional custom text for the Forward button system.
    """
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    msg  = update.effective_message
    args = context.args or []

    if not args:
        row = get_forward_settings(chat.id)
        status = "🟢 ON" if (row and int(row["enabled"])) else "🔴 OFF"
        link   = row["group_link"] if row else "—"
        itv    = int(row["fwd_interval"]) if row else 300
        txt    = row["fwd_text"] if row else "—"
        await msg.reply_text(
            f"📢 <b>Forward Button System</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Status:   <b>{status}</b>\n"
            f"Link:     <code>{html.escape(link)}</code>\n"
            f"Interval: <b>{itv}s ({itv//60}m)</b>\n"
            f"Text:     {html.escape(txt[:60])}\n\n"
            f"<b>Commands:</b>\n"
            f"<code>/setforward https://t.me/yourgroup</code>\n"
            f"<code>/setforward https://t.me/yourgroup Custom text here</code>\n"
            f"<code>/forwardon</code>  — start\n"
            f"<code>/forwardoff</code> — stop\n"
            f"<code>/setforwardinterval 300</code> — set interval in seconds",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        return

    link = args[0].strip()
    if not (link.startswith("https://") or link.startswith("http://")):
        await msg.reply_text(
            "❌ Please provide a valid URL.\n"
            "Example: <code>/setforward https://t.me/yourgroup</code>",
            parse_mode=ParseMode.HTML
        )
        return

    custom_text = " ".join(args[1:]).strip() if len(args) > 1 else ""
    fwd_text = custom_text if custom_text else (
        f"📢 Join <b>{html.escape(chat.title or 'our group')}</b>!\n"
        f"Click below to join and share with friends."
    )

    row = get_forward_settings(chat.id)
    interval = int(row["fwd_interval"]) if row else 300
    save_forward_settings(chat.id, link, chat.title or "", fwd_text, interval, 0)

    await msg.reply_text(
        f"✅ <b>Forward link saved!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Link: <code>{html.escape(link)}</code>\n"
        f"📝 Text: {html.escape(fwd_text[:60])}\n\n"
        f"Use <code>/forwardon</code> to activate.",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

async def on_forwardon(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    msg  = update.effective_message
    row  = get_forward_settings(chat.id)
    if not row or not row["group_link"]:
        await msg.reply_text(
            "❌ No link set yet.\n"
            "Use <code>/setforward https://t.me/yourgroup</code> first.",
            parse_mode=ParseMode.HTML
        )
        return
    set_forward_enabled(chat.id, 1)
    _start_forward_task(context.bot, chat.id)
    itv = int(row["fwd_interval"] or 300)
    await msg.reply_text(
        f"🟢 <b>Forward Button activated!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⏰ Interval: every <b>{itv}s</b>\n"
        f"🔗 Link: <code>{html.escape(row['group_link'])}</code>\n\n"
        f"<i>Bot will send and rotate the forward message automatically.</i>",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )

async def on_forwardoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    msg  = update.effective_message
    set_forward_enabled(chat.id, 0)
    _stop_forward_task(chat.id)
    # Delete last forward message if present
    row = get_forward_settings(chat.id)
    if row and int(row["last_msg_id"] or 0):
        try:
            await context.bot.delete_message(
                chat_id=chat.id,
                message_id=int(row["last_msg_id"])
            )
        except Exception:
            pass
        set_forward_last_msg_id(chat.id, 0)
    await msg.reply_text("🔴 <b>Forward Button stopped.</b>", parse_mode=ParseMode.HTML)

async def on_setforwardinterval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat = update.effective_chat
    msg  = update.effective_message
    if not context.args or not context.args[0].isdigit():
        await msg.reply_text(
            "Usage: <code>/setforwardinterval &lt;seconds&gt;</code>\n"
            "Example: <code>/setforwardinterval 300</code> (5 minutes)\n"
            "Minimum: 60 seconds.",
            parse_mode=ParseMode.HTML
        )
        return
    secs = max(60, int(context.args[0]))
    row = get_forward_settings(chat.id)
    if not row:
        await msg.reply_text("❌ Set a link first with /setforward.")
        return
    save_forward_settings(chat.id, row["group_link"], row["group_title"],
                           row["fwd_text"], secs, int(row["enabled"]))
    # Restart task with new interval
    if int(row["enabled"]):
        _start_forward_task(context.bot, chat.id)
    await msg.reply_text(
        f"⏰ Forward interval set to <b>{secs}s</b> ({secs//60}m {secs%60}s).",
        parse_mode=ParseMode.HTML
    )

# ─── /ban and /unban ──────────────────────────────────────────────────────────
async def on_ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat  = update.effective_chat
    msg   = update.effective_message
    admin = update.effective_user

    target = None
    reason = ""
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
        reason = " ".join(context.args).strip() if context.args else "No reason provided"
    elif context.args:
        # /ban @username reason
        mention = context.args[0]
        reason  = " ".join(context.args[1:]).strip() or "No reason provided"
        # Try to find user by username from member list (best effort)
        await msg.reply_text(
            "⚠️ Please <b>reply</b> to the user's message to ban them.\n"
            "<code>Reply to message → /ban [reason]</code>",
            parse_mode=ParseMode.HTML
        )
        return
    else:
        await msg.reply_text(
            "⚠️ <b>Ban Usage</b>\n\n"
            "Reply to a user's message and use:\n"
            "<code>/ban</code> — ban with no reason\n"
            "<code>/ban spam</code> — ban with reason",
            parse_mode=ParseMode.HTML
        )
        return

    if not target:
        await msg.reply_text("❌ Could not find the target user.")
        return
    if target.is_bot:
        await msg.reply_text("❌ Cannot ban a bot.")
        return
    if is_super_admin(target.id):
        await msg.reply_text("❌ Cannot ban the bot owner.")
        return

    tname = html.escape(clean_name(target.full_name or target.first_name or "Member"))
    aname = html.escape(clean_name(admin.full_name or admin.first_name or "Admin"))

    try:
        # Check bot permission
        bot_mem = await context.bot.get_chat_member(chat.id, context.bot.id)
        if bot_mem.status != ChatMemberStatus.ADMINISTRATOR:
            await msg.reply_text("❌ I need Admin rights to ban members.")
            return
        await context.bot.ban_chat_member(chat_id=chat.id, user_id=target.id)
        await msg.reply_text(
            f"🚫 <b>Banned</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 User:   {target.mention_html(tname)}\n"
            f"👮 By:     {aname}\n"
            f"📋 Reason: {html.escape(reason)}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await msg.reply_text(f"❌ Ban failed: {html.escape(str(e)[:100])}")

async def on_unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_group_admin(update, context):
        return
    chat  = update.effective_chat
    msg   = update.effective_message
    admin = update.effective_user

    target = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        target = msg.reply_to_message.from_user
    else:
        await msg.reply_text(
            "⚠️ Reply to a user's message to unban them.\n"
            "<code>Reply to message → /unban</code>",
            parse_mode=ParseMode.HTML
        )
        return

    tname = html.escape(clean_name(target.full_name or target.first_name or "Member"))
    aname = html.escape(clean_name(admin.full_name or admin.first_name or "Admin"))
    try:
        await context.bot.unban_chat_member(
            chat_id=chat.id, user_id=target.id, only_if_banned=True
        )
        await msg.reply_text(
            f"✅ <b>Unbanned</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"👤 User: {target.mention_html(tname)}\n"
            f"👮 By:   {aname}\n\n"
            f"<i>They can now rejoin the group.</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await msg.reply_text(f"❌ Unban failed: {html.escape(str(e)[:100])}")

# ─── LEFT MEMBER message delete ───────────────────────────────────────────────
async def on_left_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete the 'X left the chat' service message."""
    msg  = update.effective_message
    chat = update.effective_chat
    if not msg or not chat:
        return
    if chat.type not in {"group", "supergroup"}:
        return
    row = get_group(chat.id)
    if not row:
        return
    # Only delete if delete_service is ON
    if int(row["delete_service"] or 1) == 1:
        try:
            await msg.delete()
        except Exception:
            pass

async def _ultra_message_then_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Combined handler: msg-limit check → ultra features → keyword replies."""
    if await check_msg_length_limit(update, context):
        return
    await handle_ultra_message(update, context)
    await on_keyword_message(update, context)


# ─── post_init & build_app ────────────────────────────────────────────────────
async def post_init(application):
    delete_webhook()
    commands = [
        BotCommand("start",              "✨ Bot info & commands"),
        BotCommand("ping",               "🏓 Bot status check"),
        BotCommand("myid",               "🪪 Your Telegram ID"),
        BotCommand("support",            "💬 Support group"),
        BotCommand("profile",            "👤 Your group profile"),
        BotCommand("top",                "🏅 Most active members"),
        BotCommand("leaderboard",        "🏆 Game leaderboard"),
        BotCommand("rules",              "📋 View group rules"),
        BotCommand("status",             "⚙️ Bot settings overview"),
        BotCommand("analytics",          "📊 Group statistics"),
        BotCommand("groupstats",         "📈 Group activity"),
        BotCommand("ask",                "🧠 Ask Maya AI"),
        BotCommand("tr",                 "🌐 Translate BN↔EN"),
        BotCommand("setrules",           "📋 Set group rules"),
        BotCommand("warn",               "⚠️ Warn a member"),
        BotCommand("unwarn",             "✅ Clear member warns"),
        BotCommand("warns",              "⚠️ Check warn history"),
        BotCommand("ban",                "🚫 Ban a member"),
        BotCommand("unban",              "✅ Unban a member"),
        BotCommand("setreply",           "🤖 Add auto-reply rule"),
        BotCommand("listreplies",        "📄 List auto-reply rules"),
        BotCommand("delreply",           "🗑 Delete auto-reply rule"),
        BotCommand("linkguard",          "🛡️ Link protection on/off"),
        BotCommand("setforward",         "📢 Set forward button link"),
        BotCommand("forwardon",          "▶️ Start forward button"),
        BotCommand("forwardoff",         "⏹ Stop forward button"),
        BotCommand("setforwardinterval", "⏰ Set forward interval"),
        BotCommand("lang",               "🌐 Language: bn or en"),
        BotCommand("voice",              "🎙 Welcome voice: on/off"),
        BotCommand("hourly",             "📨 Hourly messages: on/off/now"),
        BotCommand("deleteservice",      "🗑 Delete service msgs: on/off"),
        BotCommand("setwelcome",         "✏️ Custom welcome text"),
        BotCommand("resetwelcome",       "↩️ Reset welcome to default"),
        BotCommand("welcomestyle",       "🎨 Welcome banner theme"),
        BotCommand("setfooter",          "📝 Welcome footer text"),
        BotCommand("setvoice",           "🎙 Bengali voice: bd or in"),
        BotCommand("festivalmode",       "🎉 Festival mode: on/off"),
        BotCommand("keywordmode",        "💬 Keyword replies: on/off"),
        BotCommand("setmsglimit",        "📏 Message length limit"),
        BotCommand("hourlyclean",        "⏰ Auto-delete hourly msgs"),
        BotCommand("setcountdown",       "⏳ Set event countdown"),
        BotCommand("countdown",          "📅 Show countdown card"),
        BotCommand("clearcountdown",     "❌ Clear countdown"),
        BotCommand("setexamday",         "📘 Set exam day reminder"),
        BotCommand("aistatus",           "🤖 AI engine status (owner)"),
        BotCommand("testwelcome",        "🧪 Test welcome message"),
        BotCommand("groupbrowser",       "🗂 Browse groups (owner)"),
        BotCommand("broadcastone",       "📢 Message one group (owner)"),
        BotCommand("groupcount",         "🔢 Count groups (owner)"),
        BotCommand("activegroups",       "📋 Active groups (owner)"),
        BotCommand("broadcast",          "📣 Broadcast (owner)"),
        BotCommand("rps",                "🎮 Rock Paper Scissors"),
        BotCommand("xo",                 "⭕ X-O / Tic-Tac-Toe"),
        BotCommand("luckybox",           "🎁 Lucky Box game"),
        BotCommand("tod",                "🎭 Truth or Dare"),
    ]
    for scope in [BotCommandScopeDefault(), BotCommandScopeAllPrivateChats(),
                  BotCommandScopeAllGroupChats(), BotCommandScopeAllChatAdministrators()]:
        try:
            await application.bot.set_my_commands(commands, scope=scope)
        except Exception:
            logger.exception("Failed to set commands for scope: %s", scope)

    # Restore active forward loops
    try:
        await restore_forward_tasks(application.bot)
    except Exception:
        logger.exception("Failed to restore forward tasks")

    logger.info("🌸 Maya Ultra v10 — ready")

def build_app():
    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # Info
    application.add_handler(CommandHandler("start",              on_start))
    application.add_handler(CommandHandler("support",            on_support))
    application.add_handler(CommandHandler("ping",               on_ping))
    application.add_handler(CommandHandler("myid",               on_myid))
    application.add_handler(CommandHandler("aistatus",           on_ai_status))

    # Profile & stats
    application.add_handler(CommandHandler("profile",            on_profile))
    application.add_handler(CommandHandler("top",                on_top))
    application.add_handler(CommandHandler("leaderboard",        on_leaderboard))

    # Group info
    application.add_handler(CommandHandler("rules",              on_rules))
    application.add_handler(CommandHandler("status",             on_status))
    application.add_handler(CommandHandler("analytics",          on_analytics))
    application.add_handler(CommandHandler("groupstats",         on_groupstats))

    # AI tools
    application.add_handler(CommandHandler("ask",                on_ask))
    application.add_handler(CommandHandler("tr",                 on_translate))
    application.add_handler(CommandHandler("translate",          on_translate))

    # Moderation
    application.add_handler(CommandHandler("warn",               on_warn))
    application.add_handler(CommandHandler("unwarn",             on_unwarn))
    application.add_handler(CommandHandler("warns",              on_warns))
    application.add_handler(CommandHandler("ban",                on_ban))
    application.add_handler(CommandHandler("unban",              on_unban))
    application.add_handler(CommandHandler("setrules",           on_setrules))
    application.add_handler(CommandHandler("setreply",           on_setreply))
    application.add_handler(CommandHandler("listreplies",        on_listreplies))
    application.add_handler(CommandHandler("delreply",           on_delreply))
    application.add_handler(CommandHandler("linkguard",          on_linkguard))

    # Forward button system
    application.add_handler(CommandHandler("setforward",         on_setforward))
    application.add_handler(CommandHandler("forwardon",          on_forwardon))
    application.add_handler(CommandHandler("forwardoff",         on_forwardoff))
    application.add_handler(CommandHandler("setforwardinterval", on_setforwardinterval))

    # Group admin settings
    application.add_handler(CommandHandler("setvoice",           on_setvoice))
    application.add_handler(CommandHandler("welcomestyle",       on_welcomestyle))
    application.add_handler(CommandHandler("setfooter",          on_setfooter))
    application.add_handler(CommandHandler("lang",               on_lang))
    application.add_handler(CommandHandler("voice",              on_voice))
    application.add_handler(CommandHandler("deleteservice",      on_delete_service))
    application.add_handler(CommandHandler("hourly",             on_hourly))
    application.add_handler(CommandHandler("setwelcome",         on_setwelcome))
    application.add_handler(CommandHandler("resetwelcome",       on_resetwelcome))
    application.add_handler(CommandHandler("hourlyclean",        on_hourlyclean))
    application.add_handler(CommandHandler("setcountdown",       on_setcountdown))
    application.add_handler(CommandHandler("countdown",          on_showcountdown))
    application.add_handler(CommandHandler("clearcountdown",     on_clearcountdown))
    application.add_handler(CommandHandler("setexamday",         on_setexamday))
    application.add_handler(CommandHandler("examday",            on_examday))
    application.add_handler(CommandHandler("clearexamday",       on_clearexamday))
    application.add_handler(CommandHandler("festivalmode",       on_festivalmode))
    application.add_handler(CommandHandler("keywordmode",        on_keywordmode))
    application.add_handler(CommandHandler("setmsglimit",        on_setmsglimit))
    application.add_handler(CommandHandler("testwelcome",        on_testwelcome))

    # Owner
    application.add_handler(CommandHandler("groupcount",         on_groupcount))
    application.add_handler(CommandHandler("activegroups",       on_activegroups))
    application.add_handler(CommandHandler("failedgroups",       on_failedgroups))
    application.add_handler(CommandHandler("lastaierrors",       on_lastaierrors))
    application.add_handler(CommandHandler("broadcastphoto",     on_broadcast))
    application.add_handler(CommandHandler("broadcastvoice",     on_broadcast))
    application.add_handler(CommandHandler("broadcast",          on_broadcast))
    application.add_handler(CommandHandler("groupbrowser",       on_groupbrowser))
    application.add_handler(CommandHandler("broadcastone",       on_broadcastone))

    # Games
    application.add_handler(CommandHandler("rps",                on_rps))
    application.add_handler(CommandHandler("xo",                 on_xo))
    application.add_handler(CommandHandler("luckybox",           on_luckybox))
    application.add_handler(CommandHandler("tod",                on_tod))

    # Callbacks
    application.add_handler(CallbackQueryHandler(on_groupbrowser_callback, pattern=r"^gb\|"))
    application.add_handler(CallbackQueryHandler(on_xo_callback,           pattern=r"^xo\|"))
    application.add_handler(CallbackQueryHandler(on_rps_callback,          pattern=r"^rps\|"))
    application.add_handler(CallbackQueryHandler(on_luckybox_callback,     pattern=r"^lb\|"))
    application.add_handler(CallbackQueryHandler(on_tod_callback,          pattern=r"^tod\|"))

    # Message handlers (ORDER MATTERS)
    application.add_handler(MessageHandler(
        filters.StatusUpdate.NEW_CHAT_MEMBERS,
        on_new_chat_members))
    application.add_handler(MessageHandler(
        filters.StatusUpdate.LEFT_CHAT_MEMBER,
        on_left_chat_member))
    application.add_handler(ChatMemberHandler(
        on_chat_member,
        ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(MessageHandler(
        filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
        _ultra_message_then_keyword))
    application.add_handler(MessageHandler(
        filters.ChatType.GROUPS & ~filters.COMMAND,
        track_group))
    return application

def main():
    init_db()
    init_games_db()
    init_extra_games_db()
    init_ultra_db()
    init_forward_db()
    threading.Thread(target=run_flask,       daemon=True).start()
    threading.Thread(target=hourly_loop,     daemon=True).start()
    threading.Thread(target=cleanup_loop,    daemon=True).start()
    threading.Thread(target=self_ping_loop,  daemon=True).start()
    logger.info("🌸 Maya Ultra v10 — starting on port %s", PORT)
    # Log service info
    svc_name = os.environ.get("RENDER_SERVICE_NAME", BOT_NAME)
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if render_url:
        logger.info("🚀 Service: %s", svc_name)
        logger.info("🌐 URL: %s", render_url)
    build_app().run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
        close_loop=False,
    )

if __name__ == "__main__":
    main()
