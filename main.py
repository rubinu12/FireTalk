# FireTalk Bot - The Final, Feature-Complete Version

import logging
import aiosqlite
import json
import asyncio
import time
import random
import secrets
import os
import asyncpg

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    filters,
)
from telegram.error import TelegramError, BadRequest


# --- ⚙️ Configuration & Setup ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_USERNAME = "@FireTalkOfficial"
ADMIN_USER_IDS = [1295160259] # Add your admin User ID(s) here

# Timers
AD_BREAK_DURATION = 10 
CHAT_LOCK_DURATION = 90 # 3 minutes
MATCH_FALLBACK_TIMEOUT = 30 # 30 seconds
MIN_CHAT_DURATION_FOR_FAVORITE = 30 # 1 minute for testing, change to 600 for production

# --- NEW & FINALIZED KEYBOARDS ---
MAIN_MENU_KEYBOARD_BASIC = InlineKeyboardMarkup([
    [InlineKeyboardButton("🚀 Find a Stranger", callback_data="find_stranger")],
    [InlineKeyboardButton("🤝 Invite a Friend", callback_data="invite_friend")],
    [InlineKeyboardButton("⚙️ My Profile & Settings", callback_data="my_profile")]
])
MAIN_MENU_KEYBOARD_PREMIUM = InlineKeyboardMarkup([
    [InlineKeyboardButton("🚀 Find a Stranger", callback_data="find_stranger")],
    [InlineKeyboardButton("🤝 Invite a Friend", callback_data="invite_friend")],
    [InlineKeyboardButton("⚙️ My Profile & Settings", callback_data="my_profile")],
    [InlineKeyboardButton("❤️ My Favorites", callback_data="my_connections")]
])
PROFILE_SETTINGS_KEYBOARD = InlineKeyboardMarkup([
    [InlineKeyboardButton("✏️ Change Intent & Kinks", callback_data="change_intent_kinks")],
    [InlineKeyboardButton("👤 Go Anonymous (Quick Chat)", callback_data="go_anonymous")],
    [InlineKeyboardButton("🗑️ Reset Full Profile", callback_data="reset_profile")],
    [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
])
CHAT_REPLY_KEYBOARD = ReplyKeyboardMarkup([["➡️ Next", "🛑 Stop"]], resize_keyboard=True)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- NEW & FINALIZED STATES ---
AGREE_TERMS, NAME, LANGUAGES, GENDER, AGE, INTENT, KINKS, PREF_GENDER, PREF_LANGUAGE = range(9)

# --- NEW & FINALIZED PROFILE OPTIONS ---
AVAILABLE_LANGUAGES = ["English", "Spanish", "Hindi", "French", "German", "Russian"]
AVAILABLE_INTENTS = ["💬 Casual Talk", "😏 Flirting", "🔥 Sexting", "🎭 Roleplay", "😈 Truth or Dare", "📸 Pic Trading", "🎬 GIF War", "🤫 Anything Goes"]
AVAILABLE_KINKS = ["Dominance", "Submission", "Switch", "Gentle", "Rough", "Romantic", "Verbal", "Role Scenarios", "Power Play", "Fantasy", "Slow Burn", "Direct"]

# Concurrency Locks
DB_LOCK = asyncio.Lock()
MATCH_LOCK = asyncio.Lock()


# --- 🗄️ Database Functions (PostgreSQL Version) ---

# Global variable to hold the database connection pool
POOL = None

async def initialize_db(application: Application):
    """Connects to the PostgreSQL database and creates tables if they don't exist."""
    global POOL
    try:
        DATABASE_URL = os.environ.get("DATABASE_URL")
        if not DATABASE_URL:
            logger.error("DATABASE_URL environment variable not set!")
            return
        
        POOL = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database connection pool created.")

        async with POOL.acquire() as connection:
            # PostgreSQL uses SERIAL PRIMARY KEY instead of INTEGER PRIMARY KEY AUTOINCREMENT
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    gender TEXT,
                    age INTEGER,
                    languages TEXT,
                    interests TEXT,
                    is_premium INTEGER DEFAULT 0,
                    intent TEXT,
                    kinks TEXT,
                    show_active_status INTEGER DEFAULT 1
                )
            """)
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    user_id BIGINT PRIMARY KEY,
                    state TEXT DEFAULT 'idle',
                    partner_id BIGINT,
                    searching_message_id BIGINT,
                    pinned_message_id BIGINT,
                    chat_start_time REAL,
                    last_chat_id INTEGER,
                    search_prefs TEXT,
                    original_search_prefs TEXT
                )
            """)
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS chat_history (
                    chat_id SERIAL PRIMARY KEY,
                    user1_id BIGINT,
                    user2_id BIGINT,
                    start_time REAL,
                    end_time REAL,
                    user1_wants_favorite INTEGER DEFAULT 0,
                    user2_wants_favorite INTEGER DEFAULT 0,
                    user1_vibe_tag TEXT,
                    user2_vibe_tag TEXT
                )
            """)
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS connections (
                    connection_id SERIAL PRIMARY KEY,
                    user1_id BIGINT,
                    user2_id BIGINT,
                    user1_snapshot TEXT,
                    user2_snapshot TEXT,
                    timestamp REAL,
                    UNIQUE(user1_id, user2_id)
                )
            """)
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS message_map (
                    chat_id INTEGER, original_user_id BIGINT, original_msg_id BIGINT,
                    forwarded_user_id BIGINT, forwarded_msg_id BIGINT,
                    PRIMARY KEY (forwarded_user_id, forwarded_msg_id)
                )
            """)
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS invites (
                    invite_token TEXT PRIMARY KEY,
                    host_user_id BIGINT NOT NULL,
                    creation_time REAL NOT NULL
                )
            """)
        logger.info("Database tables initialized.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

async def close_db(application: Application):
    """Closes the database connection pool."""
    if POOL:
        await POOL.close()
        logger.info("Database connection pool closed.")

async def get_user_data(user_id):
    """Fetches combined profile and session data for a user."""
    async with POOL.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users LEFT JOIN sessions USING(user_id) WHERE user_id = $1", user_id)
        return dict(row) if row else None

async def update_user_data(user_id, data):
    """Saves or updates a user's profile and/or session data."""
    user_cols = {"name", "gender", "age", "languages", "interests", "is_premium", "intent", "kinks", "show_active_status"}
    session_cols = {"state", "partner_id", "searching_message_id", "pinned_message_id", "chat_start_time", "last_chat_id", "search_prefs", "original_search_prefs"}
    
    user_data_to_update = {k: v for k, v in data.items() if k in user_cols}
    session_data_to_update = {k: v for k, v in data.items() if k in session_cols}

    async with POOL.acquire() as conn:
        # PostgreSQL uses ON CONFLICT ... DO NOTHING instead of INSERT OR IGNORE
        await conn.execute("INSERT INTO users (user_id, name) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING", user_id, "Stranger")
        await conn.execute("INSERT INTO sessions (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", user_id)
        
        if user_data_to_update:
            # PostgreSQL uses $1, $2, etc. for placeholders
            set_clause = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(user_data_to_update)])
            values = list(user_data_to_update.values())
            values.append(user_id)
            await conn.execute(f"UPDATE users SET {set_clause} WHERE user_id = ${len(values)}", *values)

        if session_data_to_update:
            set_clause = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(session_data_to_update)])
            values = list(session_data_to_update.values())
            values.append(user_id)
            await conn.execute(f"UPDATE sessions SET {set_clause} WHERE user_id = ${len(values)}", *values)

async def delete_user_profile(user_id):
    """Resets a user's profile but keeps premium status and connections."""
    async with POOL.acquire() as conn:
        await conn.execute(
            "UPDATE users SET name='Anonymous', gender=NULL, age=NULL, languages=NULL, interests=NULL, intent=NULL, kinks=NULL WHERE user_id = $1", 
            user_id
        )
    logger.info(f"Reset profile for user {user_id}. Connections and premium status were preserved.")

async def get_waiting_pool():
    """Gets all users currently in the 'waiting' state from the database."""
    async with POOL.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users u JOIN sessions s ON u.user_id = s.user_id WHERE s.state = 'waiting'")
        return [dict(row) for row in rows]

async def map_message(chat_id, original_user_id, original_msg_id, forwarded_user_id, forwarded_msg_id):
    """Stores a robust, two-way mapping between an original message and its forwarded version."""
    async with POOL.acquire() as conn:
        await conn.execute(
            """INSERT INTO message_map (chat_id, original_user_id, original_msg_id, forwarded_user_id, forwarded_msg_id) 
               VALUES ($1, $2, $3, $4, $5) ON CONFLICT (forwarded_user_id, forwarded_msg_id) DO NOTHING""",
            chat_id, original_user_id, original_msg_id, forwarded_user_id, forwarded_msg_id
        )

async def get_mapped_message_id(chat_id, user_id_replying, replied_to_msg_id):
    """Finds the original message ID that a user's reply corresponds to in the partner's chat."""
    async with POOL.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT original_msg_id FROM message_map WHERE chat_id = $1 AND forwarded_user_id = $2 AND forwarded_msg_id = $3",
            chat_id, user_id_replying, replied_to_msg_id
        )
        return row['original_msg_id'] if row else None

async def clear_chat_maps(chat_id):
    if not chat_id: return
    async with POOL.acquire() as conn:
        await conn.execute("DELETE FROM message_map WHERE chat_id = $1", chat_id)

async def is_premium(user_id: int) -> bool:
    data = await get_user_data(user_id)
    return data is not None and data.get("is_premium") == 1


async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 5):
    """Schedules a job to delete a message after a specified delay in seconds."""
    
    async def delete_job(ctx: ContextTypes.DEFAULT_TYPE):
        try:
            await ctx.bot.delete_message(chat_id=ctx.job.chat_id, message_id=ctx.job.data)
        except (TelegramError, BadRequest):
            pass # Ignore if message is already gone

    context.job_queue.run_once(delete_job, delay, data=message_id, chat_id=chat_id)
# --- 👋 Onboarding & Profile Setup ---

async def handle_invite_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Checks if a user is joining via a valid invite link and connects them."""
    if not context.args:
        return False

    token = context.args[0]
    guest_user_id = update.effective_user.id
    
    async with POOL.acquire() as conn:
        invite = await conn.fetchrow("SELECT * FROM invites WHERE invite_token = $1", token)
        if not invite:
            await update.message.reply_text("This invite link is invalid or has already been used.")
            return True

        if time.time() - invite['creation_time'] > 300:
            await update.message.reply_text("This invite link has expired.")
            await conn.execute("DELETE FROM invites WHERE invite_token = $1", token)
            return True
        
        host_user_id = invite['host_user_id']
        if host_user_id == guest_user_id:
            await update.message.reply_text("You cannot use your own invite link.")
            return True

        host_data = await get_user_data(host_user_id)
        if not host_data or host_data.get("state") != "hosting":
            await update.message.reply_text("The user who invited you is no longer waiting. The invite has been cancelled.")
            await conn.execute("DELETE FROM invites WHERE invite_token = $1", token)
            return True
        
        await context.bot.send_message(host_user_id, "✅ Your friend has joined! Connecting you now...")
        await update.message.reply_text("✅ Invite accepted! Connecting you now...")
        
        await conn.execute("DELETE FROM invites WHERE invite_token = $1", token)
        
        await match_users(context, host_user_id, guest_user_id)
        return True

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles new users, returning users, and users joining from an invite link."""
    user = update.effective_user
    message = update.message or update.callback_query.message
    
    # --- NEW: Check if the user is joining via an invite link ---
    if await handle_invite_join(update, context):
        return ConversationHandler.END # Stop the conversation if it was an invite

    # --- The rest of the function remains the same ---
    # Check for channel membership first
    try:
        member = await context.bot.get_chat_member(chat_id=CHANNEL_USERNAME, user_id=user.id)
        if member.status not in ["member", "administrator", "creator"]: raise Exception("User not a member")
    except Exception:
        url = f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}"
        keyboard = [[InlineKeyboardButton("👉 Join Our Channel", url=url)]]
        await message.reply_text(f"👋 Welcome to FireTalk! 🔥\n\nPlease join our channel and then press /start again.", reply_markup=InlineKeyboardMarkup(keyboard))
        return ConversationHandler.END

    # Check if user already has a full profile
    profile = await get_user_data(user.id)
    if profile and profile.get('name') and profile.get('name') != 'Anonymous':
        keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user.id) else MAIN_MENU_KEYBOARD_BASIC
        await message.reply_text(f"👋 Welcome back, {profile['name']}! Ready to chat?", reply_markup=keyboard)
        return ConversationHandler.END

    # Start of the onboarding flow with Disclaimer
    context.user_data['profile'] = {}
    disclaimer_text = (
        "⚠️ **Important Rules & Agreement** ⚠️\n\n"
        "This platform is for consensual, adult fantasy chat. The following are strictly forbidden:\n"
        "• Any content involving minors.\n"
        "• Depictions or encouragement of non-consensual acts.\n"
        "• The trade or discussion of illegal drugs or weapons.\n\n"
        "By continuing, you agree that you are **18 years or older** and that you will not engage in any illegal activity. "
        "Users reported for violating these rules will be **permanently banned**, and we will cooperate with law enforcement."
    )
    keyboard = [[InlineKeyboardButton("I Agree & Continue", callback_data="agree_terms")]]
    await message.reply_text(disclaimer_text, reply_markup=InlineKeyboardMarkup(keyboard))
    return AGREE_TERMS

async def ask_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    # --- NEW: Add "Skip All" button ---
    keyboard = [
        [InlineKeyboardButton("⚡ Skip All & Chat Now", callback_data="skip_all_setup")],
        [InlineKeyboardButton("⏩ Skip Name", callback_data="skip_name")]
    ]
    await query.edit_message_text(
        "💬 Let's create your profile.\n\nFirst, what should we call you?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return NAME

async def save_default_profile_and_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    
    default_profile = {
        "name": "Stranger",
        "gender": None,
        "age": None,
        "intent": "🤫 Anything Goes",
        "kinks": json.dumps([])
    }
    await update_user_data(user_id, default_profile)
    
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    await query.edit_message_text("✅ Profile skipped! You're all set with an anonymous profile.", reply_markup=keyboard)
    return ConversationHandler.END
    
async def handle_name_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data['profile']['name'] = update.message.text
    return await ask_gender(update, context)

async def skip_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    context.user_data['profile']['name'] = "Stranger"
    return await ask_gender(update, context, is_callback=True)

async def ask_gender(update: Update, context: ContextTypes.DEFAULT_TYPE, is_callback=False) -> int:
    keyboard = [[InlineKeyboardButton("👨 Male", callback_data="gender_Male"), InlineKeyboardButton("👩 Female", callback_data="gender_Female")], [InlineKeyboardButton("⏩ Skip", callback_data="skip_gender")]]
    text = "👍 Next, please select your gender."
    message = update.message or update.callback_query.message
    if is_callback: await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else: await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard)) # This will happen after name input
    return GENDER

async def ask_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    if not query.data.startswith("skip"): context.user_data['profile']['gender'] = query.data.split("_")[1]
    text = "🎂 And what is your age?"; keyboard = [[InlineKeyboardButton("⏩ Skip", callback_data="skip_age")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return AGE

async def ask_languages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.message or update.callback_query.message
    if update.callback_query and update.callback_query.data == 'skip_age':
        await update.callback_query.answer()
        context.user_data['profile']['age'] = None
    elif update.message:
        age = update.message.text
        if not age.isdigit() or not (13 <= int(age) <= 99):
            await message.reply_text("Please enter a valid age between 13 and 99."); return AGE
        context.user_data['profile']['age'] = int(age)

    context.user_data['selected_languages'] = set()
    text = "🗣️ What languages do you speak? (Select as many as you like)"
    keyboard = build_multi_select_keyboard(AVAILABLE_LANGUAGES, set(), "lang")

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=keyboard)
    else:
        await message.reply_text(text, reply_markup=keyboard)
    return LANGUAGES

async def handle_language_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    lang = query.data.split("_")[1]
    selected = context.user_data.get('selected_languages', set())
    if lang in selected:
        selected.remove(lang)
    else:
        selected.add(lang)
    context.user_data['selected_languages'] = selected
    keyboard = build_multi_select_keyboard(AVAILABLE_LANGUAGES, selected, "lang")
    await query.edit_message_reply_markup(reply_markup=keyboard)
    return LANGUAGES

async def ask_intent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    # --- THIS IS THE FIX: Initialize the profile dictionary ---
    if 'profile' not in context.user_data:
        context.user_data['profile'] = {}
    
    message = update.message or update.callback_query.message
    if update.callback_query and update.callback_query.data == 'skip_age':
        await update.callback_query.answer()
        context.user_data['profile']['age'] = None
    elif update.message:
        age = update.message.text
        if not age.isdigit() or not (13 <= int(age) <= 99):
            await message.reply_text("Please enter a valid age between 13 and 99."); return AGE
        context.user_data['profile']['age'] = int(age)

    keyboard = [[InlineKeyboardButton(intent, callback_data=f"intent_{intent}")] for intent in AVAILABLE_INTENTS]
    keyboard.append([InlineKeyboardButton("⏩ Skip", callback_data="skip_intent")])
    text = "🎯 Please select your new intent."

    if update.callback_query:
        # If we are just changing intent, edit the previous menu to show this new question.
        await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    return INTENT

async def skip_intent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles skipping the intent selection, defaulting to Casual Talk."""
    query = update.callback_query
    context.user_data['profile']['intent'] = "💬 Casual Talk"
    return await ask_kinks(update, context, is_skip=True)

async def ask_kinks(update: Update, context: ContextTypes.DEFAULT_TYPE, is_skip=False) -> int:
    query = update.callback_query
    await query.answer()
    if not is_skip:
        intent = query.data.split("_", 1)[1]
        context.user_data['profile']['intent'] = intent

    context.user_data['selected_kinks'] = set()
    text = "🎭 Finally, select a few tags that match your style (optional, up to 3)."
    keyboard = build_multi_select_keyboard(AVAILABLE_KINKS, set(), "kink")
    await query.edit_message_text(text, reply_markup=keyboard)
    return KINKS

def build_multi_select_keyboard(options, selected, prefix):
    buttons = [InlineKeyboardButton(f"✅ {opt}" if opt in selected else opt, callback_data=f"{prefix}_{opt}") for opt in options]
    keyboard = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    keyboard.append([InlineKeyboardButton("Done ✔️", callback_data=f"done_{prefix}")])
    return InlineKeyboardMarkup(keyboard)

async def handle_kink_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    kink = query.data.split("_")[1]
    selected = context.user_data.get('selected_kinks', set())
    if kink in selected:
        selected.remove(kink)
    elif len(selected) < 3:
        selected.add(kink)
    else:
        await query.answer("You can only select up to 3 tags.", show_alert=True)
    context.user_data['selected_kinks'] = selected
    keyboard = build_multi_select_keyboard(AVAILABLE_KINKS, selected, "kink")
    await query.edit_message_reply_markup(reply_markup=keyboard)
    return KINKS

async def profile_complete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    
    # Save languages
    selected_languages = context.user_data.get('selected_languages', set())
    context.user_data['profile']['languages'] = json.dumps(list(selected_languages))
    
    # Save kinks
    selected_kinks = context.user_data.get('selected_kinks', set())
    context.user_data['profile']['kinks'] = json.dumps(list(selected_kinks))
    
    await update_user_data(user_id, context.user_data['profile'])
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    await query.edit_message_text(text=f"🎉 Your profile is all set!", reply_markup=keyboard)

    context.user_data.clear()
    return ConversationHandler.END

# --- NEW Profile Management & Settings Handlers ---
async def my_profile_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the 'My Profile & Settings' menu with a dynamic active status button."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    
    # Dynamically create the settings keyboard
    keyboard_buttons = []
    if await is_premium(user_id):
        user_data = await get_user_data(user_id)
        status = user_data.get("show_active_status", 1)
        status_text = "🟢 Show My Status: ON" if status == 1 else "🔴 Show My Status: OFF"
        keyboard_buttons.append([InlineKeyboardButton(status_text, callback_data="toggle_status")])

    keyboard_buttons.extend([
        [InlineKeyboardButton("✏️ Change Intent & Kinks", callback_data="change_intent_kinks")],
        [InlineKeyboardButton("👤 Go Anonymous (Quick Chat)", callback_data="go_anonymous")],
        [InlineKeyboardButton("🗑️ Reset Full Profile", callback_data="reset_profile")],
        [InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")]
    ])
    
    await query.edit_message_text(
        "⚙️ **My Profile & Settings**\n\nManage your profile and chat settings here.", 
        reply_markup=InlineKeyboardMarkup(keyboard_buttons), 
        parse_mode='Markdown'
    )

async def toggle_active_status_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggles the user's show_active_status setting."""
    query = update.callback_query
    user_id = update.effective_user.id

    if not await is_premium(user_id):
        await query.answer("This is a Premium feature!", show_alert=True)
        return
        
    user_data = await get_user_data(user_id)
    current_status = user_data.get("show_active_status", 1)
    new_status = 0 if current_status == 1 else 1
    
    await update_user_data(user_id, {"show_active_status": new_status})
    await query.answer(f"Your active status is now set to {'ON' if new_status == 1 else 'OFF'}.")
    
    # Refresh the menu to show the change instantly
    await my_profile_menu(update, context)

async def go_anonymous(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the 'Go Anonymous' button."""
    query = update.callback_query
    await query.answer("Resetting to Anonymous...")
    user_id = update.effective_user.id
    default_profile = {
        "name": "Stranger", "gender": None, "age": None,
        "intent": "🤫 Anything Goes", "kinks": json.dumps([])
    }
    await update_user_data(user_id, default_profile)
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    await query.edit_message_text("✅ You are now in anonymous mode. Ready to chat!", reply_markup=keyboard)


async def create_invite_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generates a unique, one-time-use invite link for the user to share."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id

    # Generate a secure, random token for the link
    token = secrets.token_urlsafe(16)
    
    # --- THIS IS THE CHANGE ---
    # Store the invite in the PostgreSQL database using the connection pool
    async with POOL.acquire() as conn:
        # PostgreSQL uses $1, $2, $3 for placeholders instead of ?
        await conn.execute(
            "INSERT INTO invites (invite_token, host_user_id, creation_time) VALUES ($1, $2, $3)",
            token, user_id, time.time()
        )
    # --------------------------

    # Get the bot's username to build the link
    bot_username = (await context.bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={token}"
    
    await update_user_data(user_id, {"state": "hosting"})
    
    keyboard = [[InlineKeyboardButton("❌ Cancel Invite", callback_data=f"cancel_invite_{token}")]]
    await query.edit_message_text(
        "Your private invite link is ready.\n\n"
        f"🔗 **Share this link with your friend:**\n`{link}`\n\n"
        "This link is for one person and will expire in 5 minutes. I will connect you as soon as they click it.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def cancel_invite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancels a pending invite."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    token = query.data.split("_")[2]

    # --- THIS IS THE CHANGE ---
    # Connect to the PostgreSQL pool and use the correct placeholder syntax
    async with POOL.acquire() as conn:
        await conn.execute(
            "DELETE FROM invites WHERE invite_token = $1 AND host_user_id = $2", 
            token, user_id
        )
    # --------------------------
    
    await update_user_data(user_id, {"state": "idle"})
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    await query.edit_message_text("✅ Invite cancelled. You are back to the main menu.", reply_markup=keyboard)

async def change_intent_kinks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the mini-conversation to change only intent and kinks."""
    await update.callback_query.answer()
    # This now properly calls the entry point of our mini-conversation
    return await ask_intent(update, context)

async def reset_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handler for the 'Reset Full Profile' button that correctly starts the setup conversation."""
    query = update.callback_query
    user_id = update.effective_user.id
    
    # --- THIS IS THE FIX ---
    # Step 1: Delete the old profile from the database first.
    await delete_user_profile(user_id)
    context.user_data.clear()
    
    # Step 2: Now that the profile is gone, start the conversation from the beginning.
    # This calls the start_command, which will now see you as a new user.
    return await start_command(update, context)


async def main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Returns the user to the main menu from a sub-menu."""
    query = update.callback_query
    await query.answer()
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(update.effective_user.id) else MAIN_MENU_KEYBOARD_BASIC
    await query.edit_message_text(f"👋 Welcome back! Ready to chat?", reply_markup=keyboard)

# --- 🤝 Matching & Chatting Logic ---
def check_mutual_match(user1, user2):
    """
    Checks for a mutual match, prioritizing Intent, then all premium preferences.
    """
    try:
        intent1 = user1.get("intent")
        intent2 = user2.get("intent")
        if intent1 and intent2 and intent1 != "🤫 Anything Goes" and intent2 != "🤫 Anything Goes" and intent1 != intent2:
            return False

        prefs1 = json.loads(user1.get("search_prefs") or '{}')
        prefs2 = json.loads(user2.get("search_prefs") or '{}')
        gender_pref1 = prefs1.get("gender", "Any")
        lang_pref1 = prefs1.get("language", "Any")
        gender_pref2 = prefs2.get("gender", "Any")
        lang_pref2 = prefs2.get("language", "Any")

        languages1 = json.loads(user1.get("languages") or '[]')
        languages2 = json.loads(user2.get("languages") or '[]')
        gender1 = user1.get("gender")
        gender2 = user2.get("gender")

        # Check user1's prefs against user2's profile
        if gender_pref1 != "Any" and gender_pref1 != gender2: return False
        if lang_pref1 != "Any" and (not languages2 or lang_pref1 not in languages2): return False
            
        # Check user2's prefs against user1's profile
        if gender_pref2 != "Any" and gender_pref2 != gender1: return False
        if lang_pref2 != "Any" and (not languages1 or lang_pref2 not in languages1): return False
            
        logger.info(f"Mutual match check PASSED for {user1['user_id']} and {user2['user_id']}")
        return True
    except Exception as e:
        logger.error(f"Error during match check: {e}")
        return False
    
async def find_stranger_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    if await is_premium(user_id):
        keyboard = [[InlineKeyboardButton("👨 Male", callback_data="pref_gender_Male")],[InlineKeyboardButton("👩 Female", callback_data="pref_gender_Female")],[InlineKeyboardButton("👤 Anyone", callback_data="pref_gender_Any")],]
        await query.edit_message_text("🎯 Who would you like to talk to?", reply_markup=InlineKeyboardMarkup(keyboard))
        return PREF_GENDER
    else:
        await query.edit_message_text("⏳ Searching for a partner...")
        await add_to_pool_and_match(context, user_id)
        return ConversationHandler.END

async def pref_gender_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    context.user_data['pref_gender'] = query.data.split("_")[2]
    keyboard = [[InlineKeyboardButton("🇬🇧 English", callback_data="pref_lang_English")],[InlineKeyboardButton("🇪🇸 Spanish", callback_data="pref_lang_Spanish")],[InlineKeyboardButton("🌐 Any Language", callback_data="pref_lang_Any")],]
    await query.edit_message_text("🗣️ In what language?", reply_markup=InlineKeyboardMarkup(keyboard))
    return PREF_LANGUAGE

async def pref_language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    prefs = {"gender": context.user_data.get('pref_gender', 'Any'), "language": query.data.split("_")[2]}
    await update_user_data(user_id, {"search_prefs": json.dumps(prefs), "original_search_prefs": json.dumps(prefs)})
    await query.edit_message_text("✅ Preferences saved! Searching for your partner...")
    await add_to_pool_and_match(context, user_id)
    if 'pref_gender' in context.user_data: del context.user_data['pref_gender']
    return ConversationHandler.END

async def add_to_pool_and_match(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """
    Adds a user to the waiting pool and triggers matching.
    Starts the new unified fallback timer if specific criteria are set.
    """
    user_data = await get_user_data(user_id)

    if user_data and user_data.get("searching_message_id"):
        try:
            await context.bot.delete_message(chat_id=user_id, message_id=user_data["searching_message_id"])
        except (TelegramError, BadRequest): pass

    cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Search", callback_data="cancel_search")]])
    searching_msg = await context.bot.send_message(chat_id=user_id, text="⏳ Searching for a partner... Please wait!", reply_markup=cancel_keyboard)
    await update_user_data(user_id, {"state": "waiting", "searching_message_id": searching_msg.message_id})

    logger.info(f"User {user_id} has entered the waiting pool.")

    # --- Schedule the single Unified Fallback Timer ---
    prefs = json.loads(user_data.get("search_prefs") or '{}')
    current_intent = user_data.get("intent")
    
    # Check if ANY specific criteria are set
    has_premium_prefs = await is_premium(user_id) and (prefs.get("gender", "Any") != "Any" or prefs.get("language", "Any") != "Any")
    has_specific_intent = current_intent and current_intent != "🤫 Anything Goes"

    if has_premium_prefs or has_specific_intent:
        context.job_queue.run_once(unified_fallback_check, 30, data={'user_id': user_id}, name=f"fallback_{user_id}")

    await run_matching_algorithm(context)

async def unified_fallback_check(context: ContextTypes.DEFAULT_TYPE):
    """A single, smart job that finds all possible partial matches and presents them in one menu."""
    user_id = context.job.data['user_id']
    user_data = await get_user_data(user_id)
    
    if not user_data or user_data.get('state') != 'waiting':
        return

    logger.info(f"Running unified fallback for user {user_id}")
    prefs = json.loads(user_data.get("search_prefs") or '{}')
    user_intent = user_data.get("intent")
    pool = await get_waiting_pool()
    
    keyboard_buttons = []
    
    # --- Scan for Premium Preference partial matches ---
    if await is_premium(user_id):
        gender_pref = prefs.get("gender", "Any")
        lang_pref = prefs.get("language", "Any")
        
        # Scan for users who match Gender but not Language
        if gender_pref != "Any":
            for candidate in pool:
                if candidate['user_id'] != user_id and candidate.get("gender") == gender_pref and candidate.get("intent") in [user_intent, "🤫 Anything Goes"]:
                    btn_text = f"🗣️ Chat with a {gender_pref} (Any Language)"
                    keyboard_buttons.append([InlineKeyboardButton(btn_text, callback_data=f"fallback_pref_{gender_pref}_Any")])
                    break # Found one, no need to look for more
        
        # Scan for users who match Language but not Gender
        if lang_pref != "Any":
            for candidate in pool:
                if candidate['user_id'] != user_id and lang_pref in json.loads(candidate.get("languages") or '[]') and candidate.get("intent") in [user_intent, "🤫 Anything Goes"]:
                    btn_text = f"👤 Chat with an {lang_pref} Speaker (Any Gender)"
                    keyboard_buttons.append([InlineKeyboardButton(btn_text, callback_data=f"fallback_pref_Any_{lang_pref}")])
                    break

    # --- Add the Intent fallback option ---
    if user_intent and user_intent != "🤫 Anything Goes":
        keyboard_buttons.append([InlineKeyboardButton("➡️ Switch Your Intent to 'Anything Goes'", callback_data="fallback_intent_switch")])

    # --- Add the generic options ---
    keyboard_buttons.append([InlineKeyboardButton("🎲 Connect with Anyone (Random)", callback_data="fallback_random")])
    keyboard_buttons.append([InlineKeyboardButton("⏳ Keep Waiting for Perfect Match", callback_data="fallback_keep")])
    
    try:
        await context.bot.edit_message_text(
            chat_id=user_id,
            message_id=user_data['searching_message_id'],
            text="It's taking a while to find a perfect match. Here are some other options available:",
            reply_markup=InlineKeyboardMarkup(keyboard_buttons)
        )
    except (TelegramError, BadRequest):
        pass

async def unified_fallback_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """A single, smart handler for all fallback menu buttons."""
    query = update.callback_query
    user_id = update.effective_user.id
    await query.answer()
    
    parts = query.data.split('_')
    fallback_type = parts[1]

    if fallback_type == "pref":
        # e.g., "fallback_pref_Male_Any"
        gender, language = parts[2], parts[3]
        prefs = {"gender": gender, "language": language}
        await query.edit_message_text("⏳ Okay, broadening your preferences...")
        await update_user_data(user_id, {"search_prefs": json.dumps(prefs)})
        await add_to_pool_and_match(context, user_id)
        
    elif fallback_type == "intent":
        # e.g., "fallback_intent_switch"
        await query.edit_message_text("✅ Your intent has been updated to 'Anything Goes'. Searching again...")
        await update_user_data(user_id, {"intent": "🤫 Anything Goes"})
        await add_to_pool_and_match(context, user_id)

    elif fallback_type == "random":
        await query.edit_message_text("⏳ Okay, searching for any available user...")
        # Clear all specific criteria for a truly random search
        await update_user_data(user_id, {"search_prefs": json.dumps({}), "intent": "🤫 Anything Goes"})
        await add_to_pool_and_match(context, user_id)

    elif fallback_type == "keep":
        await query.edit_message_text(
            "⏳ Okay, continuing to search for a perfect match...", 
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Search", callback_data="cancel_search")]])
        )
        # Reschedule the same unified job to run again
        context.job_queue.run_once(unified_fallback_check, 30, data={'user_id': user_id}, name=f"fallback_{user_id}")

# async def fallback_random_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     query = update.callback_query; await query.answer()
#     user_id = update.effective_user.id
#     await query.edit_message_text("⏳ Okay, searching for any available user...")
#     await update_user_data(user_id, {"search_prefs": json.dumps({})})
#     await add_to_pool_and_match(context, user_id)



async def match_users(context: ContextTypes.DEFAULT_TYPE, user1_id: int, user2_id: int, is_reconnect: bool = False):
    """
    Connects two users, sends a feature-rich pinned message, and correctly handles reconnects AND favorite requests.
    (PostgreSQL Version)
    """
    logger.info(f"✅ MATCH FOUND: Connecting {user1_id} and {user2_id}. Is Reconnect: {is_reconnect}")

    user1_data_live = await get_user_data(user1_id)
    user2_data_live = await get_user_data(user2_id)
    
    # We will acquire one connection to handle multiple database tasks
    async with POOL.acquire() as conn:
        # Start a transaction to ensure all database actions succeed or fail together
        async with conn.transaction():
            # --- Database and State Updates ---
            # Use RETURNING chat_id to get the ID of the new row, which is the standard PostgreSQL way
            record = await conn.fetchrow("INSERT INTO chat_history (user1_id, user2_id, start_time) VALUES ($1, $2, $3) RETURNING chat_id", user1_id, user2_id, time.time())
            chat_id = record['chat_id']
            
            chat_start_time = time.time()
            # These functions will acquire their own connections from the pool, which is fine
            await update_user_data(user1_id, {"state": "in_chat", "partner_id": user2_id, "last_chat_id": chat_id, "chat_start_time": chat_start_time, "searching_message_id": None})
            await update_user_data(user2_id, {"state": "in_chat", "partner_id": user1_id, "last_chat_id": chat_id, "chat_start_time": chat_start_time, "searching_message_id": None})
            
            # --- Check for existing connection and load snapshots within the same transaction ---
            existing_connection = await conn.fetchrow("SELECT connection_id FROM connections WHERE (user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)", user1_id, user2_id)
            
            user1_profile_to_show = user1_data_live
            user2_profile_to_show = user2_data_live
            if is_reconnect:
                logger.info("Reconnect detected. Loading profile snapshots.")
                conn_rec = await conn.fetchrow("SELECT * FROM connections WHERE (user1_id = $1 AND user2_id = $2) OR (user1_id = $2 AND user2_id = $1)", user1_id, user2_id)
                if conn_rec:
                    user1_profile_to_show = json.loads(conn_rec['user1_snapshot'])
                    user2_profile_to_show = json.loads(conn_rec['user2_snapshot'])

    # --- Logic that happens after the database transaction is complete ---
    if not existing_connection:
        is_user1_premium = user1_data_live.get('is_premium', 0) == 1
        is_user2_premium = user2_data_live.get('is_premium', 0) == 1
        if (is_user1_premium or is_user2_premium):
            context.job_queue.run_once(send_favorite_option_job, MIN_CHAT_DURATION_FOR_FAVORITE, data={"user1_id": user1_id, "user2_id": user2_id, "chat_id": chat_id}, name=f"favorite_{chat_id}")

    for data in [user1_data_live, user2_data_live]:
        if data and data.get("searching_message_id"):
            try: await context.bot.delete_message(chat_id=data['user_id'], message_id=data['searching_message_id'])
            except (TelegramError, BadRequest): pass

    for current_user, partner_profile in [(user1_data_live, user2_profile_to_show), (user2_data_live, user1_profile_to_show)]:
        current_user_id = current_user['user_id']
        is_current_premium = current_user.get('is_premium', 0) == 1
        is_partner_premium = partner_profile.get('is_premium', 0) == 1

        p_kinks = json.loads(partner_profile.get('kinks') or '[]')
        match_text = (f"✨ **It's a match!** ✨\n\nYour partner's selected tags: {', '.join(p_kinks) if p_kinks else 'None'}")
        
        if not is_current_premium and is_partner_premium:
            match_text += f"\n\n💎 *You're connected with a Premium user! Your chat controls are locked for {int(CHAT_LOCK_DURATION/60)} minutes.*"
        
        await context.bot.send_message(current_user_id, text=match_text, reply_markup=CHAT_REPLY_KEYBOARD, parse_mode='Markdown')
        
        p_name = partner_profile.get('name', 'Stranger')
        p_gender = partner_profile.get('gender')
        p_age = partner_profile.get('age')
        p_intent = partner_profile.get('intent', 'a chat')
        gender_char = f"{p_gender[0]}" if p_gender in ["Male", "Female"] else ""
        age_str = f"{p_age}" if p_age else ""
        details_str = f", {gender_char}{age_str}" if gender_char or age_str else ""
        pin_text = f"👤 You are chatting with **{p_name}**{details_str} for **{p_intent}**"
        
        name_msg = await context.bot.send_message(current_user_id, pin_text, parse_mode='Markdown')
        try:
            await context.bot.pin_chat_message(chat_id=current_user_id, message_id=name_msg.message_id, disable_notification=True)
            await update_user_data(current_user_id, {"pinned_message_id": name_msg.message_id})
        except TelegramError as e:
            logger.error(f"Failed to pin message for {current_user_id}: {e}")

async def send_favorite_option_job(context: ContextTypes.DEFAULT_TYPE):
    """
    A scheduled job that offers premium users the option to favorite their partner.
    Now with better logging and a robust check.
    """
    job_data = context.job.data
    user1_id, user2_id = job_data["user1_id"], job_data["user2_id"]
    last_chat_id = job_data["chat_id"]

    logger.info(f"Running favorite option job for chat_id {last_chat_id} between {user1_id} and {user2_id}")

    # Get current data to ensure they are still in the same chat
    user1_data = await get_user_data(user1_id)
    user2_data = await get_user_data(user2_id)

    # This is the critical check that fixes the bug
    if not (user1_data and user2_data and
            user1_data.get("state") == "in_chat" and
            user1_data.get("partner_id") == user2_id and
            user1_data.get("last_chat_id") == last_chat_id):
        logger.info(f"Chat {last_chat_id} ended or changed before favorite option could be sent. Aborting job.")
        return

    logger.info(f"Users are still in chat {last_chat_id}. Proceeding to send favorite buttons.")
    is_user1_premium = user1_data.get("is_premium") == 1
    is_user2_premium = user2_data.get("is_premium") == 1

    keyboard = [[
        InlineKeyboardButton("⭐ Add to Favorites", callback_data=f"favorite_{last_chat_id}"),
        InlineKeyboardButton("🤔 Ask Later", callback_data=f"favorite_later_{last_chat_id}")
    ]]
    message_text = "✨ Enjoying this conversation? You can add this user to your favorites to chat again later!"

    if is_user1_premium:
        await context.bot.send_message(user1_id, message_text, reply_markup=InlineKeyboardMarkup(keyboard))

    if is_user2_premium and not is_user1_premium: # Only send to user2 if they are premium and user1 is not
        await context.bot.send_message(user2_id, message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    # If both are premium, the button will be sent to both from each respective check.


async def run_matching_algorithm(context: ContextTypes.DEFAULT_TYPE):
    async with MATCH_LOCK:
        pool = await get_waiting_pool()
        logger.info(f"Running RANDOM matching. Pool size: {len(pool)}.")
        if len(pool) < 2: return
        users_to_match = list(pool)
        while len(users_to_match) >= 2:
            searcher = users_to_match.pop(0)
            eligible_partners = [p for p in users_to_match if check_mutual_match(searcher, p)]
            if eligible_partners:
                chosen_partner = random.choice(eligible_partners)
                logger.info(f"✅ MUTUAL MATCH FOUND (Randomly selected): {searcher['user_id']} and {chosen_partner['user_id']}")
                asyncio.create_task(match_users(context, searcher['user_id'], chosen_partner['user_id']))
                users_to_match.remove(chosen_partner)
            else:
                logger.info(f"User {searcher['user_id']} found no eligible partners in this run.")

async def cancel_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    # --- THIS IS THE FIX: Clear the searching_message_id ---
    await update_user_data(user_id, {"state": "idle", "searching_message_id": None})
    logger.info(f"User {user_id} cancelled search.")
    await query.answer("Search cancelled.")
    try:
        await query.delete_message()
    except (TelegramError, BadRequest): pass
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    await context.bot.send_message(user_id, "You are back in the main menu.", reply_markup=keyboard)

async def end_chat(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """
    Helper function to end a chat session for a user and their partner.
    NOW triggers the Vibe Check for both users using a robust job. (PostgreSQL Version)
    """
    user_data = await get_user_data(user_id)
    if not user_data or user_data.get("state") != "in_chat":
        return None, 0, None

    partner_id = user_data.get("partner_id")
    last_chat_id = user_data.get("last_chat_id")
    chat_start_time = user_data.get("chat_start_time", time.time())
    chat_duration = time.time() - chat_start_time

    if last_chat_id:
        # --- THIS IS THE CHANGE ---
        async with POOL.acquire() as conn:
            await conn.execute(
                "UPDATE chat_history SET end_time = $1 WHERE chat_id = $2",
                time.time(), last_chat_id
            )
        # --------------------------

    await clear_chat_maps(last_chat_id)
    for uid in [user_id, partner_id]:
        if uid:
            data = await get_user_data(uid)
            jobs = context.job_queue.get_jobs_by_name(f"unlock_{uid}")
            for job in jobs: job.schedule_removal()
            if data and data.get("pinned_message_id"):
                try: await context.bot.unpin_chat_message(chat_id=uid, message_id=data.get("pinned_message_id"))
                except (TelegramError, BadRequest): pass

    reset_data = {"state": "idle", "partner_id": None, "pinned_message_id": None, "last_chat_id": None, "chat_start_time": None}
    await update_user_data(user_id, reset_data)
    if partner_id:
        await update_user_data(partner_id, reset_data)
        ended_msg = await context.bot.send_message(partner_id, "👋 Your partner has ended the chat.", reply_markup=ReplyKeyboardRemove())
        await schedule_message_deletion(context, partner_id, ended_msg.message_id, delay=10)

    # --- TRIGGER VIBE CHECK (Robust Method) ---
    for uid in [user_id, partner_id]:
        if uid and last_chat_id:
            context.job_queue.run_once(
                vibe_check_job,
                2, # 2 seconds delay
                data={'user_id': uid, 'chat_id': last_chat_id},
                name=f"vibe_check_{uid}_{last_chat_id}"
            )

    return partner_id, chat_duration, last_chat_id

async def vibe_check_job(context: ContextTypes.DEFAULT_TYPE):
    """A dedicated, reliable job function to send the vibe check."""
    job_data = context.job.data
    await send_vibe_check(context, job_data['user_id'], job_data['chat_id'])

async def send_vibe_check(context: ContextTypes.DEFAULT_TYPE, user_id: int, chat_id: int):
    """Sends the post-chat Vibe Check prompt."""
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔥 Intense", callback_data=f"vibe_{chat_id}_Intense"),
            InlineKeyboardButton("🎭 Creative", callback_data=f"vibe_{chat_id}_Creative")
        ],
        [
            InlineKeyboardButton("😴 Slow", callback_data=f"vibe_{chat_id}_Slow"),
            InlineKeyboardButton("🚫 Report", callback_data=f"vibe_{chat_id}_Report")
        ]
    ])
    try:
        await context.bot.send_message(
            user_id,
            "How was that last chat? Your anonymous feedback helps improve future matches.",
            reply_markup=keyboard
        )
    except (TelegramError, BadRequest):
        logger.warning(f"Could not send vibe check to user {user_id}.")

async def vibe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the Vibe Check button press and saves the feedback."""
    query = update.callback_query
    await query.answer("Thanks for your feedback!")

    user_id = update.effective_user.id
    _, chat_id_str, tag = query.data.split("_")
    chat_id = int(chat_id_str)

    # --- THIS IS THE CHANGE ---
    async with POOL.acquire() as conn:
        # fetchrow returns a dictionary-like object by default
        history = await conn.fetchrow("SELECT user1_id FROM chat_history WHERE chat_id = $1", chat_id)
        if not history:
            return

        # Determine if the current user was user1 or user2 in the chat
        column_to_update = "user1_vibe_tag" if history["user1_id"] == user_id else "user2_vibe_tag"
        # Use $1, $2 placeholders for the UPDATE query
        await conn.execute(f"UPDATE chat_history SET {column_to_update} = $1 WHERE chat_id = $2", tag, chat_id)
    # --------------------------

    await query.edit_message_text(f"Feedback received: **{tag}**. Thank you!", parse_mode='Markdown')
    await schedule_message_deletion(context, query.message.chat_id, query.message.message_id, delay=5)

async def media_timer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user's choice for the media timer and forwards the media."""
    query = update.callback_query; await query.answer()
    user_id = update.effective_user.id
    user_data = await get_user_data(user_id)
    
    # Check if the action is still valid
    if not (user_data and user_data.get("state") == "in_chat" and 'media_to_forward' in context.user_data):
        await query.edit_message_text("This action has expired.")
        return

    original_msg_id = context.user_data.pop('media_to_forward')
    partner_id = user_data.get("partner_id")
    last_chat_id = user_data.get("last_chat_id")
    timer_duration = int(query.data.split("_")[2])

    await query.edit_message_text(f"Sending with protection..." if timer_duration > 0 else "Sending normally...")

    try:
        # Forward the media to the partner, protecting it from being saved or forwarded if a timer is set
        sent_message = await context.bot.copy_message(
            chat_id=partner_id,
            from_chat_id=user_id,
            message_id=original_msg_id,
            protect_content=(timer_duration > 0),
        )
        
        # Create the message maps so replies to the media will work
        await map_message(last_chat_id, user_id, original_msg_id, partner_id, sent_message.message_id)
        await map_message(last_chat_id, partner_id, sent_message.message_id, user_id, original_msg_id)

    except (TelegramError, BadRequest) as e:
        logger.error(f"Could not forward media from {user_id} to {partner_id}: {e}")
        await query.edit_message_text("🔴 Could not deliver the media.")

async def handle_ad_break(context: ContextTypes.DEFAULT_TYPE, user_id: int, message: str):
    if await is_premium(user_id): return
    try:
        countdown_msg = await context.bot.send_message(chat_id=user_id, text=f"{message} in {AD_BREAK_DURATION}...")
        for i in range(AD_BREAK_DURATION - 1, 0, -1):
            await asyncio.sleep(1)
            await context.bot.edit_message_text(text=f"{message} in {i}...", chat_id=user_id, message_id=countdown_msg.message_id)
        await context.bot.delete_message(chat_id=user_id, message_id=countdown_msg.message_id)
    except (TelegramError, BadRequest) as e:
        logger.warning(f"Ad break interrupted for user {user_id}: {e}")
    
async def handle_initiator_action(context: ContextTypes.DEFAULT_TYPE, user_id: int, action: str, with_ad_break: bool):
    """Handles the initiator's action, showing an ad break if required."""
    if with_ad_break:
        await handle_ad_break(context, user_id, "Please wait")

    # Restore the user's original search preferences for this session
    user_data = await get_user_data(user_id)
    if original_prefs_str := user_data.get("original_search_prefs"):
        await update_user_data(user_id, {"search_prefs": original_prefs_str})

    if action == "Next":
        # Clear any leftover searching_message_id before starting a new search
        await update_user_data(user_id, {"searching_message_id": None})
        await add_to_pool_and_match(context, user_id)
    else: # Stop
        keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
        await context.bot.send_message(user_id, "You are back in the main menu.", reply_markup=keyboard)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all messages, including text, media (with premium timer), and chat controls."""
    if not update.message or not update.effective_user: return
    user_id = update.effective_user.id
    user_data = await get_user_data(user_id)

    # --- NEW, ROBUST LOGIC FOR CHAT CONTROLS ---
    if update.message.text and update.message.text in ["➡️ Next", "🛑 Stop"]:
        if user_data.get("state") == "in_chat":
            is_initiator_premium = await is_premium(user_id)
            partner_id, _, _ = await end_chat(context, user_id)
            await context.bot.send_message(user_id, "💬 Chat ended.", reply_markup=ReplyKeyboardRemove())

            # Handle the "victim" (the person who DID NOT click) instantly
            if partner_id:
                # Clear their old searching_message_id before re-queuing
                await update_user_data(partner_id, {"searching_message_id": None})
                partner_data = await get_user_data(partner_id)
                if partner_prefs_str := partner_data.get("original_search_prefs"):
                    await update_user_data(partner_id, {"search_prefs": partner_prefs_str})
                await add_to_pool_and_match(context, partner_id)

            # Handle the "initiator" (the person who CLICKED)
            action = "Next" if update.message.text == "➡️ Next" else "Stop"
            context.application.create_task(
                handle_initiator_action(context, user_id, action, with_ad_break=not is_initiator_premium)
            )
        else:
            await update.message.reply_text("You are not currently in a chat. Press /start to begin.")
        return

    if user_data.get("state") != "in_chat": return

    # --- Premium Media and Standard Forwarding Logic (No changes here) ---
    is_media = update.message.photo or update.message.video or update.message.voice or update.message.video_note or update.message.document
    if is_media and await is_premium(user_id):
        context.user_data['media_to_forward'] = update.message.message_id
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔒 Protected (10s)", callback_data="media_timer_10"), InlineKeyboardButton("🛡️ Protected (30s)", callback_data="media_timer_30")],
            [InlineKeyboardButton("Send Normally", callback_data="media_timer_0")]
        ])
        await update.message.reply_text("This media can be protected from forwarding and saving. Choose an option:", reply_markup=keyboard)
        return

    partner_id = user_data.get("partner_id")
    last_chat_id = user_data.get("last_chat_id")
    reply_to_msg_id = None
    if update.message.reply_to_message:
        reply_to_msg_id = await get_mapped_message_id(last_chat_id, user_id, update.message.reply_to_message.message_id)

    try:
        sent_message = await context.bot.copy_message(chat_id=partner_id, from_chat_id=user_id, message_id=update.message.message_id, reply_to_message_id=reply_to_msg_id)
        await map_message(last_chat_id, user_id, update.message.message_id, partner_id, sent_message.message_id)
        await map_message(last_chat_id, partner_id, sent_message.message_id, user_id, update.message.message_id)
    except (TelegramError, BadRequest) as e:
        logger.error(f"Could not forward message from {user_id} to {partner_id}: {e}")
        await update.message.reply_text("🔴 Could not deliver message. Your partner may have disconnected.")
        await end_chat(context, user_id)


# --- ⭐ Premium Feature: Favorites & Reconnect ---

async def favorite_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the 'Add to Favorites' button using the Mutual Match system. (PostgreSQL Version)"""
    query = update.callback_query
    initiator_id = update.effective_user.id
    await query.answer()

    try:
        chat_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.edit_message_text("❗️ An error occurred with this favorite request.")
        return

    async with POOL.acquire() as conn:
        async with conn.transaction():
            history = await conn.fetchrow("SELECT * FROM chat_history WHERE chat_id = $1", chat_id)
            if not history:
                await query.edit_message_text("❗️ This chat session has expired."); return

            user_column_to_update = "user1_wants_favorite" if history['user1_id'] == initiator_id else "user2_wants_favorite"
            updated_history = await conn.fetchrow(
                f"UPDATE chat_history SET {user_column_to_update} = 1 WHERE chat_id = $1 RETURNING *",
                chat_id
            )

    if updated_history and updated_history['user1_wants_favorite'] == 1 and updated_history['user2_wants_favorite'] == 1:
        partner_id = updated_history['user2_id'] if updated_history['user1_id'] == initiator_id else updated_history['user1_id']
        await query.edit_message_text("🎉 It's a mutual match! You are now favorites.")
        await create_connection(context, initiator_id, partner_id)
    else:
        partner_id = history['user2_id'] if history['user1_id'] == initiator_id else history['user1_id']
        is_initiator_premium = await is_premium(initiator_id)
        is_partner_premium = await is_premium(partner_id)

        if is_initiator_premium and not is_partner_premium:
             initiator_data = await get_user_data(initiator_id)
             keyboard = [
                 [InlineKeyboardButton("✅ Yes, connect", callback_data=f"consent_yes_{chat_id}_{initiator_id}")],
                 [InlineKeyboardButton("❌ No, thanks", callback_data=f"consent_no_{chat_id}_{initiator_id}")]
             ]
             await context.bot.send_message(
                 chat_id=partner_id,
                 text=f"Hey! Your chat partner, **{initiator_data.get('name', 'Stranger')}**, would like to add you to their favorites. Do you agree?",
                 reply_markup=InlineKeyboardMarkup(keyboard),
                 parse_mode='Markdown'
             )
             await query.edit_message_text("✅ Request sent! If they accept, you'll be connected.")
        else:
             await query.edit_message_text("✅ Great! If your partner also adds you, you will be connected instantly.")



async def consent_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the non-premium user's response to a favorite request."""
    query = update.callback_query
    accepter_id = update.effective_user.id
    
    try:
        _, consent_action, chat_id_str, initiator_id_str = query.data.split("_")
        chat_id = int(chat_id_str)
        initiator_id = int(initiator_id_str)
    except (IndexError, ValueError):
        await query.answer("❗️ Error: Invalid consent request.", show_alert=True)
        return

    await query.answer()
    initiator_data = await get_user_data(initiator_id)
    accepter_data = await get_user_data(accepter_id)

    if consent_action == "yes":
        await query.edit_message_text(f"✅ You have agreed to connect with **{initiator_data.get('name', 'Stranger')}**.", parse_mode='Markdown')
        await create_connection(context, initiator_id, accepter_id)
    else: # 'no'
        await query.edit_message_text("You have declined the request.")
        await context.bot.send_message(initiator_id, f"😔 Unfortunately, **{accepter_data.get('name', 'your partner')}** declined your request to connect.", parse_mode='Markdown')


# --- THIS IS THE NEW, CORRECTED VERSION ---
async def create_connection(context: ContextTypes.DEFAULT_TYPE, user1_id: int, user2_id: int):
    """Creates a permanent connection, saving a full snapshot of both users' profiles. (PostgreSQL Version)"""
    user1_data = await get_user_data(user1_id)
    user2_data = await get_user_data(user2_id)
    if not user1_data or not user2_data: return

    user1_snapshot = {
        "name": user1_data.get('name', 'Stranger'), "gender": user1_data.get('gender'),
        "age": user1_data.get('age'), "languages": user1_data.get('languages'),
        "intent": user1_data.get('intent'), "kinks": user1_data.get('kinks')
    }
    user2_snapshot = {
        "name": user2_data.get('name', 'Stranger'), "gender": user2_data.get('gender'),
        "age": user2_data.get('age'), "languages": user2_data.get('languages'),
        "intent": user2_data.get('intent'), "kinks": user2_data.get('kinks')
    }

    async with POOL.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO connections (user1_id, user2_id, user1_snapshot, user2_snapshot, timestamp)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (user1_id, user2_id) DO NOTHING
            """,
            user1_id, user2_id, json.dumps(user1_snapshot), json.dumps(user2_snapshot), time.time()
        )
    
    logger.info(f"Created a permanent connection between {user1_id} and {user2_id}")
    await context.bot.send_message(user1_id, f"🎉 You and **{user2_snapshot['name']}** are now favorites!", parse_mode='Markdown')
    await context.bot.send_message(user2_id, f"🎉 You and **{user1_snapshot['name']}** are now favorites!", parse_mode='Markdown')


async def my_connections_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays a list of the user's saved connections, now with live active status. (PostgreSQL Version)"""
    query = update.callback_query
    user_id = update.effective_user.id
    await query.answer()

    async with POOL.acquire() as conn:
        connections = await conn.fetch(
            "SELECT * FROM connections WHERE user1_id = $1 OR user2_id = $1",
            user_id
        )
    
    keyboard = MAIN_MENU_KEYBOARD_PREMIUM if await is_premium(user_id) else MAIN_MENU_KEYBOARD_BASIC
    if not connections:
        await query.edit_message_text("You have no saved favorites yet.", reply_markup=keyboard)
        return

    keyboard_buttons = []
    for conn_rec in connections:
        if conn_rec['user1_id'] == user_id:
            other_user_id = conn_rec['user2_id']
            other_user_snapshot = json.loads(conn_rec['user2_snapshot'])
        else:
            other_user_id = conn_rec['user1_id']
            other_user_snapshot = json.loads(conn_rec['user1_snapshot'])
        
        other_user_name = other_user_snapshot.get('name', 'Stranger')
        
        status_icon = "⚪️"
        live_data = await get_user_data(other_user_id)
        if live_data:
            is_visible = live_data.get("show_active_status", 1) == 1
            is_available = live_data.get("state") == "idle"
            if is_visible and is_available:
                status_icon = "🟢"
        
        display_name = f"{status_icon} {other_user_name}"
        
        keyboard_buttons.append([
            InlineKeyboardButton(f"💬 Chat with {display_name}", callback_data=f"reconnect_{other_user_id}"),
            InlineKeyboardButton(f"🗑️ Remove", callback_data=f"remove_{other_user_id}")
        ])
    
    keyboard_buttons.append([InlineKeyboardButton("⬅️ Back to Main Menu", callback_data="main_menu")])
    await query.edit_message_text("Here are your favorites. Green (🟢) means they are available to chat right now.", reply_markup=InlineKeyboardMarkup(keyboard_buttons))


async def remove_connection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Permanently removes a connection for both users. (PostgreSQL Version)"""
    query = update.callback_query
    user_id = update.effective_user.id
    await query.answer("Favorite removed.", show_alert=True)

    try:
        target_id = int(query.data.split("_")[1])
    except (IndexError, ValueError): return

    async with POOL.acquire() as conn:
        await conn.execute(
            "DELETE FROM connections WHERE (user1_id = $1 AND user2_id = $2) OR (user1_id = $3 AND user2_id = $4)",
            user_id, target_id, target_id, user_id
        )
    
    await my_connections_callback(update, context)

async def reconnect_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a reconnect request, including the Priority Interrupt logic."""
    query = update.callback_query
    initiator_id = update.effective_user.id
    try:
        target_id = int(query.data.split("_")[1])
    except (IndexError, ValueError): return

    await query.answer("Sending request...")

    initiator_data = await get_user_data(initiator_id)
    target_data = await get_user_data(target_id)
    
    # Priority Interrupt Logic for busy users
    if target_data and target_data.get('state') != 'idle':
        keyboard = [[
            InlineKeyboardButton("✅ Yes, switch chats", callback_data=f"accept_interrupt_{initiator_id}"),
            InlineKeyboardButton("❌ No, stay here", callback_data=f"decline_interrupt_{initiator_id}")
        ]]
        await context.bot.send_message(
            target_id, 
            f"❗️ Hey! **{initiator_data.get('name', 'A connection')}** wants to chat with you now, but you're in another chat. Would you like to switch?",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
        )
        await context.bot.send_message(initiator_id, "💌 Your connection is currently busy. They have been notified you want to chat.")

        return

    # Standard Reconnect Logic for idle users
    keyboard = [[
        InlineKeyboardButton("✅ Accept", callback_data=f"accept_reconnect_{initiator_id}"),
        InlineKeyboardButton("❌ Decline", callback_data=f"decline_reconnect_{initiator_id}")
    ]]
    await context.bot.send_message(
        target_id, 
        f"Hey! **{initiator_data.get('name', 'A connection')}** would like to chat with you again.",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown'
    )
    sent_msg = await context.bot.send_message(initiator_id, "💌 Your reconnect request has been sent!")
    await schedule_message_deletion(context, initiator_id, sent_msg.message_id, delay=5)

async def favorite_later_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the 'Ask Me Later' button, dismissing the favorite prompt."""
    query = update.callback_query
    await query.answer("Okay, you can decide in a future chat.")
    try:
        await query.delete_message()
    except (TelegramError, BadRequest):
        pass # Ignore if the message is already gone

async def accept_reconnect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, is_interrupt: bool = False):
    """Handles the target user accepting a reconnect or interrupt request."""
    query = update.callback_query
    accepter_id = update.effective_user.id
    try:
        initiator_id = int(query.data.split("_")[-1])
    except (IndexError, ValueError): return

    await query.answer("Connecting...")
    
    if is_interrupt:
        await query.edit_message_text("✅ Switching chats now...")
        interrupted_partner_id, _, _ = await end_chat(context, accepter_id)
        if interrupted_partner_id:
            await context.bot.send_message(interrupted_partner_id, "👋 Your partner switched to a priority chat. We're finding a new partner for you now!")
            await add_to_pool_and_match(context, interrupted_partner_id)
    else:
        await query.edit_message_text("✅ You have accepted the request. Connecting you now...")

    initiator_data = await get_user_data(initiator_id)
    if not initiator_data or initiator_data.get('state') != 'idle':
        await context.bot.send_message(accepter_id, "Sorry, the user who sent the request is no longer available.")
        return

    accepter_data = await get_user_data(accepter_id)
    await context.bot.send_message(initiator_id, f"🎉 **{accepter_data.get('name', 'Your connection')}** accepted your request! Connecting you now...", parse_mode='Markdown')
    
    await asyncio.sleep(1)
    
    # --- THIS IS THE KEY CHANGE ---
    # We now tell match_users that this is a special reconnect chat.
    await match_users(context, initiator_id, accepter_id, is_reconnect=True)


async def decline_reconnect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, is_interrupt: bool = False):
    """Handles the target user declining a reconnect or interrupt request."""
    query = update.callback_query
    try:
        initiator_id = int(query.data.split("_")[-1])
    except (IndexError, ValueError): return

    await query.answer()
    if is_interrupt:
        await query.edit_message_text("Okay, you will remain in your current chat.")
    else:
        await query.edit_message_text("You have declined the chat request.")

    accepter_data = await get_user_data(update.effective_user.id)
    await context.bot.send_message(initiator_id, f"😔 Sorry, **{accepter_data.get('name', 'your connection')}** declined your request to chat.", parse_mode='Markdown')

# --- 🛠️ Admin & Utility Commands ---
async def make_premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to grant premium status. Usage: /premium <user_id>"""
    if update.effective_user.id not in ADMIN_USER_IDS: return
    if not context.args:
        await update.message.reply_text("⚠️ Usage: /premium <user_id>"); return
    try:
        target_id = int(context.args[0])
        await update_user_data(target_id, {"is_premium": 1})
        await update.message.reply_text(f"✅ User {target_id} has been granted premium status.")
        await context.bot.send_message(target_id, "Congratulations! 💎 You now have Premium status!")
    except (ValueError, IndexError):
        await update.message.reply_text("⚠️ That doesn't look like a valid user ID.")
    except Exception as e:
        await update.message.reply_text(f"An error occurred: {e}")

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the user their own Telegram ID."""
    await update.message.reply_text(f"Your Telegram User ID is: `{update.effective_user.id}`", parse_mode='Markdown')

# --- 🚀 Main Application ---
def main() -> None:
    """Starts the bot and sets up the database connection."""
    # --- THIS IS THE CHANGE: Added .post_stop(close_db) ---
    application = Application.builder().token(BOT_TOKEN).post_init(initialize_db).post_stop(close_db).build()

    # A mini-conversation just for changing intent and kinks
    change_intent_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(change_intent_kinks, pattern="^change_intent_kinks$")],
        states={
            INTENT: [CallbackQueryHandler(ask_kinks, pattern="^intent_"), CallbackQueryHandler(skip_intent, pattern="^skip_intent$")],
            KINKS: [CallbackQueryHandler(handle_kink_selection, pattern="^kink_"), CallbackQueryHandler(profile_complete, pattern="^done_kink$")],
        },
        fallbacks=[CallbackQueryHandler(main_menu_callback, pattern="^main_menu$")],
        map_to_parent={
            ConversationHandler.END: -1
        }
    )

    # The main conversation for initial profile setup
    profile_conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start_command), CallbackQueryHandler(reset_profile, pattern="^reset_profile$")],
        states={
            AGREE_TERMS: [CallbackQueryHandler(ask_name, pattern="^agree_terms$")],
            NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_name_input),
                CallbackQueryHandler(skip_name, pattern="^skip_name$"),
                CallbackQueryHandler(save_default_profile_and_skip, pattern="^skip_all_setup$")
            ],
            GENDER: [CallbackQueryHandler(ask_age, pattern="^gender_"), CallbackQueryHandler(ask_age, pattern="^skip_gender$")],
            AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_languages), CallbackQueryHandler(ask_languages, pattern="^skip_age$")],
            LANGUAGES: [CallbackQueryHandler(handle_language_selection, pattern="^lang_"), CallbackQueryHandler(ask_intent, pattern="^done_lang$")],
            INTENT: [CallbackQueryHandler(ask_kinks, pattern="^intent_"), CallbackQueryHandler(skip_intent, pattern="^skip_intent$")],
            KINKS: [CallbackQueryHandler(handle_kink_selection, pattern="^kink_"), CallbackQueryHandler(profile_complete, pattern="^done_kink$")],
        },
        fallbacks=[CommandHandler("start", start_command)],
    )

    # Handler for premium users to set their chat preferences
    prefs_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(find_stranger_entry, pattern="^find_stranger$")],
        states={
            PREF_GENDER: [CallbackQueryHandler(pref_gender_callback, pattern=r"^pref_gender_")],
            PREF_LANGUAGE: [CallbackQueryHandler(pref_language_callback, pattern=r"^pref_lang_")],
        },
        fallbacks=[CommandHandler("start", start_command)],
    )

    # Add all handlers to the application
    application.add_handler(profile_conv_handler)
    application.add_handler(prefs_conv_handler)
    application.add_handler(change_intent_handler)

    # --- INVITE FRIEND HANDLERS ---
    application.add_handler(CallbackQueryHandler(create_invite_link, pattern="^invite_friend$"))
    application.add_handler(CallbackQueryHandler(cancel_invite, pattern=r"^cancel_invite_"))

    # --- PROFILE & SETTINGS MENU HANDLERS ---
    application.add_handler(CallbackQueryHandler(my_profile_menu, pattern="^my_profile$"))
    application.add_handler(CallbackQueryHandler(toggle_active_status_callback, pattern="^toggle_status$"))
    application.add_handler(CallbackQueryHandler(main_menu_callback, pattern="^main_menu$"))
    application.add_handler(CallbackQueryHandler(go_anonymous, pattern="^go_anonymous$"))

    # --- UNIFIED FALLBACK HANDLER ---
    application.add_handler(CallbackQueryHandler(unified_fallback_callback, pattern="^fallback_"))
    
    application.add_handler(CallbackQueryHandler(cancel_search, pattern="^cancel_search$"))
    
    # --- FAVORITES & CONNECTIONS HANDLERS ---
    application.add_handler(CallbackQueryHandler(favorite_callback, pattern=r"^favorite_"))
    application.add_handler(CallbackQueryHandler(favorite_later_callback, pattern=r"^favorite_later_"))
    application.add_handler(CallbackQueryHandler(consent_callback, pattern=r"^consent_"))
    application.add_handler(CallbackQueryHandler(vibe_callback, pattern=r"^vibe_"))
    application.add_handler(CallbackQueryHandler(media_timer_callback, pattern=r"^media_timer_"))
    application.add_handler(CallbackQueryHandler(my_connections_callback, pattern="^my_connections$"))
    application.add_handler(CallbackQueryHandler(remove_connection_callback, pattern=r"^remove_"))
    application.add_handler(CallbackQueryHandler(reconnect_request_callback, pattern=r"^reconnect_"))
    application.add_handler(CallbackQueryHandler(lambda u,c: accept_reconnect_callback(u,c,is_interrupt=False), pattern=r"^accept_reconnect_"))
    application.add_handler(CallbackQueryHandler(lambda u,c: decline_reconnect_callback(u,c,is_interrupt=False), pattern=r"^decline_reconnect_"))
    application.add_handler(CallbackQueryHandler(lambda u,c: accept_reconnect_callback(u,c,is_interrupt=True), pattern=r"^accept_interrupt_"))
    application.add_handler(CallbackQueryHandler(lambda u,c: decline_reconnect_callback(u,c,is_interrupt=True), pattern=r"^decline_interrupt_"))
    
    # --- ADMIN & UTILITY COMMANDS ---
    application.add_handler(CommandHandler("premium", make_premium_command))
    application.add_handler(CommandHandler("myid", myid_command))
    
    # --- MAIN MESSAGE HANDLER ---
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND & filters.ChatType.PRIVATE, message_handler))

    logger.info("Bot is starting...")
    application.run_polling()

if __name__ == "__main__":
    main()