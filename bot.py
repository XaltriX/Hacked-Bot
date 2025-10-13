import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError, TelegramConflictError
import os
import psutil
import re
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN_FILES = ["token1.txt", "tfinalbots_valid.txt", "token3.txt", "uploaded_tokens.txt"]
USER_IDS_FILE = "user_ids.txt"

ADMIN_ID = 5706788169
DASHBOARD_TOKEN = "7557269432:AAF1scybLhu5sX4E6xkktd5jGXtCFzOz1n0"

BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 10
MAX_RETRIES = 3
RETRY_DELAY = 2

CUSTOM_REPLY = """
🎬 MOVIE & ENTERTAINMENT HUB 🍿  
✨ Your Ultimate Destination for Movies & Daily Entertainment!

───────────────────────────────

➡️ MOVIE REQUEST GROUP 🎥  
💬 Request your favorite movies  
🔗 Join Now: https://t.me/MOVIE_REQUESTX

───────────────────────────────

➡️ DAILY DOSE OF MMS LE@K 💥  
🔥 Exclusive unseen drops  
🔗 Join Now: https://t.me/+Br0s4neTgL0xM2I8

───────────────────────────────

➡️ PREMIUM MMS LE@K C0RN 💎  
⚡ High-quality, premium content  
🔗 Access Now: https://t.me/+VWdELS83oeMxMWI1

───────────────────────────────

➡️ D@RK WEB VIE0S 🌑  
😈 Rare & bold videos  
🔗 Explore Now: https://t.me/+we2VaRaOfr5lM2M0

───────────────────────────────

➡️ NEW MOVIE DAILY 🎞️  
📅 Fresh movies every day  
🔗 Watch Now: https://t.me/+vkh5MVQqJzs4OGU0

───────────────────────────────

🌐 BONUS LINK — Full Hub Access  
💫 All channels in one place  
🔗 Visit Now: https://linkzwallah.netlify.app/
"""

user_ids = set()
bots = {}
bot_stats = {}
bot_tasks = {}

def extract_tokens(text):
    pattern = r'\d{6,10}:[A-Za-z0-9_-]{20,}'
    return re.findall(pattern, text)

def load_user_ids():
    try:
        if Path(USER_IDS_FILE).exists():
            with open(USER_IDS_FILE, "r") as f:
                for line in f:
                    chat_id = line.strip()
                    if chat_id.isdigit():
                        user_ids.add(int(chat_id))
        logger.info(f"Loaded {len(user_ids)} user IDs")
    except Exception as e:
        logger.error(f"Error loading user IDs: {e}")

def save_user_id(chat_id):
    try:
        if chat_id not in user_ids:
            user_ids.add(chat_id)
            with open(USER_IDS_FILE, "a") as f:
                f.write(f"{chat_id}\n")
    except Exception as e:
        logger.error(f"Error saving user ID {chat_id}: {e}")

async def delete_webhook(token):
    """Delete webhook before using polling"""
    bot = None
    try:
        bot = Bot(token)
        await bot.delete_webhook(drop_pending_updates=True)
    except TelegramConflictError:
        logger.warning(f"Webhook conflict for token {token[:10]}...")
        try:
            await asyncio.sleep(2)
            if bot:
                await bot.delete_webhook(drop_pending_updates=True)
        except TelegramAPIError as e:
            logger.error(f"Failed to delete webhook for token {token[:10]}...: {e}")
        except Exception as e:
            logger.error(f"Unexpected error deleting webhook: {e}")
    except TelegramAPIError as e:
        logger.error(f"API error deleting webhook: {e}")
    except Exception as e:
        logger.error(f"Error deleting webhook: {e}")
    finally:
        if bot:
            try:
                if bot.session and not bot.session.closed:
                    await bot.session.close()
            except Exception as e:
                logger.error(f"Error closing session: {e}")

async def get_bot_username(token):
    """Safely get bot username with retries"""
    for attempt in range(MAX_RETRIES):
        bot = None
        try:
            bot = Bot(token)
            me = await bot.get_me()
            return me.username
        except TelegramConflictError:
            logger.warning(f"Conflict error getting bot info (attempt {attempt+1}/{MAX_RETRIES})")
            await asyncio.sleep(RETRY_DELAY)
        except TelegramAPIError as e:
            logger.error(f"Telegram error getting bot info: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting bot username: {e}")
            return None
        finally:
            if bot:
                try:
                    if bot.session and not bot.session.closed:
                        await bot.session.close()
                except Exception:
                    pass
    return None

async def startup_bots(tokens):
    """Start bots in batches with proper error handling"""
    started = 0
    failed = 0
    total = len(tokens)
    
    logger.info(f"Starting {total} bots in batches of {BATCH_SIZE}")
    
    batch_num = 1
    for i in range(0, total, BATCH_SIZE):
        batch = tokens[i:i+BATCH_SIZE]
        logger.info(f"Starting batch {batch_num}/{(total+BATCH_SIZE-1)//BATCH_SIZE}...")
        
        tasks = [start_single_bot(token) for token in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if result is True:
                started += 1
            else:
                failed += 1
        
        logger.info(f"[{started}/{total}] bots started, {failed} failed. Sleeping {DELAY_BETWEEN_BATCHES}s...")
        print_resource_usage()
        batch_num += 1
        await asyncio.sleep(DELAY_BETWEEN_BATCHES)
    
    logger.info(f"Bot startup complete: {started} successful, {failed} failed")

async def start_single_bot(token):
    """Start a single bot with comprehensive error handling"""
    try:
        await delete_webhook(token)
        await asyncio.sleep(1)
        
        username = await get_bot_username(token)
        if not username:
            logger.error(f"Token invalid or unreachable: {token[:10]}...")
            return False
        
        if username in bots:
            logger.warning(f"Already running: @{username}")
            return False
        
        bot = Bot(token)
        dp = Dispatcher()
        bots[username] = bot
        bot_stats[username] = {"messages": 0, "users": set()}
        
        @dp.message()
        async def handler(msg: types.Message):
            try:
                bot_stats[username]["messages"] += 1
                bot_stats[username]["users"].add(msg.from_user.id)
                save_user_id(msg.from_user.id)
                await msg.answer(CUSTOM_REPLY)
            except TelegramAPIError as e:
                logger.error(f"Error handling message for @{username}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in handler: {e}")
        
        async def poll_with_error_handling():
            try:
                await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
            except TelegramConflictError as e:
                logger.error(f"Conflict error for @{username}: {e}")
                await asyncio.sleep(5)
            except TelegramAPIError as e:
                logger.error(f"Telegram error for @{username}: {e}")
            except Exception as e:
                logger.error(f"Error polling @{username}: {e}")
            finally:
                try:
                    if bot.session and not bot.session.closed:
                        await bot.session.close()
                except Exception as e:
                    logger.error(f"Error closing bot session: {e}")
        
        task = asyncio.create_task(poll_with_error_handling())
        bot_tasks[username] = task
        logger.info(f"@{username}: OK")
        return True
        
    except Exception as e:
        logger.error(f"Critical error starting bot: {e}")
        return False

def load_all_tokens():
    """Load tokens from files with error handling"""
    all_tokens = []
    try:
        for token_file in TOKEN_FILES:
            if not Path(token_file).exists():
                continue
            try:
                with open(token_file, "r", encoding="utf-8") as f:
                    content = f.read()
                    tokens = extract_tokens(content)
                    all_tokens.extend(tokens)
                    logger.info(f"Loaded {len(tokens)} tokens from {token_file}")
            except Exception as e:
                logger.error(f"Error reading {token_file}: {e}")
        
        logger.info(f"Total tokens loaded: {len(all_tokens)}")
        return all_tokens
    except Exception as e:
        logger.error(f"Error loading tokens: {e}")
        return []

def print_resource_usage():
    """Print system resource usage"""
    try:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage('/').percent
        logger.info(f"Resources - CPU: {cpu}% | RAM: {ram}% | Disk: {disk}%")
    except Exception as e:
        logger.error(f"Error getting resource usage: {e}")

def get_resource_usage_str():
    """Get resource usage as string"""
    try:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        disk = psutil.disk_usage('/').percent
        return f"CPU: {cpu}% | RAM: {ram}% | Disk: {disk}%"
    except Exception as e:
        logger.error(f"Error getting resource usage: {e}")
        return "Resource usage unavailable"

def get_bot_list():
    try:
        bot_list = list(bots.keys())
        if not bot_list:
            return "No bots running"
        return "\n".join(f"@{uname}" for uname in bot_list[:50])  # Limit to 50 bots per message
    except Exception as e:
        logger.error(f"Error getting bot list: {e}")
        return "Error getting bot list"

def get_stats():
    try:
        total_users = len(user_ids)
        total_bots = len(bots)
        total_messages = sum(stat["messages"] for stat in bot_stats.values())
        return (
            f"Bots running: {total_bots}\n"
            f"Total users (all bots): {total_users}\n"
            f"Total messages: {total_messages}\n"
            f"Resource usage: {get_resource_usage_str()}"
        )
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return "Stats unavailable"

async def dashboard():
    """Dashboard bot with error handling"""
    dashboard_bot = None
    try:
        await delete_webhook(DASHBOARD_TOKEN)
        dashboard_bot = Bot(DASHBOARD_TOKEN)
        dp = Dispatcher()

        @dp.message(Command("start"))
        async def cmd_start(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    await msg.answer("Unauthorized.")
                    return
                await msg.answer(
                    "Dashboard Commands:\n"
                    "/stats - Show stats\n"
                    "/bots - List bots\n"
                    "/broadcast <msg> - Send to all users\n"
                    "Send a .txt file to upload tokens."
                )
            except Exception as e:
                logger.error(f"Error in start command: {e}")

        @dp.message(Command("stats"))
        async def cmd_stats(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    await msg.answer("Unauthorized.")
                    return
                await msg.answer(get_stats())
            except Exception as e:
                logger.error(f"Error in stats command: {e}")

        @dp.message(Command("bots"))
        async def cmd_bots(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    await msg.answer("Unauthorized.")
                    return
                await msg.answer(get_bot_list())
            except Exception as e:
                logger.error(f"Error in bots command: {e}")

        @dp.message(Command("broadcast"))
        async def cmd_broadcast(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    await msg.answer("Unauthorized.")
                    return
                txt = msg.text.split(None, 1)
                if len(txt) < 2:
                    await msg.answer("Usage: /broadcast <message>")
                    return
                message = txt[1]
                
                # Create a snapshot of users and bots to avoid "Set changed size during iteration"
                user_list = list(user_ids)
                bot_list = list(bots.items())
                
                status_msg = await msg.answer(
                    "🚀 BROADCAST STARTING\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Total Users: {len(user_list)}\n"
                    f"🤖 Active Bots: {len(bot_list)}\n"
                    f"📨 Total Messages: {len(user_list) * len(bot_list)}\n\n"
                    "⏳ Processing..."
                )
                
                total_users = len(user_list)
                total_bots = len(bot_list)
                successful = 0
                failed = 0
                
                for bot_idx, (uname, bot_instance) in enumerate(bot_list, 1):
                    for user_idx, uid in enumerate(user_list, 1):
                        try:
                            await bot_instance.send_message(uid, message)
                            successful += 1
                        except TelegramAPIError as e:
                            failed += 1
                            logger.error(f"Failed to send to user {uid} from @{uname}: {e}")
                        except Exception as e:
                            failed += 1
                            logger.error(f"Unexpected error sending broadcast: {e}")
                        
                        if (successful + failed) % 50 == 0:
                            progress = f"({successful + failed}/{total_users * total_bots})"
                            try:
                                await dashboard_bot.edit_message_text(
                                    chat_id=msg.chat.id,
                                    message_id=status_msg.message_id,
                                    text=f"🚀 BROADCAST IN PROGRESS\n"
                                         f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                         f"📊 Total Users: {total_users}\n"
                                         f"🤖 Active Bots: {total_bots}\n"
                                         f"📨 Total Messages: {total_users * total_bots}\n\n"
                                         f"✅ Successful: {successful}\n"
                                         f"❌ Failed: {failed}\n"
                                         f"⏳ Progress: {progress}\n\n"
                                         f"📈 Success Rate: {(successful/(successful+failed)*100):.1f}%" if (successful+failed) > 0 else "N/A"
                                )
                            except Exception as e:
                                logger.error(f"Error updating status: {e}")
                
                success_rate = (successful / (successful + failed) * 100) if (successful + failed) > 0 else 0
                
                final_report = (
                    "✅ BROADCAST COMPLETED\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Total Users: {total_users}\n"
                    f"🤖 Active Bots: {total_bots}\n"
                    f"📨 Total Messages: {successful + failed}\n\n"
                    f"✅ Successful: {successful}\n"
                    f"❌ Failed: {failed}\n"
                    f"📈 Success Rate: {success_rate:.1f}%\n\n"
                    f"💰 Messages/Bot: {successful // total_bots if total_bots > 0 else 0}\n"
                    f"⏱️ Total Attempts: {successful + failed}"
                )
                
                try:
                    await dashboard_bot.edit_message_text(
                        chat_id=msg.chat.id,
                        message_id=status_msg.message_id,
                        text=final_report
                    )
                except Exception as e:
                    logger.error(f"Error sending final report: {e}")
                    await msg.answer(final_report)
                
                logger.info(f"Broadcast completed: {successful} successful, {failed} failed")
                
            except Exception as e:
                logger.error(f"Error in broadcast command: {e}")
                await msg.answer(f"❌ Broadcast Error: {str(e)}")

        @dp.message()
        async def handle_document(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    return
                if msg.document and msg.document.file_name.endswith(".txt"):
                    file_info = await dashboard_bot.get_file(msg.document.file_id)
                    file_path = file_info.file_path
                    dest = f"uploads/{msg.document.file_name}"
                    os.makedirs("uploads", exist_ok=True)
                    with open(dest, "wb") as f:
                        f.write(await dashboard_bot.download_file(file_path))
                    await msg.answer(f"File uploaded. Extracting tokens...")
                    with open(dest, "r", encoding="utf-8") as f:
                        content = f.read()
                    tokens = extract_tokens(content)
                    await msg.answer(f"Found {len(tokens)} tokens. Starting...")
                    await startup_bots(tokens)
                    await msg.answer("Upload & startup complete.")

                elif msg.text and re.match(r'^\d{6,10}:[A-Za-z0-9_-]{20,}$', msg.text.strip()):
                    token = msg.text.strip()
                    await msg.answer("Token received. Starting bot...")
                    await startup_bots([token])
                    await msg.answer("Bot started.")
            except Exception as e:
                logger.error(f"Error handling document: {e}")
                await msg.answer(f"Error: {str(e)}")

        await dp.start_polling(dashboard_bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
    finally:
        if dashboard_bot:
            try:
                if dashboard_bot.session and not dashboard_bot.session.closed:
                    await dashboard_bot.session.close()
            except Exception as e:
                logger.error(f"Error closing dashboard session: {e}")

async def main():
    try:
        load_user_ids()
        all_tokens = load_all_tokens()
        
        if not all_tokens:
            logger.warning("No tokens found!")
        
        asyncio.create_task(dashboard())
        await startup_bots(all_tokens)
        
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
