import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramAPIError, TelegramConflictError
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
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
BOTS_PER_PAGE = 50

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
broadcast_cancelled = False

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

def get_bot_list_page(page=0):
    """Get paginated bot list"""
    try:
        bot_list = list(bots.keys())
        if not bot_list:
            return "No bots running", None
        
        total_bots = len(bot_list)
        total_pages = (total_bots + BOTS_PER_PAGE - 1) // BOTS_PER_PAGE
        
        start_idx = page * BOTS_PER_PAGE
        end_idx = min(start_idx + BOTS_PER_PAGE, total_bots)
        page_bots = bot_list[start_idx:end_idx]
        
        # Create bot list with user counts
        bot_text = f"🤖 Bot List (Page {page+1}/{total_pages})\n"
        bot_text += f"📊 Total Bots: {total_bots}\n"
        bot_text += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for idx, uname in enumerate(page_bots, start=start_idx+1):
            user_count = len(bot_stats.get(uname, {}).get("users", set()))
            bot_text += f"{idx}. @{uname} - 👥 {user_count} users\n"
        
        # Create navigation buttons
        buttons = []
        nav_row = []
        
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="◀️ Previous", callback_data=f"botlist_{page-1}"))
        
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"botlist_{page+1}"))
        
        if nav_row:
            buttons.append(nav_row)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
        
        return bot_text, keyboard
        
    except Exception as e:
        logger.error(f"Error getting bot list: {e}")
        return "Error getting bot list", None

def get_stats():
    try:
        total_users = len(user_ids)
        total_bots = len(bots)
        total_messages = sum(stat["messages"] for stat in bot_stats.values())
        
        # Calculate per-bot stats
        bot_user_stats = []
        for uname, stats in bot_stats.items():
            bot_user_stats.append((uname, len(stats.get("users", set()))))
        
        return (
            f"📊 SYSTEM STATISTICS\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Bots Running: {total_bots}\n"
            f"👥 Total Users (all bots): {total_users}\n"
            f"📨 Total Messages: {total_messages}\n"
            f"💻 {get_resource_usage_str()}"
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
                    "🎛️ DASHBOARD COMMANDS\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "/stats - Show statistics\n"
                    "/bots - List all bots (paginated)\n"
                    "/broadcast <msg> - Broadcast to all users\n"
                    "\n📤 Send a .txt file to upload tokens."
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
                bot_text, keyboard = get_bot_list_page(0)
                await msg.answer(bot_text, reply_markup=keyboard)
            except Exception as e:
                logger.error(f"Error in bots command: {e}")

        @dp.callback_query(lambda c: c.data.startswith("botlist_"))
        async def handle_bot_pagination(callback: CallbackQuery):
            try:
                if callback.from_user.id != ADMIN_ID:
                    await callback.answer("Unauthorized.", show_alert=True)
                    return
                
                page = int(callback.data.split("_")[1])
                bot_text, keyboard = get_bot_list_page(page)
                
                await callback.message.edit_text(bot_text, reply_markup=keyboard)
                await callback.answer()
            except Exception as e:
                logger.error(f"Error in pagination: {e}")
                await callback.answer("Error loading page", show_alert=True)

        @dp.callback_query(lambda c: c.data == "cancel_broadcast")
        async def handle_cancel_broadcast(callback: CallbackQuery):
            global broadcast_cancelled
            try:
                if callback.from_user.id != ADMIN_ID:
                    await callback.answer("Unauthorized.", show_alert=True)
                    return
                
                broadcast_cancelled = True
                await callback.answer("🛑 Broadcast cancellation requested!", show_alert=True)
                await callback.message.edit_text(
                    callback.message.text + "\n\n🛑 CANCELLATION REQUESTED..."
                )
            except Exception as e:
                logger.error(f"Error cancelling broadcast: {e}")

        @dp.message(Command("broadcast"))
        async def cmd_broadcast(msg: types.Message):
            global broadcast_cancelled
            try:
                if msg.from_user.id != ADMIN_ID:
                    await msg.answer("Unauthorized.")
                    return
                
                txt = msg.text.split(None, 1)
                if len(txt) < 2:
                    await msg.answer("Usage: /broadcast <message>")
                    return
                
                message = txt[1]
                
                if not bots:
                    await msg.answer("❌ No bots available for broadcast!")
                    return
                
                # Reset cancel flag
                broadcast_cancelled = False
                
                # Cancel button
                cancel_btn = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🛑 Cancel Broadcast", callback_data="cancel_broadcast")]
                ])
                
                # Calculate total messages to send
                total_messages = 0
                for uname in bots.keys():
                    bot_users = bot_stats.get(uname, {}).get("users", set())
                    total_messages += len(bot_users)
                
                status_msg = await msg.answer(
                    "🚀 BROADCAST STARTING\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🤖 Active Bots: {len(bots)}\n"
                    f"📨 Total Messages to Send: {total_messages}\n\n"
                    "⏳ Processing...",
                    reply_markup=cancel_btn
                )
                
                total_successful = 0
                total_failed = 0
                bots_processed = 0
                
                # Each bot sends to its own users
                for uname, bot_instance in bots.items():
                    if broadcast_cancelled:
                        await dashboard_bot.edit_message_text(
                            chat_id=msg.chat.id,
                            message_id=status_msg.message_id,
                            text=f"🛑 BROADCAST CANCELLED\n"
                                 f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                 f"✅ Successful: {total_successful}\n"
                                 f"❌ Failed: {total_failed}\n"
                                 f"🤖 Bots Processed: {bots_processed}/{len(bots)}"
                        )
                        break
                    
                    # Get users for this specific bot
                    bot_users = list(bot_stats.get(uname, {}).get("users", set()))
                    
                    if not bot_users:
                        continue
                    
                    successful = 0
                    failed = 0
                    
                    for uid in bot_users:
                        if broadcast_cancelled:
                            break
                        
                        try:
                            await bot_instance.send_message(uid, message)
                            successful += 1
                            total_successful += 1
                        except TelegramAPIError as e:
                            failed += 1
                            total_failed += 1
                            logger.error(f"Failed to send to user {uid} from @{uname}: {e}")
                        except Exception as e:
                            failed += 1
                            total_failed += 1
                            logger.error(f"Unexpected error: {e}")
                        
                        # Update every 50 messages
                        if (total_successful + total_failed) % 50 == 0:
                            try:
                                progress = f"({total_successful + total_failed}/{total_messages})"
                                success_rate = (total_successful/(total_successful+total_failed)*100) if (total_successful+total_failed) > 0 else 0
                                
                                await dashboard_bot.edit_message_text(
                                    chat_id=msg.chat.id,
                                    message_id=status_msg.message_id,
                                    text=f"🚀 BROADCAST IN PROGRESS\n"
                                         f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                         f"🤖 Current Bot: @{uname}\n"
                                         f"📊 Bots Processed: {bots_processed+1}/{len(bots)}\n"
                                         f"📨 Total Messages: {total_messages}\n\n"
                                         f"✅ Successful: {total_successful}\n"
                                         f"❌ Failed: {total_failed}\n"
                                         f"⏳ Progress: {progress}\n"
                                         f"📈 Success Rate: {success_rate:.1f}%",
                                    reply_markup=cancel_btn
                                )
                            except Exception as e:
                                logger.error(f"Error updating status: {e}")
                        
                        # Rate limiting
                        if (successful + failed) % 30 == 0:
                            await asyncio.sleep(1)
                    
                    bots_processed += 1
                    logger.info(f"Bot @{uname}: {successful} sent, {failed} failed")
                
                if broadcast_cancelled:
                    return
                
                success_rate = (total_successful / (total_successful + total_failed) * 100) if (total_successful + total_failed) > 0 else 0
                
                final_report = (
                    "✅ BROADCAST COMPLETED\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🤖 Bots Used: {bots_processed}\n"
                    f"📨 Total Messages: {total_successful + total_failed}\n\n"
                    f"✅ Successful: {total_successful}\n"
                    f"❌ Failed: {total_failed}\n"
                    f"📈 Success Rate: {success_rate:.1f}%"
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
                
                logger.info(f"Broadcast completed: {total_successful} successful, {total_failed} failed")
                
            except Exception as e:
                logger.error(f"Error in broadcast command: {e}")
                await msg.answer(f"❌ Broadcast Error: {str(e)}")

        @dp.message()
        async def handle_document(msg: types.Message):
            try:
                if msg.from_user.id != ADMIN_ID:
                    return
                
                if msg.document and msg.document.file_name.endswith(".txt"):
                    file = await dashboard_bot.get_file(msg.document.file_id)
                    dest = f"uploads/{msg.document.file_name}"
                    os.makedirs("uploads", exist_ok=True)
                    
                    await dashboard_bot.download_file(file.file_path, dest)
                    
                    await msg.answer(f"📥 File uploaded. Extracting tokens...")
                    
                    with open(dest, "r", encoding="utf-8") as f:
                        content = f.read()
                    
                    tokens = extract_tokens(content)
                    
                    if not tokens:
                        await msg.answer("❌ No valid tokens found in file!")
                        return
                    
                    await msg.answer(f"✅ Found {len(tokens)} tokens. Starting bots...")
                    await startup_bots(tokens)
                    await msg.answer(f"🎉 Upload complete! {len(tokens)} bots started.")

                elif msg.text and re.match(r'^\d{6,10}:[A-Za-z0-9_-]{20,}$', msg.text.strip()):
                    token = msg.text.strip()
                    await msg.answer("🔄 Token received. Starting bot...")
                    success = await startup_bots([token])
                    if success:
                        await msg.answer("✅ Bot started successfully!")
                    else:
                        await msg.answer("❌ Failed to start bot!")
                        
            except Exception as e:
                logger.error(f"Error handling document: {e}")
                await msg.answer(f"❌ Error: {str(e)}")

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
