import telebot
import threading
from pathlib import Path
import time
from datetime import datetime
import logging
import re
import os

# Suppress TeleBot logging
logging.getLogger("telebot").setLevel(logging.CRITICAL)
logging.getLogger("telebot.apihelper").setLevel(logging.CRITICAL)
telebot.logger.disabled = True

TOKEN_FILES = ["token1.txt", "token2.txt", "token3.txt", "uploaded_tokens.txt"]
CONFIG_FILE = "config.txt"
USER_IDS_FILE = "user_ids.txt"
UPLOAD_DIR = "uploads"

MAIN_BOT_TOKEN = "7557269432:AAED0WgIrklGg9JNsI1hpkYR5pH8VRw5Kjc"
ADMIN_ID = 5706788169

CUSTOM_REPLY = """🎬 MOVIE & ENTERTAINMENT HUB 🍿  
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

def load_config():
    global MAIN_BOT_TOKEN, ADMIN_ID
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith("MAIN_BOT_TOKEN:"):
                    token = line.split(":", 1)[1].strip()
                    if token:
                        MAIN_BOT_TOKEN = token
                elif line.startswith("ADMIN_ID:"):
                    try:
                        admin = int(line.split(":", 1)[1].strip())
                        if admin:
                            ADMIN_ID = admin
                    except ValueError:
                        pass
    except FileNotFoundError:
        pass

def create_sample_config():
    if not Path(CONFIG_FILE).exists():
        sample_config = f"""MAIN_BOT_TOKEN:{MAIN_BOT_TOKEN}
ADMIN_ID:{ADMIN_ID}"""
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            f.write(sample_config)

def extract_tokens(text):
    pattern = r'\d{6,10}:[A-Za-z0-9_-]{20,}'
    tokens = re.findall(pattern, text)
    return tokens

def get_bot_username(token):
    try:
        test_bot = telebot.TeleBot(token, threaded=True)
        test_bot.delete_webhook()
        me = test_bot.get_me()
        return me.username if me.username else f"bot_{me.id}"
    except Exception:
        return None

def load_all_tokens():
    all_tokens = []
    all_bot_names = []
    total_found = 0
    
    print("\n[SCANNING TOKEN FILES]")
    print("-" * 60)
    
    for token_file in TOKEN_FILES:
        if not Path(token_file).exists():
            print(f"[{token_file}] Not found (skipped)")
            continue
        
        try:
            with open(token_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            found_tokens = extract_tokens(content)
            file_total = len(found_tokens)
            total_found += file_total
            
            print(f"[{token_file}] Found {file_total} tokens")
            
            for idx, token in enumerate(found_tokens, 1):
                token_clean = token.strip()
                print(f"  [{idx}/{file_total}] Loading token... ", end='', flush=True)
                
                username = get_bot_username(token_clean)
                if username:
                    print(f"@{username}")
                    all_tokens.append(token_clean)
                    all_bot_names.append(username)
                else:
                    print(f"Invalid/Unauthorized")
        
        except Exception as e:
            print(f"[{token_file}] Error: {str(e)[:30]}")
    
    print("-" * 60)
    print(f"Total tokens found: {total_found}")
    print(f"Valid tokens: {len(all_tokens)}\n")
    
    return all_tokens, all_bot_names

def load_user_ids():
    if Path(USER_IDS_FILE).exists():
        with open(USER_IDS_FILE, 'r') as f:
            for line in f:
                chat_id = line.strip()
                if chat_id.isdigit():
                    user_ids.add(int(chat_id))

def save_user_id(chat_id):
    if chat_id not in user_ids:
        user_ids.add(chat_id)
        with open(USER_IDS_FILE, 'a') as f:
            f.write(f"{chat_id}\n")

dashboard_data = {
    "total_tokens": 0,
    "running_bots": 0,
    "failed_bots": 0,
    "invalid_bots": 0,
    "start_time": datetime.now(),
    "bot_list": [],
    "messages_received": 0
}

def setup_bot(token, bot_name):
    try:
        bot = telebot.TeleBot(token, threaded=True)
        try:
            bot.delete_webhook()
        except:
            pass
        
        @bot.message_handler(func=lambda message: True)
        def handle_message(message):
            dashboard_data['messages_received'] += 1
            save_user_id(message.chat.id)
            try:
                bot.send_message(message.chat.id, CUSTOM_REPLY)
            except:
                pass
        
        return bot, True
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg:
            dashboard_data['invalid_bots'] += 1
            return None, False
        elif "409" in error_msg:
            return None, False
        else:
            return None, False

def get_uptime():
    delta = datetime.now() - dashboard_data['start_time']
    hours = delta.seconds // 3600
    minutes = (delta.seconds % 3600) // 60
    return f"{hours}h {minutes}m"

def run_bot_safe(bot, bot_name):
    retry_count = 0
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            error_str = str(e)
            if "401" in error_str:
                print(f"[{bot_name}] Stopped: Token invalid/expired")
                dashboard_data['invalid_bots'] += 1
                dashboard_data['running_bots'] -= 1
                break
            elif "409" in error_str:
                retry_count += 1
                if retry_count > 3:
                    print(f"[{bot_name}] Failed after 3 retries (webhook conflict)")
                    dashboard_data['failed_bots'] += 1
                    dashboard_data['running_bots'] -= 1
                    break
                print(f"[{bot_name}] Webhook conflict, retry {retry_count}/3...")
                time.sleep(5)
            else:
                retry_count += 1
                if retry_count > 5:
                    print(f"[{bot_name}] Failed after 5 retries")
                    dashboard_data['failed_bots'] += 1
                    dashboard_data['running_bots'] -= 1
                    break
                time.sleep(10)

def broadcast_message(text):
    print(f"[BROADCAST] Sending to {len(user_ids)} users...")
    sent_count = 0
    for bot_info in dashboard_data['bot_list']:
        bot = bot_info.get('bot_instance')
        if not bot:
            continue
        for chat_id in user_ids:
            try:
                bot.send_message(chat_id, text)
                sent_count += 1
            except Exception:
                pass
    print(f"[BROADCAST] Sent {sent_count} messages.")

def process_uploaded_tokens(bot, chat_id, content):
    tokens = extract_tokens(content)
    total = len(tokens)
    if total == 0:
        bot.send_message(chat_id, "No tokens found in your upload.")
        return
    progress_msg = bot.send_message(chat_id, "Upload Progress: [----------] 0%")
    valid = 0
    failed = 0
    for idx, token in enumerate(tokens, 1):
        # Progress bar
        percent = int((idx / total) * 100)
        bar_len = 10
        filled_len = int(bar_len * percent // 100)
        bar = "[" + "#" * filled_len + "-" * (bar_len - filled_len) + f"] {percent}%"
        try:
            bot.edit_message_text(
                f"Upload Progress: {bar}\nProcessing token {idx}/{total}...",
                chat_id=chat_id, message_id=progress_msg.message_id)
        except Exception:
            pass
        username = None
        retries = 0
        while retries < 3:
            try:
                username = get_bot_username(token)
                break
            except Exception:
                retries += 1
                time.sleep(2 * retries)
        if username:
            valid += 1
            with open("uploaded_tokens.txt", "a") as f:
                f.write(token + "\n")
            bot_instance, success = setup_bot(token, username)
            if success:
                dashboard_data['running_bots'] += 1
                dashboard_data['bot_list'].append({
                    'name': username,
                    'status': 'running',
                    'bot_instance': bot_instance
                })
                thread = threading.Thread(
                    target=lambda b=bot_instance, name=username: run_bot_safe(b, name),
                    daemon=True
                )
                thread.start()
            else:
                failed += 1
        else:
            failed += 1
        time.sleep(0.5)
    bot.edit_message_text(
        f"Upload Complete!\nValid tokens: {valid}\nFailed tokens: {failed}\nTotal processed: {total}\nTotal users: {len(user_ids)}",
        chat_id=chat_id, message_id=progress_msg.message_id
    )

def setup_main_bot():
    main_bot = telebot.TeleBot(MAIN_BOT_TOKEN, threaded=True)
    
    @main_bot.message_handler(commands=['start'])
    def start(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        text = "MULTI-BOT DASHBOARD\n\n"
        text += "Commands:\n"
        text += "/status - Bot status\n"
        text += "/stats - Statistics\n"
        text += "/bots - List bots\n"
        text += "/broadcast <your message> - Send message to all users\n"
        text += "Send a .txt file or a bot token to upload and activate bots.\n"
        main_bot.send_message(message.chat.id, text)
    
    @main_bot.message_handler(commands=['status'])
    def status(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        text = "BOT STATUS\n"
        text += "-" * 40 + "\n"
        text += f"Running Bots: {dashboard_data['running_bots']}\n"
        text += f"Failed Bots: {dashboard_data['failed_bots']}\n"
        text += f"Invalid Bots: {dashboard_data['invalid_bots']}\n"
        text += f"Total Tokens: {dashboard_data['total_tokens']}\n"
        text += f"Messages Received: {dashboard_data['messages_received']}\n"
        text += f"Total Users: {len(user_ids)}\n"
        text += f"Uptime: {get_uptime()}\n"
        main_bot.send_message(message.chat.id, text)
    
    @main_bot.message_handler(commands=['bots'])
    def bots_list(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        text = "RUNNING BOTS\n"
        text += "-" * 40 + "\n"
        for bot_info in dashboard_data['bot_list']:
            text += f"@{bot_info['name']}\n"
        main_bot.send_message(message.chat.id, text)
    
    @main_bot.message_handler(commands=['stats'])
    def stats(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        text = "DETAILED STATS\n"
        text += "-" * 40 + "\n"
        text += f"Total Bots: {dashboard_data['total_tokens']}\n"
        text += f"Active: {dashboard_data['running_bots']}\n"
        text += f"Failed: {dashboard_data['failed_bots']}\n"
        text += f"Invalid: {dashboard_data['invalid_bots']}\n"
        text += f"Total Messages: {dashboard_data['messages_received']}\n"
        text += f"Total Users: {len(user_ids)}\n"
        text += f"Started: {dashboard_data['start_time'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        text += f"Running: {get_uptime()}\n"
        main_bot.send_message(message.chat.id, text)
    
    @main_bot.message_handler(commands=['broadcast'])
    def broadcast_cmd(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            main_bot.send_message(message.chat.id, "Usage: /broadcast <your message>")
            return
        broadcast_text = args[1]
        threading.Thread(target=broadcast_message, args=(broadcast_text,), daemon=True).start()
        main_bot.send_message(message.chat.id, "Broadcast started!")

    @main_bot.message_handler(content_types=['document'])
    def handle_document(message):
        if message.from_user.id != ADMIN_ID:
            main_bot.send_message(message.chat.id, "Unauthorized!")
            return
        file_info = main_bot.get_file(message.document.file_id)
        file_path = file_info.file_path
        if not os.path.exists(UPLOAD_DIR):
            os.makedirs(UPLOAD_DIR)
        download_path = os.path.join(UPLOAD_DIR, message.document.file_name)
        with open(download_path, 'wb') as f:
            f.write(main_bot.download_file(file_path))
        main_bot.send_message(message.chat.id, f"File '{message.document.file_name}' uploaded. Extracting tokens...")
        with open(download_path, 'r', encoding='utf-8') as f:
            content = f.read()
        threading.Thread(target=process_uploaded_tokens, args=(main_bot, message.chat.id, content), daemon=True).start()
    
    @main_bot.message_handler(content_types=['text'])
    def handle_bot_token(message):
        if message.from_user.id != ADMIN_ID:
            return
        pattern = r'^\d{6,10}:[A-Za-z0-9_-]{20,}$'
        if re.match(pattern, message.text.strip()):
            main_bot.send_message(message.chat.id, "Token received! Validating and loading bot...")
            threading.Thread(target=process_uploaded_tokens, args=(main_bot, message.chat.id, message.text.strip()), daemon=True).start()

    return main_bot

def main():
    print("\n" + "="*60)
    print("MULTI-BOT SYSTEM STARTING".center(60))
    print("="*60)
    
    create_sample_config()
    load_config()
    load_user_ids()
    
    print("\n[CONFIG]")
    print(f"Main Bot Token: {MAIN_BOT_TOKEN[:20]}...")
    print(f"Admin ID: {ADMIN_ID}")
    print(f"Custom Message: {CUSTOM_REPLY}\n")
    
    tokens, bot_names = load_all_tokens()
    if not tokens:
        print("[ERROR] No valid tokens found!")
        return
    dashboard_data['total_tokens'] = len(tokens)
    
    print("[LOADING BOTS]")
    print("-" * 60)
    threads = []
    for idx, (token, bot_name) in enumerate(zip(tokens, bot_names), 1):
        try:
            print(f"[{idx}/{len(tokens)}] @{bot_name}: ", end='', flush=True)
            bot, success = setup_bot(token, bot_name)
            if success:
                dashboard_data['running_bots'] += 1
                dashboard_data['bot_list'].append({
                    'name': bot_name,
                    'status': 'running',
                    'bot_instance': bot
                })
                thread = threading.Thread(
                    target=lambda b=bot, name=bot_name: run_bot_safe(b, name),
                    daemon=True
                )
                thread.start()
                threads.append(thread)
                print("OK")
            else:
                dashboard_data['failed_bots'] += 1
                print("FAILED")
        except Exception:
            dashboard_data['failed_bots'] += 1
            print(f"ERROR")
    
    print("-" * 60)
    print(f"Running: {dashboard_data['running_bots']} | Failed: {dashboard_data['failed_bots']} | Invalid: {dashboard_data['invalid_bots']}\n")
    
    print("[DASHBOARD BOT]")
    try:
        main_bot = setup_main_bot()
        main_thread = threading.Thread(
            target=lambda: run_bot_safe(main_bot, "DASHBOARD_BOT"),
            daemon=True
        )
        main_thread.start()
        print("Status: OK")
        print(f"Admin ID: {ADMIN_ID}")
        print("Commands: /status, /stats, /bots, /broadcast <your message>\n")
        print("="*60)
        print("ALL SYSTEMS RUNNING".center(60))
        print("="*60 + "\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("SHUTTING DOWN".center(60))
        print("="*60)
    except Exception as e:
        print(f"\n[ERROR] Main Bot: {str(e)}")

if __name__ == "__main__":
    main()
