import asyncio
import os
import re
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import RetryAfter, Forbidden, BadRequest, TelegramError

import config
import database as db
import api_utils

users_state = {}
admin_states = {}

def is_admin(user_id: int) -> bool:
    return user_id in config.ADMINS


# ========================================================
# 📢 FAST NON-BLOCKING BROADCAST SYSTEM
# ========================================================

BROADCAST_CONCURRENCY = 15
BROADCAST_RETRY_LIMIT = 5

broadcast_running = False

async def send_to_user(context, uid: int, source_chat_id: int, message_id: int, delay: int):
    retries = 0
    while retries < BROADCAST_RETRY_LIMIT:
        try:
            sent = await context.bot.copy_message(
                chat_id=uid,
                from_chat_id=source_chat_id,
                message_id=message_id
            )
            # Auto delete
            context.application.create_task(
                delete_message_after(context, uid, sent.message_id, delay)
            )
            return "success", uid

        # Telegram flood control
        except RetryAfter as e:
            retries += 1
            wait_time = int(e.retry_after) + 1
            await asyncio.sleep(wait_time)

        # User blocked bot / chat inaccessible
        except Forbidden:
            return "dead", uid

        # Permanent Telegram errors
        except BadRequest as e:
            error_text = str(e).lower()
            permanent_errors = (
                "chat not found",
                "user not found",
                "bot was blocked",
                "user is deactivated",
                "forbidden"
            )
            if any(error in error_text for error in permanent_errors):
                return "dead", uid
            return "failed", uid

        # Other Telegram errors
        except TelegramError:
            retries += 1
            if retries >= BROADCAST_RETRY_LIMIT:
                return "failed", uid
            await asyncio.sleep(min(2 ** retries, 10))

        # Unknown errors
        except Exception:
            retries += 1
            if retries >= BROADCAST_RETRY_LIMIT:
                return "failed", uid
            await asyncio.sleep(min(2 ** retries, 10))

    return "failed", uid


async def run_broadcast(context, admin_chat_id: int, users: list, source_chat_id: int, message_id: int, delay: int):
    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    sent_count = 0
    dead_count = 0
    failed_count = 0
    dead_users = []

    async def worker(uid):
        nonlocal sent_count, dead_count, failed_count
        async with semaphore:
            status, user_id = await send_to_user(
                context=context, uid=uid, source_chat_id=source_chat_id,
                message_id=message_id, delay=delay
            )
            if status == "success":
                sent_count += 1
            elif status == "dead":
                dead_count += 1
                dead_users.append(user_id)
            else:
                failed_count += 1

    tasks = [asyncio.create_task(worker(uid)) for uid in users]
    await asyncio.gather(*tasks)

    for uid in dead_users:
        try: db.remove_user(uid)
        except Exception: pass

    try:
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "✅ <b>Broadcast Finished</b>\n\n"
                f"📨 Sent: <b>{sent_count}</b>\n"
                f"💀 Dead/Blocked Removed: <b>{dead_count}</b>\n"
                f"⚠️ Failed: <b>{failed_count}</b>\n"
                f"👥 Total Processed: <b>{len(users)}</b>"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass


async def start_background_broadcast(context, admin_chat_id: int, users: list, source_chat_id: int, message_id: int, delay: int):
    global broadcast_running

    if broadcast_running:
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text="⚠️ A broadcast is already running."
        )
        return

    broadcast_running = True
    try:
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "🚀 <b>Broadcast Started</b>\n\n"
                f"👥 Live Users: <b>{len(users)}</b>\n"
                "⚡ Running in background..."
            ),
            parse_mode="HTML"
        )
        await run_broadcast(
            context=context, admin_chat_id=admin_chat_id, users=users,
            source_chat_id=source_chat_id, message_id=message_id, delay=delay
        )
    finally:
        broadcast_running = False


async def send_file_to_user(context, uid, file_bytes, file_path, delay):
    retries = 0
    while retries < BROADCAST_RETRY_LIMIT:
        try:
            if file_path.lower().endswith(".mp4"):
                sent = await context.bot.send_video(chat_id=uid, video=BytesIO(file_bytes))
            else:
                sent = await context.bot.send_document(chat_id=uid, document=BytesIO(file_bytes))
            
            context.application.create_task(delete_message_after(context, uid, sent.message_id, delay))
            return "success", uid

        except RetryAfter as e:
            retries += 1
            await asyncio.sleep(int(e.retry_after) + 1)
        except Forbidden:
            return "dead", uid
        except BadRequest as e:
            error_text = str(e).lower()
            permanent_errors = ("chat not found", "user not found", "bot was blocked", "user is deactivated", "forbidden")
            if any(error in error_text for error in permanent_errors):
                return "dead", uid
            return "failed", uid
        except Exception:
            retries += 1
            if retries >= BROADCAST_RETRY_LIMIT:
                return "failed", uid
            await asyncio.sleep(min(2 ** retries, 10))
    return "failed", uid


async def run_file_broadcast(context, admin_chat_id, users, file_path, file_bytes, delay):
    semaphore = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    sent_count = 0
    dead_count = 0
    failed_count = 0
    dead_users = []

    async def worker(uid):
        nonlocal sent_count, dead_count, failed_count
        async with semaphore:
            status, user_id = await send_file_to_user(context=context, uid=uid, file_bytes=file_bytes, file_path=file_path, delay=delay)
            if status == "success":
                sent_count += 1
            elif status == "dead":
                dead_count += 1
                dead_users.append(user_id)
            else:
                failed_count += 1

    tasks = [asyncio.create_task(worker(uid)) for uid in users]
    await asyncio.gather(*tasks)

    for uid in dead_users:
        try: db.remove_user(uid)
        except Exception: pass

    try:
        await context.bot.send_message(
            chat_id=admin_chat_id,
            text=(
                "✅ <b>File Broadcast Finished</b>\n\n"
                f"📨 Sent: <b>{sent_count}</b>\n"
                f"💀 Dead/Blocked Removed: <b>{dead_count}</b>\n"
                f"⚠️ Failed: <b>{failed_count}</b>\n"
                f"👥 Total Processed: <b>{len(users)}</b>"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass


# ========================================================
# 🎨 DYNAMIC COLOR LOGIC
# ========================================================

def get_menu_style(btn_id: int) -> str:
    color = db.get_setting(f"menu_color_{btn_id}")
    if color:
        color = color.upper()
        if color == "R": return "danger"    # Red
        if color == "G": return "success"   # Green
        if color == "B": return "primary"   # Blue

    # Default Pattern
    if btn_id == 1: return "success"        
    if btn_id == 2: return "primary"        
    if btn_id == 3: return "danger"         
    
    if btn_id % 2 == 0:
        return "primary" 
    else:
        return "success" 

def get_premium_style(btn_id: int) -> str:
    color = db.get_setting(f"btn_color_{btn_id}")
    if color:
        color = color.upper()
        if color == "R": return "danger"    
        if color == "G": return "success"   
        if color == "B": return "primary"   

    if btn_id % 2 != 0:
        return "success" 
    else:
        return "primary" 


# ========================================================
# 🎨 BOT BUTTONS & MENUS
# ========================================================

def main_menu():
    demo_link = db.get_setting("demo_link")
    contact_link = db.get_setting("contact_link")
    m_btns = db.get_menu_buttons()

    keyboard = []
    for btn_id, (text, link) in m_btns.items():
        btn_style = get_menu_style(btn_id)

        if btn_id == 1:
            keyboard.append([InlineKeyboardButton(text, callback_data="premium", style=btn_style)])
        elif btn_id == 2:
            url = link if link else demo_link
            keyboard.append([InlineKeyboardButton(text, url=url, style=btn_style)])
        elif btn_id == 3:
            url = link if link else contact_link
            keyboard.append([InlineKeyboardButton(text, url=url, style=btn_style)])
        else:
            if link:
                keyboard.append([InlineKeyboardButton(text, url=link, style=btn_style)])
            else:
                keyboard.append([InlineKeyboardButton(text, callback_data=f"custom_btn_{btn_id}", style=btn_style)])

    return InlineKeyboardMarkup(keyboard)

def premium_menu():
    btns = db.get_premium_buttons()
    keyboard = []
    for btn in btns:
        btn_id = btn[0]
        btn_style = get_premium_style(btn_id)
        keyboard.append([InlineKeyboardButton(f"{btn[1]} - ₹{btn[2]}", callback_data=f"plan_{btn_id}", style=btn_style)])
    keyboard.append([InlineKeyboardButton("⬅️ BACK", callback_data="back_main", style="danger")])
    return InlineKeyboardMarkup(keyboard)

def payment_menu(btn_id):
    keyboard = [
        [InlineKeyboardButton("✅ VERIFY PAYMENT", callback_data=f"verify_{btn_id}", style="success")],
        [InlineKeyboardButton("⬅️ BACK", callback_data="premium", style="danger")]
    ]
    return InlineKeyboardMarkup(keyboard)

def admin_menu():
    keyboard = [
        [
            InlineKeyboardButton("📊 Dashboard", callback_data="admin_dashboard"),
            InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")
        ],
        [
            InlineKeyboardButton("📦 Manage Plans", callback_data="admin_manage_button"),
            InlineKeyboardButton("💳 Edit UPI", callback_data="admin_edit_upi")
        ],
        [
            InlineKeyboardButton("📝 Edit Welcome", callback_data="admin_edit_welcome"),
            InlineKeyboardButton("💎 Edit Premium", callback_data="admin_edit_premium")
        ],
        [
            InlineKeyboardButton("🔗 Manage Demo", callback_data="admin_manage_demo"),
            InlineKeyboardButton("⚙️ Edit Menu", callback_data="admin_edit_menu")
        ],
        [
            InlineKeyboardButton("✅ Set Approved", callback_data="admin_set_approved"),
            InlineKeyboardButton("📞 Contact Link", callback_data="admin_contact_link")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


async def delete_message_after(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 30):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


# ========================================================
# 🚀 USER START & WELCOME LOGIC
# ========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    db.add_user(user_id)
    users_state[user_id] = {"status": "idle", "rejects": 0}

    w_msg_id = db.get_setting("welcome_msg_id")
    w_chat_id = db.get_setting("welcome_chat_id")

    if update.message:
        if w_msg_id and w_chat_id:
            try:
                await context.bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=int(w_chat_id),
                    message_id=int(w_msg_id),
                    reply_markup=main_menu()
                )
                return
            except Exception:
                pass
        
        await update.message.reply_text("Welcome to the Bot!", reply_markup=main_menu())


# ========================================================
# 📜 ALL COMMANDS GUIDE (/cmds)
# ========================================================

async def list_all_cmds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    cmds_guide = """
🛠 <b>COMPLETE ADMIN COMMANDS GUIDE (A-Z)</b>

🔹 <b>/adddata</b>
👉 Add users manually: <code>/adddata 123456 789012</code>
👉 From file: Reply to .txt file with <code>/adddata</code>

🔹 <b>/addlink</b>
👉 <code>/addlink [LINK] [BUTTON_ID]</code>

🔹 <b>/admin</b> or <b>/dashboard</b>
👉 Open Admin Panel

🔹 <b>/broadcast</b> or <b>/sendall</b>
👉 Reply to msg: <code>/broadcast [60]</code> (60s auto-delete)
👉 Local file: <code>/broadcast video.mp4 [120]</code>
<i>*Without time brackets, defaults to 6 hours.</i>

🔹 <b>/button</b>
👉 <code>/button VIP PLAN [99] [1]</code>

🔹 <b>/buttoncolor</b> (🌟 NEW)
👉 Change Premium Button Color: <code>/buttoncolor [BUTTON_ID] [R/G/B]</code>
👉 Example: <code>/buttoncolor [2] [G]</code>

🔹 <b>/cmds</b>
👉 Show this guide

🔹 <b>/database</b>
👉 Download User Database (.txt)

🔹 <b>/demo</b> & <b>/link</b>
👉 <code>/demo https://t.me/your_demo_channel</code>
👉 <code>/link https://t.me/your_contact_link</code>

🔹 <b>/menu</b>
👉 With Link: <code>/menu [MY CHANNEL][4][https://t.me/example]</code>
👉 Without Link: <code>/menu [MY CHANNEL][4]</code>

🔹 <b>/menucolor</b> (🌟 NEW)
👉 Change Main Menu Color: <code>/menucolor [BUTTON_ID] [R/G/B]</code>
👉 Example: <code>/menucolor [5] [R]</code>

🔹 <b>/remove</b>
👉 Remove menu button: <code>/remove 4</code>

🔹 <b>/save</b> & <b>/fset</b>
👉 Backup Bot Settings: <code>/save</code>
👉 Restore Settings: Reply to the backup file with <code>/fset</code>

🔹 <b>/setapprove</b>
👉 Reply to approval message/video: <code>/setapprove</code>

🔹 <b>/setupi</b>
👉 <code>/setupi your_upi@ybl</code>

🔹 <b>/setwelcome</b> & <b>/setpremium</b>
👉 Reply to message/photo: <code>/setwelcome</code>
👉 Reply to message/photo: <code>/setpremium</code>

🔹 <b>/sms</b>
👉 Reply to msg: <code>/sms 123456789 [60]</code> (60s auto-delete)
    """
    await update.message.reply_text(cmds_guide, parse_mode='HTML')


# ========================================================
# 🛠 INTERACTIVE ADMIN PANEL & COMMANDS
# ========================================================

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return
    reply_markup = admin_menu()
    await update.message.reply_text("🛠 **Admin Control Panel**", reply_markup=reply_markup, parse_mode='Markdown')

async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if not is_admin(user_id):
        return

    if data == 'admin_edit_upi':
        admin_states[user_id] = 'WAITING_FOR_UPI'
        await query.message.reply_text("WRITE DOWN 👇 YOUR UPI\nexample: yourupi@fam")
        
    elif data == 'admin_broadcast':
        admin_states[user_id] = 'WAITING_FOR_BROADCAST'
        await query.message.reply_text("SEND BROADCAST MASSAGE (TEXT , VIDEO, IMAGES) 👇")
        
    elif data == 'admin_edit_welcome':
        admin_states[user_id] = 'WAITING_FOR_WELCOME'
        await query.message.reply_text("SEND WELCOME MASSAGE 👇\n(You can send text, or a photo with caption. It will be saved exactly as you send it!)")

    elif data == 'admin_edit_premium':
        admin_states[user_id] = 'WAITING_FOR_PREMIUM'
        await query.message.reply_text("SEND PREMIUM MASSAGE 👇\n(You can send text, or a photo like 1000275769.jpg with a caption!)")
        
    elif data == 'admin_manage_button':
        admin_states[user_id] = 'WAITING_FOR_MANAGE_BUTTON'
        await query.message.reply_text("CUSTOMIZE YOUR BOT BUTTON\n\nSEND WHICH BUTTON YOU WANT TO SAVE\nExample: VIP VIP Plan 1 [99] [1]")
        
    elif data == 'admin_edit_menu':
        admin_states[user_id] = 'WAITING_FOR_EDIT_MENU'
        await query.message.reply_text("CUSTOMIZE YOUR BOT BUTTON\n\nSEND WHICH BUTTON YOU WANT TO SAVE\nWith Link Example: `[MY CHANNEL][4][https://t.me/example]`\nWithout Link Example: `[MY CHANNEL][4]`", parse_mode="Markdown")
        
    elif data == 'admin_manage_demo':
        admin_states[user_id] = 'WAITING_FOR_DEMO'
        await query.message.reply_text("WRITE DOWN 👇 LINK\nexample: https://t.me/+TJ2DyxyVX3BlMzg1")
        
    elif data == 'admin_contact_link':
        admin_states[user_id] = 'WAITING_FOR_CONTACT'
        await query.message.reply_text("WRITE DOWN 👇 LINK\nexample: https://t.me/yourusername")
        
    elif data == 'admin_set_approved':
        admin_states[user_id] = 'WAITING_FOR_SET_APPROVED'
        await query.message.reply_text("Send the file/video to set for approved users.")
        
    elif data == 'admin_dashboard':
        total_users = db.get_total_users_count()
        total_payments = db.get_stat("payments")
        upi = db.get_setting("upi_id")
        
        try:
            new_admin_chat = await context.bot.get_chat(config.NEW_ADMIN)
            new_admin_name = f"@{new_admin_chat.username}" if new_admin_chat.username else new_admin_chat.first_name
        except:
            new_admin_name = "New Admin"
            
        dash_text = f"""⚡⚡⚡ <b>𝗗𝗔𝗦𝗛𝗕𝗢𝗔𝗥𝗗</b> ⚡⚡⚡⚡
===========================================
👥 TOTAL USER JOIN: {total_users}
💰 TOTAL PAYMENT COUNT: {total_payments}
🤖 UPI : {upi}
🆔 BOTID: @{context.bot.username}
🙍 BOT OWNER : {new_admin_name}
:::::::::::::::::::::::::::::::::::::::::::::
🛑BOT DEVLOPED BY @HEXAZONxHERE 🛑"""
        await query.message.reply_text(dash_text, parse_mode='HTML')

async def admin_input_receiver(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id) or user_id not in admin_states:
        return

    state = admin_states[user_id]
    msg_text = update.message.text or update.message.caption or ""

    if state == 'WAITING_FOR_UPI':
        db.update_setting("upi_id", msg_text.strip())
        await update.message.reply_text(f"✅ UPI ID successfully updated to: `{msg_text}`", parse_mode="Markdown")
        del admin_states[user_id]

    elif state == 'WAITING_FOR_BROADCAST':
        # Safely fall back if get_live_users isn't updated in db yet
        users = db.get_live_users() if hasattr(db, "get_live_users") else db.get_all_users()
        delay = 21600 # 6 hours default
        
        context.application.create_task(
            start_background_broadcast(
                context=context,
                admin_chat_id=user_id,
                users=users,
                source_chat_id=update.message.chat_id,
                message_id=update.message.message_id,
                delay=delay
            )
        )
        del admin_states[user_id]

    elif state == 'WAITING_FOR_WELCOME':
        db.update_setting("welcome_msg_id", update.message.message_id)
        db.update_setting("welcome_chat_id", update.message.chat_id)
        await update.message.reply_text("✅ Welcome Message Set Successfully!")
        del admin_states[user_id]

    elif state == 'WAITING_FOR_PREMIUM':
        db.update_setting("premium_msg_id", update.message.message_id)
        db.update_setting("premium_chat_id", update.message.chat_id)
        await update.message.reply_text("✅ Premium Message Set Successfully!")
        del admin_states[user_id]

    elif state == 'WAITING_FOR_MANAGE_BUTTON':
        match = re.search(r"(.*)\[(\d+)\]\s*\[(\d+)\]", msg_text)
        if match:
            text = match.group(1).strip()
            price = int(match.group(2))
            btn_id = int(match.group(3))
            db.set_button_data(btn_id, text, price)
            await update.message.reply_text(f"✅ Button {btn_id} updated: {text} | ₹{price}")
            del admin_states[user_id]
        else:
            await update.message.reply_text("❌ Format Error. Ex: VIP VIP Plan 1 [99] [1]")

    elif state == 'WAITING_FOR_EDIT_MENU':
        match = re.search(r"\[(.*?)\]\s*\[(\d+)\](?:\s*\[(.*?)\])?", msg_text)
        if match:
            text = match.group(1).strip()
            btn_id = int(match.group(2))
            link = match.group(3).strip() if match.group(3) else ""
            db.update_menu_button(btn_id, text, link)
            await update.message.reply_text(f"✅ Menu Button {btn_id} added successfully!")
            del admin_states[user_id]
        else:
            await update.message.reply_text("❌ Format Error. Ex:\n`[NAME][NUMBER][LINK]`", parse_mode="Markdown")

    elif state == 'WAITING_FOR_DEMO':
        db.update_setting("demo_link", msg_text.strip())
        await update.message.reply_text(f"✅ Demo link updated!")
        del admin_states[user_id]

    elif state == 'WAITING_FOR_CONTACT':
        db.update_setting("contact_link", msg_text.strip())
        await update.message.reply_text(f"✅ Contact link updated!")
        del admin_states[user_id]
        
    elif state == 'WAITING_FOR_SET_APPROVED':
        db.update_setting("approve_msg_id", update.message.message_id)
        db.update_setting("approve_chat_id", update.message.chat_id)
        await update.message.reply_text("✅ Approval Media/Message Set! (Auto-deletes in 30s)")
        del admin_states[user_id]


# ========================================================
# 🛡 CONFIGURATION BACKUP & RESTORE (/save & /fset)
# ========================================================

async def save_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
        
    config_lines = []
    upi = db.get_setting("upi_id")
    if upi: config_lines.append(f"/setupi {upi}")
    
    demo = db.get_setting("demo_link")
    if demo: config_lines.append(f"/demo {demo}")
    
    contact = db.get_setting("contact_link")
    if contact: config_lines.append(f"/link {contact}")
    
    for btn_id, (text, link) in db.get_menu_buttons().items():
        if link: config_lines.append(f"/menu [{text}][{btn_id}][{link}]")
        else: config_lines.append(f"/menu [{text}][{btn_id}]")
            
    for btn in db.get_premium_buttons():
        config_lines.append(f"/button {btn[1]} [{btn[2]}] [{btn[0]}]")
        
    file_content = "\n".join(config_lines)
    file = BytesIO(file_content.encode('utf-8'))
    file.name = "bot_settings_backup.txt"
    
    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=file,
        caption="✅ **BOT CONFIGURATION BACKUP**\n\nRestart k baad settings restore karne ke liye is file ko reply me tag karke `/fset` use karein.",
        parse_mode="Markdown"
    )

async def fset_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
        
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("❌ Kripya backup `.txt` file ko reply karke `/fset` use karein.")
        return
        
    doc = update.message.reply_to_message.document
    if doc.mime_type == 'text/plain' or doc.file_name.endswith('.txt'):
        processing_msg = await update.message.reply_text("⏳ Restoring settings from file...")
        try:
            file = await context.bot.get_file(doc.file_id)
            file_bytes = await file.download_as_bytearray()
            content = file_bytes.decode('utf-8')
            
            for line in content.splitlines():
                line = line.strip()
                if not line: continue
                
                if line.startswith("/setupi"):
                    db.update_setting("upi_id", line.replace("/setupi", "").strip())
                elif line.startswith("/demo"):
                    db.update_setting("demo_link", line.replace("/demo", "").strip())
                elif line.startswith("/link"):
                    db.update_setting("contact_link", line.replace("/link", "").strip())
                elif line.startswith("/menu"):
                    match = re.search(r"\[(.*?)\]\s*\[(\d+)\](?:\s*\[(.*?)\])?", line)
                    if match:
                        text = match.group(1).strip()
                        btn_id = int(match.group(2))
                        link = match.group(3).strip() if match.group(3) else ""
                        db.update_menu_button(btn_id, text, link)
                elif line.startswith("/button"):
                    match = re.search(r"/button\s+(.*?)\[(\d+)\]\s*\[(\d+)\]", line)
                    if match:
                        text = match.group(1).strip()
                        price = int(match.group(2))
                        btn_id = int(match.group(3))
                        db.set_button_data(btn_id, text, price)
                        
            await processing_msg.edit_text("✅ All settings have been successfully restored!")
        except Exception as e:
            await processing_msg.edit_text(f"❌ Error restoring settings: {e}")


# ========================================================
# 🛡 RESTRICTED COMMANDS (COLOR SETTINGS & OTHERS)
# ========================================================

async def set_menu_color_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        raw = update.message.text.replace("/menucolor", "").strip()
        match = re.search(r"\[(\d+)\]\s*\[([RGBrgb])\]", raw)
        if match:
            btn_id = int(match.group(1))
            color = match.group(2).upper()
            db.update_setting(f"menu_color_{btn_id}", color)
            color_name = "🔴 RED" if color == "R" else "🟢 GREEN" if color == "G" else "🔵 BLUE"
            await update.message.reply_text(f"✅ Main Menu Button {btn_id} color successfully updated to {color_name}!")
        else:
            await update.message.reply_text("❌ Format Error!\nSahi format: `/menucolor [5] [R]`", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def set_button_color_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        raw = update.message.text.replace("/buttoncolor", "").strip()
        match = re.search(r"\[(\d+)\]\s*\[([RGBrgb])\]", raw)
        if match:
            btn_id = int(match.group(1))
            color = match.group(2).upper()
            db.update_setting(f"btn_color_{btn_id}", color)
            color_name = "🔴 RED" if color == "R" else "🟢 GREEN" if color == "G" else "🔵 BLUE"
            await update.message.reply_text(f"✅ Premium Button {btn_id} color successfully updated to {color_name}!")
        else:
            await update.message.reply_text("❌ Format Error!\nSahi format: `/buttoncolor [5] [G]`", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")

async def set_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not update.message.reply_to_message: return
    db.update_setting("welcome_msg_id", update.message.reply_to_message.message_id)
    db.update_setting("welcome_chat_id", update.message.chat_id)
    await update.message.reply_text("✅ Live Welcome message set successfully with exact formatting!")

async def set_premium_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not update.message.reply_to_message: return
    db.update_setting("premium_msg_id", update.message.reply_to_message.message_id)
    db.update_setting("premium_chat_id", update.message.chat_id)
    await update.message.reply_text("✅ Live Premium menu message set successfully!")

async def add_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != config.PERMANENT_ADMIN: return
    if update.message.reply_to_message and update.message.reply_to_message.document:
        doc = update.message.reply_to_message.document
        if doc.mime_type == 'text/plain' or doc.file_name.endswith('.txt'):
            try:
                file = await context.bot.get_file(doc.file_id)
                content = (await file.download_as_bytearray()).decode('utf-8')
                added = db.add_users_bulk(re.findall(r'\b\d+\b', content))
                await update.message.reply_text(f"✅ Added {added} users!")
            except: pass
            return
    if not context.args: return
    added = db.add_users_bulk(context.args)
    await update.message.reply_text(f"✅ Successfully added {added} users!")

async def dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if is_admin(update.effective_user.id): await admin_panel(update, context)

async def set_approve_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not update.message.reply_to_message: return
    db.update_setting("approve_msg_id", update.message.reply_to_message.message_id)
    db.update_setting("approve_chat_id", update.message.chat_id)
    await update.message.reply_text("✅ Approval content updated!")

async def set_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        raw = update.message.text.replace("/button", "").strip()
        parts = [p.strip("[] ") for p in raw.split("]") if p.strip()]
        text, price, btn_id = parts[0], int(parts[1]), int(parts[2])
        db.set_button_data(btn_id, text, price)
        await update.message.reply_text(f"✅ Button {btn_id} updated: {text} | ₹{price}")
    except Exception: pass

async def edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    try:
        raw = update.message.text.replace("/menu", "").strip()
        match = re.search(r"\[(.*?)\]\s*\[(\d+)\](?:\s*\[(.*?)\])?", raw)
        if match:
            text = match.group(1).strip()
            btn_id = int(match.group(2))
            link = match.group(3).strip() if match.group(3) else ""
            db.update_menu_button(btn_id, text, link)
            await update.message.reply_text(f"✅ Menu Button {btn_id} [{text}] updated successfully!")
    except Exception: pass

async def add_link_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    raw = update.message.text.replace("/addlink", "").strip()
    parts = [p.strip("[] ") for p in raw.split() if p.strip()]
    if len(parts) >= 2:
        p1, p2 = parts[0], parts[1]
        link, btn_id = (p1, int(p2)) if p2.isdigit() else (p2, int(p1))
        db.set_menu_button_link(btn_id, link)
        await update.message.reply_text(f"✅ Link added!")

async def remove_menu_button_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    try:
        db.delete_menu_button(int(context.args[0]))
        await update.message.reply_text(f"✅ Button {context.args[0]} removed successfully!")
    except Exception: pass

async def setupi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id) or not context.args: return
    db.update_setting("upi_id", context.args[0])
    await update.message.reply_text(f"✅ UPI ID successfully updated!")

async def simple_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    cmd = update.message.text.split()[0].lower()
    link = " ".join(context.args)
    if cmd == "/demo": db.update_setting("demo_link", link)
    elif cmd == "/link": db.update_setting("contact_link", link)
    await update.message.reply_text("✅ Link updated!")


# ========================================================
# 📢 TIMED BROADCAST COMMANDS
# ========================================================

async def broadcast_or_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return

    cmd = update.message.text.split()[0].lower()
    delay = 21600 # Default 6 hours
    cleaned_args = []

    for arg in context.args:
        if re.match(r'^\[\d+\]$', arg):
            delay = int(arg.strip("[]"))
        else:
            cleaned_args.append(arg)

    if cmd == "/sms":
        if not update.message.reply_to_message or not cleaned_args:
            await update.message.reply_text("❌ Reply to a message and use:\n`/sms 123456789 [60]`", parse_mode="Markdown")
            return

        target_user = int(cleaned_args[0])
        try:
            sent = await context.bot.copy_message(
                chat_id=target_user,
                from_chat_id=update.message.chat_id,
                message_id=update.message.reply_to_message.message_id
            )
            context.application.create_task(delete_message_after(context, target_user, sent.message_id, delay))
            await update.message.reply_text(f"✅ SMS sent successfully!\n🗑 Auto-delete: {delay}s")
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send:\n{e}")
        return

    if cmd not in ["/broadcast", "/sendall"]: return

    users = db.get_live_users() if hasattr(db, "get_live_users") else db.get_all_users()
    if not users:
        await update.message.reply_text("❌ No live users found.")
        return

    if cleaned_args and os.path.exists(cleaned_args[0]):
        file_path = cleaned_args[0]
        file_bytes = await asyncio.to_thread(lambda: open(file_path, "rb").read())
        
        context.application.create_task(
            run_file_broadcast(
                context=context, admin_chat_id=update.effective_chat.id,
                users=users, file_path=file_path, file_bytes=file_bytes, delay=delay
            )
        )
        await update.message.reply_text(
            "🚀 <b>File Broadcast Started!</b>\n\n"
            f"👥 Live Users: <b>{len(users)}</b>\n"
            "⚡ Bot will remain responsive.", parse_mode="HTML"
        )
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("❌ Reply to a message and use:\n`/broadcast [60]`", parse_mode="Markdown")
        return

    msg_id = update.message.reply_to_message.message_id
    context.application.create_task(
        start_background_broadcast(
            context=context, admin_chat_id=update.effective_chat.id,
            users=users, source_chat_id=update.message.chat_id,
            message_id=msg_id, delay=delay
        )
    )

    await update.message.reply_text(
        "🚀 <b>Broadcast Started!</b>\n\n"
        f"👥 Live Users: <b>{len(users)}</b>\n"
        "⚡ Running in background.\n"
        "🤖 Bot remains responsive.", parse_mode="HTML"
    )

# ========================================================
# 🗃️ DATABASE / LIVE USERS FILE
# ========================================================

async def view_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != config.PERMANENT_ADMIN:
        await update.message.reply_text("⛔ Sirf Permanent Admin ye command use kar sakta hai.")
        return

    # Fallback applied incase get_live_users() doesn't exist in your db file yet
    raw_users = db.get_live_users() if hasattr(db, "get_live_users") else db.get_all_users()
    users = [str(u) for u in raw_users]

    total_live = len(users)
    file_content = "\n".join(users)
    file = BytesIO(file_content.encode("utf-8"))
    file.name = "database_live_users.txt"
    caption = (
        f"📊 <b>LIVE USERS:</b> {total_live}\n\n"
        "💀 Dead/Blocked users are excluded.\n"
        "👨‍💻 DEV: @HEXAZONxHERE"
    )

    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=file, caption=caption, parse_mode="HTML"
    )

# ========================================================
# 💳 PAYMENT & BUTTON HANDLING
# ========================================================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    upi = db.get_setting("upi_id")

    if data == "premium":
        p_msg_id = db.get_setting("premium_msg_id")
        p_chat_id = db.get_setting("premium_chat_id")
        try: await query.message.delete()
        except: pass

        if p_msg_id and p_chat_id:
            try:
                await context.bot.copy_message(
                    chat_id=user_id, from_chat_id=int(p_chat_id),
                    message_id=int(p_msg_id), reply_markup=premium_menu()
                )
            except Exception:
                await context.bot.send_message(chat_id=user_id, text="✨ Select a Premium Plan:", reply_markup=premium_menu())
        else:
            await context.bot.send_message(chat_id=user_id, text="✨ Select a Premium Plan:", reply_markup=premium_menu())

    elif data.startswith("plan_"):
        btn_id = int(data.split("_")[1])
        b_data = db.get_button_by_id(btn_id)
        if not b_data: return

        qr_url = await asyncio.to_thread(api_utils.generate_upi_qr_url, upi, b_data[1])
        plan_text = f"✨ <b>{b_data[0]}</b>\n💰 Price: ₹{b_data[1]}\n\nScan QR Code below to pay:"
        
        kb = payment_menu(btn_id)
        
        try: await query.message.delete()
        except: pass
        await context.bot.send_photo(chat_id=user_id, photo=qr_url, caption=plan_text, parse_mode="HTML", reply_markup=kb)
        users_state[user_id] = {"status": "pending"}

    elif data == "back_main":
        try: await query.message.delete()
        except: pass
        await start(update, context)

    elif data.startswith("verify_"):
        await query.message.reply_text("📸 Please send your payment screenshot now.")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = users_state.get(user_id, {})
    if state.get("status") != "pending": return
    if state.get("rejects", 0) >= config.MAX_REJECT:
        await update.message.reply_text("🚫 You are blocked for spamming.")
        return

    users_state[user_id]["status"] = "sent"
    caption = f"NEW PAYMENT SCREENSHOT\nUser ID: {user_id}\nUsername: @{update.effective_user.username}"
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ APPROVE", callback_data=f"app_{user_id}"),
        InlineKeyboardButton("❌ REJECT", callback_data=f"rej_{user_id}")
    ]])

    await context.bot.send_photo(chat_id=config.PERMANENT_ADMIN, photo=update.message.photo[-1].file_id, caption=caption, reply_markup=kb)
    await context.bot.send_photo(chat_id=config.NEW_ADMIN, photo=update.message.photo[-1].file_id, caption=caption, reply_markup=kb)
    await update.message.reply_text("✅ Screenshot sent to Admin for verification! Please wait.")

async def admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    query = update.callback_query
    await query.answer()
    data = query.data
    target_user = int(data.split("_")[1])

    if data.startswith("app_"):
        db.increment_stat("payments")
        app_msg_id = db.get_setting("approve_msg_id")
        app_chat_id = db.get_setting("approve_chat_id")

        if app_msg_id and app_chat_id:
            try:
                sent_msg = await context.bot.copy_message(chat_id=target_user, from_chat_id=int(app_chat_id), message_id=int(app_msg_id))
                await query.message.reply_text(f"✅ User {target_user} Approved!")
                if sent_msg.video or sent_msg.document:
                    context.application.create_task(delete_message_after(context, target_user, sent_msg.message_id, 30))
            except Exception: pass
        else:
            await context.bot.send_message(target_user, "✅ Payment Verified! Access Granted.")
            await query.message.reply_text(f"✅ User {target_user} Approved!")

    elif data.startswith("rej_"):
        if target_user not in users_state: users_state[target_user] = {"rejects": 0}
        users_state[target_user]["rejects"] += 1
        users_state[target_user]["status"] = "idle"
        await context.bot.send_message(target_user, "🫣 FAKE PAYMENT SCREENSHOT 🚫 TRY AGAIN AND SEND SCREENSHOT 💓")
        await query.message.reply_text(f"❌ User {target_user} Rejected!")
