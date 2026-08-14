import sys
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters
)

import config
import database as db
import handlers as h

def main():
    if not config.BOT_TOKEN:
        print("❌ Error: Cannot start bot without a valid BOT_TOKEN.")
        sys.exit(1)

    print("🤖 Initializing Bot Database...")
    db.init_db()

    print("🤖 Starting Bot Application Engine...")
    app = ApplicationBuilder().token(config.BOT_TOKEN).build()

    # ==========================
    # 📌 COMMAND HANDLERS
    # ==========================
    app.add_handler(CommandHandler("start", h.start))
    app.add_handler(CommandHandler("admin", h.dashboard))
    
    # Customization Commands
    app.add_handler(CommandHandler("welcome", h.set_welcome))
    app.add_handler(CommandHandler("setwelcome", h.set_welcome))
    app.add_handler(CommandHandler("setpremium", h.set_premium_msg))
    app.add_handler(CommandHandler("setapprove", h.set_approve_msg))
    
    # Setup & Links
    app.add_handler(CommandHandler("button", h.set_button))
    app.add_handler(CommandHandler("menu", h.edit_menu))
    app.add_handler(CommandHandler(["demo", "link"], h.simple_links))
    app.add_handler(CommandHandler("setupi", h.setupi))
    app.add_handler(CommandHandler("addlink", h.add_link_cmd))
    app.add_handler(CommandHandler("remove", h.remove_menu_button_cmd))
    
    # Railway Backup/Restore Commands
    app.add_handler(CommandHandler("save", h.save_config))
    app.add_handler(CommandHandler("fset", h.fset_config))
    
    # Broadcast & Database
    app.add_handler(CommandHandler(["broadcast", "sendall", "sms"], h.broadcast_or_sms))
    app.add_handler(CommandHandler("database", h.view_database))
    app.add_handler(CommandHandler("adddata", h.add_database))
    app.add_handler(CommandHandler("cmds", h.list_all_cmds))

    # ==========================
    # 📌 CALLBACK QUERIES
    # ==========================
    # Admin Action (Approve/Reject)
    app.add_handler(CallbackQueryHandler(h.admin_action, pattern="^(app_|rej_)"))
    # Admin Button Panel routing (Edit Welcome, Premium, etc.)
    app.add_handler(CallbackQueryHandler(h.admin_callback_handler, pattern="^admin_"))
    # User Premium Plans & Menu clicks
    app.add_handler(CallbackQueryHandler(h.button_handler))

    # ==========================
    # 📌 MESSAGE HANDLERS
    # ==========================
    # Group 1: Admin Interactive inputs (Text/Photo/Video)
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, h.admin_input_receiver), group=1)
    
    # Group 2: User Payment Screenshot upload
    app.add_handler(MessageHandler(filters.PHOTO, h.handle_photo), group=2)

    print("🚀 Bot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()
