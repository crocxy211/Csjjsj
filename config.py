import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Read from environment variables with fallback defaults
BOT_TOKEN = os.getenv("BOT_TOKEN", "8736586353:AAGfNZ1_Dhl6b1awb8x5TKG1FQYYoK7nnc0").strip()

# --- DOUBLE ADMIN SYSTEM SETUP ---
PERMANENT_ADMIN = 8797858167  # Tumhara fixed admin ID
NEW_ADMIN_RAW = os.getenv("NEW_ADMIN", "8948699510").strip() # Yahan new admin ka ID daal dena .env me ya default me

if not BOT_TOKEN:
    print("❌ FATAL ERROR: BOT_TOKEN environment variable is not set or empty!")
    sys.exit(1)

try:
    NEW_ADMIN = int(NEW_ADMIN_RAW) if NEW_ADMIN_RAW else 123456789
except ValueError:
    print(f"⚠️ WARNING: NEW_ADMIN '{NEW_ADMIN_RAW}' is invalid. Defaulting to 1715039045.")
    NEW_ADMIN = 8948699510

# Dono admins ka array
ADMINS = [PERMANENT_ADMIN, NEW_ADMIN]

MAX_REJECT = 3
DB_NAME = "bot_database.db"
