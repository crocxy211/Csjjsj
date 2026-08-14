# ========================================================
# 🗄️ DATABASE - USER MANAGEMENT (UPDATED)
# ========================================================

import sqlite3
from config import DB_NAME

def get_connection():
    return sqlite3.connect(DB_NAME, check_same_thread=False)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # ----------------------------------------------------
    # TABLES SETUP
    # ----------------------------------------------------
    # Total users (Ever joined)
    cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY)")
    
    # User status (Live/Dead for Broadcasts)
    cursor.execute("CREATE TABLE IF NOT EXISTS user_status (user_id INTEGER PRIMARY KEY, is_live INTEGER DEFAULT 1)")
    
    cursor.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS buttons (btn_id INTEGER PRIMARY KEY, text TEXT, price INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS menu_buttons (btn_id INTEGER PRIMARY KEY, text TEXT, link TEXT)")
    
    try:
        cursor.execute("ALTER TABLE menu_buttons ADD COLUMN link TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()

    # ----------------------------------------------------
    # DEFAULT DATA SEEDING
    # ----------------------------------------------------
    cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('payments', 0)")
    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('upi_id', 'your_upi@ybl')")
    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('demo_link', 'https://t.me/your_demo_channel')")
    cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('contact_link', 'https://t.me/your_contact_link')")

    for i in range(1, 4):
        cursor.execute("INSERT OR IGNORE INTO buttons (btn_id, text, price) VALUES (?, ?, ?)", (i, f"Premium Plan {i}", i * 50))

    cursor.execute("INSERT OR IGNORE INTO menu_buttons (btn_id, text, link) VALUES (1, 'PREMIUM', '')")
    cursor.execute("INSERT OR IGNORE INTO menu_buttons (btn_id, text, link) VALUES (2, 'DEMO', '')")
    cursor.execute("INSERT OR IGNORE INTO menu_buttons (btn_id, text, link) VALUES (3, 'CONTACT', '')")
    
    conn.commit()
    conn.close()

# ========================================================
# 👤 USER MANAGEMENT (TOTAL & LIVE)
# ========================================================

def add_user(user_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    cursor.execute("INSERT OR IGNORE INTO user_status (user_id, is_live) VALUES (?, 1)", (user_id,))
    conn.commit()
    conn.close()

def add_users_bulk(user_ids: list) -> int:
    if not user_ids:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    
    data = [(int(uid),) for uid in user_ids]
    cursor.executemany("INSERT OR IGNORE INTO users (user_id) VALUES (?)", data)
    
    status_data = [(int(uid), 1) for uid in user_ids]
    cursor.executemany("INSERT OR IGNORE INTO user_status (user_id, is_live) VALUES (?, ?)", status_data)
    
    conn.commit()
    conn.close()
    return len(data)

def mark_user_live(user_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO user_status (user_id, is_live) 
        VALUES (?, 1) ON CONFLICT(user_id) DO UPDATE SET is_live = 1
    """, (int(user_id),))
    conn.commit()
    conn.close()

def mark_user_dead(user_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO user_status (user_id, is_live) 
        VALUES (?, 0) ON CONFLICT(user_id) DO UPDATE SET is_live = 0
    """, (int(user_id),))
    conn.commit()
    conn.close()

# Alias so handlers.py broadcast doesn't crash when deleting dead users
def remove_user(user_id: int):
    mark_user_dead(user_id)

def get_live_users() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT u.user_id FROM users u
        LEFT JOIN user_status s ON u.user_id = s.user_id
        WHERE s.is_live = 1 OR s.user_id IS NULL
    """)
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    return users

def get_all_users() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    users = [row[0] for row in cursor.fetchall()]
    conn.close()
    return users

def get_total_users_count() -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_live_users_count() -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM users u
        LEFT JOIN user_status s ON u.user_id = s.user_id
        WHERE s.is_live = 1 OR s.user_id IS NULL
    """)
    count = cursor.fetchone()[0]
    conn.close()
    return count

# ========================================================
# ⚙️ SETTINGS & STATS
# ========================================================

def get_setting(key: str) -> str:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else ""

def update_setting(key: str, value: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_stat(key: str) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM stats WHERE key = ?", (key,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else 0

def increment_stat(key: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE stats SET value = value + 1 WHERE key = ?", (key,))
    conn.commit()
    conn.close()

# ========================================================
# 🔘 MENUS & BUTTONS
# ========================================================

def get_menu_buttons() -> dict:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT btn_id, text, link FROM menu_buttons ORDER BY btn_id ASC")
    rows = cursor.fetchall()
    conn.close()
    return {row[0]: (row[1], row[2] if len(row) > 2 and row[2] else "") for row in rows}

def update_menu_button(btn_id: int, text: str, link: str = ""):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO menu_buttons (btn_id, text, link) VALUES (?, ?, ?)", (btn_id, text.upper(), link))
    conn.commit()
    conn.close()

def set_menu_button_link(btn_id: int, link: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE menu_buttons SET link = ? WHERE btn_id = ?", (link, btn_id))
    conn.commit()
    conn.close()

def delete_menu_button(btn_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM menu_buttons WHERE btn_id = ?", (btn_id,))
    conn.commit()
    conn.close()

def get_premium_buttons() -> list:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT btn_id, text, price FROM buttons ORDER BY btn_id ASC")
    btns = cursor.fetchall()
    conn.close()
    return btns

def get_button_by_id(btn_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT text, price FROM buttons WHERE btn_id = ?", (btn_id,))
    res = cursor.fetchone()
    conn.close()
    return res

def set_button_data(btn_id: int, text: str, price: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO buttons (btn_id, text, price) VALUES (?, ?, ?)", (btn_id, text, price))
    conn.commit()
    conn.close()
