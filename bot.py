import asyncio
import logging
import random
import string
import shutil
import aiosqlite
import aiohttp
from datetime import datetime, timedelta

from aiogram import Router, F, Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ╔══════════════════════════════════════════════════════════════╗
# ║                     НАСТРОЙКИ                               ║
# ╚══════════════════════════════════════════════════════════════╝

BOT_TOKEN = "8638504935:AAG4ptZyoLp8EedzB70DB5HTyrzSsxAtJ8o"
CRYPTO_BOT_TOKEN = "543147:AA4D9iRdBYEdKJgOFEnfEFaoBvEOUodBbCc"
ADMIN_IDS = [8373491856]
REFERRAL_BONUS = 5
SUPPORT_USERNAME = "termvd"
LOG_CHANNEL_ID = None   # Установите ID канала для логов, например -1001234567890
LOW_BALANCE_THRESHOLD = 2.0   # Уведомление при балансе ниже этой суммы
INACTIVE_DAYS = 7              # Дней неактивности для напоминания

# ╔══════════════════════════════════════════════════════════════╗
# ║                     БАЗА ДАННЫХ                             ║
# ╚══════════════════════════════════════════════════════════════╝

DB = "store.db"


async def init_db():
    async with aiosqlite.connect(DB) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT,
                full_name       TEXT,
                balance         REAL    DEFAULT 0,
                referral_code   TEXT    UNIQUE,
                referred_by     INTEGER,
                referral_count  INTEGER DEFAULT 0,
                referral_earned REAL    DEFAULT 0,
                is_banned       INTEGER DEFAULT 0,
                notify_products INTEGER DEFAULT 1,
                last_active     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS products (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                name         TEXT    NOT NULL,
                description  TEXT,
                price        REAL    NOT NULL,
                content      TEXT,
                file_id      TEXT,
                file_name    TEXT,
                post_message TEXT,
                is_active    INTEGER DEFAULT 1,
                stock_limit  INTEGER DEFAULT NULL,
                sold_count   INTEGER DEFAULT 0,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS purchases (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                product_id   INTEGER NOT NULL,
                product_name TEXT    NOT NULL,
                price        REAL    NOT NULL,
                discount     REAL    DEFAULT 0,
                promo_code   TEXT,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                amount      REAL    NOT NULL,
                type        TEXT    NOT NULL,
                description TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS invoices (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                invoice_id  INTEGER NOT NULL UNIQUE,
                amount      REAL    NOT NULL,
                status      TEXT    DEFAULT 'pending',
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS promo_codes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT    NOT NULL UNIQUE,
                discount    REAL    NOT NULL,
                type        TEXT    NOT NULL DEFAULT 'percent',
                max_uses    INTEGER DEFAULT NULL,
                used_count  INTEGER DEFAULT 0,
                is_active   INTEGER DEFAULT 1,
                expires_at  TIMESTAMP DEFAULT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS promo_uses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                promo_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                used_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS chat_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                sender      TEXT    NOT NULL,
                text        TEXT    NOT NULL,
                is_read     INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS chat_sessions (
                user_id     INTEGER PRIMARY KEY,
                status      TEXT    NOT NULL DEFAULT 'active',
                opened_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at   TIMESTAMP,
                closed_by   TEXT
            );
            CREATE TABLE IF NOT EXISTS admins (
                user_id     INTEGER PRIMARY KEY,
                role        TEXT    NOT NULL DEFAULT 'moderator',
                added_by    INTEGER,
                note        TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS action_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                username    TEXT,
                full_name   TEXT,
                is_admin    INTEGER DEFAULT 0,
                action      TEXT    NOT NULL,
                details     TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()

        # Миграции для существующих БД
        existing_users = {row[1] async for row in await db.execute("PRAGMA table_info(users)")}
        existing_products = {row[1] async for row in await db.execute("PRAGMA table_info(products)")}
        existing_purchases = {row[1] async for row in await db.execute("PRAGMA table_info(purchases)")}

        migrations = [
            ("users", "is_banned", "INTEGER DEFAULT 0"),
            ("users", "notify_products", "INTEGER DEFAULT 1"),
            ("users", "last_active", "TIMESTAMP"),
            ("products", "post_message", "TEXT"),
            ("products", "stock_limit", "INTEGER"),
            ("products", "sold_count", "INTEGER DEFAULT 0"),
            ("purchases", "discount", "REAL DEFAULT 0"),
            ("purchases", "promo_code", "TEXT"),
        ]
        for table, col, typedef in migrations:
            existing = {row[1] async for row in await db.execute(f"PRAGMA table_info({table})")}
            if col not in existing:
                await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")

        await db.commit()


# ── Admin roles ───────────────────────────────────────────────
# Roles: superadmin > admin > moderator
# superadmin — all rights (hardcoded in ADMIN_IDS), cannot be removed
# admin      — all except adding/removing admins
# moderator  — chats, view users, view analytics only

ROLE_SUPERADMIN = "superadmin"
ROLE_ADMIN      = "admin"
ROLE_MODERATOR  = "moderator"

ROLE_LABELS = {
    ROLE_SUPERADMIN: "👑 Суперадмин",
    ROLE_ADMIN:      "⚙️ Администратор",
    ROLE_MODERATOR:  "🔧 Модератор",
}

# What each role can do
ROLE_PERMS = {
    ROLE_SUPERADMIN: {"all"},
    ROLE_ADMIN:      {"products", "users", "balance", "analytics", "broadcast", "promos", "chats", "backup", "settings"},
    ROLE_MODERATOR:  {"chats", "users_view", "analytics"},
}


async def db_get_admin(user_id: int):
    if user_id in ADMIN_IDS:
        return {"user_id": user_id, "role": ROLE_SUPERADMIN, "added_by": None, "note": "Владелец"}
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM admins WHERE user_id=?", (user_id,)) as c:
            return await c.fetchone()


async def db_get_all_admins():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT a.*, u.full_name, u.username
               FROM admins a LEFT JOIN users u ON a.user_id = u.user_id
               ORDER BY a.created_at"""
        ) as c:
            return await c.fetchall()


async def db_add_admin(user_id: int, role: str, added_by: int, note: str = ""):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR REPLACE INTO admins (user_id, role, added_by, note) VALUES (?,?,?,?)",
            (user_id, role, added_by, note)
        )
        await db.commit()


async def db_update_admin_role(user_id: int, role: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE admins SET role=? WHERE user_id=?", (role, user_id))
        await db.commit()


async def db_remove_admin(user_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        await db.commit()


async def is_admin(user_id: int) -> bool:
    """Check if user has any admin role."""
    if user_id in ADMIN_IDS:
        return True
    a = await db_get_admin(user_id)
    return a is not None


async def has_perm(user_id: int, perm: str) -> bool:
    """Check if admin has specific permission."""
    if user_id in ADMIN_IDS:
        return True
    a = await db_get_admin(user_id)
    if not a:
        return False
    role = a["role"]
    perms = ROLE_PERMS.get(role, set())
    return "all" in perms or perm in perms


async def get_role(user_id: int) -> str | None:
    if user_id in ADMIN_IDS:
        return ROLE_SUPERADMIN
    a = await db_get_admin(user_id)
    return a["role"] if a else None


async def get_all_admin_ids() -> list[int]:
    """Return all admin user_ids: superadmins + db admins."""
    ids = list(ADMIN_IDS)
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT user_id FROM admins") as c:
            for row in await c.fetchall():
                if row[0] not in ids:
                    ids.append(row[0])
    return ids


# ── Users ─────────────────────────────────────────────────────

async def db_get_user(user_id: int):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id=?", (user_id,)) as c:
            return await c.fetchone()


async def db_create_user(user_id: int, username: str, full_name: str, referred_by: int = None):
    code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, full_name, referral_code, referred_by) VALUES (?,?,?,?,?)",
            (user_id, username, full_name, code, referred_by)
        )
        await db.commit()
    return await db_get_user(user_id)


async def db_get_user_by_ref(code: str):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE referral_code=?", (code,)) as c:
            return await c.fetchone()


async def db_get_user_by_id_or_username(query: str):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        query = query.strip().lstrip("@")
        if query.isdigit():
            async with db.execute("SELECT * FROM users WHERE user_id=?", (int(query),)) as c:
                return await c.fetchone()
        else:
            async with db.execute("SELECT * FROM users WHERE username=?", (query,)) as c:
                return await c.fetchone()


async def db_update_balance(user_id: int, amount: float, desc: str = ""):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET balance=balance+? WHERE user_id=?", (amount, user_id))
        await db.execute(
            "INSERT INTO transactions (user_id, amount, type, description) VALUES (?,?,?,?)",
            (user_id, amount, "credit" if amount > 0 else "debit", desc)
        )
        await db.commit()


async def db_update_last_active(user_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET last_active=CURRENT_TIMESTAMP WHERE user_id=?", (user_id,))
        await db.commit()


async def db_referral_bonus(new_uid: int, ref_uid: int):
    bonus_str = await db_get_setting("referral_bonus")
    bonus = float(bonus_str) if bonus_str else REFERRAL_BONUS
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE users SET balance=balance+?, referral_count=referral_count+1, referral_earned=referral_earned+? WHERE user_id=?",
            (bonus, bonus, ref_uid)
        )
        await db.execute(
            "INSERT INTO transactions (user_id, amount, type, description) VALUES (?,?,?,?)",
            (ref_uid, bonus, "referral", f"Реферал: пользователь {new_uid}")
        )
        await db.commit()


async def db_total_users() -> int:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c:
            r = await c.fetchone()
            return r[0] if r else 0


async def db_all_user_ids():
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned=0") as c:
            return [r[0] for r in await c.fetchall()]


async def db_get_all_users():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, full_name, balance, referral_count, is_banned, last_active, created_at FROM users ORDER BY created_at DESC"
        ) as c:
            return await c.fetchall()


async def db_ban_user(user_id: int, ban: bool):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET is_banned=? WHERE user_id=?", (1 if ban else 0, user_id))
        await db.commit()


async def db_get_setting(key: str) -> str | None:
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as c:
            row = await c.fetchone()
            return row[0] if row else None


async def db_set_setting(key: str, value: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
        await db.commit()


# ── New feature DB helpers ─────────────────────────────────────

async def db_get_users_by_min_balance(min_bal: float):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, full_name, balance FROM users WHERE balance >= ? AND is_banned=0 ORDER BY balance DESC",
            (min_bal,)
        ) as c:
            return await c.fetchall()


async def db_get_banned_users():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, full_name, balance FROM users WHERE is_banned=1 ORDER BY full_name"
        ) as c:
            return await c.fetchall()


async def db_get_all_transactions(limit: int = 5000):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT t.*, u.username, u.full_name
               FROM transactions t LEFT JOIN users u ON t.user_id = u.user_id
               ORDER BY t.created_at DESC LIMIT ?""",
            (limit,)
        ) as c:
            return await c.fetchall()


async def db_get_product_stats():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.id, p.name, p.price, p.sold_count, p.is_active,
                      COUNT(pu.id) as purchase_count,
                      COALESCE(SUM(pu.price), 0) as revenue
               FROM products p LEFT JOIN purchases pu ON p.id = pu.product_id
               GROUP BY p.id ORDER BY revenue DESC"""
        ) as c:
            return await c.fetchall()


async def db_get_suspicious_users(start_threshold: int = 5, window_minutes: int = 10):
    """Users who triggered /start many times in a short window."""
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT user_id, username, full_name, COUNT(*) as cnt, MAX(created_at) as last_at
               FROM action_logs
               WHERE action = 'Запустил бота /start'
                 AND created_at >= datetime('now', ?)
               GROUP BY user_id
               HAVING cnt >= ?
               ORDER BY cnt DESC""",
            (f"-{window_minutes} minutes", start_threshold)
        ) as c:
            return await c.fetchall()


async def db_count_unanswered_chat_msgs(user_id: int) -> int:
    """Count consecutive user messages since last admin reply."""
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            """SELECT COUNT(*) FROM chat_messages
               WHERE user_id=? AND sender='user'
                 AND id > COALESCE(
                     (SELECT MAX(id) FROM chat_messages WHERE user_id=? AND sender='admin'), 0
                 )""",
            (user_id, user_id)
        ) as c:
            row = await c.fetchone()
            return row[0] if row else 0



    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, full_name, username, created_at FROM users WHERE referred_by=? ORDER BY created_at DESC",
            (user_id,)
        ) as c:
            return await c.fetchall()


# ── Products ──────────────────────────────────────────────────

async def db_get_products():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM products WHERE is_active=1 AND (stock_limit IS NULL OR stock_limit > sold_count) ORDER BY id DESC"
        ) as c:
            return await c.fetchall()


async def db_get_all_products():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products ORDER BY id DESC") as c:
            return await c.fetchall()


async def db_get_product(pid: int):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM products WHERE id=?", (pid,)) as c:
            return await c.fetchone()


async def db_add_product(name, description, price, content, file_id=None, file_name=None, stock_limit=None):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO products (name, description, price, content, file_id, file_name, stock_limit) VALUES (?,?,?,?,?,?,?)",
            (name, description, price, content, file_id, file_name, stock_limit)
        )
        await db.commit()


async def db_toggle_product(pid: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE products SET is_active=CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE id=?", (pid,)
        )
        await db.commit()


async def db_delete_product(pid: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM products WHERE id=?", (pid,))
        await db.commit()


async def db_update_price(pid: int, price: float):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE products SET price=? WHERE id=?", (price, pid))
        await db.commit()


async def db_set_product_message(pid: int, message: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE products SET post_message=? WHERE id=?", (message, pid))
        await db.commit()


async def db_set_stock_limit(pid: int, limit):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE products SET stock_limit=? WHERE id=?", (limit, pid))
        await db.commit()


# ── Purchases ─────────────────────────────────────────────────

async def db_buy(user_id: int, pid: int, promo_code: str = None):
    product = await db_get_product(pid)
    if not product:
        return None, "Товар не найден"
    user = await db_get_user(user_id)
    if not user:
        return None, "Пользователь не найден"
    if product["stock_limit"] is not None and product["sold_count"] >= product["stock_limit"]:
        return None, "Товар закончился"

    final_price = product["price"]
    discount = 0.0
    used_promo = None

    if promo_code:
        promo = await db_get_promo(promo_code)
        if promo and promo["is_active"]:
            already_used = await db_promo_used_by(promo["id"], user_id)
            if not already_used:
                if promo["type"] == "percent":
                    discount = round(final_price * promo["discount"] / 100, 2)
                else:
                    discount = min(promo["discount"], final_price)
                final_price = max(0.0, final_price - discount)
                used_promo = promo_code

    if user["balance"] < final_price:
        return None, "Недостаточно средств"

    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET balance=balance-? WHERE user_id=?", (final_price, user_id))
        await db.execute(
            "INSERT INTO purchases (user_id, product_id, product_name, price, discount, promo_code) VALUES (?,?,?,?,?,?)",
            (user_id, pid, product["name"], final_price, discount, used_promo)
        )
        await db.execute(
            "INSERT INTO transactions (user_id, amount, type, description) VALUES (?,?,?,?)",
            (user_id, -final_price, "debit", f"Покупка: {product['name']}")
        )
        await db.execute("UPDATE products SET sold_count=sold_count+1 WHERE id=?", (pid,))
        if used_promo:
            await db.execute("UPDATE promo_codes SET used_count=used_count+1 WHERE code=?", (used_promo,))
            promo_row = await (await db.execute("SELECT id FROM promo_codes WHERE code=?", (used_promo,))).fetchone()
            if promo_row:
                await db.execute(
                    "INSERT INTO promo_uses (promo_id, user_id) VALUES (?,?)",
                    (promo_row[0], user_id)
                )
        await db.commit()

    return product, None, final_price, discount


async def db_get_purchases(user_id: int):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT p.*, pr.content, pr.file_id, pr.file_name FROM purchases p LEFT JOIN products pr ON p.product_id=pr.id WHERE p.user_id=? ORDER BY p.created_at DESC",
            (user_id,)
        ) as c:
            return await c.fetchall()


async def db_get_all_purchases():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.*, u.full_name, u.username
               FROM purchases p
               LEFT JOIN users u ON p.user_id = u.user_id
               ORDER BY p.created_at DESC"""
        ) as c:
            return await c.fetchall()


# ── Invoices ──────────────────────────────────────────────────

async def db_save_invoice(user_id: int, invoice_id: int, amount: float):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT OR IGNORE INTO invoices (user_id, invoice_id, amount) VALUES (?,?,?)",
            (user_id, invoice_id, amount)
        )
        await db.commit()


async def db_get_invoice(invoice_id: int):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM invoices WHERE invoice_id=?", (invoice_id,)) as c:
            return await c.fetchone()


async def db_mark_paid(invoice_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE invoices SET status='paid' WHERE invoice_id=?", (invoice_id,))
        await db.commit()


# ── Promo codes ───────────────────────────────────────────────

async def db_get_promo(code: str):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes WHERE code=? COLLATE NOCASE", (code,)) as c:
            return await c.fetchone()


async def db_get_all_promos():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes ORDER BY created_at DESC") as c:
            return await c.fetchall()


async def db_add_promo(code: str, discount: float, ptype: str, max_uses=None, expires_at=None):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO promo_codes (code, discount, type, max_uses, expires_at) VALUES (?,?,?,?,?)",
            (code.upper(), discount, ptype, max_uses, expires_at)
        )
        await db.commit()


async def db_delete_promo(promo_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM promo_codes WHERE id=?", (promo_id,))
        await db.commit()


async def db_toggle_promo(promo_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE promo_codes SET is_active=CASE WHEN is_active=1 THEN 0 ELSE 1 END WHERE id=?",
            (promo_id,)
        )
        await db.commit()


async def db_promo_used_by(promo_id: int, user_id: int) -> bool:
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT id FROM promo_uses WHERE promo_id=? AND user_id=?", (promo_id, user_id)
        ) as c:
            return bool(await c.fetchone())


# ── Chat ──────────────────────────────────────────────────────

async def db_send_chat_msg(user_id: int, sender: str, text: str):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO chat_messages (user_id, sender, text) VALUES (?,?,?)",
            (user_id, sender, text)
        )
        if sender == "user":
            await db.execute(
                """INSERT INTO chat_sessions (user_id, status, opened_at)
                   VALUES (?, 'active', CURRENT_TIMESTAMP)
                   ON CONFLICT(user_id) DO UPDATE SET
                       status='active', opened_at=CURRENT_TIMESTAMP, closed_at=NULL, closed_by=NULL""",
                (user_id,)
            )
        await db.commit()


async def db_get_chat_history(user_id: int, limit: int = 20):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM chat_messages WHERE user_id=? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        ) as c:
            rows = await c.fetchall()
            return list(reversed(rows))


async def db_get_chat_session(user_id: int):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM chat_sessions WHERE user_id=?", (user_id,)) as c:
            return await c.fetchone()


async def db_open_chat_session(user_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """INSERT INTO chat_sessions (user_id, status, opened_at)
               VALUES (?, 'active', CURRENT_TIMESTAMP)
               ON CONFLICT(user_id) DO UPDATE SET
                   status='active', opened_at=CURRENT_TIMESTAMP, closed_at=NULL, closed_by=NULL""",
            (user_id,)
        )
        await db.commit()


async def db_close_chat_session(user_id: int, closed_by: str = "admin"):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            """INSERT INTO chat_sessions (user_id, status, closed_at, closed_by)
               VALUES (?, 'closed', CURRENT_TIMESTAMP, ?)
               ON CONFLICT(user_id) DO UPDATE SET
                   status='closed', closed_at=CURRENT_TIMESTAMP, closed_by=?""",
            (user_id, closed_by, closed_by)
        )
        await db.commit()


async def db_mark_read(user_id: int):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "UPDATE chat_messages SET is_read=1 WHERE user_id=? AND sender='user'", (user_id,)
        )
        await db.commit()


async def db_get_active_chats():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT cs.user_id, u.full_name, u.username, cs.opened_at,
               SUM(CASE WHEN cm.sender='user' AND cm.is_read=0 THEN 1 ELSE 0 END) as unread,
               MAX(cm.created_at) as last_msg,
               (SELECT text FROM chat_messages WHERE user_id=cs.user_id ORDER BY created_at DESC LIMIT 1) as last_text
               FROM chat_sessions cs
               LEFT JOIN users u ON cs.user_id = u.user_id
               LEFT JOIN chat_messages cm ON cm.user_id = cs.user_id
               WHERE cs.status='active'
               GROUP BY cs.user_id
               ORDER BY last_msg DESC"""
        ) as c:
            return await c.fetchall()


async def db_get_closed_chats():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT cs.user_id, u.full_name, u.username, cs.opened_at, cs.closed_at, cs.closed_by,
               COUNT(cm.id) as msg_count,
               (SELECT text FROM chat_messages WHERE user_id=cs.user_id ORDER BY created_at DESC LIMIT 1) as last_text
               FROM chat_sessions cs
               LEFT JOIN users u ON cs.user_id = u.user_id
               LEFT JOIN chat_messages cm ON cm.user_id = cs.user_id
               WHERE cs.status='closed'
               GROUP BY cs.user_id
               ORDER BY cs.closed_at DESC"""
        ) as c:
            return await c.fetchall()


async def db_get_all_chats():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT cs.user_id, u.full_name, u.username, cs.status,
               SUM(CASE WHEN cm.sender='user' AND cm.is_read=0 THEN 1 ELSE 0 END) as unread,
               MAX(cm.created_at) as last_msg
               FROM chat_sessions cs
               LEFT JOIN users u ON cs.user_id = u.user_id
               LEFT JOIN chat_messages cm ON cm.user_id = cs.user_id
               GROUP BY cs.user_id
               ORDER BY last_msg DESC"""
        ) as c:
            return await c.fetchall()


# ── Analytics ─────────────────────────────────────────────────

async def db_analytics_revenue(period: str = "day"):
    """Revenue for day/week/month"""
    if period == "day":
        since = "datetime('now', '-1 day')"
    elif period == "week":
        since = "datetime('now', '-7 days')"
    else:
        since = "datetime('now', '-30 days')"
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            f"SELECT COALESCE(SUM(price),0) FROM purchases WHERE created_at >= {since}"
        ) as c:
            r = await c.fetchone()
            return r[0] if r else 0


async def db_analytics_sales_by_day(days: int = 7):
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            """SELECT date(created_at) as day, COUNT(*) as cnt, SUM(price) as revenue
               FROM purchases
               WHERE created_at >= datetime('now', ?)
               GROUP BY day ORDER BY day""",
            (f"-{days} days",)
        ) as c:
            return await c.fetchall()


async def db_analytics_top_products(limit: int = 5):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT product_name, COUNT(*) as cnt, SUM(price) as revenue
               FROM purchases GROUP BY product_name ORDER BY cnt DESC LIMIT ?""",
            (limit,)
        ) as c:
            return await c.fetchall()


async def db_analytics_top_buyers(limit: int = 5):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT p.user_id, u.full_name, u.username, COUNT(*) as cnt, SUM(p.price) as total
               FROM purchases p LEFT JOIN users u ON p.user_id=u.user_id
               GROUP BY p.user_id ORDER BY total DESC LIMIT ?""",
            (limit,)
        ) as c:
            return await c.fetchall()


async def db_analytics_top_buyers_10():
    """Top-10 buyers by total spend."""
    return await db_analytics_top_buyers(10)


async def db_get_all_users_for_log():
    """Return all users: id, username, full_name, created_at for daily log."""
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, full_name, created_at FROM users ORDER BY created_at DESC"
        ) as c:
            return await c.fetchall()


async def db_log_action(user_id: int, username: str, full_name: str, is_admin: bool, action: str, details: str = ""):
    """Write action to action_logs table."""
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO action_logs (user_id, username, full_name, is_admin, action, details) VALUES (?,?,?,?,?,?)",
            (user_id, username or "", full_name or "", 1 if is_admin else 0, action, details or "")
        )
        await db.commit()


async def db_get_logs(is_admin: bool = None, limit: int = 200):
    """Fetch action logs. is_admin=None means all, True=admins, False=users."""
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        if is_admin is None:
            async with db.execute(
                "SELECT * FROM action_logs ORDER BY created_at DESC LIMIT ?", (limit,)
            ) as c:
                return await c.fetchall()
        else:
            async with db.execute(
                "SELECT * FROM action_logs WHERE is_admin=? ORDER BY created_at DESC LIMIT ?",
                (1 if is_admin else 0, limit)
            ) as c:
                return await c.fetchall()


async def db_get_logs_by_user(user_id: int) -> list:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM action_logs WHERE user_id=? ORDER BY created_at DESC LIMIT 10000",
            (user_id,)
        ) as c:
            return await c.fetchall()


async def db_get_logs_txt(is_admin: bool = None) -> bytes:
    """Build TXT log content for download."""
    logs = await db_get_logs(is_admin=is_admin, limit=5000)
    label = "АДМИНИСТРАТОРЫ" if is_admin is True else ("ПОЛЬЗОВАТЕЛИ" if is_admin is False else "ВСЕ")
    lines = [
        f"=== ЛОГИ ДЕЙСТВИЙ — {label} ===",
        f"Дата выгрузки: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        f"Записей: {len(logs)}",
        "=" * 60,
        ""
    ]
    for row in logs:
        uname = f"@{row['username']}" if row["username"] else f"ID:{row['user_id']}"
        role_tag = "[ADMIN]" if row["is_admin"] else "[USER]"
        lines.append(
            f"{str(row['created_at'])[:16]}  {role_tag}  {uname}  {row['full_name'] or '—'}\n"
            f"  ➤ {row['action']}"
            + (f"\n  ℹ {row['details']}" if row["details"] else "")
        )
        lines.append("")
    return "\n".join(lines).encode("utf-8")


# ── Inactive users ────────────────────────────────────────────

async def db_get_inactive_users(days: int):
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE last_active <= datetime('now', ?) AND is_banned=0",
            (f"-{days} days",)
        ) as c:
            return [r[0] for r in await c.fetchall()]


async def db_get_product_subscribers():
    async with aiosqlite.connect(DB) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE notify_products=1 AND is_banned=0"
        ) as c:
            return [r[0] for r in await c.fetchall()]


# ╔══════════════════════════════════════════════════════════════╗
# ║                     CRYPTOBOT API                           ║
# ╚══════════════════════════════════════════════════════════════╝

CRYPTO_API_URL = "https://pay.crypt.bot/api"


async def crypto_create_invoice(amount: float, desc: str = "") -> dict | None:
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    payload = {
        "asset": "USDT",
        "amount": str(round(amount, 2)),
        "description": desc[:100] if desc else "Пополнение баланса",
        "expires_in": 3600,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(
                f"{CRYPTO_API_URL}/createInvoice",
                json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
                if data.get("ok"):
                    return data["result"]
                logging.error(f"CryptoBot error: {data}")
    except Exception as e:
        logging.error(f"CryptoBot create_invoice: {e}")
    return None


async def crypto_check_invoice(invoice_id: int) -> dict | None:
    headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"{CRYPTO_API_URL}/getInvoices",
                params={"invoice_ids": str(invoice_id)},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
                if data.get("ok") and data["result"]["items"]:
                    return data["result"]["items"][0]
    except Exception as e:
        logging.error(f"CryptoBot check_invoice: {e}")
    return None


# ╔══════════════════════════════════════════════════════════════╗
# ║                  ВСПОМОГАТЕЛЬНЫЕ                            ║
# ╚══════════════════════════════════════════════════════════════╝

def usd(amount: float) -> str:
    return f"${amount:.2f}"


def fmt_date(dt) -> str:
    return str(dt)[:16].replace("T", " ") if dt else "—"


def fmt_chat_history(history: list, user_name: str, user_uname: str, bot_name: str, bot_uname: str) -> str:
    """Format chat history with full usernames and bot name side by side."""
    lines = []
    for m in history:
        time = fmt_date(m["created_at"])[5:16]
        if m["sender"] == "user":
            who = f"👤 {user_name}"
            if user_uname:
                who += f" (@{user_uname})"
        else:
            who = f"🤖 {bot_name}"
            if bot_uname:
                who += f" (@{bot_uname})"
        lines.append(f"<b>{who}</b> <i>[{time}]</i>\n└ {m['text']}")
    return "\n\n".join(lines)


async def safe_edit(cb: CallbackQuery, text: str, kb=None):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest:
        pass
    await cb.answer()


async def log_to_channel(bot: Bot, text: str):
    if LOG_CHANNEL_ID:
        try:
            await bot.send_message(LOG_CHANNEL_ID, text)
        except Exception as e:
            logging.warning(f"Log channel error: {e}")


async def log_action_from_msg(message: Message, is_admin_flag: bool, action: str, details: str = ""):
    uid = message.from_user.id
    uname = message.from_user.username or ""
    fname = message.from_user.full_name or ""
    await db_log_action(uid, uname, fname, is_admin_flag, action, details)


async def log_action_from_cb(cb: CallbackQuery, is_admin_flag: bool, action: str, details: str = ""):
    uid = cb.from_user.id
    uname = cb.from_user.username or ""
    fname = cb.from_user.full_name or ""
    await db_log_action(uid, uname, fname, is_admin_flag, action, details)


# ╔══════════════════════════════════════════════════════════════╗
# ║                    КЛАВИАТУРЫ                               ║
# ╚══════════════════════════════════════════════════════════════╝

def kb_main():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🛒 Магазин", callback_data="shop"))
    b.row(
        InlineKeyboardButton(text="👤 Профиль",   callback_data="profile"),
        InlineKeyboardButton(text="🛍 Покупки",   callback_data="my_purchases"),
    )
    b.row(
        InlineKeyboardButton(text="👥 Рефералы",  callback_data="referrals"),
        InlineKeyboardButton(text="💬 Поддержка", callback_data="support"),
    )
    b.row(InlineKeyboardButton(text="ℹ️ О нас",   callback_data="about"))
    return b.as_markup()


def kb_back():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu"))
    return b.as_markup()


def kb_profile():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="💰 Пополнить баланс", callback_data="topup"))
    b.row(InlineKeyboardButton(text="◀️ Назад",            callback_data="main_menu"))
    return b.as_markup()


def kb_shop(products: list, page: int = 0, per: int = 6):
    b = InlineKeyboardBuilder()
    s, e = page * per, page * per + per
    for p in products[s:e]:
        stock_info = ""
        if p["stock_limit"] is not None:
            left = p["stock_limit"] - p["sold_count"]
            stock_info = f" [{left}шт]"
        b.row(InlineKeyboardButton(
            text=f"{p['name']}  —  {usd(p['price'])}{stock_info}",
            callback_data=f"product:{p['id']}"
        ))
    nav = []
    if page > 0:     nav.append(InlineKeyboardButton(text="◀️", callback_data=f"shop_page:{page-1}"))
    if e < len(products): nav.append(InlineKeyboardButton(text="▶️", callback_data=f"shop_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_product(pid: int):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🎟 Применить промокод", callback_data=f"promo_enter:{pid}"))
    b.row(InlineKeyboardButton(text="💳 Купить без промокода", callback_data=f"buy:{pid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",  callback_data="shop"))
    return b.as_markup()


def kb_confirm(pid: int, promo: str = None):
    data_confirm = f"confirm_buy:{pid}" + (f":{promo}" if promo else "")
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=data_confirm),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"product:{pid}"),
    )
    return b.as_markup()


def kb_topup():
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="$5",   callback_data="topup_amount:5"),
        InlineKeyboardButton(text="$10",  callback_data="topup_amount:10"),
        InlineKeyboardButton(text="$25",  callback_data="topup_amount:25"),
    )
    b.row(
        InlineKeyboardButton(text="$50",  callback_data="topup_amount:50"),
        InlineKeyboardButton(text="$100", callback_data="topup_amount:100"),
    )
    b.row(InlineKeyboardButton(text="✏️ Своя сумма", callback_data="topup_custom"))
    b.row(InlineKeyboardButton(text="◀️ Назад",      callback_data="profile"))
    return b.as_markup()


def kb_invoice(pay_url: str, invoice_id: int):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="💎 Оплатить USDT",    url=pay_url))
    b.row(InlineKeyboardButton(text="✅ Проверить оплату", callback_data=f"check_payment:{invoice_id}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",            callback_data="topup"))
    return b.as_markup()


def kb_purchases(purchases: list, page: int = 0, per: int = 5):
    b = InlineKeyboardBuilder()
    s, e = page * per, page * per + per
    for p in purchases[s:e]:
        b.row(InlineKeyboardButton(
            text=f"🛍 {p['product_name']} — {usd(p['price'])}",
            callback_data=f"purchase_detail:{p['id']}"
        ))
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="◀️", callback_data=f"purchases_page:{page-1}"))
    if e < len(purchases): nav.append(InlineKeyboardButton(text="▶️", callback_data=f"purchases_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_referrals():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="👥 Список рефералов", callback_data="my_referrals"))
    b.row(InlineKeyboardButton(text="◀️ Назад",            callback_data="main_menu"))
    return b.as_markup()


def kb_ref_list(total: int, page: int, per: int = 8):
    b = InlineKeyboardBuilder()
    nav = []
    if page > 0:               nav.append(InlineKeyboardButton(text="◀️", callback_data=f"referrals_page:{page-1}"))
    if (page+1)*per < total:   nav.append(InlineKeyboardButton(text="▶️", callback_data=f"referrals_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="referrals"))
    return b.as_markup()


def kb_support():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✉️ Написать в поддержку", callback_data="open_chat"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    return b.as_markup()


def kb_admin(role: str = ROLE_SUPERADMIN):
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="📊 Аналитика",      callback_data="admin_analytics"),
        InlineKeyboardButton(text="💬 Чаты",            callback_data="admin_chats"),
    )
    if role in (ROLE_SUPERADMIN, ROLE_ADMIN):
        b.row(
            InlineKeyboardButton(text="📦 Товары",          callback_data="admin_products"),
            InlineKeyboardButton(text="➕ Добавить товар",   callback_data="admin_add_product"),
        )
        b.row(
            InlineKeyboardButton(text="🧾 Покупки",          callback_data="admin_purchases"),
            InlineKeyboardButton(text="💸 Баланс юзеру",     callback_data="admin_give_balance"),
        )
        b.row(
            InlineKeyboardButton(text="🎟 Промокоды",        callback_data="admin_promos"),
            InlineKeyboardButton(text="📢 Рассылка",         callback_data="admin_broadcast"),
        )
        b.row(
            InlineKeyboardButton(text="✉️ После покупки",    callback_data="admin_post_purchase"),
            InlineKeyboardButton(text="🔍 Поиск юзера",      callback_data="admin_search_user"),
        )
        b.row(
            InlineKeyboardButton(text="🔔 Неактивные",       callback_data="admin_notify_inactive"),
            InlineKeyboardButton(text="🏆 Топ покупателей",  callback_data="admin_top_buyers"),
        )
        # ── Новые фичи ──
        b.row(
            InlineKeyboardButton(text="📨 Сообщение юзеру",  callback_data="admin_direct_message"),
            InlineKeyboardButton(text="📈 Статистика товаров",callback_data="admin_product_stats"),
        )
        b.row(
            InlineKeyboardButton(text="💰 Фильтр по балансу",callback_data="admin_filter_balance"),
            InlineKeyboardButton(text="🚫 Забаненные",        callback_data="admin_banned_list"),
        )
        b.row(
            InlineKeyboardButton(text="🧾 Все транзакции",   callback_data="admin_all_transactions"),
            InlineKeyboardButton(text="⚠️ Подозрительные",   callback_data="admin_suspicious"),
        )
        b.row(
            InlineKeyboardButton(text="🏪 Режим магазина",   callback_data="admin_shop_mode"),
            InlineKeyboardButton(text="🎁 Реф. бонус",       callback_data="admin_set_referral_bonus"),
        )
        b.row(InlineKeyboardButton(text="💾 Бэкап БД",       callback_data="admin_backup"))
        if role == ROLE_SUPERADMIN:
            b.row(InlineKeyboardButton(text="📋 Быстрые логи",              callback_data="admin_logs_menu"))
            b.row(InlineKeyboardButton(text="📤 Отчёт по пользователю",     callback_data="admin_send_digest"))
            b.row(InlineKeyboardButton(text="🛡 Управление правами",         callback_data="admin_roles"))
    b.row(InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"))
    return b.as_markup()


def kb_admin_products(products: list):
    b = InlineKeyboardBuilder()
    for p in products:
        icon = "✅" if p["is_active"] else "❌"
        stock = f" [{p['stock_limit']-p['sold_count']}шт]" if p["stock_limit"] is not None else ""
        b.row(InlineKeyboardButton(
            text=f"{icon} {p['name']}  |  {usd(p['price'])}{stock}",
            callback_data=f"admin_product:{p['id']}"
        ))
    b.row(InlineKeyboardButton(text="➕ Добавить", callback_data="admin_add_product"))
    b.row(InlineKeyboardButton(text="◀️ Назад",   callback_data="admin_panel"))
    return b.as_markup()


def kb_admin_users(total: int, page: int, per: int = 8):
    b = InlineKeyboardBuilder()
    nav = []
    if page > 0:             nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_page:{page-1}"))
    if (page+1)*per < total: nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    return b.as_markup()


def kb_admin_product(pid: int, is_active: int):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text="❌ Скрыть" if is_active else "✅ Показать",
        callback_data=f"admin_toggle:{pid}"
    ))
    b.row(InlineKeyboardButton(text="✏️ Изменить цену",        callback_data=f"admin_edit_price:{pid}"))
    b.row(InlineKeyboardButton(text="📦 Лимит товара",          callback_data=f"admin_stock_limit:{pid}"))
    b.row(InlineKeyboardButton(text="✉️ Сообщение после покупки", callback_data=f"admin_product_msg:{pid}"))
    b.row(InlineKeyboardButton(text="🗑 Удалить",               callback_data=f"admin_delete:{pid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",                 callback_data="admin_products"))
    return b.as_markup()


def kb_admin_user_actions(user_id: int, is_banned: bool):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text="✅ Разбанить" if is_banned else "🚫 Забанить",
        callback_data=f"admin_ban_toggle:{user_id}"
    ))
    b.row(InlineKeyboardButton(text="💸 Выдать/Списать баланс", callback_data=f"admin_give_bal_uid:{user_id}"))
    b.row(InlineKeyboardButton(text="💬 Открыть чат", callback_data=f"admin_open_chat:{user_id}"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_users"))
    return b.as_markup()


def kb_chat_user():
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="support"))
    return b.as_markup()


# ╔══════════════════════════════════════════════════════════════╗
# ║                       FSM STATES                            ║
# ╚══════════════════════════════════════════════════════════════╝

class TopupFSM(StatesGroup):
    custom_amount = State()


class AddProductFSM(StatesGroup):
    name       = State()
    desc       = State()
    price      = State()
    content    = State()
    stock      = State()


class EditPriceFSM(StatesGroup):
    waiting = State()


class StockLimitFSM(StatesGroup):
    waiting = State()


class BroadcastFSM(StatesGroup):
    waiting = State()
    confirm = State()


class GiveBalanceFSM(StatesGroup):
    user_id = State()
    amount  = State()


class GiveBalanceDirectFSM(StatesGroup):
    amount = State()


class PostPurchaseFSM(StatesGroup):
    waiting = State()


class EditProductMsgFSM(StatesGroup):
    waiting = State()


class SearchUserFSM(StatesGroup):
    waiting = State()


class AdminChatFSM(StatesGroup):
    chatting = State()


class UserChatFSM(StatesGroup):
    chatting = State()


class PromoAddFSM(StatesGroup):
    code     = State()
    discount = State()
    ptype    = State()
    max_uses = State()


class PromoEnterFSM(StatesGroup):
    code = State()


class GreetingFSM(StatesGroup):
    waiting = State()


class AddAdminFSM(StatesGroup):
    user_id = State()
    role    = State()
    note    = State()


class UserLogFSM(StatesGroup):
    waiting_id = State()


class FilterBalanceFSM(StatesGroup):
    waiting_amount = State()


class DirectMessageFSM(StatesGroup):
    waiting_id   = State()
    waiting_text = State()


class SetReferralBonusFSM(StatesGroup):
    waiting_amount = State()


# ╔══════════════════════════════════════════════════════════════╗
# ║                       ХЭНДЛЕРЫ                              ║
# ╚══════════════════════════════════════════════════════════════╝

router = Router()


# ── /start ────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    uid       = message.from_user.id
    username  = message.from_user.username  or ""
    full_name = message.from_user.full_name or "Пользователь"

    referred_by = None
    args = message.text.split()
    if len(args) > 1:
        referrer = await db_get_user_by_ref(args[1])
        if referrer and referrer["user_id"] != uid:
            referred_by = referrer["user_id"]

    user   = await db_get_user(uid)
    is_new = user is None

    if is_new:
        user = await db_create_user(uid, username, full_name, referred_by)

    if user and user["is_banned"]:
        await message.answer("🚫 Ваш аккаунт заблокирован.")
        return

    await db_update_last_active(uid)

    greeting_text = await db_get_setting("greeting_message") or ""
    if is_new:
        greet = "🎊 Вы успешно зарегистрированы!"
    else:
        greet = "✅ С возвращением!"

    text = f"👋 Привет, <b>{full_name}</b>!\n\n{greet}"
    if greeting_text:
        text += f"\n\n{greeting_text}"
    else:
        text += "\n\nВыберите раздел:"

    await message.answer(text, reply_markup=kb_main())
    await db_log_action(uid, username, full_name, False, "Запустил бота /start", "новый" if is_new else "вернулся")


# ── /backup ───────────────────────────────────────────────────

@router.message(Command("backup"))
async def cmd_backup(message: Message, bot: Bot):
    if not await is_admin(message.from_user.id): return
    try:
        backup_path = f"/tmp/store_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(DB, backup_path)
        with open(backup_path, "rb") as f:
            await bot.send_document(
                message.from_user.id,
                f,
                caption=f"💾 <b>Резервная копия БД</b>\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
    except Exception as e:
        await message.answer(f"❌ Ошибка создания бэкапа: {e}")


# ── Главное меню ──────────────────────────────────────────────

@router.callback_query(F.data == "main_menu")
async def cb_main_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await db_get_user(cb.from_user.id)
    if not user:
        await cb.answer("Напишите /start", show_alert=True); return
    if user["is_banned"]:
        await cb.answer("🚫 Ваш аккаунт заблокирован", show_alert=True); return
    await db_update_last_active(cb.from_user.id)
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Нажал ◀️ Главное меню")
    await safe_edit(
        cb,
        f"👋 Привет, <b>{user['full_name']}</b>!\n\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"Выберите раздел:",
        kb_main()
    )


# ── Магазин ───────────────────────────────────────────────────

@router.callback_query(F.data == "shop")
@router.callback_query(F.data.startswith("shop_page:"))
async def cb_shop(cb: CallbackQuery):
    # Проверка режима техобслуживания
    shop_mode = await db_get_setting("shop_mode") or "open"
    if shop_mode == "closed" and cb.from_user.id not in ADMIN_IDS:
        await safe_edit(
            cb,
            "🔧 <b>Магазин временно недоступен</b>\n\n"
            "Ведутся технические работы. Скоро всё заработает!\n"
            "Приносим извинения за неудобства.",
            kb_back()
        )
        return
    page     = int(cb.data.split(":")[1]) if ":" in cb.data else 0
    products = await db_get_products()
    if not products:
        await safe_edit(cb, "🛒 <b>Магазин</b>\n\n😔 Товаров пока нет.", kb_back()); return
    await safe_edit(
        cb,
        f"🛒 <b>Магазин</b>\n\nДоступно товаров: <b>{len(products)}</b>\n\nВыберите товар:",
        kb_shop(list(products), page=page)
    )
    if cb.data == "shop":
        await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Нажал кнопку 🛒 Магазин")


@router.callback_query(F.data.startswith("product:"))
async def cb_product(cb: CallbackQuery):
    pid     = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    if not product:
        await cb.answer("Товар не найден", show_alert=True); return
    user    = await db_get_user(cb.from_user.id)
    balance = user["balance"] if user else 0
    stock_info = ""
    if product["stock_limit"] is not None:
        left = product["stock_limit"] - product["sold_count"]
        stock_info = f"\n📦 Осталось: <b>{left} шт.</b>"
    await safe_edit(
        cb,
        f"📦 <b>{product['name']}</b>\n\n"
        f"📝 {product['description'] or 'Нет описания'}\n\n"
        f"💰 Цена: <b>{usd(product['price'])}</b>\n"
        f"💳 Ваш баланс: <b>{usd(balance)}</b>{stock_info}",
        kb_product(pid)
    )


# ── Промокод при покупке ──────────────────────────────────────

@router.callback_query(F.data.startswith("promo_enter:"))
async def cb_promo_enter(cb: CallbackQuery, state: FSMContext):
    pid = int(cb.data.split(":")[1])
    await state.set_state(PromoEnterFSM.code)
    await state.update_data(product_id=pid)
    await safe_edit(cb, "🎟 <b>Введите промокод:</b>\n\nЕсли у вас нет промокода, нажмите отмену.")
    # Fallback кнопка
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data=f"product:{pid}"))
    await cb.message.edit_reply_markup(reply_markup=b.as_markup())


@router.message(StateFilter(PromoEnterFSM.code))
async def fsm_promo_code(message: Message, state: FSMContext):
    user = await db_get_user(message.from_user.id)
    if not user: return
    data = await state.get_data()
    pid  = data["product_id"]
    code = message.text.strip().upper()
    await state.clear()

    promo = await db_get_promo(code)
    product = await db_get_product(pid)

    if not promo or not promo["is_active"]:
        await message.answer("❌ Промокод не найден или неактивен.", reply_markup=kb_product(pid))
        return

    already_used = await db_promo_used_by(promo["id"], user["user_id"])
    if already_used:
        await message.answer("❌ Вы уже использовали этот промокод.", reply_markup=kb_product(pid))
        return

    if promo["type"] == "percent":
        discount = round(product["price"] * promo["discount"] / 100, 2)
        disc_text = f"{promo['discount']}%  (-{usd(discount)})"
    else:
        discount = min(promo["discount"], product["price"])
        disc_text = f"-{usd(discount)}"

    final_price = max(0.0, product["price"] - discount)

    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_buy:{pid}:{code}"),
        InlineKeyboardButton(text="❌ Отмена",       callback_data=f"product:{pid}"),
    )
    await message.answer(
        f"🎟 <b>Промокод применён!</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n"
        f"Скидка: <b>{disc_text}</b>\n"
        f"Цена со скидкой: <b>{usd(final_price)}</b>\n"
        f"Ваш баланс: <b>{usd(user['balance'])}</b>",
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data.startswith("buy:"))
async def cb_buy(cb: CallbackQuery):
    pid     = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    if not product:
        await cb.answer("Товар не найден", show_alert=True); return
    user = await db_get_user(cb.from_user.id)
    if not user:
        await cb.answer("Напишите /start", show_alert=True); return
    enough = user["balance"] >= product["price"]
    await safe_edit(
        cb,
        f"🛒 <b>Подтверждение покупки</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n"
        f"Цена: <b>{usd(product['price'])}</b>\n"
        f"Баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"{'✅ Средств достаточно' if enough else '❌ Недостаточно средств — пополните баланс'}",
        kb_confirm(pid)
    )


@router.callback_query(F.data.startswith("confirm_buy:"))
async def cb_confirm_buy(cb: CallbackQuery, bot: Bot):
    parts = cb.data.split(":")
    pid   = int(parts[1])
    promo = parts[2] if len(parts) > 2 else None

    result = await db_buy(cb.from_user.id, pid, promo)
    if result is None or (isinstance(result, tuple) and len(result) == 2 and result[0] is None):
        err = result[1] if result else "Ошибка"
        await cb.answer(f"❌ {err}", show_alert=True); return

    product, err, final_price, discount = result
    if err:
        await cb.answer(f"❌ {err}", show_alert=True); return

    user = await db_get_user(cb.from_user.id)
    uname = f"@{user['username']}" if user["username"] else "без username"

    # Уведомление всем админам
    for admin_id in await get_all_admin_ids():
        try:
            disc_text = f"\n🎟 Скидка: -{usd(discount)} (промокод {promo})" if discount > 0 else ""
            await bot.send_message(
                admin_id,
                f"🛍 <b>Новая покупка!</b>\n\n"
                f"👤 Покупатель: <b>{user['full_name']}</b> ({uname})\n"
                f"🆔 ID: <code>{user['user_id']}</code>\n"
                f"📦 Товар: <b>{product['name']}</b>\n"
                f"💰 Сумма: <b>{usd(final_price)}</b>{disc_text}\n"
                f"💳 Остаток у покупателя: <b>{usd(user['balance'])}</b>"
            )
        except Exception:
            pass

    # Лог в канал
    await log_to_channel(
        bot,
        f"🛒 Покупка: {user['full_name']} ({uname}) — {product['name']} — {usd(final_price)}"
    )
    await db_log_action(cb.from_user.id, user["username"] or "", user["full_name"] or "", False,
                        f"Купил товар: {product['name']}", f"Цена: {usd(final_price)}")

    # Проверка низкого баланса
    if user["balance"] < LOW_BALANCE_THRESHOLD:
        try:
            await bot.send_message(
                cb.from_user.id,
                f"⚠️ <b>Внимание!</b> На вашем балансе осталось <b>{usd(user['balance'])}</b>.\n"
                f"Пополните баланс, чтобы продолжить покупки!",
                reply_markup=kb_profile()
            )
        except Exception:
            pass

    # Отправка содержимого
    if product["file_id"]:
        await safe_edit(
            cb,
            f"✅ <b>Покупка успешна!</b>\n\n"
            f"Товар: <b>{product['name']}</b>\n"
            f"Стоимость: <b>{usd(final_price)}</b>\n"
            f"Остаток: <b>{usd(user['balance'])}</b>\n\n"
            f"📎 Ваш файл отправлен следующим сообщением:",
            kb_back()
        )
        try:
            await bot.send_document(
                cb.from_user.id,
                document=product["file_id"],
                caption=f"📦 <b>{product['name']}</b>\n{product['content'] or ''}"
            )
        except Exception as e:
            await bot.send_message(cb.from_user.id, f"❌ Ошибка отправки файла: {e}")
    else:
        await safe_edit(
            cb,
            f"✅ <b>Покупка успешна!</b>\n\n"
            f"Товар: <b>{product['name']}</b>\n"
            f"Стоимость: <b>{usd(final_price)}</b>\n"
            f"Остаток: <b>{usd(user['balance'])}</b>\n\n"
            f"📦 <b>Содержимое:</b>\n<code>{product['content']}</code>",
            kb_back()
        )

    post_msg = product["post_message"] or await db_get_setting("post_purchase_message")
    if post_msg:
        try:
            await bot.send_message(cb.from_user.id, post_msg)
        except Exception:
            pass


# ── Профиль ───────────────────────────────────────────────────

@router.callback_query(F.data == "profile")
async def cb_profile(cb: CallbackQuery):
    user = await db_get_user(cb.from_user.id)
    if not user:
        await cb.answer("Напишите /start", show_alert=True); return
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл раздел Профиль")
    purchases = await db_get_purchases(cb.from_user.id)
    notify = "🔔 Вкл" if user["notify_products"] else "🔕 Выкл"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="💰 Пополнить баланс", callback_data="topup"))
    b.row(InlineKeyboardButton(
        text=f"Уведомления о товарах: {notify}",
        callback_data="toggle_notify"
    ))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    await safe_edit(
        cb,
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"👤 Имя: <b>{user['full_name']}</b>\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n"
        f"🛍 Покупок: <b>{len(purchases)}</b>\n"
        f"👥 Рефералов: <b>{user['referral_count']}</b>\n"
        f"🎁 Заработано: <b>{usd(user['referral_earned'])}</b>\n"
        f"📅 Регистрация: <b>{fmt_date(user['created_at'])}</b>",
        b.as_markup()
    )


@router.callback_query(F.data == "toggle_notify")
async def cb_toggle_notify(cb: CallbackQuery):
    user = await db_get_user(cb.from_user.id)
    if not user: return
    new_val = 0 if user["notify_products"] else 1
    async with aiosqlite.connect(DB) as db:
        await db.execute("UPDATE users SET notify_products=? WHERE user_id=?", (new_val, user["user_id"]))
        await db.commit()
    status_str = "включил" if new_val else "выключил"
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, f"Уведомления о товарах — {status_str}")
    await cb.answer("✅ Настройки уведомлений обновлены")
    await cb_profile(cb)


# ── Пополнение баланса ────────────────────────────────────────

@router.callback_query(F.data == "topup")
async def cb_topup(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    user = await db_get_user(cb.from_user.id)
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл раздел Пополнение баланса")
    await safe_edit(
        cb,
        f"💰 <b>Пополнение баланса</b>\n\n"
        f"Текущий баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"Оплата принимается в <b>USDT</b> через CryptoBot.\n"
        f"Выберите сумму или введите свою:",
        kb_topup()
    )


@router.callback_query(F.data == "topup_custom")
async def cb_topup_custom(cb: CallbackQuery, state: FSMContext):
    await state.set_state(TopupFSM.custom_amount)
    await safe_edit(
        cb,
        "✏️ <b>Введите сумму в $</b>\n\n"
        "Например: <code>15</code> или <code>7.50</code>\n\n"
        "Минимальная сумма: <b>$1.00</b>"
    )


@router.message(StateFilter(TopupFSM.custom_amount))
async def fsm_custom_amount(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Введите сумму числом, например: <code>10</code>"); return
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount < 1:
            raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Некорректная сумма. Введите число от 1:"); return
    await state.clear()
    await _send_invoice(message, message.from_user.id, amount, send_new=True)


@router.callback_query(F.data.startswith("topup_amount:"))
async def cb_topup_amount(cb: CallbackQuery):
    amount = float(cb.data.split(":")[1])
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, f"Выбрал сумму пополнения: {usd(amount)}")
    await cb.answer("⏳ Создаём счёт...")
    await _send_invoice(cb.message, cb.from_user.id, amount, send_new=False, cb_obj=cb)


async def _send_invoice(msg, user_id: int, amount: float, send_new: bool = True, cb_obj=None):
    invoice = await crypto_create_invoice(amount, f"Пополнение {usd(amount)} | ID:{user_id}")
    if not invoice:
        text = "❌ Ошибка создания счёта. Проверьте CRYPTO_BOT_TOKEN."
        if send_new:
            await msg.answer(text, reply_markup=kb_topup())
        else:
            await safe_edit(cb_obj, text, kb_topup())
        return

    await db_save_invoice(user_id, invoice["invoice_id"], amount)

    text = (
        f"💎 <b>Счёт на оплату создан</b>\n\n"
        f"Сумма: <b>{usd(amount)}</b>\n"
        f"Валюта оплаты: <b>USDT</b>\n\n"
        f"1. Нажмите <b>«Оплатить USDT»</b>\n"
        f"2. Оплатите в CryptoBot\n"
        f"3. Нажмите <b>«Проверить оплату»</b>\n\n"
        f"⏳ Счёт действует <b>1 час</b>"
    )
    kb = kb_invoice(invoice["bot_invoice_url"], invoice["invoice_id"])

    if send_new:
        await msg.answer(text, reply_markup=kb)
    else:
        await safe_edit(cb_obj, text, kb)


@router.callback_query(F.data.startswith("check_payment:"))
async def cb_check_payment(cb: CallbackQuery, bot: Bot):
    invoice_id = int(cb.data.split(":")[1])
    await cb.answer("⏳ Проверяем оплату...")

    local = await db_get_invoice(invoice_id)
    if not local:
        await cb.answer("❌ Счёт не найден", show_alert=True); return
    if local["status"] == "paid":
        await cb.answer("✅ Этот счёт уже был оплачен ранее", show_alert=True); return

    remote = await crypto_check_invoice(invoice_id)
    if not remote:
        await cb.answer("❌ Ошибка связи с CryptoBot. Попробуйте позже.", show_alert=True); return

    if remote["status"] == "paid":
        amount = local["amount"]
        user_id = cb.from_user.id
        await db_mark_paid(invoice_id)
        await db_update_balance(user_id, amount, f"Пополнение через CryptoBot #{invoice_id}")

        # Лог
        user = await db_get_user(user_id)
        uname = f"@{user['username']}" if user and user["username"] else str(user_id)
        await log_to_channel(bot, f"💰 Пополнение: {uname} +{usd(amount)}")

        depositor = await db_get_user(user_id)
        if depositor and depositor["referred_by"]:
            await db_referral_bonus(user_id, depositor["referred_by"])
            try:
                await cb.bot.send_message(
                    depositor["referred_by"],
                    f"🎉 Ваш реферал пополнил баланс!\n"
                    f"Вам начислен бонус <b>{usd(REFERRAL_BONUS)}</b>."
                )
            except Exception:
                pass

        user = await db_get_user(user_id)
        await safe_edit(
            cb,
            f"✅ <b>Оплата получена!</b>\n\n"
            f"Зачислено: <b>+{usd(amount)}</b>\n"
            f"Новый баланс: <b>{usd(user['balance'])}</b>",
            kb_profile()
        )
    else:
        await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Проверил оплату — не поступила", f"Счёт #{invoice_id} | сумма: {usd(local['amount'])}")
        await cb.answer("⏳ Оплата ещё не поступила. Подождите и попробуйте снова.", show_alert=True)


# ── Мои покупки ───────────────────────────────────────────────

@router.callback_query(F.data == "my_purchases")
@router.callback_query(F.data.startswith("purchases_page:"))
async def cb_my_purchases(cb: CallbackQuery):
    page      = int(cb.data.split(":")[1]) if ":" in cb.data else 0
    purchases = await db_get_purchases(cb.from_user.id)
    if not purchases:
        await safe_edit(cb, "🛍 <b>Мои покупки</b>\n\nУ вас ещё нет покупок.", kb_back()); return
    if cb.data == "my_purchases":
        await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл Мои покупки", f"Всего покупок: {len(purchases)}")
    total = sum(p["price"] for p in purchases)
    await safe_edit(
        cb,
        f"🛍 <b>Мои покупки</b>\n\n"
        f"Всего: <b>{len(purchases)}</b>  |  Потрачено: <b>{usd(total)}</b>\n\n"
        f"Нажмите на покупку для просмотра:",
        kb_purchases(list(purchases), page=page)
    )


@router.callback_query(F.data.startswith("purchase_detail:"))
async def cb_purchase_detail(cb: CallbackQuery, bot: Bot):
    pid       = int(cb.data.split(":")[1])
    purchases = await db_get_purchases(cb.from_user.id)
    p         = next((x for x in purchases if x["id"] == pid), None)
    if not p:
        await cb.answer("Покупка не найдена", show_alert=True); return
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False,
                        "Посмотрел детали покупки", f"{p['product_name']} | {usd(p['price'])}")
    disc_text = f"\n🎟 Скидка: -{usd(p['discount'])} ({p['promo_code']})" if p["discount"] and p["discount"] > 0 else ""
    text = (
        f"🛍 <b>Детали покупки</b>\n\n"
        f"📦 Товар: <b>{p['product_name']}</b>\n"
        f"💰 Цена: <b>{usd(p['price'])}</b>{disc_text}\n"
        f"📅 Дата: <b>{fmt_date(p['created_at'])}</b>"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="my_purchases"))
    if p["file_id"]:
        text += f"\n\n📎 Файл: <code>{p['file_name'] or 'file'}</code>"
        await safe_edit(cb, text, b.as_markup())
        try:
            await bot.send_document(
                cb.from_user.id,
                document=p["file_id"],
                caption=f"📦 <b>{p['product_name']}</b>"
            )
        except Exception as e:
            await bot.send_message(cb.from_user.id, f"❌ Ошибка повторной отправки файла: {e}")
    else:
        if p["content"]:
            text += f"\n\n📋 <b>Содержимое:</b>\n<code>{p['content']}</code>"
        await safe_edit(cb, text, b.as_markup())


# ── Рефералы ──────────────────────────────────────────────────

@router.callback_query(F.data == "referrals")
async def cb_referrals(cb: CallbackQuery):
    user = await db_get_user(cb.from_user.id)
    if not user:
        await cb.answer("Напишите /start", show_alert=True); return
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл Реферальную программу")
    bot_info = await cb.bot.get_me()
    link     = f"https://t.me/{bot_info.username}?start={user['referral_code']}"
    await safe_edit(
        cb,
        f"👥 <b>Реферальная программа</b>\n\n"
        f"🔗 Ваша ссылка:\n<code>{link}</code>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"├ Приглашено: <b>{user['referral_count']}</b> чел.\n"
        f"└ Заработано: <b>{usd(user['referral_earned'])}</b>\n\n"
        f"💡 За каждого приглашённого вы получаете <b>{usd(REFERRAL_BONUS)}</b> на баланс!",
        kb_referrals()
    )


@router.callback_query(F.data == "my_referrals")
@router.callback_query(F.data.startswith("referrals_page:"))
async def cb_my_referrals(cb: CallbackQuery):
    page      = int(cb.data.split(":")[1]) if ":" in cb.data else 0
    per       = 8
    referrals = await db_get_referrals(cb.from_user.id)
    if not referrals:
        await safe_edit(cb, "👥 <b>Мои рефералы</b>\n\nВы ещё никого не пригласили.", kb_ref_list(0, 0)); return
    chunk = list(referrals)[page*per:(page+1)*per]
    lines = [f"{page*per+i+1}. <b>{r['full_name'] or r['username'] or 'id'+str(r['user_id'])}</b> — {fmt_date(r['created_at'])}"
             for i, r in enumerate(chunk)]
    await safe_edit(
        cb,
        f"👥 <b>Мои рефералы ({len(referrals)})</b>\n\n" + "\n".join(lines),
        kb_ref_list(len(referrals), page)
    )


# ── Поддержка / Чат пользователя ─────────────────────────────

@router.callback_query(F.data == "support")
async def cb_support(cb: CallbackQuery):
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл раздел Поддержка")
    await safe_edit(
        cb,
        f"💬 <b>Поддержка</b>\n\n"
        f"Если у вас возникли вопросы — напишите нам!\n\n"
        f"Нажмите кнопку «Открыть чат» и введите ваш вопрос — администратор ответит вам здесь.",
        kb_support()
    )


@router.callback_query(F.data == "open_chat")
async def cb_open_chat(cb: CallbackQuery, state: FSMContext, bot: Bot):
    uid = cb.from_user.id
    user = await db_get_user(uid)
    session = await db_get_chat_session(uid)
    is_closed = session and session["status"] == "closed"

    await db_open_chat_session(uid)
    await state.set_state(UserChatFSM.chatting)
    await db_log_action(uid, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл чат поддержки")

    history = await db_get_chat_history(uid, 10)
    bot_info = await bot.get_me()

    user_name = user["full_name"] if user else cb.from_user.full_name
    user_uname = user["username"] if user else (cb.from_user.username or "")
    bot_name = bot_info.full_name
    bot_uname = bot_info.username or ""

    text = "💬 <b>Чат с поддержкой</b>\n"
    text += f"<i>Бот: @{bot_uname} · Вы: @{user_uname or user_name}</i>\n\n"
    if is_closed:
        text += "🔓 <i>Чат переоткрыт</i>\n\n"
    if history:
        text += "<b>Последние сообщения:</b>\n\n"
        text += fmt_chat_history(history, user_name, user_uname, bot_name, bot_uname)
        text += "\n\n"
    text += "✏️ Введите ваше сообщение:"

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔒 Закрыть обращение", callback_data="close_user_chat"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="support"))
    await safe_edit(cb, text, b.as_markup())


@router.message(StateFilter(UserChatFSM.chatting))
async def fsm_user_chat(message: Message, state: FSMContext, bot: Bot):
    if message.text and message.text.startswith("/"):
        await state.clear()
        await message.answer("Чат закрыт.", reply_markup=kb_main())
        return

    user = await db_get_user(message.from_user.id)
    if not user: return

    # ── Антиспам: не более 3 сообщений без ответа ──
    CHAT_SPAM_LIMIT = 3
    unanswered = await db_count_unanswered_chat_msgs(user["user_id"])
    if unanswered >= CHAT_SPAM_LIMIT:
        await message.answer(
            f"⏳ <b>Ожидайте ответа оператора.</b>\n\n"
            f"Вы уже отправили {unanswered} сообщений без ответа.\n"
            f"Как только оператор ответит — вы снова сможете писать."
        )
        return

    await db_send_chat_msg(user["user_id"], "user", message.text or "[медиа]")
    await db_log_action(user["user_id"], user["username"] or "", user["full_name"] or "", False,
                        "Написал в поддержку", (message.text or "[медиа]")[:80])

    # Уведомляем всех админов
    uname = f"@{user['username']}" if user["username"] else f"ID:{user['user_id']}"
    for admin_id in await get_all_admin_ids():
        try:
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(text="💬 Ответить", callback_data=f"admin_open_chat:{user['user_id']}"))
            await bot.send_message(
                admin_id,
                f"💬 <b>Новое сообщение</b>\n\n"
                f"👤 <b>{user['full_name']}</b> ({uname})\n"
                f"📝 {message.text or '[медиа]'}",
                reply_markup=b.as_markup()
            )
        except Exception:
            pass

    await message.answer("✅ Отправлено. Ожидайте ответа.")


@router.callback_query(F.data == "close_user_chat")
async def cb_close_user_chat(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await db_close_chat_session(cb.from_user.id, closed_by="user")
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Закрыл обращение в поддержку")
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📨 Открыть новое обращение", callback_data="open_chat"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu"))
    await safe_edit(cb, "🔒 <b>Обращение закрыто.</b>\n\nСпасибо за обращение! Если появятся новые вопросы — напишите снова.", b.as_markup())


# ── О нас ─────────────────────────────────────────────────────

@router.callback_query(F.data == "about")
async def cb_about(cb: CallbackQuery):
    await db_log_action(cb.from_user.id, cb.from_user.username or "", cb.from_user.full_name or "", False, "Открыл раздел О нас")
    await safe_edit(
        cb,
        "╭───────────────────────────────╮\n"
"        🌿 TREANT PROJECT\n"
"   Инновационные цифровые решения\n"
"╰───────────────────────────────╯\n"
"\n"
"Treant Project — команда разработчиков,\n"
"создающая технологичные инструменты\n"
"для автоматизации, анализа данных\n"
"и управления цифровыми сервисами.\n"
"\n"
"━━━━━━━━━━━━━━━━━━━━━━━━\n"
"📌 НАШИ НАПРАВЛЕНИЯ\n"
"━━━━━━━━━━━━━━━━━━━━━━━━\n"
"\n"
"🔍 OSINT-инструменты\n"
"Системы поиска, анализа и обработки\n"
"открытых данных.\n"
"\n"
"🤖 Telegram-боты\n"
"Автоматизация сервисов, панели управления,\n"
"системы продаж, уведомления и интеграции.\n"
"\n"
"🎮 Discord-боты\n"
"Инструменты для управления сообществами,\n"
"автоматизации серверов и кастомных функций.\n"
"\n"
"━━━━━━━━━━━━━━━━━━━━━━━━\n"
"⭐ ПОЧЕМУ ВЫБИРАЮТ НАС\n"
"━━━━━━━━━━━━━━━━━━━━━━━━\n"
"\n"
"➤ Фокус на результат\n"
"Мы создаём решения, которые работают.\n"
"\n"
"➤ Индивидуальный подход\n"
"Каждый проект разрабатывается под задачи клиента.\n"
"\n"
"➤ Надёжность и безопасность\n"
"Стабильная архитектура и защита данных.\n"
"\n"
"━━━━━━━━━━━━━━━━━━━━━━━━\n"
"\n"
"Treant Project — технологии,\n"
"которые работают на вас.\n",
        kb_back()
    )


# ── ADMIN панель ──────────────────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    await state.clear()
    uid   = message.from_user.id
    role  = await get_role(uid) or ROLE_MODERATOR
    total = await db_total_users()
    role_label = ROLE_LABELS.get(role, role)
    rev_day = await db_analytics_revenue("day")
    await message.answer(
        f"⚙️ <b>Панель администратора</b>\n\n"
        f"👥 Пользователей: <b>{total}</b>\n"
        f"💰 Выручка сегодня: <b>{usd(rev_day)}</b>\n"
        f"🎭 Ваша роль: <b>{role_label}</b>",
        reply_markup=kb_admin(role)
    )
    await log_action_from_msg(message, True, "Открыл админ-панель")


@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.clear()
    uid   = cb.from_user.id
    role  = await get_role(uid) or ROLE_MODERATOR
    total = await db_total_users()
    role_label = ROLE_LABELS.get(role, role)
    rev_day = await db_analytics_revenue("day")
    await safe_edit(
        cb,
        f"⚙️ <b>Панель администратора</b>\n\n"
        f"👥 Пользователей: <b>{total}</b>\n"
        f"💰 Выручка сегодня: <b>{usd(rev_day)}</b>\n"
        f"🎭 Ваша роль: <b>{role_label}</b>",
        kb_admin(role)
    )


# ── Бэкап БД (кнопка) ─────────────────────────────────────────

@router.callback_query(F.data == "admin_backup")
async def cb_admin_backup(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb.answer("⏳ Создаём бэкап...")
    try:
        backup_path = f"/tmp/store_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(DB, backup_path)
        with open(backup_path, "rb") as f:
            await bot.send_document(
                cb.from_user.id,
                f,
                caption=f"💾 <b>Резервная копия БД</b>\n📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}"
            )
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка создания бэкапа: {e}")


# ── Аналитика ─────────────────────────────────────────────────

@router.callback_query(F.data == "admin_analytics")
async def cb_admin_analytics(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return

    rev_day   = await db_analytics_revenue("day")
    rev_week  = await db_analytics_revenue("week")
    rev_month = await db_analytics_revenue("month")
    top_prods = await db_analytics_top_products(5)
    top_buyers = await db_analytics_top_buyers(5)
    sales_days = await db_analytics_sales_by_day(7)
    total_users = await db_total_users()

    text = (
        f"📊 <b>Аналитика</b>\n\n"
        f"<b>Выручка:</b>\n"
        f"• Сегодня: <b>{usd(rev_day)}</b>\n"
        f"• За неделю: <b>{usd(rev_week)}</b>\n"
        f"• За месяц: <b>{usd(rev_month)}</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n\n"
    )

    if sales_days:
        text += "<b>📈 Продажи по дням (7 дней):</b>\n"
        for row in sales_days:
            bar = "█" * min(int(row[1]), 10)
            text += f"• {row[0]}: {row[1]} шт. / {usd(row[2])} {bar}\n"
        text += "\n"

    if top_prods:
        text += "<b>🏆 Топ-5 товаров:</b>\n"
        for i, p in enumerate(top_prods, 1):
            text += f"{i}. <b>{p['product_name']}</b> — {p['cnt']} шт. / {usd(p['revenue'])}\n"
        text += "\n"

    if top_buyers:
        text += "<b>💎 Топ-5 покупателей:</b>\n"
        for i, u in enumerate(top_buyers, 1):
            uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
            text += f"{i}. {u['full_name'] or uname} — {u['cnt']} покупок / {usd(u['total'])}\n"

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🏆 Топ-10 покупателей", callback_data="admin_top_buyers"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await safe_edit(cb, text, b.as_markup())


# ── Статистика (старая) ────────────────────────────────────────

@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb_admin_analytics(cb)


# ── Топ-10 покупателей ────────────────────────────────────────

@router.message(Command("top"))
async def cmd_top(message: Message):
    if not await is_admin(message.from_user.id): return
    buyers = await db_analytics_top_buyers_10()
    if not buyers:
        await message.answer("🏆 <b>Топ покупателей</b>\n\nПокупок пока нет.", reply_markup=kb_admin())
        return
    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, u in enumerate(buyers):
        medal = medals[i] if i < 3 else f"{i+1}."
        uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
        name = u["full_name"] or uname
        lines.append(f"{medal} <b>{name}</b> ({uname})\n   💰 {usd(u['total'])}  •  {u['cnt']} покупок")
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад к аналитике", callback_data="admin_analytics"))
    await message.answer(
        f"🏆 <b>Топ-10 покупателей</b>\n\n" + "\n\n".join(lines),
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data == "admin_top_buyers")
async def cb_admin_top_buyers(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    buyers = await db_analytics_top_buyers_10()
    if not buyers:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_analytics"))
        await safe_edit(cb, "🏆 <b>Топ покупателей</b>\n\nПокупок пока нет.", b.as_markup())
        return
    lines = []
    medals = ["🥇", "🥈", "🥉"]
    for i, u in enumerate(buyers):
        medal = medals[i] if i < 3 else f"{i+1}."
        uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
        name = u["full_name"] or uname
        lines.append(f"{medal} <b>{name}</b> ({uname})\n   💰 {usd(u['total'])}  •  {u['cnt']} покупок")
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад к аналитике", callback_data="admin_analytics"))
    await safe_edit(cb, f"🏆 <b>Топ-10 покупателей</b>\n\n" + "\n\n".join(lines), b.as_markup())


# ── Пользователи ──────────────────────────────────────────────

@router.callback_query(F.data == "admin_users")
@router.callback_query(F.data.startswith("admin_users_page:"))
async def cb_admin_users(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    page = int(cb.data.split(":")[1]) if ":" in cb.data else 0
    per  = 8
    users = await db_get_all_users()

    if not users:
        await safe_edit(cb, "👥 <b>Пользователи</b>\n\nНет зарегистрированных пользователей.", kb_admin())
        return

    start, end = page * per, page * per + per
    chunk = list(users)[start:end]
    b = InlineKeyboardBuilder()

    lines = []
    for i, u in enumerate(chunk, start=start+1):
        uname = f"@{u['username']}" if u["username"] else "—"
        ban = " 🚫" if u["is_banned"] else ""
        lines.append(
            f"<b>{i}.</b> {u['full_name'] or 'Без имени'} ({uname}){ban}\n"
            f"    🆔 <code>{u['user_id']}</code>  💰 {usd(u['balance'])}\n"
            f"    📅 {fmt_date(u['created_at'])}"
        )
        b.row(InlineKeyboardButton(
            text=f"{'🚫 ' if u['is_banned'] else ''}{u['full_name'] or uname}",
            callback_data=f"admin_user_info:{u['user_id']}"
        ))

    nav = []
    if page > 0:           nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_users_page:{page-1}"))
    if end < len(users):   nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_users_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))

    await safe_edit(
        cb,
        f"👥 <b>Все пользователи</b>  [{len(users)} чел.]\n"
        f"Страница {page+1}/{(len(users)-1)//per+1}\n\n"
        + "\n\n".join(lines),
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_user_info:"))
async def cb_admin_user_info(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    user = await db_get_user(uid)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True); return
    uname = f"@{user['username']}" if user["username"] else "—"
    ban_status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
    await safe_edit(
        cb,
        f"👤 <b>Пользователь</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"Имя: <b>{user['full_name']}</b>\n"
        f"Username: {uname}\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n"
        f"👥 Рефералов: <b>{user['referral_count']}</b>\n"
        f"🕐 Последняя активность: <b>{fmt_date(user['last_active'])}</b>\n"
        f"📅 Регистрация: <b>{fmt_date(user['created_at'])}</b>\n"
        f"Статус: {ban_status}",
        kb_admin_user_actions(uid, bool(user["is_banned"]))
    )


@router.callback_query(F.data.startswith("admin_ban_toggle:"))
async def cb_admin_ban_toggle(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    user = await db_get_user(uid)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True); return
    new_ban = not bool(user["is_banned"])
    await db_ban_user(uid, new_ban)
    action = "🚫 Заблокирован" if new_ban else "✅ Разблокирован"
    await cb.answer(f"{action}")
    try:
        msg = "🚫 Ваш аккаунт заблокирован администратором." if new_ban else "✅ Ваш аккаунт разблокирован."
        await bot.send_message(uid, msg)
    except Exception:
        pass
    user = await db_get_user(uid)
    target_name = user["full_name"] if user else f"ID:{uid}"
    target_uname = f'@{user["username"]}' if user and user["username"] else f"ID:{uid}"
    await log_action_from_cb(cb, True, f"{"Заблокировал" if new_ban else "Разблокировал"} пользователя", f"{target_name} ({target_uname})")
    await safe_edit(
        cb,
        f"👤 <b>{user['full_name']}</b>\nСтатус: {'🚫 Заблокирован' if user['is_banned'] else '✅ Активен'}",
        kb_admin_user_actions(uid, bool(user["is_banned"]))
    )


# ── Поиск пользователя ────────────────────────────────────────

@router.callback_query(F.data == "admin_search_user")
async def cb_admin_search_user(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(SearchUserFSM.waiting)
    await safe_edit(cb, "🔍 <b>Поиск пользователя</b>\n\nВведите <b>Telegram ID</b> или <b>@username</b>:")


@router.message(StateFilter(SearchUserFSM.waiting))
async def fsm_search_user(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    await state.clear()
    user = await db_get_user_by_id_or_username(message.text.strip())
    if not user:
        await message.answer("❌ Пользователь не найден.", reply_markup=kb_admin()); return
    uname = f"@{user['username']}" if user["username"] else "—"
    ban_status = "🚫 Заблокирован" if user["is_banned"] else "✅ Активен"
    await message.answer(
        f"🔍 <b>Найден пользователь</b>\n\n"
        f"🆔 ID: <code>{user['user_id']}</code>\n"
        f"Имя: <b>{user['full_name']}</b>\n"
        f"Username: {uname}\n"
        f"💰 Баланс: <b>{usd(user['balance'])}</b>\n"
        f"👥 Рефералов: <b>{user['referral_count']}</b>\n"
        f"Статус: {ban_status}",
        reply_markup=kb_admin_user_actions(user["user_id"], bool(user["is_banned"]))
    )


# ── Уведомление неактивных ────────────────────────────────────

@router.callback_query(F.data == "admin_notify_inactive")
async def cb_admin_notify_inactive(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    ids = await db_get_inactive_users(INACTIVE_DAYS)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text=f"✅ Отправить ({len(ids)} чел.)", callback_data="notify_inactive_confirm"),
        InlineKeyboardButton(text="❌ Отмена",                        callback_data="admin_panel"),
    )
    await safe_edit(
        cb,
        f"🔔 <b>Уведомить неактивных</b>\n\n"
        f"Пользователей без активности {INACTIVE_DAYS}+ дней: <b>{len(ids)}</b>\n\n"
        f"Им отправится сообщение:\n"
        f"<i>«👋 Мы скучали по тебе! Давно не заходили? Загляните в магазин — появились новые товары!»</i>\n\n"
        f"Подтвердить отправку?",
        b.as_markup()
    )


@router.callback_query(F.data == "notify_inactive_confirm")
async def cb_notify_inactive_confirm(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb.answer("⏳ Отправляем...")
    ids = await db_get_inactive_users(INACTIVE_DAYS)
    sent, fail = 0, 0
    for uid in ids:
        try:
            await bot.send_message(
                uid,
                f"👋 <b>Мы скучали по тебе!</b>\n\n"
                f"Давно не заходили? Загляните в магазин — появились новые товары!",
                reply_markup=kb_main()
            )
            sent += 1
        except Exception:
            fail += 1
    await safe_edit(
        cb,
        f"🔔 <b>Готово!</b>\n\n"
        f"Неактивных ({INACTIVE_DAYS}+ дней): <b>{len(ids)}</b>\n"
        f"📨 Отправлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>",
        kb_admin()
    )
    await log_action_from_cb(cb, True, "Уведомил неактивных пользователей", f"Отправлено: {sent}, ошибок: {fail}")


# ── Чаты (Админ) ──────────────────────────────────────────────

@router.callback_query(F.data == "admin_chats")
async def cb_admin_chats(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    active = await db_get_active_chats()
    closed = await db_get_closed_chats()

    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text=f"🟢 Активные ({len(active)})", callback_data="admin_chats_active"),
        InlineKeyboardButton(text=f"🔒 Закрытые ({len(closed)})", callback_data="admin_chats_closed"),
    )
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))

    total_unread = sum(c["unread"] for c in active if c["unread"])
    text = f"💬 <b>Чаты</b>\n\n🟢 Активных: <b>{len(active)}</b>\n🔒 Закрытых: <b>{len(closed)}</b>"
    if total_unread:
        text += f"\n🔴 Непрочитанных: <b>{total_unread}</b>"
    await safe_edit(cb, text, b.as_markup())


@router.callback_query(F.data == "admin_chats_active")
async def cb_admin_chats_active(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    chats = await db_get_active_chats()

    b = InlineKeyboardBuilder()
    if not chats:
        b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_chats"))
        await safe_edit(cb, "🟢 <b>Активные чаты</b>\n\nПока нет активных обращений.", b.as_markup())
        return

    for chat in chats[:15]:
        uname = f"@{chat['username']}" if chat["username"] else f"ID:{chat['user_id']}"
        name = chat["full_name"] or uname
        unread = f" 🔴{chat['unread']}" if chat["unread"] and int(chat["unread"]) > 0 else ""
        last = (chat["last_text"] or "")[:25] + ("…" if chat["last_text"] and len(chat["last_text"]) > 25 else "")
        b.row(InlineKeyboardButton(
            text=f"🟢 {name}{unread}",
            callback_data=f"admin_open_chat:{chat['user_id']}"
        ))

    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_chats"))
    total_unread = sum(int(c["unread"]) for c in chats if c["unread"])
    text = f"🟢 <b>Активные чаты</b> ({len(chats)})"
    if total_unread:
        text += f"\n🔴 Непрочитанных: <b>{total_unread}</b>"
    await safe_edit(cb, text, b.as_markup())


@router.callback_query(F.data == "admin_chats_closed")
async def cb_admin_chats_closed(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    chats = await db_get_closed_chats()

    b = InlineKeyboardBuilder()
    if not chats:
        b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_chats"))
        await safe_edit(cb, "🔒 <b>Закрытые чаты</b>\n\nНет закрытых обращений.", b.as_markup())
        return

    for chat in chats[:15]:
        uname = f"@{chat['username']}" if chat["username"] else f"ID:{chat['user_id']}"
        name = chat["full_name"] or uname
        closed_time = fmt_date(chat["closed_at"])[5:16] if chat["closed_at"] else "—"
        b.row(InlineKeyboardButton(
            text=f"🔒 {name}  [{closed_time}]",
            callback_data=f"admin_open_chat:{chat['user_id']}"
        ))

    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_chats"))
    await safe_edit(cb, f"🔒 <b>Закрытые чаты</b> ({len(chats)})", b.as_markup())


@router.callback_query(F.data.startswith("admin_open_chat:"))
async def cb_admin_open_chat(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    user = await db_get_user(uid)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True); return

    session = await db_get_chat_session(uid)
    is_closed = session and session["status"] == "closed"

    await state.set_state(AdminChatFSM.chatting)
    await state.update_data(chat_user_id=uid)
    await db_mark_read(uid)

    history = await db_get_chat_history(uid, 20)
    uname = f"@{user['username']}" if user["username"] else f"ID:{uid}"
    status_badge = "🔒 Закрыт" if is_closed else "🟢 Активен"
    bot_info = await cb.bot.get_me()
    bot_name = bot_info.full_name
    bot_uname = bot_info.username or ""

    text = f"💬 <b>{user['full_name']}</b> ({uname})  {status_badge}\n"
    text += f"<i>Бот: @{bot_uname}</i>\n"
    text += "─" * 32 + "\n\n"
    if history:
        text += fmt_chat_history(history, user["full_name"], user["username"] or "", bot_name, bot_uname)
        text += "\n\n"
    else:
        text += "<i>История пуста</i>\n\n"
    text += "─" * 32 + "\n"
    if is_closed:
        text += "\n<i>Чат закрыт. Вы можете переоткрыть его.</i>"
    else:
        text += "\n✏️ Введите ответ:"

    b = InlineKeyboardBuilder()
    if is_closed:
        b.row(InlineKeyboardButton(text="🔓 Переоткрыть чат", callback_data=f"admin_reopen_chat:{uid}"))
    else:
        b.row(InlineKeyboardButton(text="🔒 Закрыть обращение", callback_data=f"admin_close_chat_uid:{uid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад к чатам", callback_data="admin_chats"))
    await safe_edit(cb, text, b.as_markup())


@router.callback_query(F.data.startswith("admin_reopen_chat:"))
async def cb_admin_reopen_chat(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    await db_open_chat_session(uid)
    await cb.answer("✅ Чат переоткрыт")
    # Re-open the chat view and enter chatting state
    await state.set_state(AdminChatFSM.chatting)
    await state.update_data(chat_user_id=uid)
    user = await db_get_user(uid)
    uname = f"@{user['username']}" if user["username"] else f"ID:{uid}"
    history = await db_get_chat_history(uid, 20)
    bot_info = await cb.bot.get_me()
    bot_name = bot_info.full_name
    bot_uname = bot_info.username or ""
    text = f"💬 <b>{user['full_name']}</b> ({uname})  🟢 Активен\n"
    text += f"<i>Бот: @{bot_uname}</i>\n"
    text += "─" * 32 + "\n\n"
    if history:
        text += fmt_chat_history(history, user["full_name"], user["username"] or "", bot_name, bot_uname)
        text += "\n\n"
    text += "─" * 32 + "\n✏️ Введите ответ:"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔒 Закрыть обращение", callback_data=f"admin_close_chat_uid:{uid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад к чатам", callback_data="admin_chats"))
    await safe_edit(cb, text, b.as_markup())


@router.callback_query(F.data.startswith("admin_close_chat_uid:"))
async def cb_admin_close_chat_uid(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    await state.clear()
    await db_close_chat_session(uid, closed_by="admin")
    try:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="📨 Открыть новое обращение", callback_data="open_chat"))
        await bot.send_message(uid,
            "🔒 <b>Ваше обращение закрыто администратором.</b>\n\nЕсли остались вопросы — напишите снова.",
            reply_markup=b.as_markup()
        )
    except Exception:
        pass
    await cb.answer("🔒 Обращение закрыто")
    await cb_admin_chats(cb)


@router.message(StateFilter(AdminChatFSM.chatting))
async def fsm_admin_chat(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    if message.text and message.text.startswith("/"):
        await state.clear()
        await message.answer("Вышли из чата.", reply_markup=kb_admin())
        return

    data = await state.get_data()
    uid  = data["chat_user_id"]

    # Check if chat is still active
    session = await db_get_chat_session(uid)
    if session and session["status"] == "closed":
        await message.answer("⚠️ Чат закрыт. Сначала переоткройте его.",
                             reply_markup=(InlineKeyboardBuilder()
                                           .row(InlineKeyboardButton(text="🔓 Переоткрыть", callback_data=f"admin_reopen_chat:{uid}"))
                                           .as_markup()))
        return

    await db_send_chat_msg(uid, "admin", message.text or "[медиа]")
    await log_action_from_msg(message, True, "Ответил в чат поддержки", f"Юзер ID:{uid} | {(message.text or '[медиа]')[:60]}")

    try:
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="💬 Ответить", callback_data="open_chat"))
        await bot.send_message(
            uid,
            f"💬 <b>Ответ от поддержки:</b>\n\n{message.text or '[медиа]'}",
            reply_markup=b.as_markup()
        )
        await message.answer("✅ Отправлено.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}")


@router.callback_query(F.data == "admin_close_chat")
async def cb_admin_close_chat(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(cb, "💬 Вышли из чата.", kb_admin())



# ── Управление правами (только superadmin) ────────────────────

@router.callback_query(F.data == "admin_roles")
async def cb_admin_roles(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return

    admins = await db_get_all_admins()
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="➕ Добавить администратора", callback_data="admin_roles_add"))

    text = "🛡 <b>Управление правами</b>\n\n"
    text += f"👑 <b>Суперадмин (владелец):</b>\n"
    for sid in ADMIN_IDS:
        text += f"  • <code>{sid}</code>\n"

    if admins:
        text += "\n<b>Выданные права:</b>\n"
        for a in admins:
            name = a["full_name"] or f"ID:{a['user_id']}"
            uname = f" @{a['username']}" if a["username"] else ""
            role_label = ROLE_LABELS.get(a["role"], a["role"])
            text += f"  • {name}{uname} — {role_label}\n"
            b.row(InlineKeyboardButton(
                text=f"{role_label}: {name}{uname}",
                callback_data=f"admin_roles_manage:{a['user_id']}"
            ))
    else:
        text += "\n<i>Дополнительных администраторов нет.</i>"

    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await safe_edit(cb, text, b.as_markup())


@router.callback_query(F.data == "admin_roles_add")
async def cb_admin_roles_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    await state.set_state(AddAdminFSM.user_id)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_roles"))
    await safe_edit(cb,
        "🛡 <b>Добавить администратора</b>\n\n"
        "Введите <b>ID пользователя</b> или <b>@username</b>:\n\n"
        "<i>Пользователь должен быть зарегистрирован в боте.</i>",
        b.as_markup()
    )


@router.message(StateFilter(AddAdminFSM.user_id))
async def fsm_add_admin_uid(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS: return
    query = message.text.strip().lstrip("@")
    user = await db_get_user_by_id_or_username(query)
    if not user:
        await message.answer("❌ Пользователь не найден. Попробуйте ещё раз или отправьте /cancel:")
        return
    if user["user_id"] in ADMIN_IDS:
        await message.answer("⚠️ Этот пользователь уже является суперадмином.")
        await state.clear(); return

    await state.update_data(target_uid=user["user_id"])
    await state.set_state(AddAdminFSM.role)

    uname = f"@{user['username']}" if user["username"] else f"ID:{user['user_id']}"
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="⚙️ Администратор", callback_data=f"admin_roles_setrole:admin"),
        InlineKeyboardButton(text="🔧 Модератор",     callback_data=f"admin_roles_setrole:moderator"),
    )
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_roles"))
    await message.answer(
        f"👤 Найден: <b>{user['full_name']}</b> ({uname})\n\n"
        f"Выберите роль:\n\n"
        f"⚙️ <b>Администратор</b> — всё кроме управления правами\n"
        f"🔧 <b>Модератор</b> — только чаты, просмотр пользователей и аналитика",
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_roles_setrole:"))
async def cb_admin_roles_setrole(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    role = cb.data.split(":")[1]
    await state.update_data(role=role)
    await state.set_state(AddAdminFSM.note)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="⏭ Пропустить", callback_data="admin_roles_confirm_add"))
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_roles"))
    role_label = ROLE_LABELS.get(role, role)
    await safe_edit(cb,
        f"Роль: <b>{role_label}</b>\n\nВведите заметку (необязательно), например «Менеджер»:",
        b.as_markup()
    )


@router.message(StateFilter(AddAdminFSM.note))
async def fsm_add_admin_note(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in ADMIN_IDS: return
    await state.update_data(note=message.text.strip())
    await _confirm_add_admin(message, state, bot)


@router.callback_query(F.data == "admin_roles_confirm_add")
async def cb_admin_roles_confirm_add(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    await state.update_data(note="")
    await _do_add_admin_from_cb(cb, state)


async def _do_add_admin_from_cb(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    uid  = data["target_uid"]
    role = data["role"]
    note = data.get("note", "")
    await db_add_admin(uid, role, cb.from_user.id, note)
    await state.clear()

    user = await db_get_user(uid)
    uname = f"@{user['username']}" if user and user["username"] else f"ID:{uid}"
    role_label = ROLE_LABELS.get(role, role)
    await log_action_from_cb(cb, True, "Выдал права администратора", f"{user['full_name'] if user else uid} ({uname}) → {role_label}")

    try:
        await cb.bot.send_message(
            uid,
            f"🎉 <b>Вам выданы права администратора!</b>\n\n"
            f"🎭 Роль: <b>{role_label}</b>\n\n"
            f"Введите /admin для открытия панели."
        )
    except Exception:
        pass

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ К списку", callback_data="admin_roles"))
    await safe_edit(
        cb,
        f"✅ <b>{user['full_name'] if user else uid}</b> ({uname}) назначен как <b>{role_label}</b>.",
        b.as_markup()
    )


async def _confirm_add_admin(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    uid  = data["target_uid"]
    role = data["role"]
    note = data.get("note", "")
    await db_add_admin(uid, role, message.from_user.id, note)
    await state.clear()

    user = await db_get_user(uid)
    uname = f"@{user['username']}" if user and user["username"] else f"ID:{uid}"
    role_label = ROLE_LABELS.get(role, role)

    try:
        await bot.send_message(
            uid,
            f"🎉 <b>Вам выданы права администратора!</b>\n\n"
            f"🎭 Роль: <b>{role_label}</b>\n\n"
            f"Введите /admin для открытия панели."
        )
    except Exception:
        pass

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ К списку", callback_data="admin_roles"))
    await message.answer(
        f"✅ <b>{user['full_name'] if user else uid}</b> ({uname}) назначен как <b>{role_label}</b>.",
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_roles_manage:"))
async def cb_admin_roles_manage(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    user  = await db_get_user(uid)
    admin = await db_get_admin(uid)
    if not admin:
        await cb.answer("Не найден"); return

    uname = f"@{user['username']}" if user and user["username"] else f"ID:{uid}"
    name  = user["full_name"] if user else f"ID:{uid}"
    role  = admin["role"]
    role_label = ROLE_LABELS.get(role, role)
    note  = admin["note"] or "—"

    b = InlineKeyboardBuilder()
    # Change role buttons (all except current)
    for r, label in ROLE_LABELS.items():
        if r != role and r != ROLE_SUPERADMIN:
            b.row(InlineKeyboardButton(
                text=f"🔄 Сменить на {label}",
                callback_data=f"admin_roles_changerole:{uid}:{r}"
            ))
    b.row(InlineKeyboardButton(text="🗑 Снять права",  callback_data=f"admin_roles_remove:{uid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",        callback_data="admin_roles"))

    await safe_edit(cb,
        f"🛡 <b>Управление: {name}</b> ({uname})\n\n"
        f"🎭 Роль: <b>{role_label}</b>\n"
        f"📝 Заметка: <i>{note}</i>\n"
        f"📅 Выдано: {fmt_date(admin['created_at'])}",
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_roles_changerole:"))
async def cb_admin_roles_changerole(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    _, uid_str, new_role = cb.data.split(":")
    uid = int(uid_str)
    await db_update_admin_role(uid, new_role)
    role_label = ROLE_LABELS.get(new_role, new_role)
    user = await db_get_user(uid)
    try:
        await bot.send_message(uid,
            f"🔄 <b>Ваша роль изменена!</b>\n\nНовая роль: <b>{role_label}</b>"
        )
    except Exception:
        pass
    user2 = await db_get_user(uid)
    uname2 = f'@{user2["username"]}' if user2 and user2["username"] else f"ID:{uid}"
    await log_action_from_cb(cb, True, "Изменил роль администратора", f"{user2['full_name'] if user2 else uid} ({uname2}) → {role_label}")
    await cb.answer(f"✅ Роль изменена на {role_label}")
    await cb_admin_roles(cb)


@router.callback_query(F.data.startswith("admin_roles_remove:"))
async def cb_admin_roles_remove(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только владелец", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    await db_remove_admin(uid)
    user = await db_get_user(uid)
    name = user["full_name"] if user else f"ID:{uid}"
    try:
        await bot.send_message(uid,
            "⚠️ <b>Ваши права администратора сняты.</b>"
        )
    except Exception:
        pass
    uname_removed = f'@{user["username"]}' if user and user["username"] else f"ID:{uid}"
    await log_action_from_cb(cb, True, "Снял права администратора", f"{name} ({uname_removed})")
    await cb.answer(f"✅ Права сняты у {name}")
    await cb_admin_roles(cb)

# ── Промокоды (Админ) ─────────────────────────────────────────

@router.callback_query(F.data == "admin_promos")
async def cb_admin_promos(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    promos = await db_get_all_promos()
    b = InlineKeyboardBuilder()
    for p in promos:
        status = "✅" if p["is_active"] else "❌"
        disc = f"{p['discount']}%" if p["type"] == "percent" else f"-{usd(p['discount'])}"
        uses = f"{p['used_count']}" + (f"/{p['max_uses']}" if p["max_uses"] else "")
        b.row(InlineKeyboardButton(
            text=f"{status} {p['code']}  {disc}  [{uses}]",
            callback_data=f"admin_promo_info:{p['id']}"
        ))
    b.row(InlineKeyboardButton(text="➕ Создать промокод", callback_data="admin_promo_add"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await safe_edit(cb, f"🎟 <b>Промокоды</b>\n\nВсего: <b>{len(promos)}</b>", b.as_markup())


@router.callback_query(F.data.startswith("admin_promo_info:"))
async def cb_admin_promo_info(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM promo_codes WHERE id=?", (pid,)) as c:
            promo = await c.fetchone()
    if not promo:
        await cb.answer("Не найдено", show_alert=True); return
    disc = f"{promo['discount']}%" if promo["type"] == "percent" else f"-{usd(promo['discount'])}"
    uses = f"{promo['used_count']}" + (f"/{promo['max_uses']}" if promo["max_uses"] else " (неограничено)")
    status = "✅ Активен" if promo["is_active"] else "❌ Отключён"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text="❌ Отключить" if promo["is_active"] else "✅ Включить",
        callback_data=f"admin_promo_toggle:{pid}"
    ))
    b.row(InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_promo_delete:{pid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_promos"))
    await safe_edit(
        cb,
        f"🎟 <b>Промокод: {promo['code']}</b>\n\n"
        f"Скидка: <b>{disc}</b>\n"
        f"Использований: <b>{uses}</b>\n"
        f"Создан: {fmt_date(promo['created_at'])}\n"
        f"Статус: {status}",
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_promo_toggle:"))
async def cb_admin_promo_toggle(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    await db_toggle_promo(pid)
    await log_action_from_cb(cb, True, "Изменил статус промокода", f"ID: {pid}")
    await cb.answer("✅ Статус изменён")
    await cb_admin_promos(cb)


@router.callback_query(F.data.startswith("admin_promo_delete:"))
async def cb_admin_promo_delete(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    await db_delete_promo(pid)
    await log_action_from_cb(cb, True, "Удалил промокод", f"ID: {cb.data.split(':')[1]}")
    await cb.answer("🗑 Промокод удалён")
    await cb_admin_promos(cb)


@router.callback_query(F.data == "admin_promo_add")
async def cb_admin_promo_add(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(PromoAddFSM.code)
    await safe_edit(cb, "🎟 <b>Создание промокода</b>\n\nШаг 1/3 — Введите <b>код</b> (латинские буквы и цифры):")


@router.message(StateFilter(PromoAddFSM.code))
async def fsm_promo_code_input(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    code = message.text.strip().upper()
    if not code.replace("_","").replace("-","").isalnum():
        await message.answer("❌ Код может содержать только буквы, цифры, _ и -"); return
    existing = await db_get_promo(code)
    if existing:
        await message.answer("❌ Такой промокод уже существует. Введите другой:"); return
    await state.update_data(code=code)
    await state.set_state(PromoAddFSM.discount)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="% Процент от цены", callback_data="promo_type:percent"),
        InlineKeyboardButton(text="$ Фиксированная сумма", callback_data="promo_type:fixed"),
    )
    await message.answer(
        f"🎟 Промокод: <b>{code}</b>\n\nШаг 2/3 — Выберите <b>тип скидки</b>:",
        reply_markup=b.as_markup()
    )
    await state.set_state(PromoAddFSM.ptype)


@router.callback_query(F.data.startswith("promo_type:"))
async def cb_promo_type(cb: CallbackQuery, state: FSMContext):
    ptype = cb.data.split(":")[1]
    await state.update_data(ptype=ptype)
    await state.set_state(PromoAddFSM.discount)
    hint = "Введите процент скидки (например: <code>10</code> = 10%)" if ptype == "percent" else "Введите сумму скидки в $ (например: <code>2.50</code>)"
    await safe_edit(cb, f"🎟 Шаг 2/3 — {hint}:")


@router.message(StateFilter(PromoAddFSM.discount))
async def fsm_promo_discount(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    try:
        discount = float(message.text.strip().replace(",", "."))
        if discount <= 0: raise ValueError
    except Exception:
        await message.answer("❌ Введите корректное число больше 0:"); return
    await state.update_data(discount=discount)
    await state.set_state(PromoAddFSM.max_uses)
    await message.answer(
        "🎟 Шаг 3/3 — Введите <b>лимит использований</b>\n\n"
        "Например: <code>50</code> — ограничено\n"
        "Или <code>0</code> — безлимитно:"
    )


@router.message(StateFilter(PromoAddFSM.max_uses))
async def fsm_promo_max_uses(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    try:
        max_uses = int(message.text.strip())
        if max_uses < 0: raise ValueError
    except Exception:
        await message.answer("❌ Введите целое число (0 = безлимит):"); return
    data = await state.get_data()
    await state.clear()
    await db_add_promo(
        data["code"],
        data["discount"],
        data["ptype"],
        max_uses if max_uses > 0 else None
    )
    disc = f"{data['discount']}%" if data["ptype"] == "percent" else f"-{usd(data['discount'])}"
    uses_text = f"до {max_uses} раз" if max_uses > 0 else "безлимитно"
    await message.answer(
        f"✅ <b>Промокод создан!</b>\n\n"
        f"🎟 Код: <b>{data['code']}</b>\n"
        f"Скидка: <b>{disc}</b>\n"
        f"Использований: <b>{uses_text}</b>",
        reply_markup=kb_admin()
    )


# ── Лимит товара ──────────────────────────────────────────────

@router.callback_query(F.data.startswith("admin_stock_limit:"))
async def cb_admin_stock_limit(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    await state.set_state(StockLimitFSM.waiting)
    await state.update_data(product_id=pid)
    cur = product["stock_limit"] if product["stock_limit"] is not None else "∞ (не ограничено)"
    await safe_edit(
        cb,
        f"📦 <b>Лимит товара</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n"
        f"Текущий лимит: <b>{cur}</b>\n"
        f"Продано: <b>{product['sold_count']}</b>\n\n"
        f"Введите новый лимит (число) или <code>0</code> для снятия ограничения:"
    )


@router.message(StateFilter(StockLimitFSM.waiting))
async def fsm_stock_limit(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    try:
        limit = int(message.text.strip())
        if limit < 0: raise ValueError
    except Exception:
        await message.answer("❌ Введите целое число (0 = без ограничений):"); return
    data = await state.get_data()
    pid = data["product_id"]
    await state.clear()
    await db_set_stock_limit(pid, limit if limit > 0 else None)
    product = await db_get_product(pid)
    await message.answer(
        f"✅ <b>Лимит обновлён!</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n"
        f"Лимит: <b>{'∞' if product['stock_limit'] is None else product['stock_limit']}</b>",
        reply_markup=kb_admin_product(pid, product["is_active"])
    )


# ── Товары (Админ) ────────────────────────────────────────────

@router.callback_query(F.data == "admin_products")
async def cb_admin_products(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    products = await db_get_all_products()
    await safe_edit(
        cb,
        f"📦 <b>Управление товарами</b>\n\n"
        f"Всего: <b>{len(products)}</b>   ✅ активен  |  ❌ скрыт",
        kb_admin_products(list(products))
    )


@router.callback_query(F.data.startswith("admin_product:"))
async def cb_admin_product_view(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid     = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    if not product:
        await cb.answer("Не найдено", show_alert=True); return
    status = "✅ Активен" if product["is_active"] else "❌ Скрыт"
    content_info = f"📎 Файл: <code>{product['file_name']}</code>" if product["file_id"] else f"📋 <code>{product['content'] or '—'}</code>"
    msg_info = f"\n✉️ Сообщение: <i>{product['post_message']}</i>" if product["post_message"] else "\n✉️ Сообщение: <i>не задано</i>"
    stock_info = f"\n📦 Лимит: {product['stock_limit']} (продано: {product['sold_count']})" if product["stock_limit"] is not None else "\n📦 Лимит: ∞"
    await safe_edit(
        cb,
        f"📦 <b>{product['name']}</b>\n\n"
        f"📝 {product['description'] or '—'}\n"
        f"💰 Цена: <b>{usd(product['price'])}</b>\n"
        f"Содержимое: {content_info}\n"
        f"Статус: {status}"
        f"{stock_info}"
        f"{msg_info}",
        kb_admin_product(pid, product["is_active"])
    )


@router.callback_query(F.data.startswith("admin_toggle:"))
async def cb_admin_toggle(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    await db_toggle_product(pid)
    product = await db_get_product(pid)
    await log_action_from_cb(cb, True, f"Товар {'показан' if product['is_active'] else 'скрыт'}", f"{product['name']} (ID:{pid})")
    await cb.answer("✅ Статус изменён")
    await safe_edit(
        cb,
        f"📦 <b>{product['name']}</b>\n\n"
        f"💰 Цена: <b>{usd(product['price'])}</b>\n"
        f"Статус: {'✅ Активен' if product['is_active'] else '❌ Скрыт'}",
        kb_admin_product(pid, product["is_active"])
    )


@router.callback_query(F.data.startswith("admin_delete:"))
async def cb_admin_delete(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    prod_del = await db_get_product(pid)
    await db_delete_product(pid)
    await log_action_from_cb(cb, True, "Удалил товар", f"{prod_del['name'] if prod_del else pid} (ID:{pid})")
    await cb.answer("🗑 Товар удалён")
    products = await db_get_all_products()
    await safe_edit(cb, f"📦 <b>Управление товарами</b>\n\nВсего: <b>{len(products)}</b>", kb_admin_products(list(products)))


@router.callback_query(F.data.startswith("admin_product_msg:"))
async def cb_admin_product_msg(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid     = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    if not product:
        await cb.answer("Не найдено", show_alert=True); return
    await state.set_state(EditProductMsgFSM.waiting)
    await state.update_data(product_id=pid)
    b = InlineKeyboardBuilder()
    if product["post_message"]:
        b.row(InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data=f"admin_product_msg_clear:{pid}"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data=f"admin_product:{pid}"))
    current = f"\n\n📋 Текущее:\n<i>{product['post_message']}</i>" if product["post_message"] else "\n\n<i>Сообщение не задано</i>"
    await safe_edit(
        cb,
        f"✉️ <b>Сообщение после покупки</b>\n"
        f"Товар: <b>{product['name']}</b>{current}\n\n"
        f"Введите новый текст или нажмите кнопку ниже:",
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_product_msg_clear:"))
async def cb_admin_product_msg_clear(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    await state.clear()
    await db_set_product_message(pid, "")
    await cb.answer("🗑 Сообщение удалено")
    await cb_admin_product_view(cb)


@router.message(StateFilter(EditProductMsgFSM.waiting))
async def fsm_edit_product_msg(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите текст сообщения:"); return
    data = await state.get_data()
    pid  = data["product_id"]
    await state.clear()
    await db_set_product_message(pid, message.text)
    product = await db_get_product(pid)
    await message.answer(
        f"✅ <b>Сообщение сохранено!</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n\n"
        f"После покупки покупатель получит:\n<i>{message.text}</i>",
        reply_markup=kb_admin_product(pid, product["is_active"])
    )


# ── Изменение цены ────────────────────────────────────────────

@router.callback_query(F.data.startswith("admin_edit_price:"))
async def cb_admin_edit_price(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid     = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    await state.set_state(EditPriceFSM.waiting)
    await state.update_data(product_id=pid)
    await safe_edit(
        cb,
        f"✏️ <b>Изменение цены</b>\n\n"
        f"Товар: <b>{product['name']}</b>\n"
        f"Текущая цена: <b>{usd(product['price'])}</b>\n\n"
        f"Введите новую цену в $:"
    )


@router.message(StateFilter(EditPriceFSM.waiting))
async def fsm_edit_price(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите цену числом, например: <code>9.99</code>"); return
    try:
        price = float(message.text.strip().replace(",", "."))
        if price <= 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Неверная цена. Введите число больше 0:"); return
    data = await state.get_data()
    pid  = data["product_id"]
    await state.clear()
    await db_update_price(pid, price)
    product = await db_get_product(pid)
    await log_action_from_msg(message, True, "Изменил цену товара", f"{product['name'] if product else pid}: {usd(price)}")
    await message.answer(
        f"✅ Цена обновлена!\n\nТовар: <b>{product['name']}</b>\nНовая цена: <b>{usd(price)}</b>",
        reply_markup=kb_admin_products(list(await db_get_all_products()))
    )


# ── Добавление товара (FSM) ───────────────────────────────────

@router.callback_query(F.data == "admin_add_product")
async def cb_admin_add(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(AddProductFSM.name)
    await safe_edit(cb, "➕ <b>Добавление товара</b>\n\nШаг 1/5 — Введите <b>название</b>:")


@router.message(StateFilter(AddProductFSM.name))
async def fsm_add_name(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите название текстом:"); return
    await state.update_data(name=message.text.strip())
    await state.set_state(AddProductFSM.desc)
    await message.answer("➕ Шаг 2/5 — Введите <b>описание</b>\n(<code>-</code> чтобы пропустить):")


@router.message(StateFilter(AddProductFSM.desc))
async def fsm_add_desc(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите описание текстом (или <code>-</code> чтобы пропустить):"); return
    desc = "" if message.text.strip() == "-" else message.text.strip()
    await state.update_data(description=desc)
    await state.set_state(AddProductFSM.price)
    await message.answer("➕ Шаг 3/5 — Введите <b>цену</b> в $ (например: <code>9.99</code>):")


@router.message(StateFilter(AddProductFSM.price))
async def fsm_add_price(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите цену числом, например: <code>9.99</code>"); return
    try:
        price = float(message.text.strip().replace(",", "."))
        if price <= 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Неверная цена. Введите число больше 0:"); return
    await state.update_data(price=price)
    await state.set_state(AddProductFSM.content)
    await message.answer(
        "➕ Шаг 4/5 — Отправьте <b>содержимое товара</b>:\n\n"
        "• Текст/ключ/ссылку — просто напишите\n"
        "• Файл (.exe, .zip и т.д.) — прикрепите файл\n\n"
        "<i>Покупатель получит именно это</i>"
    )


@router.message(StateFilter(AddProductFSM.content))
async def fsm_add_content(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    file_id, file_name, content = None, None, ""
    if message.document:
        file_id   = message.document.file_id
        file_name = message.document.file_name or "file"
        content   = f"[Файл: {file_name}]"
    elif message.text:
        content = message.text.strip()
    else:
        await message.answer("❌ Отправьте текст или файл."); return
    await state.update_data(content=content, file_id=file_id, file_name=file_name)
    await state.set_state(AddProductFSM.stock)
    await message.answer(
        "➕ Шаг 5/5 — Введите <b>лимит количества</b> товара\n\n"
        "Например: <code>10</code> — только 10 штук\n"
        "Или <code>0</code> — без ограничений:"
    )


@router.message(StateFilter(AddProductFSM.stock))
async def fsm_add_stock(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    try:
        stock = int(message.text.strip()) if message.text else 0
        if stock < 0: raise ValueError
    except Exception:
        await message.answer("❌ Введите целое число (0 = без ограничений):"); return

    data = await state.get_data()
    await state.clear()
    stock_limit = stock if stock > 0 else None
    await db_add_product(data["name"], data["description"], data["price"], data["content"],
                         data.get("file_id"), data.get("file_name"), stock_limit)

    type_label = f"📎 Файл: <code>{data.get('file_name')}</code>" if data.get("file_id") else "📋 Текст"
    stock_text = f"{stock_limit} шт." if stock_limit else "∞"
    await message.answer(
        f"✅ <b>Товар добавлен!</b>\n\n"
        f"📦 Название: <b>{data['name']}</b>\n"
        f"💰 Цена: <b>{usd(data['price'])}</b>\n"
        f"📦 Лимит: <b>{stock_text}</b>\n"
        f"Тип содержимого: {type_label}",
        reply_markup=kb_admin()
    )
    await log_action_from_msg(message, True, "Добавил новый товар", f"{data['name']} | {usd(data['price'])} | лимит: {stock_text}")

    # Получаем ID только что добавленного товара
    async with aiosqlite.connect(DB) as db:
        async with db.execute("SELECT id FROM products ORDER BY id DESC LIMIT 1") as c:
            row = await c.fetchone()
            new_pid = row[0] if row else 0

    # Спрашиваем, уведомлять ли подписчиков
    subscribers = await db_get_product_subscribers()
    sub_count = len([u for u in subscribers if u != message.from_user.id])
    if sub_count > 0:
        b = InlineKeyboardBuilder()
        b.row(
            InlineKeyboardButton(text=f"🔔 Уведомить ({sub_count} чел.)", callback_data=f"notify_new_product:{new_pid}"),
            InlineKeyboardButton(text="❌ Не уведомлять", callback_data="notify_skip"),
        )
        await message.answer(
            f"📢 <b>Уведомить подписчиков?</b>\n\n"
            f"Подписчиков на товары: <b>{sub_count}</b>\n"
            f"Они получат сообщение о новом товаре <b>{data['name']}</b>.",
            reply_markup=b.as_markup()
        )


@router.callback_query(F.data.startswith("notify_new_product:"))
async def cb_notify_new_product(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    pid = int(cb.data.split(":")[1])
    product = await db_get_product(pid)
    if not product:
        await cb.answer("Товар не найден", show_alert=True); return
    await cb.answer("⏳ Рассылаем уведомления...")
    subscribers = await db_get_product_subscribers()
    sent = 0
    for uid in subscribers:
        if uid == cb.from_user.id: continue
        try:
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(text="🛒 Перейти в магазин", callback_data="shop"))
            await bot.send_message(
                uid,
                f"🆕 <b>Новый товар в магазине!</b>\n\n"
                f"📦 {product['name']}\n💰 {usd(product['price'])}\n\n"
                f"📝 {product['description'] or 'Без описания'}",
                reply_markup=b.as_markup()
            )
            sent += 1
        except Exception:
            pass
    await cb.message.edit_text(
        f"✅ <b>Уведомления отправлены!</b>\n\n"
        f"📨 Отправлено: <b>{sent}</b> из <b>{len(subscribers)}</b> подписчиков.",
        reply_markup=kb_admin()
    )


@router.callback_query(F.data == "notify_skip")
async def cb_notify_skip(cb: CallbackQuery):
    await cb.answer("Уведомление отменено")
    await cb.message.edit_text("❌ <b>Уведомление не отправлено.</b>", reply_markup=kb_admin())


# ── Все покупки (Админ) ───────────────────────────────────────

@router.callback_query(F.data == "admin_purchases")
@router.callback_query(F.data.startswith("admin_purchases_page:"))
async def cb_admin_purchases(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    page  = int(cb.data.split(":")[1]) if ":" in cb.data else 0
    per   = 8
    all_p = await db_get_all_purchases()

    if not all_p:
        await safe_edit(cb, "🧾 <b>Все покупки</b>\n\nПокупок пока нет.", kb_admin()); return

    total_sum = sum(p["price"] for p in all_p)
    start, end = page * per, page * per + per
    chunk = list(all_p)[start:end]
    lines = []
    for p in chunk:
        uname = f"@{p['username']}" if p["username"] else str(p["user_id"])
        lines.append(
            f"• <b>{p['product_name']}</b> — {usd(p['price'])}\n"
            f"  👤 {p['full_name']} ({uname}) | {fmt_date(p['created_at'])}"
        )

    b = InlineKeyboardBuilder()
    nav = []
    if page > 0:          nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_purchases_page:{page-1}"))
    if end < len(all_p):  nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_purchases_page:{page+1}"))
    if nav: b.row(*nav)
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))

    await safe_edit(
        cb,
        f"🧾 <b>Все покупки</b>\n\n"
        f"Всего: <b>{len(all_p)}</b>  |  Сумма: <b>{usd(total_sum)}</b>\n\n"
        + "\n\n".join(lines),
        b.as_markup()
    )


# ── Выдача баланса ────────────────────────────────────────────

@router.callback_query(F.data == "admin_give_balance")
async def cb_admin_give_balance(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(GiveBalanceFSM.user_id)
    await safe_edit(
        cb,
        "💸 <b>Выдача / Списание баланса</b>\n\n"
        "Шаг 1/2 — Введите <b>Telegram ID</b> пользователя:"
    )


@router.callback_query(F.data.startswith("admin_give_bal_uid:"))
async def cb_admin_give_bal_direct(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    uid = int(cb.data.split(":")[1])
    user = await db_get_user(uid)
    if not user:
        await cb.answer("Пользователь не найден", show_alert=True); return
    await state.set_state(GiveBalanceDirectFSM.amount)
    await state.update_data(target_uid=uid, target_name=user["full_name"])
    await safe_edit(
        cb,
        f"💸 <b>Выдача / Списание баланса</b>\n\n"
        f"Пользователь: <b>{user['full_name']}</b>\n"
        f"Текущий баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"Введите <b>сумму в $</b> (отрицательную для списания):"
    )


@router.message(StateFilter(GiveBalanceFSM.user_id))
async def fsm_give_balance_uid(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите Telegram ID числом:"); return
    try:
        uid = int(message.text.strip())
    except (ValueError, AttributeError):
        await message.answer("❌ Некорректный ID. Введите числовой Telegram ID:"); return
    user = await db_get_user(uid)
    if not user:
        await message.answer("❌ Пользователь не найден. Проверьте ID:"); return
    await state.update_data(target_uid=uid, target_name=user["full_name"])
    await state.set_state(GiveBalanceFSM.amount)
    await message.answer(
        f"💸 <b>Выдача баланса</b>\n\n"
        f"Пользователь: <b>{user['full_name']}</b>\n"
        f"Текущий баланс: <b>{usd(user['balance'])}</b>\n\n"
        f"Введите <b>сумму в $</b> (отрицательную для списания):"
    )


async def _apply_balance_change(message: Message, state: FSMContext, bot: Bot, data: dict, amount: float):
    await state.clear()
    target_id = data["target_uid"]
    target_nm = data["target_name"]
    action = "Начисление" if amount > 0 else "Списание"
    await db_update_balance(target_id, amount, f"{action} от администратора")
    user = await db_get_user(target_id)
    # Лог
    await log_to_channel(bot, f"💸 {action}: {target_nm} ({target_id}) — {usd(abs(amount))}")
    await db_log_action(message.from_user.id, message.from_user.username or "", message.from_user.full_name or "",
                        True, f"Изменил баланс пользователя", f"{target_nm} (ID:{target_id}): {'+'if amount>0 else ''}{usd(amount)}")
    try:
        if amount > 0:
            await bot.send_message(target_id,
                f"💰 <b>Вам начислен баланс!</b>\n\nСумма: <b>+{usd(amount)}</b>\nНовый баланс: <b>{usd(user['balance'])}</b>")
        else:
            await bot.send_message(target_id,
                f"💳 <b>С вашего баланса списана сумма</b>\n\nСумма: <b>{usd(amount)}</b>\nНовый баланс: <b>{usd(user['balance'])}</b>")
    except Exception:
        pass
    await message.answer(
        f"✅ <b>Готово!</b>\n\n"
        f"Пользователь: <b>{target_nm}</b> (<code>{target_id}</code>)\n"
        f"{'Начислено' if amount > 0 else 'Списано'}: <b>{usd(abs(amount))}</b>\n"
        f"Новый баланс: <b>{usd(user['balance'])}</b>",
        reply_markup=kb_admin()
    )


@router.message(StateFilter(GiveBalanceFSM.amount))
async def fsm_give_balance_amount(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите сумму числом:"); return
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount == 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Некорректная сумма:"); return
    data = await state.get_data()
    await _apply_balance_change(message, state, bot, data, amount)


@router.message(StateFilter(GiveBalanceDirectFSM.amount))
async def fsm_give_balance_direct_amount(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите сумму числом:"); return
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount == 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Некорректная сумма:"); return
    data = await state.get_data()
    await _apply_balance_change(message, state, bot, data, amount)


# ── Сообщение после покупки (глобальное) ──────────────────────

@router.callback_query(F.data == "admin_post_purchase")
async def cb_admin_post_purchase(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    current = await db_get_setting("post_purchase_message")
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✏️ Изменить сообщение", callback_data="admin_post_purchase_edit"))
    b.row(InlineKeyboardButton(text="✏️ Изменить приветствие", callback_data="admin_edit_greeting"))
    if current:
        b.row(InlineKeyboardButton(text="🗑 Удалить сообщение", callback_data="admin_post_purchase_clear"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    preview = f"\n\n📋 <b>Текущее сообщение:</b>\n{current}" if current else "\n\n<i>Сообщение не задано</i>"
    await safe_edit(
        cb,
        f"✉️ <b>Сообщение после покупки</b>\n"
        f"Отправляется покупателю сразу после успешной оплаты.{preview}",
        b.as_markup()
    )


@router.callback_query(F.data == "admin_post_purchase_edit")
async def cb_admin_post_purchase_edit(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(PostPurchaseFSM.waiting)
    await safe_edit(cb, "✏️ <b>Введите новое сообщение после покупки:</b>")


@router.callback_query(F.data == "admin_post_purchase_clear")
async def cb_admin_post_purchase_clear(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await db_set_setting("post_purchase_message", "")
    await cb.answer("🗑 Сообщение удалено")
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="✏️ Задать сообщение", callback_data="admin_post_purchase_edit"))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await safe_edit(cb, "✉️ <b>Сообщение после покупки</b>\n\n<i>Сообщение не задано</i>", b.as_markup())


@router.message(StateFilter(PostPurchaseFSM.waiting))
async def fsm_post_purchase(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите текст сообщения:"); return
    await state.clear()
    await db_set_setting("post_purchase_message", message.text)
    await log_action_from_msg(message, True, "Обновил сообщение после покупки", (message.text or "")[:80])
    await message.answer(
        f"✅ <b>Сообщение сохранено!</b>\n\nПосле покупки покупатель получит:\n\n{message.text}",
        reply_markup=kb_admin()
    )


# ── Приветствие ───────────────────────────────────────────────

@router.callback_query(F.data == "admin_edit_greeting")
async def cb_admin_edit_greeting(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    current = await db_get_setting("greeting_message") or "<i>не задано</i>"
    await state.set_state(GreetingFSM.waiting)
    await safe_edit(
        cb,
        f"👋 <b>Текст приветствия</b>\n\nТекущее:\n{current}\n\nВведите новый текст приветствия:"
    )


@router.message(StateFilter(GreetingFSM.waiting))
async def fsm_greeting(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    await state.clear()
    await db_set_setting("greeting_message", message.text)
    await log_action_from_msg(message, True, "Обновил приветствие", (message.text or "")[:80])
    await message.answer(f"✅ Приветствие обновлено!\n\n{message.text}", reply_markup=kb_admin())


# ── Рассылка ──────────────────────────────────────────────────

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(BroadcastFSM.waiting)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    await safe_edit(cb, "📢 <b>Рассылка</b>\n\nВведите текст сообщения.\nПосле ввода вам покажут предпросмотр и предложат подтвердить:", b.as_markup())


@router.message(StateFilter(BroadcastFSM.waiting))
async def fsm_broadcast(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    text = message.text
    if not text:
        await message.answer("❌ Введите текстовое сообщение:"); return
    await state.clear()
    ids = await db_all_user_ids()
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text=f"✅ Отправить ({len(ids)} чел.)", callback_data="broadcast_confirm"),
        InlineKeyboardButton(text="❌ Отмена",                        callback_data="admin_panel"),
    )
    # Сохраняем текст во временное хранилище через FSM data после clear — используем message.answer + state
    await state.set_state(BroadcastFSM.confirm)
    await state.update_data(broadcast_text=text)
    preview = text[:300] + ("..." if len(text) > 300 else "")
    await message.answer(
        f"📢 <b>Подтверждение рассылки</b>\n\n"
        f"👥 Получателей: <b>{len(ids)}</b>\n\n"
        f"📋 <b>Предпросмотр:</b>\n"
        f"┌────────────────\n"
        f"{preview}\n"
        f"└────────────────\n\n"
        f"Отправить это сообщение всем пользователям?",
        reply_markup=b.as_markup()
    )


@router.callback_query(F.data == "broadcast_confirm")
async def cb_broadcast_confirm(cb: CallbackQuery, state: FSMContext, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    data = await state.get_data()
    text = data.get("broadcast_text", "")
    await state.clear()
    if not text:
        await cb.answer("❌ Текст не найден", show_alert=True); return
    ids = await db_all_user_ids()
    sent, fail = 0, 0
    await safe_edit(cb, f"📢 <b>Рассылка...</b> 0/{len(ids)}")
    for i, uid in enumerate(ids):
        try:
            await bot.send_message(uid, f"📢 <b>Сообщение от администратора</b>\n\n{text}")
            sent += 1
        except Exception:
            fail += 1
        if i % 30 == 0 and i > 0:
            try:
                await cb.message.edit_text(f"📢 <b>Рассылка...</b> {sent}/{len(ids)}")
            except Exception:
                pass
    await safe_edit(
        cb,
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📨 Отправлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{fail}</b>",
        kb_admin()
    )
    await log_action_from_cb(cb, True, "Сделал рассылку", f"Отправлено: {sent}, ошибок: {fail}, текст: {text[:60]}")


# ── Быстрые логи ──────────────────────────────────────────────

@router.callback_query(F.data == "admin_logs_menu")
async def cb_admin_logs_menu(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только суперадмин", show_alert=True); return

    logs_all = await db_get_logs(is_admin=None,  limit=99999)
    logs_adm = await db_get_logs(is_admin=True,  limit=99999)
    logs_usr = await db_get_logs(is_admin=False, limit=99999)

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text=f"👑 Логи администраторов  ({len(logs_adm)} зап.)",
        callback_data="logs_confirm:admin"
    ))
    b.row(InlineKeyboardButton(
        text=f"👤 Логи пользователей  ({len(logs_usr)} зап.)",
        callback_data="logs_confirm:users"
    ))
    b.row(InlineKeyboardButton(
        text=f"📋 Все логи  ({len(logs_all)} зап.)",
        callback_data="logs_confirm:all"
    ))
    b.row(InlineKeyboardButton(
        text="🔎 Логи конкретного пользователя",
        callback_data="admin_logs_by_user"
    ))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))

    await safe_edit(
        cb,
        "📋 <b>Быстрые логи</b>  <i>(только суперадмин)</i>\n\n"
        "Нажмите категорию — появится подтверждение перед скачиванием.\n\n"
        "В файле на каждое действие:\n"
        "• точная дата и время\n"
        "• ID, @username, имя\n"
        "• что нажал / что сделал\n"
        "• детали (товар, сумма, сообщение...)",
        b.as_markup()
    )
    await log_action_from_cb(cb, True, "Открыл раздел логов")


@router.callback_query(F.data.startswith("logs_confirm:"))
async def cb_logs_confirm(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только суперадмин", show_alert=True); return
    mode = cb.data.split(":")[1]
    label_ru = {"admin": "администраторов", "users": "пользователей", "all": "всех"}[mode]
    count = len(await db_get_logs(
        is_admin=True if mode == "admin" else (False if mode == "users" else None),
        limit=99999
    ))
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="⬇️ Скачать файл", callback_data=f"admin_logs_dl:{mode}"),
        InlineKeyboardButton(text="❌ Отмена",         callback_data="admin_logs_menu"),
    )
    await safe_edit(
        cb,
        f"📋 <b>Подтверждение скачивания</b>\n\n"
        f"Категория: <b>Логи {label_ru}</b>\n"
        f"Записей в файле: <b>{count}</b>\n\n"
        f"Файл будет отправлен вам в личные сообщения. Продолжить?",
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_logs_dl:"))
async def cb_admin_logs_dl(cb: CallbackQuery, bot: Bot):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только суперадмин", show_alert=True); return
    await cb.answer("⏳ Формируем файл...")

    mode = cb.data.split(":")[1]
    is_admin_filter = True if mode == "admin" else (False if mode == "users" else None)
    label_map = {"admin": "admins", "users": "users", "all": "all"}
    label_ru  = {"admin": "Администраторы", "users": "Пользователи", "all": "Все"}
    label = label_map[mode]

    logs = await db_get_logs(is_admin=is_admin_filter, limit=100000)
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    filename = f"logs_{label}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

    lines_txt = _build_log_txt(logs, label_ru[mode], now_str)
    txt_bytes = "\n".join(lines_txt).encode("utf-8")

    from aiogram.types import BufferedInputFile
    doc = BufferedInputFile(txt_bytes, filename=filename)
    await log_action_from_cb(cb, True, f"Скачал логи: {label}", f"{len(logs)} записей")
    try:
        await bot.send_document(
            cb.from_user.id, doc,
            caption=(
                f"📄 <b>Логи — {label_ru[mode]}</b>\n"
                f"📅 {now_str}\n"
                f"📝 Записей: <b>{len(logs)}</b>"
            )
        )
    except Exception as e:
        await cb.message.answer(f"❌ Ошибка отправки файла: {e}")


def _build_log_txt(logs, title: str, now_str: str) -> list:
    result = [
        "=" * 72,
        f"  ОТЧЁТ ПО ЛОГАМ — {title.upper()}",
        f"  Дата выгрузки: {now_str} (UTC)",
        f"  Всего записей: {len(logs)}",
        "=" * 72,
        "",
    ]
    if not logs:
        result.append("Записей не найдено.")
    else:
        for i, row in enumerate(logs, 1):
            uname    = f"@{row['username']}" if row["username"] else "—"
            role_tag = "👑 ADMIN" if row["is_admin"] else "👤 USER "
            ts       = str(row["created_at"])[:19].replace("T", " ")
            name     = row["full_name"] or "—"
            result.append(f"{'─'*72}")
            result.append(f"  #{i}  {ts} UTC   [{role_tag}]")
            result.append(f"  ID: {row['user_id']}   Ник: {uname}   Имя: {name}")
            result.append(f"  ➤ Действие: {row['action']}")
            if row["details"]:
                result.append(f"  ℹ Детали:   {row['details']}")
            result.append("")
    result.append("=" * 72)
    result.append(f"  Конец отчёта. Всего записей: {len(logs)}")
    result.append("=" * 72)
    return result


@router.callback_query(F.data == "admin_logs_by_user")
async def cb_admin_logs_by_user(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только суперадмин", show_alert=True); return
    await state.set_state(UserLogFSM.waiting_id)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_logs_menu"))
    await safe_edit(
        cb,
        "🔎 <b>Логи пользователя</b>\n\n"
        "Введите <b>Telegram ID</b> (числовой) или <b>@username</b>:",
        b.as_markup()
    )


@router.message(StateFilter(UserLogFSM.waiting_id))
async def fsm_user_log_id(message: Message, state: FSMContext, bot: Bot):
    if message.from_user.id not in ADMIN_IDS:
        await state.clear(); return
    await state.clear()
    query = message.text.strip()
    user = await db_get_user_by_id_or_username(query)
    if not user:
        await message.answer(
            "❌ Пользователь не найден. Проверьте ID или @username.",
            reply_markup=kb_admin()
        )
        return

    uid = user["user_id"]
    logs = await db_get_logs_by_user(uid)
    uname = f"@{user['username']}" if user["username"] else f"ID:{uid}"
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    filename = f"logs_user_{uid}_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

    lines_txt = [
        "=" * 72,
        f"  ЛОГИ ПОЛЬЗОВАТЕЛЯ: {user['full_name'] or '—'}  ({uname})",
        f"  ID: {uid}",
        f"  Дата выгрузки: {now_str}",
        f"  Всего записей: {len(logs)}",
        "=" * 72,
        "",
    ]
    if not logs:
        lines_txt.append("Записей не найдено.")
    else:
        for i, row in enumerate(logs, 1):
            role_tag = "👑 ADMIN" if row["is_admin"] else "👤 USER "
            ts = str(row["created_at"])[:19].replace("T", " ")
            lines_txt.append(f"{'─'*60}")
            lines_txt.append(f"  #{i}  {ts} UTC   [{role_tag}]")
            lines_txt.append(f"  ➤ Действие: {row['action']}")
            if row["details"]:
                lines_txt.append(f"  ℹ Детали:   {row['details']}")
            lines_txt.append("")

    txt_bytes = "\n".join(lines_txt).encode("utf-8")
    from aiogram.types import BufferedInputFile
    doc = BufferedInputFile(txt_bytes, filename=filename)
    await log_action_from_msg(message, True, f"Скачал логи юзера {uid}", f"{len(logs)} записей")
    await message.answer(
        f"📄 <b>Логи пользователя</b>\n"
        f"👤 {user['full_name'] or '—'} ({uname})\n"
        f"📝 Всего действий: <b>{len(logs)}</b>"
    )
    await bot.send_document(
        message.from_user.id, doc,
        caption=f"📄 Логи: {user['full_name'] or uname}\n{now_str} • {len(logs)} записей"
    )


# ── Отправить отчёт по пользователю (из кнопки в панели) ──────

@router.callback_query(F.data == "admin_send_digest")
async def cb_admin_send_digest(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("⛔ Только суперадмин", show_alert=True); return
    await state.set_state(UserLogFSM.waiting_id)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    await safe_edit(
        cb,
        "📤 <b>Отправить отчёт по пользователю</b>\n\n"
        "Введите <b>Telegram ID</b> или <b>@username</b> — получите полный лог его действий:",
        b.as_markup()
    )


# ── Ручная отправка общего дайджеста ──────────────────────────

@router.message(Command("sendlog"))
async def cmd_sendlog(message: Message, bot: Bot):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Только суперадмин"); return
    await message.answer("⏳ Генерируем отчёт...")
    await send_daily_digest(bot, only_to=message.from_user.id)
    await message.answer("✅ Ежедневный отчёт отправлен вам.", reply_markup=kb_admin())
    await log_action_from_msg(message, True, "Команда /sendlog")



# ══════════════════════════════════════════════════════════════
# ║              НОВЫЕ ФИЧИ — ХЭНДЛЕРЫ                         ║
# ══════════════════════════════════════════════════════════════

# ── 1. Фильтр пользователей по балансу ────────────────────────

@router.callback_query(F.data == "admin_filter_balance")
async def cb_admin_filter_balance(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(FilterBalanceFSM.waiting_amount)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    await safe_edit(
        cb,
        "💰 <b>Фильтр по балансу</b>\n\n"
        "Введите минимальную сумму в $.\n"
        "Например: <code>10</code> — покажет всех у кого ≥ $10.00",
        b.as_markup()
    )


@router.message(StateFilter(FilterBalanceFSM.waiting_amount))
async def fsm_filter_balance(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    try:
        min_bal = float(message.text.strip().replace(",", "."))
        if min_bal < 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Введите корректное число, например: <code>10</code>"); return
    await state.clear()
    users = await db_get_users_by_min_balance(min_bal)
    if not users:
        await message.answer(f"😔 Пользователей с балансом ≥ {usd(min_bal)} не найдено.", reply_markup=kb_admin()); return

    lines = [
        f"💰 <b>Пользователи с балансом ≥ {usd(min_bal)}</b>",
        f"Найдено: <b>{len(users)}</b>\n",
    ]
    for u in users[:50]:
        uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
        lines.append(f"• {u['full_name'] or '—'} ({uname}) — <b>{usd(u['balance'])}</b>")
    if len(users) > 50:
        lines.append(f"\n<i>...и ещё {len(users)-50} пользователей</i>")

    await message.answer("\n".join(lines), reply_markup=kb_admin())
    await log_action_from_msg(message, True, "Фильтр по балансу", f"≥{usd(min_bal)}, найдено: {len(users)}")


# ── 2. Список забаненных ───────────────────────────────────────

@router.callback_query(F.data == "admin_banned_list")
async def cb_admin_banned_list(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    banned = await db_get_banned_users()
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    if not banned:
        await safe_edit(cb, "🚫 <b>Забаненные пользователи</b>\n\n✅ Нет заблокированных пользователей.", b.as_markup()); return

    lines = [f"🚫 <b>Забаненные ({len(banned)})</b>\n"]
    for u in banned:
        uname = f"@{u['username']}" if u["username"] else f"ID:{u['user_id']}"
        lines.append(f"• <code>{u['user_id']}</code> {u['full_name'] or '—'} ({uname}) — {usd(u['balance'])}")

    # Кнопки разбана
    for u in banned[:10]:
        b.row(InlineKeyboardButton(
            text=f"✅ Разбанить {u['full_name'] or u['user_id']}",
            callback_data=f"admin_ban_toggle:{u['user_id']}"
        ))
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await safe_edit(cb, "\n".join(lines), b.as_markup())
    await log_action_from_cb(cb, True, "Открыл список забаненных")


# ── 3. Личное сообщение пользователю ──────────────────────────

@router.callback_query(F.data == "admin_direct_message")
async def cb_admin_direct_message(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await state.set_state(DirectMessageFSM.waiting_id)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    await safe_edit(
        cb,
        "📨 <b>Сообщение пользователю</b>\n\n"
        "Введите <b>Telegram ID</b> или <b>@username</b>:",
        b.as_markup()
    )


@router.message(StateFilter(DirectMessageFSM.waiting_id))
async def fsm_direct_msg_id(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    user = await db_get_user_by_id_or_username(message.text.strip())
    if not user:
        await message.answer("❌ Пользователь не найден. Введите ID или @username:"); return
    await state.update_data(target_uid=user["user_id"], target_name=user["full_name"] or str(user["user_id"]))
    await state.set_state(DirectMessageFSM.waiting_text)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    uname = f"@{user['username']}" if user["username"] else f"ID:{user['user_id']}"
    await message.answer(
        f"📨 Отправка сообщения: <b>{user['full_name'] or '—'}</b> ({uname})\n\n"
        f"Введите текст сообщения:",
        reply_markup=b.as_markup()
    )


@router.message(StateFilter(DirectMessageFSM.waiting_text))
async def fsm_direct_msg_text(message: Message, state: FSMContext, bot: Bot):
    if not await is_admin(message.from_user.id): return
    if not message.text:
        await message.answer("❌ Введите текстовое сообщение:"); return
    data = await state.get_data()
    await state.clear()
    uid  = data["target_uid"]
    name = data["target_name"]
    try:
        await bot.send_message(
            uid,
            f"📨 <b>Сообщение от администратора:</b>\n\n{message.text}"
        )
        await message.answer(f"✅ Сообщение отправлено пользователю <b>{name}</b>.", reply_markup=kb_admin())
        await log_action_from_msg(message, True, "Отправил личное сообщение юзеру", f"{name} (ID:{uid}): {message.text[:60]}")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}", reply_markup=kb_admin())


# ── 4. Статистика по товарам ───────────────────────────────────

@router.callback_query(F.data == "admin_product_stats")
async def cb_admin_product_stats(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb.answer("⏳ Собираем статистику...")
    stats = await db_get_product_stats()
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = [
        "=" * 60,
        f"  СТАТИСТИКА ТОВАРОВ",
        f"  Дата: {now_str}",
        f"  Всего товаров: {len(stats)}",
        "=" * 60,
        "",
    ]
    total_revenue = 0
    total_sold = 0
    for i, p in enumerate(stats, 1):
        status = "✅ Активен" if p["is_active"] else "❌ Скрыт"
        lines.append(f"{'─'*60}")
        lines.append(f"  #{i}  {p['name']}")
        lines.append(f"  Цена: ${p['price']:.2f}   Статус: {status}")
        lines.append(f"  Продано: {p['purchase_count']} шт.   Выручка: ${p['revenue']:.2f}")
        lines.append("")
        total_revenue += p["revenue"]
        total_sold += p["purchase_count"]
    lines.append("=" * 60)
    lines.append(f"  ИТОГО: продано {total_sold} шт. / выручка ${total_revenue:.2f}")
    lines.append("=" * 60)

    txt_bytes = "\n".join(lines).encode("utf-8")
    filename = f"product_stats_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    from aiogram.types import BufferedInputFile
    doc = BufferedInputFile(txt_bytes, filename=filename)

    # Также показываем краткую сводку в сообщении
    msg_lines = [f"📈 <b>Статистика товаров</b>\n"]
    for p in stats:
        icon = "✅" if p["is_active"] else "❌"
        msg_lines.append(f"{icon} <b>{p['name']}</b> — {p['purchase_count']} прод. / ${p['revenue']:.2f}")
    msg_lines.append(f"\n💰 <b>Итого выручка: ${total_revenue:.2f}</b>")

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel"))
    await cb.message.answer("\n".join(msg_lines), reply_markup=b.as_markup())
    await bot.send_document(
        cb.from_user.id, doc,
        caption=f"📈 Детальная статистика товаров\n{now_str}"
    )
    await log_action_from_cb(cb, True, "Открыл статистику товаров")


# ── 5. Все транзакции ──────────────────────────────────────────

@router.callback_query(F.data == "admin_all_transactions")
async def cb_admin_all_transactions(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return

    txs = await db_get_all_transactions(limit=5000)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="⬇️ Скачать файл", callback_data="admin_transactions_dl"),
        InlineKeyboardButton(text="❌ Отмена",         callback_data="admin_panel"),
    )
    total_in  = sum(t["amount"] for t in txs if t["amount"] > 0)
    total_out = sum(abs(t["amount"]) for t in txs if t["amount"] < 0)
    await safe_edit(
        cb,
        f"🧾 <b>Все транзакции</b>\n\n"
        f"Всего записей: <b>{len(txs)}</b>\n"
        f"💚 Пополнений: <b>{usd(total_in)}</b>\n"
        f"💸 Списаний: <b>{usd(total_out)}</b>\n\n"
        f"Скачать полный файл?",
        b.as_markup()
    )


@router.callback_query(F.data == "admin_transactions_dl")
async def cb_admin_transactions_dl(cb: CallbackQuery, bot: Bot):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    await cb.answer("⏳ Формируем файл...")
    txs = await db_get_all_transactions(limit=10000)
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = [
        "=" * 72,
        "  ИСТОРИЯ ТРАНЗАКЦИЙ",
        f"  Дата выгрузки: {now_str}",
        f"  Всего записей: {len(txs)}",
        "=" * 72, "",
    ]
    type_map = {"purchase": "Покупка", "topup": "Пополнение", "admin": "Администратор",
                "referral": "Реферал", "deposit": "Депозит"}
    for t in txs:
        uname = f"@{t['username']}" if t["username"] else "—"
        ts = str(t["created_at"])[:19].replace("T", " ")
        sign = "+" if t["amount"] > 0 else ""
        ttype = type_map.get(t["type"], t["type"])
        lines.append(f"{'─'*72}")
        lines.append(f"  {ts}   ID:{t['user_id']}  {uname}  {t['full_name'] or '—'}")
        lines.append(f"  Тип: {ttype}   Сумма: {sign}{usd(t['amount'])}")
        if t["description"]:
            lines.append(f"  Описание: {t['description']}")
        lines.append("")
    txt_bytes = "\n".join(lines).encode("utf-8")
    filename = f"transactions_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    from aiogram.types import BufferedInputFile
    doc = BufferedInputFile(txt_bytes, filename=filename)
    await bot.send_document(
        cb.from_user.id, doc,
        caption=f"🧾 Все транзакции\n{now_str} • {len(txs)} записей"
    )
    await log_action_from_cb(cb, True, "Скачал все транзакции", f"{len(txs)} записей")


# ── 6. Подозрительные пользователи ────────────────────────────

@router.callback_query(F.data == "admin_suspicious")
async def cb_admin_suspicious(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    suspects = await db_get_suspicious_users(start_threshold=5, window_minutes=10)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_suspicious"))
    b.row(InlineKeyboardButton(text="◀️ Назад",    callback_data="admin_panel"))

    if not suspects:
        await safe_edit(
            cb,
            "⚠️ <b>Подозрительные действия</b>\n\n"
            "✅ Подозрительных пользователей не обнаружено.\n\n"
            "<i>Критерий: 5+ запусков /start за 10 минут</i>",
            b.as_markup()
        ); return

    lines = [
        f"⚠️ <b>Подозрительные пользователи</b>",
        f"<i>5+ запусков /start за последние 10 минут</i>\n",
    ]
    for s in suspects:
        uname = f"@{s['username']}" if s["username"] else f"ID:{s['user_id']}"
        last = str(s["last_at"])[:16].replace("T", " ")
        lines.append(f"• <code>{s['user_id']}</code> {s['full_name'] or '—'} ({uname})")
        lines.append(f"  🔁 Запусков: <b>{s['cnt']}</b>  |  Последний: {last}")
    # Кнопки быстрого бана
    for s in suspects[:5]:
        b.row(InlineKeyboardButton(
            text=f"🚫 Бан {s['full_name'] or s['user_id']}",
            callback_data=f"admin_ban_toggle:{s['user_id']}"
        ))

    await safe_edit(cb, "\n".join(lines), b.as_markup())
    await log_action_from_cb(cb, True, "Открыл раздел подозрительных пользователей")


# ── 7. Режим магазина (техобслуживание) ───────────────────────

@router.callback_query(F.data == "admin_shop_mode")
async def cb_admin_shop_mode(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    current = await db_get_setting("shop_mode") or "open"
    status_text = "🟢 Открыт" if current == "open" else "🔴 Закрыт (техобслуживание)"
    toggle_text = "🔴 Закрыть магазин" if current == "open" else "🟢 Открыть магазин"
    toggle_val  = "close_shop" if current == "open" else "open_shop"

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text=toggle_text, callback_data=f"admin_shop_toggle:{toggle_val}"))
    b.row(InlineKeyboardButton(text="◀️ Назад",  callback_data="admin_panel"))
    await safe_edit(
        cb,
        f"🏪 <b>Режим магазина</b>\n\n"
        f"Текущий статус: <b>{status_text}</b>\n\n"
        f"При закрытии пользователи увидят сообщение о техобслуживании.\n"
        f"Администраторы видят магазин в любом режиме.",
        b.as_markup()
    )


@router.callback_query(F.data.startswith("admin_shop_toggle:"))
async def cb_admin_shop_toggle(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    action = cb.data.split(":")[1]
    if action == "close_shop":
        await db_set_setting("shop_mode", "closed")
        await cb.answer("🔴 Магазин закрыт")
        await log_action_from_cb(cb, True, "Закрыл магазин (техобслуживание)")
    else:
        await db_set_setting("shop_mode", "open")
        await cb.answer("🟢 Магазин открыт")
        await log_action_from_cb(cb, True, "Открыл магазин")
    await cb_admin_shop_mode(cb)


# ── 8. Реферальный бонус ──────────────────────────────────────

@router.callback_query(F.data == "admin_set_referral_bonus")
async def cb_admin_set_referral_bonus(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        await cb.answer("Нет доступа", show_alert=True); return
    current_bonus = await db_get_setting("referral_bonus") or str(REFERRAL_BONUS)
    await state.set_state(SetReferralBonusFSM.waiting_amount)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel"))
    await safe_edit(
        cb,
        f"🎁 <b>Реферальный бонус</b>\n\n"
        f"Текущий бонус: <b>{usd(float(current_bonus))}</b>\n\n"
        f"Введите новую сумму в $ (например: <code>5</code> или <code>2.50</code>):",
        b.as_markup()
    )


@router.message(StateFilter(SetReferralBonusFSM.waiting_amount))
async def fsm_set_referral_bonus(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id): return
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount < 0: raise ValueError
    except (ValueError, AttributeError):
        await message.answer("❌ Введите корректное число, например: <code>5</code>"); return
    await state.clear()
    old_val = await db_get_setting("referral_bonus") or str(REFERRAL_BONUS)
    await db_set_setting("referral_bonus", str(amount))
    await message.answer(
        f"✅ <b>Реферальный бонус обновлён!</b>\n\n"
        f"Было: <b>{usd(float(old_val))}</b>\n"
        f"Стало: <b>{usd(amount)}</b>\n\n"
        f"<i>Новый бонус применяется к следующим пополнениям рефералов.</i>",
        reply_markup=kb_admin()
    )
    await log_action_from_msg(message, True, "Изменил реферальный бонус", f"{usd(float(old_val))} → {usd(amount)}")


# ── Fallback ──────────────────────────────────────────────────

@router.message()
async def fallback(message: Message, state: FSMContext):
    if await state.get_state(): return
    user = await db_get_user(message.from_user.id)
    if not user:
        await message.answer("👋 Для начала работы напишите /start")
    elif user["is_banned"]:
        await message.answer("🚫 Ваш аккаунт заблокирован.")
    else:
        await message.answer("Используйте меню:", reply_markup=kb_main())


# ╔══════════════════════════════════════════════════════════════╗
# ║                        ЗАПУСК                               ║
# ╚══════════════════════════════════════════════════════════════╝

async def send_daily_digest(bot: Bot, only_to: int = None):
    """Send daily log with all users + top buyers to all admins (or only_to if specified)."""
    import io
    from aiogram.types import BufferedInputFile
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    users = await db_get_all_users_for_log()
    buyers = await db_analytics_top_buyers_10()
    total_users = await db_total_users()
    rev_day = await db_analytics_revenue("day")
    rev_week = await db_analytics_revenue("week")

    # Build TXT log content
    lines_txt = [
        f"=== ЕЖЕДНЕВНЫЙ ОТЧЁТ {now} ===",
        f"Всего пользователей: {total_users}",
        f"Выручка за день:     {usd(rev_day)}",
        f"Выручка за неделю:   {usd(rev_week)}",
        "",
        "=" * 60,
        "--- СПИСОК ВСЕХ ПОЛЬЗОВАТЕЛЕЙ ---",
        f"{'ID':<15} {'Username':<25} {'Имя':<30} {'Дата рег.'}",
        "-" * 80,
    ]
    for u in users:
        uname = f"@{u['username']}" if u["username"] else "—"
        lines_txt.append(
            f"{str(u['user_id']):<15} {uname:<25} {(u['full_name'] or '—'):<30} {str(u['created_at'])[:10]}"
        )
    lines_txt += [
        "",
        "=" * 60,
        "--- ТОП-10 ПОКУПАТЕЛЕЙ ---",
        f"{'#':<4} {'Username':<25} {'Имя':<30} {'Сумма':<12} {'Покупок'}",
        "-" * 80,
    ]
    if buyers:
        for i, b_row in enumerate(buyers, 1):
            uname = f"@{b_row['username']}" if b_row["username"] else f"ID:{b_row['user_id']}"
            lines_txt.append(
                f"{i:<4} {uname:<25} {(b_row['full_name'] or '—'):<30} {usd(b_row['total']):<12} {b_row['cnt']}"
            )
    else:
        lines_txt.append("Покупок пока нет.")

    txt_bytes = "\n".join(lines_txt).encode("utf-8")
    filename = f"daily_log_{datetime.now().strftime('%Y%m%d')}.txt"

    # Build short message
    medals = ["🥇", "🥈", "🥉"]
    top_text = ""
    for i, b_row in enumerate(buyers[:5]):
        medal = medals[i] if i < 3 else f"{i+1}."
        uname = f"@{b_row['username']}" if b_row["username"] else f"ID:{b_row['user_id']}"
        top_text += f"{medal} {b_row['full_name'] or uname} — {usd(b_row['total'])} ({b_row['cnt']} пок.)\n"

    msg_text = (
        f"📋 <b>Ежедневный отчёт</b>  {now}\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"💰 Выручка за сегодня: <b>{usd(rev_day)}</b>\n"
        f"📈 Выручка за неделю: <b>{usd(rev_week)}</b>\n\n"
        f"🏆 <b>Топ-5 покупателей:</b>\n{top_text if top_text else '<i>нет данных</i>'}\n"
        f"📎 Полный список пользователей и ников — в прикреплённом .txt файле."
    )

    admin_ids = [only_to] if only_to else await get_all_admin_ids()
    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, msg_text)
            doc = BufferedInputFile(txt_bytes, filename=filename)
            await bot.send_document(
                admin_id,
                doc,
                caption=f"📄 <b>Лог пользователей</b> — {now}\nВсего: {total_users} чел."
            )
        except Exception as e:
            logging.warning(f"Daily digest to {admin_id} failed: {e}")


async def daily_digest_loop(bot: Bot):
    """Background task: send digest every 24 hours."""
    while True:
        await asyncio.sleep(86400)  # 24 hours
        try:
            await send_daily_digest(bot)
        except Exception as e:
            logging.error(f"Daily digest error: {e}")


async def main():
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    print("✅ Treant Bot v2 запущен")
    asyncio.create_task(daily_digest_loop(bot))
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())