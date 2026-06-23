import os
import json
import re
import asyncio
import logging
import time
import sqlite3
import random
import string
from datetime import datetime, timedelta
from threading import Thread
from queue import Queue

from telegram import (
    Bot,
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    InputFile
)
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler
)

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# تكوين البوت - بدون أي توكن أو آيدي افتراضي داخل الكود
# كل بوت يأخذ بياناته من أوامر export في Termux.
def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"❌ المتغير {name} غير موجود. استخدم export {name}=... قبل تشغيل البوت.")
    return value


def _safe_slug(value: str, fallback: str = "bot") -> str:
    value = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")
    return value[:64] or fallback


BOT_TOKEN = _required_env("BOT_TOKEN")
try:
    OWNER_ID = int(_required_env("OWNER_ID"))
except ValueError:
    raise SystemExit("❌ OWNER_ID يجب أن يكون رقماً فقط.")

# كل توكن/بوت يأخذ مجلد وقاعدة بيانات مستقلة تلقائياً.
# يمكنك تغييرها يدوياً عبر export DATA_DIR=... أو export DB_NAME=...
BOT_ID = _safe_slug(BOT_TOKEN.split(":", 1)[0], "bot")
DATA_DIR = os.environ.get("DATA_DIR", os.path.join("bots_data", BOT_ID))
DB_NAME = os.environ.get("DB_NAME", os.path.join(DATA_DIR, "bot_database.db"))
ADS_DIR = os.path.join(DATA_DIR, "ads")
PROFILE_PHOTOS_DIR = os.path.join(DATA_DIR, "profile_photos")
GROUP_REPLIES_DIR = os.path.join(DATA_DIR, "group_replies")

for _directory in (DATA_DIR, ADS_DIR, PROFILE_PHOTOS_DIR, GROUP_REPLIES_DIR):
    os.makedirs(_directory, exist_ok=True)

# حالات المحادثة
(
    ADD_ACCOUNT, ADD_AD_TYPE, ADD_AD_TEXT, ADD_AD_MEDIA, ADD_GROUP, 
    ADD_PRIVATE_REPLY, ADD_GROUP_REPLY, ADD_ADMIN, 
    ADD_USERNAME, ADD_RANDOM_REPLY, ADD_PRIVATE_TEXT, ADD_GROUP_TEXT, 
    ADD_GROUP_PHOTO, SET_LINK_TARGETS
) = range(14)

# تهيئة السجل
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class BotDatabase:
    def __init__(self):
        self.init_database()
    
    def init_database(self):
        """تهيئة قاعدة البيانات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA synchronous=NORMAL')
        
        # جدول الحسابات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_string TEXT UNIQUE,
                phone TEXT,
                name TEXT,
                username TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الإعلانات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT,
                text TEXT,
                media_path TEXT,
                file_type TEXT,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول المجموعات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link TEXT,
                status TEXT DEFAULT 'pending',
                join_date DATETIME,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول المشرفين
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE,
                username TEXT,
                full_name TEXT,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_super_admin BOOLEAN DEFAULT 0
            )
        ''')
        
        # جدول الردود الخاصة
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS private_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reply_text TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الردود الجماعية النصية
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_text_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger TEXT,
                reply_text TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الردود الجماعية مع الصور
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_photo_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger TEXT,
                reply_text TEXT,
                media_path TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول الردود العشوائية في القروبات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_random_replies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reply_text TEXT,
                is_active BOOLEAN DEFAULT 1,
                added_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER DEFAULT 0
            )
        ''')
        
        # جدول نشر الحسابات
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS account_publishing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER,
                status TEXT DEFAULT 'active',
                last_publish DATETIME,
                FOREIGN KEY (account_id) REFERENCES accounts (id)
            )
        ''')

        # إعدادات تجميع روابط واتساب وتليجرام
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS link_collection_settings (
                admin_id INTEGER PRIMARY KEY,
                telegram_target TEXT,
                whatsapp_target TEXT,
                updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # الروابط التي تم تجميعها مسبقاً لمنع التكرار
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collected_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                link_type TEXT,
                link TEXT,
                source_dialog TEXT,
                source_account_id INTEGER,
                collected_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(admin_id, link_type, link)
            )
        ''')

        # طابور إرسال دائم: إذا حصل خطأ مؤقت لا يضيع الرابط، ويعاد إرساله لاحقاً.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS link_collection_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                link_type TEXT,
                link TEXT,
                target TEXT,
                source_dialog TEXT,
                source_account_id INTEGER,
                account_name TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(admin_id, link_type, link)
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_link_outbox_admin
            ON link_collection_outbox(admin_id, id)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_collected_links_admin_type_link
            ON collected_links(admin_id, link_type, link)
        ''')

        # حالة فحص كل قناة/مجموعة: أول مرة يتم فحص كل التاريخ، وبعدها يتم التقاط الرسائل الجديدة فقط.
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS link_collection_dialog_state (
                admin_id INTEGER,
                account_id INTEGER,
                dialog_id INTEGER,
                last_message_id INTEGER DEFAULT 0,
                full_scan_done BOOLEAN DEFAULT 0,
                updated_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (admin_id, account_id, dialog_id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_account(self, session_string, phone, name, username, admin_id=0):
        """إضافة حساب جديد"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO accounts (session_string, phone, name, username, admin_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (session_string, phone, name, username, admin_id))
            account_id = cursor.lastrowid
            
            cursor.execute('''
                INSERT INTO account_publishing (account_id)
                VALUES (?)
            ''', (account_id,))
            
            conn.commit()
            return True, "تم إضافة الحساب بنجاح"
        except sqlite3.IntegrityError:
            return False, "هذا الحساب مضاف مسبقاً"
        except Exception as e:
            return False, f"خطأ في إضافة الحساب: {str(e)}"
        finally:
            conn.close()
    
    def get_accounts(self, admin_id=None):
        """الحصول على جميع الحسابات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('''
                SELECT id, session_string, phone, name, username, is_active 
                FROM accounts 
                WHERE admin_id = ? OR admin_id = 0
                ORDER BY id
            ''', (admin_id,))
        else:
            cursor.execute('''
                SELECT id, session_string, phone, name, username, is_active 
                FROM accounts 
                ORDER BY id
            ''')
            
        accounts = cursor.fetchall()
        conn.close()
        return accounts
    
    def delete_account(self, account_id, admin_id=None):
        """حذف حساب"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id:
            cursor.execute('DELETE FROM accounts WHERE id = ? AND (admin_id = ? OR admin_id = 0)', (account_id, admin_id))
        else:
            cursor.execute('DELETE FROM accounts WHERE id = ?', (account_id,))
            
        cursor.execute('DELETE FROM account_publishing WHERE account_id = ?', (account_id,))
        
        conn.commit()
        conn.close()
        return True
    
    def add_ad(self, ad_type, text, media_path=None, file_type=None, admin_id=0):
        """إضافة إعلان"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO ads (type, text, media_path, file_type, admin_id)
            VALUES (?, ?, ?, ?, ?)
        ''', (ad_type, text, media_path, file_type, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_ads(self, admin_id=None):
        """الحصول على جميع الإعلانات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM ads WHERE admin_id = ? OR admin_id = 0 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM ads ORDER BY id')
            
        ads = cursor.fetchall()
        conn.close()
        return ads
    
    def delete_ad(self, ad_id, admin_id=None):
        """حذف إعلان"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id:
            cursor.execute('DELETE FROM ads WHERE id = ? AND (admin_id = ? OR admin_id = 0)', (ad_id, admin_id))
        else:
            cursor.execute('DELETE FROM ads WHERE id = ?', (ad_id,))
            
        conn.commit()
        conn.close()
        return True
    
    def add_group(self, link, admin_id=0):
        """إضافة مجموعة"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO groups (link, admin_id)
            VALUES (?, ?)
        ''', (link, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_groups(self, admin_id=None):
        """الحصول على جميع المجموعات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM groups WHERE admin_id = ? OR admin_id = 0 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM groups ORDER BY id')
            
        groups = cursor.fetchall()
        conn.close()
        return groups
    
    def update_group_status(self, group_id, status):
        """تحديث حالة المجموعة"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE groups 
            SET status = ?, join_date = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (status, group_id))
        
        conn.commit()
        conn.close()
        return True
    
    def add_admin(self, user_id, username, full_name, is_super_admin=False):
        """إضافة مشرف"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO admins (user_id, username, full_name, is_super_admin)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, full_name, is_super_admin))
            conn.commit()
            return True, "تم إضافة المشرف بنجاح"
        except sqlite3.IntegrityError:
            return False, "هذا المشرف مضاف مسبقاً"
        finally:
            conn.close()
    
    def get_admins(self):
        """الحصول على جميع المشرفين"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM admins ORDER BY id')
        admins = cursor.fetchall()
        conn.close()
        return admins
    
    def delete_admin(self, admin_id):
        """حذف مشرف"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM admins WHERE id = ?', (admin_id,))
        conn.commit()
        conn.close()
        return True
    
    def is_admin(self, user_id):
        """التحقق إذا كان المستخدم مشرف"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM admins WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def is_super_admin(self, user_id):
        """التحقق إذا كان المستخدم مشرف رئيسي"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM admins WHERE user_id = ? AND is_super_admin = 1', (user_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def add_private_reply(self, reply_text, admin_id=0):
        """إضافة رد خاص"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO private_replies (reply_text, admin_id)
            VALUES (?, ?)
        ''', (reply_text, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_private_replies(self, admin_id=None):
        """الحصول على الردود الخاصة"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM private_replies WHERE (admin_id = ? OR admin_id = 0) AND is_active = 1 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM private_replies WHERE is_active = 1 ORDER BY id')
            
        replies = cursor.fetchall()
        conn.close()
        return replies
    
    def add_group_text_reply(self, trigger, reply_text, admin_id=0):
        """إضافة رد نصي جماعي"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO group_text_replies (trigger, reply_text, admin_id)
            VALUES (?, ?, ?)
        ''', (trigger, reply_text, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_group_text_replies(self, admin_id=None):
        """الحصول على الردود النصية الجماعية"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM group_text_replies WHERE (admin_id = ? OR admin_id = 0) AND is_active = 1 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM group_text_replies WHERE is_active = 1 ORDER BY id')
            
        replies = cursor.fetchall()
        conn.close()
        return replies
    
    def add_group_photo_reply(self, trigger, reply_text, media_path, admin_id=0):
        """إضافة رد جماعي مع صورة"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO group_photo_replies (trigger, reply_text, media_path, admin_id)
            VALUES (?, ?, ?, ?)
        ''', (trigger, reply_text, media_path, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_group_photo_replies(self, admin_id=None):
        """الحصول على الردود الجماعية مع الصور"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM group_photo_replies WHERE (admin_id = ? OR admin_id = 0) AND is_active = 1 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM group_photo_replies WHERE is_active = 1 ORDER BY id')
            
        replies = cursor.fetchall()
        conn.close()
        return replies
    
    def add_group_random_reply(self, reply_text, admin_id=0):
        """إضافة رد عشوائي في القروبات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO group_random_replies (reply_text, admin_id)
            VALUES (?, ?)
        ''', (reply_text, admin_id))
        
        conn.commit()
        conn.close()
        return True
    
    def get_group_random_replies(self, admin_id=None):
        """الحصول على الردود العشوائية في القروبات"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('SELECT * FROM group_random_replies WHERE (admin_id = ? OR admin_id = 0) AND is_active = 1 ORDER BY id', (admin_id,))
        else:
            cursor.execute('SELECT * FROM group_random_replies WHERE is_active = 1 ORDER BY id')
            
        replies = cursor.fetchall()
        conn.close()
        return replies
    
    def set_link_collection_settings(self, admin_id, telegram_target, whatsapp_target):
        """حفظ وجهات تجميع روابط تليجرام وواتساب."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO link_collection_settings (admin_id, telegram_target, whatsapp_target, updated_date)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(admin_id) DO UPDATE SET
                telegram_target = excluded.telegram_target,
                whatsapp_target = excluded.whatsapp_target,
                updated_date = CURRENT_TIMESTAMP
        ''', (admin_id, telegram_target, whatsapp_target))
        conn.commit()
        conn.close()
        return True

    def get_link_collection_settings(self, admin_id):
        """جلب وجهات تجميع الروابط للمشرف."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT telegram_target, whatsapp_target, updated_date
            FROM link_collection_settings
            WHERE admin_id = ?
        ''', (admin_id,))
        result = cursor.fetchone()
        conn.close()
        return result

    def add_collected_link(self, admin_id, link_type, link, source_dialog=None, source_account_id=None):
        """حفظ رابط جديد، ويرجع False إذا كان الرابط موجوداً مسبقاً."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO collected_links (admin_id, link_type, link, source_dialog, source_account_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (admin_id, link_type, link, source_dialog, source_account_id))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        finally:
            conn.close()

    def enqueue_link_for_sending(self, admin_id, link_type, link, target, source_dialog=None, source_account_id=None, account_name=None):
        # إضافة رابط لطابور الإرسال بدون تكرار. لا يحفظ كمرسل إلا بعد نجاح الإرسال للقناة.
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT 1
                FROM collected_links
                WHERE admin_id = ? AND link_type = ? AND link = ?
                LIMIT 1
            """, (admin_id, link_type, link))
            if cursor.fetchone() is not None:
                return None

            cursor.execute("""
                INSERT INTO link_collection_outbox
                    (admin_id, link_type, link, target, source_dialog, source_account_id, account_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (admin_id, link_type, link, target, source_dialog, source_account_id, account_name))
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None
        finally:
            conn.close()

    def get_pending_outbox_links(self, admin_id, limit=1000):
        # جلب الروابط التي لم يتم إرسالها بعد من الطابور الدائم.
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, admin_id, link_type, link, target, source_dialog, source_account_id, account_name, attempts
            FROM link_collection_outbox
            WHERE admin_id = ?
            ORDER BY id ASC
            LIMIT ?
        """, (admin_id, int(limit)))
        rows = cursor.fetchall()
        conn.close()
        return rows

    def mark_outbox_link_sent(self, outbox_id):
        # نقل رابط من طابور الإرسال إلى جدول الروابط المرسلة بعد نجاح الإرسال.
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT admin_id, link_type, link, source_dialog, source_account_id
            FROM link_collection_outbox
            WHERE id = ?
        """, (outbox_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return False

        admin_id, link_type, link, source_dialog, source_account_id = row
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO collected_links
                    (admin_id, link_type, link, source_dialog, source_account_id)
                VALUES (?, ?, ?, ?, ?)
            """, (admin_id, link_type, link, source_dialog, source_account_id))
            cursor.execute('DELETE FROM link_collection_outbox WHERE id = ?', (outbox_id,))
            conn.commit()
            return True
        finally:
            conn.close()

    def mark_outbox_link_failed(self, outbox_id, error_text):
        # تسجيل الخطأ مع إبقاء الرابط في الطابور لإعادة المحاولة لاحقاً.
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE link_collection_outbox
            SET attempts = attempts + 1,
                last_error = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (str(error_text)[:500], outbox_id))
        conn.commit()
        conn.close()
        return True

    def is_collected_link(self, admin_id, link_type, link):
        """التحقق هل الرابط تم إرساله/حفظه مسبقاً."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT 1
            FROM collected_links
            WHERE admin_id = ? AND link_type = ? AND link = ?
            LIMIT 1
        ''', (admin_id, link_type, link))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def get_collected_links_count(self, admin_id):
        """إحصائيات الروابط المجمعة للمشرف."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT link_type, COUNT(*)
            FROM collected_links
            WHERE admin_id = ?
            GROUP BY link_type
        ''', (admin_id,))
        rows = dict(cursor.fetchall())
        conn.close()
        return rows

    def reset_link_collection_memory(self, admin_id):
        """إعادة ضبط ذاكرة تجميع الروابط للمشرف فقط."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM collected_links
            WHERE admin_id = ?
        ''', (admin_id,))
        cursor.execute('''
            DELETE FROM link_collection_dialog_state
            WHERE admin_id = ?
        ''', (admin_id,))
        cursor.execute('''
            DELETE FROM link_collection_outbox
            WHERE admin_id = ?
        ''', (admin_id,))
        conn.commit()
        conn.close()
        return True

    def get_link_collection_dialog_state(self, admin_id, account_id, dialog_id):
        """جلب آخر رسالة تم فحصها في قناة/مجموعة معينة."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_message_id, full_scan_done
            FROM link_collection_dialog_state
            WHERE admin_id = ? AND account_id = ? AND dialog_id = ?
        ''', (admin_id, account_id, dialog_id))
        result = cursor.fetchone()
        conn.close()
        return result

    def update_link_collection_dialog_state(self, admin_id, account_id, dialog_id, last_message_id, full_scan_done=True):
        """حفظ حالة فحص قناة/مجموعة حتى لا يعاد فحص التاريخ كله في كل دورة."""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO link_collection_dialog_state
                (admin_id, account_id, dialog_id, last_message_id, full_scan_done, updated_date)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(admin_id, account_id, dialog_id) DO UPDATE SET
                last_message_id = MAX(link_collection_dialog_state.last_message_id, excluded.last_message_id),
                full_scan_done = excluded.full_scan_done,
                updated_date = CURRENT_TIMESTAMP
        ''', (admin_id, account_id, dialog_id, int(last_message_id or 0), 1 if full_scan_done else 0))
        conn.commit()
        conn.close()
        return True

    def get_active_publishing_accounts(self, admin_id=None):
        """الحصول على الحسابات النشطة للنشر"""
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        if admin_id is not None:
            cursor.execute('''
                SELECT a.id, a.session_string, a.name, a.username
                FROM accounts a
                JOIN account_publishing ap ON a.id = ap.account_id
                WHERE ap.status = 'active' AND a.is_active = 1 
                AND (a.admin_id = ? OR a.admin_id = 0)
            ''', (admin_id,))
        else:
            cursor.execute('''
                SELECT a.id, a.session_string, a.name, a.username
                FROM accounts a
                JOIN account_publishing ap ON a.id = ap.account_id
                WHERE ap.status = 'active' AND a.is_active = 1
            ''')
            
        accounts = cursor.fetchall()
        conn.close()
        return accounts

class TelegramBotManager:
    def __init__(self, db):
        self.db = db
        self.publishing_active = False
        self.publishing_thread = None
        self.private_reply_active = False
        self.private_reply_thread = None
        self.group_reply_active = False
        self.group_reply_thread = None
        self.random_reply_active = False
        self.random_reply_thread = None
        self.private_replied_messages = set()
        self.group_replied_messages = set()
        self.random_replied_messages = set()
        self.link_collection_active = False
        self.link_collection_thread = None
        self.link_collection_admin_id = None
        self.link_collection_seen_messages = set()
        self._max_seen_messages = 5000
        # كاش لوجهات الإرسال حتى لا يعمل Telethon ResolveUsernameRequest عند كل رابط.
        self._send_target_cache = {}
        # تبريد مؤقت عند حدوث FloodWait حتى لا يكرر نفس الخطأ آلاف المرات.
        self._send_target_flood_until = {}
        # بوت الإشعارات للمالك/المشرف عند اكتمال الفحص التاريخي الأول.
        self.notify_bot = Bot(BOT_TOKEN)
        self.link_collection_initial_cycle_notified = False
        self.link_send_queue = None
        self.link_sender_tasks = []
        self._queued_outbox_ids = set()
        self._target_next_send_time = {}

    def _remember_message(self, storage, key):
        """حفظ الرسائل التي تمت معالجتها لمنع تكرار الردود."""
        if key in storage:
            return False
        storage.add(key)
        if len(storage) > self._max_seen_messages:
            # تنظيف بسيط للذاكرة عند امتلاء القائمة
            for old_key in list(storage)[:1000]:
                storage.discard(old_key)
        return True

    async def _controlled_sleep(self, seconds, flag_name):
        """انتظار قابل للإيقاف حتى لا تتجمد أزرار الإيقاف."""
        for _ in range(int(seconds)):
            if not getattr(self, flag_name):
                return False
            await asyncio.sleep(1)
        return True
    
    def extract_group_links(self, text):
        """استخراج روابط تليجرام وواتساب من النص بدون فتحها أو التحقق منها.

        القواعد:
        - يجمع روابط واتساب chat.whatsapp.com فقط.
        - يجمع كل روابط تليجرام t.me و telegram.me المسموحة، مثل:
          http://t.me/Groupmanagmentphd
          http://t.me/xhdiigsshwso
          http://t.me/alfaqih_almuqarin
          https://t.me/addlist/_G8Gz8pTae0zMDI0
        - لا يفتح الرابط ولا يستدعي get_entity ولا يتحقق من نوع الرابط.
        - يتجاهل روابط تليجرام التي ينتهي اسمها الأول بكلمة bot بأي حالة أحرف.
        - يتجاهل روابط الدعوة الخاصة التي تبدأ بـ t.me/+ مثل https://t.me/+xxxxxxx.
        - يتجاهل روابط رسائل تليجرام مثل:
          https://t.me/channelname/12345
          https://t.me/c/123456789/12345
          https://t.me/s/channelname/12345
        """
        if not text:
            return {"telegram": set(), "whatsapp": set()}

        results = {"telegram": set(), "whatsapp": set()}
        trailing = '.,;:!?)"]}،؛\n\r\t '

        # روابط واتساب: chat.whatsapp.com/<code>
        whatsapp_pattern = re.compile(
            r'(?:https?://)?chat\.whatsapp\.com/[A-Za-z0-9_-]{10,}',
            re.IGNORECASE
        )
        for match in whatsapp_pattern.findall(text):
            link = match.strip().strip(trailing)
            link = link.split('?', 1)[0].split('#', 1)[0]
            if not link.startswith(('http://', 'https://')):
                link = 'https://' + link
            results["whatsapp"].add(link)

        # كل روابط تليجرام t.me و telegram.me بدون فحص أو get_entity.
        telegram_pattern = re.compile(
            r'''(?:https?://)?(?:t\.me|telegram\.me)/[^\s<>"']+''',
            re.IGNORECASE
        )
        for match in telegram_pattern.findall(text):
            raw = match.strip().strip(trailing)
            clean = re.sub(r'^https?://', '', raw, flags=re.IGNORECASE)
            clean = clean.split('?', 1)[0].split('#', 1)[0].strip('/')

            parts = clean.split('/')
            if len(parts) < 2:
                continue

            domain = parts[0].lower()
            path_parts = [part.strip().strip(trailing) for part in parts[1:] if part.strip().strip(trailing)]
            if not path_parts:
                continue

            first_segment = path_parts[0]
            first_lower = first_segment.lower()

            # استثناء روابط الدعوة الخاصة مثل t.me/+xxxxxxx
            if first_segment.startswith('+'):
                continue

            # استثناء روابط البوتات مثل t.me/examplebot
            if first_lower.endswith('bot'):
                continue

            # استثناء روابط رسائل القنوات العامة مثل:
            # t.me/channelname/819602
            # أي رابط عام يكون الجزء الثاني فيه رقم فقط يعتبر رابط رسالة وليس رابط قناة/قروب.
            if len(path_parts) >= 2 and path_parts[1].isdigit():
                continue

            # استثناء روابط رسائل القنوات الخاصة/الداخلية مثل:
            # t.me/c/123456789/819602
            if first_lower == 'c':
                continue

            # استثناء روابط عرض الويب/رسائل القنوات مثل:
            # t.me/s/channelname أو t.me/s/channelname/819602
            if first_lower == 's':
                continue

            normalized_path = '/'.join(path_parts)
            if domain in ('t.me', 'telegram.me'):
                results["telegram"].add(f"https://t.me/{normalized_path}")

        return results

    async def _is_telegram_group_or_channel_link(self, client, link):
        """دالة قديمة لم تعد مستخدمة: تم إلغاء التحقق الإضافي لتقليل FloodWait."""
        if '/+' in link or '/joinchat/' in link:
            return True
        try:
            entity = await client.get_entity(link)
            return hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast') or entity.__class__.__name__ in ('Chat', 'Channel')
        except Exception:
            # إذا لم نستطع التحقق لا نحذف الرابط؛ قد يكون خاصاً أو غير قابل للوصول من الحساب الحالي.
            return True

    async def _resolve_send_target(self, client, account_id, target):
        """حل وجهة الإرسال مرة واحدة فقط لكل حساب/وجهة لتقليل ResolveUsernameRequest."""
        target = str(target).strip()
        cache_key = (account_id, target)

        if cache_key in self._send_target_cache:
            return self._send_target_cache[cache_key]

        # دعم الآيدي الرقمي إذا أدخله المستخدم بدلاً من @username.
        if re.fullmatch(r"-?\d+", target):
            resolved = int(target)
        else:
            # هنا قد يحدث ResolveUsernameRequest، لذلك نجعله مرة واحدة فقط ونحترم FloodWait.
            resolved = await client.get_input_entity(target)

        self._send_target_cache[cache_key] = resolved
        return resolved

    def _normalize_bot_send_target(self, target):
        """تحويل وجهة الإرسال إلى صيغة يقبلها Bot API.

        الصيغ المقبولة:
        - @channel_username
        - channel_username
        - https://t.me/channel_username
        - -1001234567890

        روابط الدعوة الخاصة مثل https://t.me/+xxxx لا تصلح كوجهة إرسال للبوت.
        """
        target = str(target or "").strip()
        target = target.replace("https://t.me/", "")
        target = target.replace("http://t.me/", "")
        target = target.replace("https://telegram.me/", "")
        target = target.replace("http://telegram.me/", "")
        target = target.replace("t.me/", "")
        target = target.replace("telegram.me/", "")
        target = target.strip().strip("/")

        if not target:
            raise ValueError("وجهة الإرسال فارغة")

        # ID رقمي مثل -1001234567890
        if re.fullmatch(r"-?\d+", target):
            return int(target)

        # Bot API لا يستطيع الإرسال إلى رابط دعوة خاص كوجهة.
        if target.startswith("+") or target.lower().startswith("joinchat/"):
            raise ValueError("استخدم @username للقناة أو آيدي القناة -100... وليس رابط دعوة خاص")

        if target.startswith("@"):
            return target

        if re.fullmatch(r"[A-Za-z0-9_]{5,32}", target):
            return f"@{target}"

        return target

    async def _send_collected_link(self, target, link_type, link, source_dialog, account_name):
        # إرسال الرابط فوراً إلى قناة التخزين عن طريق البوت نفسه. لا يتم فتح الرابط ولا فحصه.
        # يرجع: (نجاح, مدة انتظار عند الخطأ المؤقت, نص الخطأ)
        label = "تليجرام" if link_type == 'telegram' else "واتساب"
        message = (
            f"🔗 رابط {label} جديد\n"
            f"{link}\n\n"
            f"📌 المصدر: {source_dialog or 'غير معروف'}\n"
            f"👤 الحساب: {account_name or 'غير معروف'}"
        )

        try:
            chat_id = self._normalize_bot_send_target(target)

            # تبريد بسيط لكل قناة حتى لا يتوقف البوت بسبب RetryAfter.
            # هذا ليس فحصاً للروابط؛ فقط تنظيم إرسال الرسائل للقناة.
            delay = float(os.environ.get('LINK_SEND_DELAY', '0.35'))
            target_key = str(chat_id)
            now = time.time()
            next_allowed = self._target_next_send_time.get(target_key, 0)
            if next_allowed > now:
                await asyncio.sleep(next_allowed - now)
            self._target_next_send_time[target_key] = time.time() + max(delay, 0)

            await self.notify_bot.send_message(
                chat_id=chat_id,
                text=message,
                disable_web_page_preview=True
            )
            return True, 0, None

        except RetryAfter as e:
            retry_after = int(getattr(e, 'retry_after', 5) or 5)
            logger.warning(f"Bot API RetryAfter عند الإرسال إلى {target}: {retry_after} ثانية")
            return False, max(retry_after, 1), f"RetryAfter {retry_after}s"
        except (TimedOut, NetworkError) as e:
            logger.warning(f"خطأ شبكة مؤقت عند الإرسال إلى {target}: {str(e)}")
            return False, 10, str(e)
        except TelegramError as e:
            logger.error(f"فشل إرسال الرابط عبر البوت إلى {target}: {str(e)}")
            return False, 60, str(e)
        except Exception as e:
            logger.error(f"خطأ غير متوقع أثناء إرسال الرابط إلى {target}: {str(e)}")
            return False, 60, str(e)

    async def _enqueue_outbox_row(self, row):
        # وضع صف من طابور قاعدة البيانات داخل طابور الرام للإرسال السريع.
        if self.link_send_queue is None:
            return False
        outbox_id = row[0]
        if outbox_id in self._queued_outbox_ids:
            return False
        self._queued_outbox_ids.add(outbox_id)
        await self.link_send_queue.put(row)
        return True

    async def _load_pending_outbox(self, admin_id):
        # تحميل أي روابط قديمة لم تُرسل بسبب إعادة تشغيل أو خطأ مؤقت.
        rows = self.db.get_pending_outbox_links(admin_id, limit=5000)
        loaded = 0
        for row in rows:
            if await self._enqueue_outbox_row(row):
                loaded += 1
        if loaded:
            logger.info(f"تم تحميل {loaded} رابط من طابور الإرسال الدائم")
        return loaded

    async def _link_sender_worker(self, worker_id):
        # عامل إرسال مستقل: يرسل الروابط من الطابور دون تعطيل فحص الرسائل.
        while self.link_collection_active:
            try:
                row = await asyncio.wait_for(self.link_send_queue.get(), timeout=3)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"عامل الإرسال #{worker_id} توقف مؤقتاً: {str(e)}")
                await asyncio.sleep(2)
                continue

            outbox_id, admin_id, link_type, link, target, source_dialog, source_account_id, account_name, attempts = row
            try:
                ok, retry_after, error_text = await self._send_collected_link(
                    target=target,
                    link_type=link_type,
                    link=link,
                    source_dialog=source_dialog,
                    account_name=account_name,
                )
                if ok:
                    self.db.mark_outbox_link_sent(outbox_id)
                    logger.info(f"تم إرسال وحفظ الرابط #{outbox_id}: {link}")
                    self._queued_outbox_ids.discard(outbox_id)
                else:
                    self.db.mark_outbox_link_failed(outbox_id, error_text or 'send failed')
                    await asyncio.sleep(min(max(int(retry_after or 10), 3), 300))
                    if self.link_collection_active:
                        self._queued_outbox_ids.discard(outbox_id)
                        rows = self.db.get_pending_outbox_links(admin_id, limit=5000)
                        for pending in rows:
                            if pending[0] == outbox_id:
                                await self._enqueue_outbox_row(pending)
                                break
            except Exception as e:
                self.db.mark_outbox_link_failed(outbox_id, str(e))
                self._queued_outbox_ids.discard(outbox_id)
                logger.error(f"خطأ داخل عامل الإرسال #{worker_id} للرابط #{outbox_id}: {str(e)}")
                await asyncio.sleep(5)
            finally:
                try:
                    self.link_send_queue.task_done()
                except Exception:
                    pass

    async def _notify_initial_collection_finished(self, admin_id, scanned_accounts, scanned_dialogs, historical_dialogs, new_telegram, new_whatsapp):
        """إرسال إشعار للمشرف بعد اكتمال أول دورة فحص كاملة."""
        notify_chat_id = admin_id or OWNER_ID
        counts = self.db.get_collected_links_count(admin_id)
        total_telegram = counts.get('telegram', 0)
        total_whatsapp = counts.get('whatsapp', 0)
        text = (
            "✅ اكتمل التجميع التاريخي للروابط.\n\n"
            f"👥 الحسابات المفحوصة: {scanned_accounts}\n"
            f"📚 القنوات/المجموعات المفحوصة: {scanned_dialogs}\n"
            f"🧾 القنوات/المجموعات التي تم فحص تاريخها كاملًا: {historical_dialogs}\n\n"
            f"🆕 روابط تليجرام الجديدة في هذه الدورة: {new_telegram}\n"
            f"🆕 روابط واتساب الجديدة في هذه الدورة: {new_whatsapp}\n\n"
            f"📊 إجمالي روابط تليجرام المحفوظة: {total_telegram}\n"
            f"📊 إجمالي روابط واتساب المحفوظة: {total_whatsapp}\n\n"
            "🔄 سيستمر البوت الآن في التقاط الروابط الجديدة التي تنزل مستقبلًا وإرسالها فوراً."
        )
        try:
            await self.notify_bot.send_message(chat_id=notify_chat_id, text=text)
        except Exception as e:
            logger.error(f"فشل إرسال إشعار اكتمال التجميع إلى {notify_chat_id}: {str(e)}")

    async def collect_links_from_accounts(self, admin_id=None):
        # تجميع روابط واتساب وتليجرام من كل رسائل مجموعات وقنوات الحسابات المضافة.
        # لا يتم فتح الروابط ولا التحقق منها ولا الدخول عليها.
        # الإرسال يتم فور العثور على الرابط عبر طابور إرسال مستقل حتى لا يتوقف الفحص.
        scan_interval = int(os.environ.get('LINK_COLLECT_INTERVAL', '900'))
        queue_size = int(os.environ.get('LINK_SEND_QUEUE_SIZE', '50000'))
        sender_workers = int(os.environ.get('LINK_SENDER_WORKERS', '3'))
        sender_workers = max(1, min(sender_workers, 10))

        self.link_send_queue = asyncio.Queue(maxsize=max(queue_size, 1000))
        self._queued_outbox_ids.clear()
        self.link_sender_tasks = [
            asyncio.create_task(self._link_sender_worker(i + 1))
            for i in range(sender_workers)
        ]

        try:
            await self._load_pending_outbox(admin_id)

            while self.link_collection_active:
                try:
                    settings = self.db.get_link_collection_settings(admin_id)
                    if not settings:
                        logger.warning("لا توجد وجهات محفوظة لتجميع الروابط")
                        await self._controlled_sleep(30, 'link_collection_active')
                        continue

                    telegram_target, whatsapp_target, _updated_date = settings
                    accounts = self.db.get_active_publishing_accounts(admin_id)

                    if not accounts:
                        logger.warning("لا توجد حسابات نشطة لتجميع الروابط")
                        await self._controlled_sleep(60, 'link_collection_active')
                        continue

                    cycle_new_telegram = 0
                    cycle_new_whatsapp = 0
                    scanned_accounts = 0
                    scanned_dialogs = 0
                    historical_dialogs = 0

                    for account in accounts:
                        if not self.link_collection_active:
                            break

                        account_id, session_string, name, username = account
                        client = None

                        try:
                            client = TelegramClient(StringSession(session_string), 1, "b")
                            await client.connect()

                            if not await client.is_user_authorized():
                                await client.disconnect()
                                continue

                            scanned_accounts += 1
                            dialogs = await client.get_dialogs()

                            for dialog in dialogs:
                                if not self.link_collection_active:
                                    break

                                # فحص القنوات والمجموعات فقط، وتجاهل الخاص.
                                if not (dialog.is_group or dialog.is_channel):
                                    continue

                                dialog_id = int(dialog.id)
                                scanned_dialogs += 1

                                state = self.db.get_link_collection_dialog_state(admin_id, account_id, dialog_id)
                                if state:
                                    last_message_id, full_scan_done = state
                                    last_message_id = int(last_message_id or 0)
                                    full_scan_done = bool(full_scan_done)
                                else:
                                    last_message_id = 0
                                    full_scan_done = False

                                iter_kwargs = {}
                                if full_scan_done and last_message_id > 0:
                                    iter_kwargs['min_id'] = last_message_id
                                else:
                                    historical_dialogs += 1

                                max_seen_message_id = last_message_id

                                try:
                                    async for message in client.iter_messages(dialog.id, limit=None, **iter_kwargs):
                                        if not self.link_collection_active:
                                            break

                                        message_id = int(getattr(message, 'id', 0) or 0)
                                        if message_id > max_seen_message_id:
                                            max_seen_message_id = message_id

                                        text = getattr(message, 'message', None) or getattr(message, 'text', None) or ''
                                        links = self.extract_group_links(text)

                                        for link in links['telegram']:
                                            if self.db.is_collected_link(admin_id, 'telegram', link):
                                                continue
                                            outbox_id = self.db.enqueue_link_for_sending(
                                                admin_id=admin_id,
                                                link_type='telegram',
                                                link=link,
                                                target=telegram_target,
                                                source_dialog=dialog.name,
                                                source_account_id=account_id,
                                                account_name=name,
                                            )
                                            if outbox_id:
                                                row = (outbox_id, admin_id, 'telegram', link, telegram_target, dialog.name, account_id, name, 0)
                                                await self._enqueue_outbox_row(row)
                                                cycle_new_telegram += 1

                                        for link in links['whatsapp']:
                                            if self.db.is_collected_link(admin_id, 'whatsapp', link):
                                                continue
                                            outbox_id = self.db.enqueue_link_for_sending(
                                                admin_id=admin_id,
                                                link_type='whatsapp',
                                                link=link,
                                                target=whatsapp_target,
                                                source_dialog=dialog.name,
                                                source_account_id=account_id,
                                                account_name=name,
                                            )
                                            if outbox_id:
                                                row = (outbox_id, admin_id, 'whatsapp', link, whatsapp_target, dialog.name, account_id, name, 0)
                                                await self._enqueue_outbox_row(row)
                                                cycle_new_whatsapp += 1

                                    self.db.update_link_collection_dialog_state(
                                        admin_id, account_id, dialog_id, max_seen_message_id, True
                                    )

                                except FloodWaitError as e:
                                    wait_seconds = int(getattr(e, 'seconds', 60) or 60)
                                    logger.warning(f"FloodWait أثناء قراءة الرسائل من {dialog.name}: انتظار {wait_seconds} ثانية")
                                    await self._controlled_sleep(min(wait_seconds, 600), 'link_collection_active')
                                    continue
                                except Exception as e:
                                    logger.error(f"فشل فحص {dialog.name}: {str(e)}")
                                    continue

                            await client.disconnect()

                        except FloodWaitError as e:
                            wait_seconds = int(getattr(e, 'seconds', 60) or 60)
                            logger.warning(f"FloodWait على الحساب {name}: انتظار {wait_seconds} ثانية")
                            if client:
                                try:
                                    await client.disconnect()
                                except Exception:
                                    pass
                            await self._controlled_sleep(min(wait_seconds, 600), 'link_collection_active')
                            continue
                        except Exception as e:
                            logger.error(f"خطأ أثناء تجميع الروابط من الحساب {name}: {str(e)}")
                            if client:
                                try:
                                    await client.disconnect()
                                except Exception:
                                    pass
                            continue

                    if self.link_collection_active and not self.link_collection_initial_cycle_notified:
                        await self._notify_initial_collection_finished(
                            admin_id=admin_id,
                            scanned_accounts=scanned_accounts,
                            scanned_dialogs=scanned_dialogs,
                            historical_dialogs=historical_dialogs,
                            new_telegram=cycle_new_telegram,
                            new_whatsapp=cycle_new_whatsapp,
                        )
                        self.link_collection_initial_cycle_notified = True

                    await self._load_pending_outbox(admin_id)
                    await self._controlled_sleep(scan_interval, 'link_collection_active')

                except Exception as e:
                    logger.error(f"خطأ عام في تجميع الروابط: {str(e)}")
                    await self._controlled_sleep(60, 'link_collection_active')
        finally:
            for task in self.link_sender_tasks:
                task.cancel()
            self.link_sender_tasks = []
            self.link_send_queue = None
            self._queued_outbox_ids.clear()

    def start_link_collection(self, admin_id=None):
        """بدء تجميع الروابط."""
        if not self.link_collection_active:
            self.link_collection_active = True
            self.link_collection_admin_id = admin_id
            self.link_collection_initial_cycle_notified = False
            self.link_collection_thread = Thread(
                target=lambda: asyncio.run(self.collect_links_from_accounts(admin_id)),
                daemon=True
            )
            self.link_collection_thread.start()
            return True
        return False

    def stop_link_collection(self):
        """إيقاف تجميع الروابط."""
        if self.link_collection_active:
            self.link_collection_active = False
            if self.link_collection_thread:
                self.link_collection_thread.join(timeout=3)
            self.link_collection_admin_id = None
            return True
        return False

    def reset_link_collection_runtime_memory(self):
        """مسح ذاكرة التجميع المؤقتة من الرام."""
        self.link_collection_seen_messages.clear()
        self._send_target_cache.clear()
        self._send_target_flood_until.clear()
        self.link_collection_initial_cycle_notified = False
        self._queued_outbox_ids.clear()
        self._target_next_send_time.clear()
        return True

    async def test_session(self, session_string):
        """اختبار جلسة تيليجرام"""
        try:
            client = TelegramClient(StringSession(session_string), 1, "b")
            await client.connect()
            
            if await client.is_user_authorized():
                me = await client.get_me()
                await client.disconnect()
                return True, me
            else:
                await client.disconnect()
                return False, None
        except Exception as e:
            logger.error(f"خطأ في اختبار الجلسة: {str(e)}")
            return False, None
    
    async def join_groups(self, admin_id=None):
        """الانضمام إلى المجموعات"""
        groups = self.db.get_groups(admin_id)
        pending_groups = [g for g in groups if g[2] == 'pending']
        
        accounts = self.db.get_active_publishing_accounts(admin_id)
        
        for group in pending_groups:
            group_id, group_link, status, join_date, added_date, group_admin_id = group
            
            for account in accounts:
                account_id, session_string, name, username = account
                
                try:
                    client = TelegramClient(StringSession(session_string), 1, "b")
                    await client.connect()
                    
                    if await client.is_user_authorized():
                        try:
                            if 't.me/+' in group_link:
                                invite_hash = group_link.split('+')[1]
                                await client(ImportChatInviteRequest(invite_hash))
                            else:
                                await client(JoinChannelRequest(group_link))
                            
                            self.db.update_group_status(group_id, 'joined')
                            logger.info(f"انضم الحساب {name} إلى المجموعة {group_link}")
                            
                        except Exception as e:
                            logger.error(f"فشل الانضمام للمجموعة {group_link}: {str(e)}")
                            self.db.update_group_status(group_id, 'failed')
                    
                    await client.disconnect()
                    await asyncio.sleep(120)
                    
                except Exception as e:
                    logger.error(f"خطأ في الحساب {name}: {str(e)}")
                    continue
    
    async def publish_to_groups(self, admin_id=None):
        """النشر في المجموعات"""
        while self.publishing_active:
            try:
                accounts = self.db.get_active_publishing_accounts(admin_id)
                ads = self.db.get_ads(admin_id)
                
                if not accounts or not ads:
                    await self._controlled_sleep(60, 'publishing_active')
                    continue
                
                for account in accounts:
                    if not self.publishing_active:
                        break
                        
                    account_id, session_string, name, username = account
                    
                    try:
                        client = TelegramClient(StringSession(session_string), 1, "b")
                        await client.connect()
                        
                        if await client.is_user_authorized():
                            dialogs = await client.get_dialogs()
                            
                            for dialog in dialogs:
                                if not self.publishing_active:
                                    break
                                    
                                if dialog.is_group or dialog.is_channel:
                                    try:
                                        for ad in ads:
                                            if not self.publishing_active:
                                                break
                                                
                                            ad_id, ad_type, ad_text, media_path, file_type, added_date, ad_admin_id = ad
                                            
                                            try:
                                                if ad_type == 'text':
                                                    await client.send_message(dialog.id, ad_text)
                                                elif ad_type == 'photo' and media_path and os.path.exists(media_path):
                                                    await client.send_file(dialog.id, media_path, caption=ad_text)
                                                elif ad_type == 'video' and media_path and os.path.exists(media_path):
                                                    await client.send_file(dialog.id, media_path, caption=ad_text)
                                                elif ad_type == 'document' and media_path and os.path.exists(media_path):
                                                    await client.send_file(dialog.id, media_path, caption=ad_text)
                                                elif ad_type == 'contact' and media_path and os.path.exists(media_path):
                                                    await client.send_file(dialog.id, media_path, caption=ad_text)
                                                
                                                logger.info(f"تم النشر في {dialog.name} بواسطة {name}")
                                                await asyncio.sleep(1)
                                                
                                            except Exception as e:
                                                logger.error(f"فشل نشر الإعلان {ad_id} في {dialog.name}: {str(e)}")
                                                continue
                                                
                                    except Exception as e:
                                        logger.error(f"فشل النشر في {dialog.name}: {str(e)}")
                                        continue
                        
                        await client.disconnect()
                        
                    except Exception as e:
                        logger.error(f"خطأ في الحساب {name}: {str(e)}")
                        continue
                
                await self._controlled_sleep(300, 'publishing_active')
                
            except Exception as e:
                logger.error(f"خطأ في عملية النشر: {str(e)}")
                await self._controlled_sleep(60, 'publishing_active')
    
    def start_publishing(self, admin_id=None):
        """بدء النشر التلقائي"""
        if not self.publishing_active:
            self.publishing_active = True
            self.publishing_thread = Thread(target=lambda: asyncio.run(self.publish_to_groups(admin_id)), daemon=True)
            self.publishing_thread.start()
            return True
        return False
    
    def stop_publishing(self):
        """إيقاف النشر التلقائي"""
        if self.publishing_active:
            self.publishing_active = False
            if self.publishing_thread:
                self.publishing_thread.join(timeout=3)
            return True
        return False
    
    async def handle_private_messages(self, admin_id=None):
        """معالجة الرسائل الخاصة"""
        while self.private_reply_active:
            try:
                accounts = self.db.get_active_publishing_accounts(admin_id)
                private_replies = self.db.get_private_replies(admin_id)
                
                if not accounts or not private_replies:
                    await self._controlled_sleep(60, 'private_reply_active')
                    continue
                
                for account in accounts:
                    if not self.private_reply_active:
                        break
                        
                    account_id, session_string, name, username = account
                    
                    try:
                        client = TelegramClient(StringSession(session_string), 1, "b")
                        await client.connect()
                        
                        if await client.is_user_authorized():
                            async for message in client.iter_messages(None, limit=10):
                                if not self.private_reply_active:
                                    break
                                    
                                if message.is_private and not message.out:
                                    message_key = (account_id, message.sender_id, message.id)
                                    if not self._remember_message(self.private_replied_messages, message_key):
                                        continue

                                    for reply in private_replies:
                                        reply_id, reply_text, is_active, added_date, reply_admin_id = reply
                                        await client.send_message(message.sender_id, reply_text)
                                        logger.info(f"تم الرد على رسالة خاصة بواسطة {name}")
                                        break
                                    await asyncio.sleep(2)
                        
                        await client.disconnect()
                        
                    except Exception as e:
                        logger.error(f"خطأ في الحساب {name}: {str(e)}")
                        continue
                
                await self._controlled_sleep(30, 'private_reply_active')
                
            except Exception as e:
                logger.error(f"خطأ في معالجة الرسائل الخاصة: {str(e)}")
                await self._controlled_sleep(60, 'private_reply_active')
    
    def start_private_reply(self, admin_id=None):
        """بدء الرد على الرسائل الخاصة"""
        if not self.private_reply_active:
            self.private_reply_active = True
            self.private_reply_thread = Thread(target=lambda: asyncio.run(self.handle_private_messages(admin_id)), daemon=True)
            self.private_reply_thread.start()
            return True
        return False
    
    def stop_private_reply(self):
        """إيقاف الرد على الرسائل الخاصة"""
        if self.private_reply_active:
            self.private_reply_active = False
            if self.private_reply_thread:
                self.private_reply_thread.join(timeout=3)
            return True
        return False
    
    async def handle_group_replies(self, admin_id=None):
        """معالجة الردود في المجموعات"""
        while self.group_reply_active:
            try:
                accounts = self.db.get_active_publishing_accounts(admin_id)
                text_replies = self.db.get_group_text_replies(admin_id)
                photo_replies = self.db.get_group_photo_replies(admin_id)
                
                if not accounts or (not text_replies and not photo_replies):
                    await self._controlled_sleep(60, 'group_reply_active')
                    continue
                
                for account in accounts:
                    if not self.group_reply_active:
                        break
                        
                    account_id, session_string, name, username = account
                    
                    try:
                        client = TelegramClient(StringSession(session_string), 1, "b")
                        await client.connect()
                        
                        if await client.is_user_authorized():
                            dialogs = await client.get_dialogs()
                            
                            for dialog in dialogs:
                                if not self.group_reply_active:
                                    break
                                    
                                if dialog.is_group:
                                    try:
                                        async for message in client.iter_messages(dialog.id, limit=10):
                                            if not self.group_reply_active:
                                                break
                                                
                                            if message.text and not message.out:
                                                message_key = (account_id, dialog.id, message.id)
                                                if message_key in self.group_replied_messages:
                                                    continue

                                                handled = False

                                                # الردود النصية
                                                for reply in text_replies:
                                                    reply_id, trigger, reply_text, is_active, added_date, reply_admin_id = reply
                                                    
                                                    if trigger.lower() in message.text.lower():
                                                        if self._remember_message(self.group_replied_messages, message_key):
                                                            await client.send_message(dialog.id, reply_text, reply_to=message.id)
                                                            logger.info(f"تم الرد على رسالة في {dialog.name} بواسطة {name}")
                                                            await asyncio.sleep(2)
                                                        handled = True
                                                        break

                                                if handled:
                                                    continue
                                                
                                                # الردود مع الصور
                                                for reply in photo_replies:
                                                    reply_id, trigger, reply_text, media_path, is_active, added_date, reply_admin_id = reply
                                                    
                                                    if trigger.lower() in message.text.lower() and os.path.exists(media_path):
                                                        if self._remember_message(self.group_replied_messages, message_key):
                                                            await client.send_file(dialog.id, media_path, caption=reply_text, reply_to=message.id)
                                                            logger.info(f"تم الرد بصورة على رسالة في {dialog.name} بواسطة {name}")
                                                            await asyncio.sleep(2)
                                                        break
                                        
                                    except Exception as e:
                                        logger.error(f"فشل الرد في {dialog.name}: {str(e)}")
                                        continue
                        
                        await client.disconnect()
                        
                    except Exception as e:
                        logger.error(f"خطأ في الحساب {name}: {str(e)}")
                        continue
                
                await self._controlled_sleep(30, 'group_reply_active')
                
            except Exception as e:
                logger.error(f"خطأ في معالجة الردود الجماعية: {str(e)}")
                await self._controlled_sleep(60, 'group_reply_active')
    
    def start_group_reply(self, admin_id=None):
        """بدء الردود في المجموعات"""
        if not self.group_reply_active:
            self.group_reply_active = True
            self.group_reply_thread = Thread(target=lambda: asyncio.run(self.handle_group_replies(admin_id)), daemon=True)
            self.group_reply_thread.start()
            return True
        return False
    
    def stop_group_reply(self):
        """إيقاف الردود في المجموعات"""
        if self.group_reply_active:
            self.group_reply_active = False
            if self.group_reply_thread:
                self.group_reply_thread.join(timeout=3)
            return True
        return False
    
    async def handle_random_replies(self, admin_id=None):
        """معالجة الردود العشوائية في القروبات"""
        while self.random_reply_active:
            try:
                accounts = self.db.get_active_publishing_accounts(admin_id)
                random_replies = self.db.get_group_random_replies(admin_id)
                
                if not accounts or not random_replies:
                    await self._controlled_sleep(60, 'random_reply_active')
                    continue
                
                for account in accounts:
                    if not self.random_reply_active:
                        break
                        
                    account_id, session_string, name, username = account
                    
                    try:
                        client = TelegramClient(StringSession(session_string), 1, "b")
                        await client.connect()
                        
                        if await client.is_user_authorized():
                            dialogs = await client.get_dialogs()
                            
                            for dialog in dialogs:
                                if not self.random_reply_active:
                                    break
                                    
                                if dialog.is_group:
                                    try:
                                        # مراقبة الرسائل الجديدة في المجموعة
                                        async for message in client.iter_messages(dialog.id, limit=20):
                                            if not self.random_reply_active:
                                                break
                                                
                                            # الرد على أي رسالة من الأعضاء (ليست من الحساب نفسه) بنسبة 100%
                                            if message.text and not message.out:
                                                message_key = (account_id, dialog.id, message.id)
                                                if not self._remember_message(self.random_replied_messages, message_key):
                                                    continue

                                                random_reply = random.choice(random_replies)
                                                reply_id, reply_text, is_active, added_date, reply_admin_id = random_reply
                                                
                                                await client.send_message(dialog.id, reply_text, reply_to=message.id)
                                                logger.info(f"تم الرد العشوائي على عضو في {dialog.name} بواسطة {name}")
                                                await asyncio.sleep(5)  # تأخير بين الردود
                                                break
                                        
                                    except Exception as e:
                                        logger.error(f"فشل الرد العشوائي في {dialog.name}: {str(e)}")
                                        continue
                        
                        await client.disconnect()
                        
                    except Exception as e:
                        logger.error(f"خطأ في الحساب {name}: {str(e)}")
                        continue
                
                await self._controlled_sleep(20, 'random_reply_active')  # فحص المجموعات كل 20 ثانية
                
            except Exception as e:
                logger.error(f"خطأ في معالجة الردود العشوائية: {str(e)}")
                await self._controlled_sleep(60, 'random_reply_active')
    
    def start_random_reply(self, admin_id=None):
        """بدء الردود العشوائية في القروبات"""
        if not self.random_reply_active:
            self.random_reply_active = True
            self.random_reply_thread = Thread(target=lambda: asyncio.run(self.handle_random_replies(admin_id)), daemon=True)
            self.random_reply_thread.start()
            return True
        return False
    
    def stop_random_reply(self):
        """إيقاف الردود العشوائية في القروبات"""
        if self.random_reply_active:
            self.random_reply_active = False
            if self.random_reply_thread:
                self.random_reply_thread.join(timeout=3)
            return True
        return False

class BotHandler:
    def __init__(self):
        self.db = BotDatabase()
        self.manager = TelegramBotManager(self.db)
        self.application = None
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء البوت"""
        user = update.effective_user
        user_id = user.id
        
        if not self.db.is_admin(user_id):
            await update.message.reply_text("❌ ليس لديك صلاحية للوصول إلى هذا البوت.")
            return
        
        if context.user_data.get('conversation_active'):
            context.user_data['conversation_active'] = False
        
        # ترتيب جديد للوحة التحكم
        keyboard = [
            [InlineKeyboardButton("👥 إدارة الحسابات", callback_data="manage_accounts")],
            [InlineKeyboardButton("📢 إدارة الإعلانات", callback_data="manage_ads")],
            [InlineKeyboardButton("👥 إدارة المجموعات", callback_data="manage_groups")],
            [InlineKeyboardButton("💬 إدارة الردود", callback_data="manage_replies")],
            [InlineKeyboardButton("🔗 تجميع الروابط", callback_data="manage_link_collector")],
            [InlineKeyboardButton("👨‍💼 إدارة المشرفين", callback_data="manage_admins")],
            [InlineKeyboardButton("⚙️ الإعدادات", callback_data="settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "🎮 **لوحة تحكم البوت المتكامل**\n\n"
            "اختر القسم الذي تريد إدارته:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """إلغاء الأمر الحالي"""
        user_id = update.message.from_user.id
        if not self.db.is_admin(user_id):
            await update.message.reply_text("❌ ليس لديك صلاحية للوصول إلى هذا البوت.")
            return
        
        context.user_data['conversation_active'] = False
        await update.message.reply_text("❌ تم إلغاء الأمر.")
        await self.start(update, context)
        return ConversationHandler.END
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة الأزرار"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        if not self.db.is_admin(user_id):
            await query.edit_message_text("❌ ليس لديك صلاحية للوصول إلى هذا البوت.")
            return
        
        data = query.data
        
        if context.user_data.get('conversation_active'):
            context.user_data['conversation_active'] = False
        
        if data == "manage_accounts":
            await self.manage_accounts(query, context)
        elif data == "manage_ads":
            await self.manage_ads(query, context)
        elif data == "manage_groups":
            await self.manage_groups(query, context)
        elif data == "manage_replies":
            await self.manage_replies(query, context)
        elif data == "manage_link_collector":
            await self.manage_link_collector(query, context)
        elif data == "manage_admins":
            await self.manage_admins(query, context)
        elif data == "settings":
            await self.settings_menu(query, context)
        elif data == "bot_status":
            await self.bot_status(query, context)
        
        # إدارة الحسابات
        elif data == "add_account":
            await self.add_account_start(update, context)
        elif data == "show_accounts":
            await self.show_accounts(query, context)
        elif data.startswith("delete_account_"):
            account_id = int(data.split("_")[2])
            await self.delete_account(query, context, account_id)
        
        # إدارة الإعلانات
        elif data == "add_ad":
            await self.add_ad_start(query, context)
        elif data == "show_ads":
            await self.show_ads(query, context)
        elif data.startswith("delete_ad_"):
            ad_id = int(data.split("_")[2])
            await self.delete_ad(query, context, ad_id)
        elif data.startswith("ad_type_"):
            await self.add_ad_type(update, context)
        
        # إدارة المجموعات
        elif data == "add_group":
            await self.add_group_start(update, context)
        elif data == "show_groups":
            await self.show_groups(query, context)
        elif data == "start_publishing":
            await self.start_publishing(query, context)
        elif data == "stop_publishing":
            await self.stop_publishing(query, context)

        # تجميع الروابط
        elif data == "set_link_targets":
            await self.set_link_targets_start(update, context)
        elif data == "start_link_collection":
            await self.start_link_collection(query, context)
        elif data == "stop_link_collection":
            await self.stop_link_collection(query, context)
        elif data == "confirm_reset_link_collection":
            await self.confirm_reset_link_collection(query, context)
        elif data == "reset_link_collection":
            await self.reset_link_collection(query, context)
        
        # إدارة الردود
        elif data == "private_replies":
            await self.manage_private_replies(query, context)
        elif data == "group_replies":
            await self.manage_group_replies(query, context)
        elif data == "add_private_reply":
            await self.add_private_reply_start(update, context)
        elif data == "add_group_text_reply":
            await self.add_group_text_reply_start(update, context)
        elif data == "add_group_photo_reply":
            await self.add_group_photo_reply_start(update, context)
        elif data == "add_random_reply":
            await self.add_random_reply_start(update, context)
        elif data == "start_private_reply":
            await self.start_private_reply(query, context)
        elif data == "stop_private_reply":
            await self.stop_private_reply(query, context)
        elif data == "start_group_reply":
            await self.start_group_reply(query, context)
        elif data == "stop_group_reply":
            await self.stop_group_reply(query, context)
        elif data == "start_random_reply":
            await self.start_random_reply(query, context)
        elif data == "stop_random_reply":
            await self.stop_random_reply(query, context)
        
        # إدارة المشرفين
        elif data == "add_admin":
            await self.add_admin_start(update, context)
        elif data == "show_admins":
            await self.show_admins(query, context)
        elif data.startswith("delete_admin_"):
            admin_id = int(data.split("_")[2])
            await self.delete_admin(query, context, admin_id)
        
        # الرجوع
        elif data == "back_to_main":
            await self.start_from_query(query, context)
        elif data == "back_to_accounts":
            await self.manage_accounts(query, context)
        elif data == "back_to_ads":
            await self.manage_ads(query, context)
        elif data == "back_to_groups":
            await self.manage_groups(query, context)
        elif data == "back_to_replies":
            await self.manage_replies(query, context)
        elif data == "back_to_link_collector":
            await self.manage_link_collector(query, context)
        elif data == "back_to_admins":
            await self.manage_admins(query, context)
    
    async def start_from_query(self, query, context):
        """بدء البوت من استعلام"""
        if context.user_data.get('conversation_active'):
            context.user_data['conversation_active'] = False
            
        keyboard = [
            [InlineKeyboardButton("👥 إدارة الحسابات", callback_data="manage_accounts")],
            [InlineKeyboardButton("📢 إدارة الإعلانات", callback_data="manage_ads")],
            [InlineKeyboardButton("👥 إدارة المجموعات", callback_data="manage_groups")],
            [InlineKeyboardButton("💬 إدارة الردود", callback_data="manage_replies")],
            [InlineKeyboardButton("🔗 تجميع الروابط", callback_data="manage_link_collector")],
            [InlineKeyboardButton("👨‍💼 إدارة المشرفين", callback_data="manage_admins")],
            [InlineKeyboardButton("⚙️ الإعدادات", callback_data="settings")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🎮 **لوحة تحكم البوت المتكامل**\n\n"
            "اختر القسم الذي تريد إدارته:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    # قسم إدارة الحسابات
    async def manage_accounts(self, query, context):
        """إدارة الحسابات"""
        keyboard = [
            [InlineKeyboardButton("➕ إضافة حساب", callback_data="add_account")],
            [InlineKeyboardButton("👥 عرض الحسابات", callback_data="show_accounts")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "👥 **إدارة الحسابات**\n\n"
            "اختر الإجراء الذي تريد تنفيذه:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_account_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة حساب"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "📱 **إضافة حساب جديد**\n\n"
                "يرجى إرسال كود الجلسة (Session String):\n\n"
                "يمكنك الحصول على كود الجلسة من @SessionStringBot\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "📱 **إضافة حساب جديد**\n\n"
                "يرجى إرسال كود الجلسة (Session String):\n\n"
                "يمكنك الحصول على كود الجلسة من @SessionStringBot\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_ACCOUNT
    
    async def add_account_session(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة كود الجلسة"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        session_string = update.message.text
        admin_id = update.message.from_user.id
        
        success, me = await self.manager.test_session(session_string)
        
        if success:
            phone = me.phone if me.phone else "غير معروف"
            name = f"{me.first_name} {me.last_name}" if me.last_name else me.first_name
            username = f"@{me.username}" if me.username else "لا يوجد"
            
            result, message = self.db.add_account(session_string, phone, name, username, admin_id)
            
            if result:
                await update.message.reply_text(f"✅ {message}\n\n📱 الحساب: {name}\n📞 الهاتف: {phone}\n👤 المستخدم: {username}")
            else:
                await update.message.reply_text(f"❌ {message}")
        else:
            await update.message.reply_text("❌ كود الجلسة غير صالح أو الحساب غير مفعل")
        
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def show_accounts(self, query, context):
        """عرض الحسابات"""
        admin_id = query.from_user.id
        accounts = self.db.get_accounts(admin_id)
        
        if not accounts:
            await query.edit_message_text("❌ لا توجد حسابات مضافة")
            return
        
        text = "👥 **الحسابات المضافة:**\n\n"
        keyboard = []
        
        for account in accounts:
            account_id, session_string, phone, name, username, is_active = account
            status = "🟢 نشط" if is_active else "🔴 غير نشط"
            
            text += f"**#{account_id}** - {name}\n"
            text += f"📱 {phone} | {username}\n"
            text += f"الحالة: {status}\n"
            text += "─" * 20 + "\n"
            
            keyboard.append([InlineKeyboardButton(f"🗑️ حذف #{account_id}", callback_data=f"delete_account_{account_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_to_accounts")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def delete_account(self, query, context, account_id):
        """حذف حساب"""
        admin_id = query.from_user.id
        self.db.delete_account(account_id, admin_id)
        await query.edit_message_text(f"✅ تم حذف الحساب #{account_id}")
        await self.show_accounts(query, context)
    
    # قسم إدارة الإعلانات
    async def manage_ads(self, query, context):
        """إدارة الإعلانات"""
        keyboard = [
            [InlineKeyboardButton("➕ إضافة إعلان", callback_data="add_ad")],
            [InlineKeyboardButton("📋 عرض الإعلانات", callback_data="show_ads")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📢 **إدارة الإعلانات**\n\n"
            "اختر الإجراء الذي تريد تنفيذه:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_ad_start(self, query, context):
        """بدء إضافة إعلان"""
        keyboard = [
            [InlineKeyboardButton("📝 نص فقط", callback_data="ad_type_text")],
            [InlineKeyboardButton("🖼️ صورة مع نص", callback_data="ad_type_photo")],
            [InlineKeyboardButton("🎥 فيديو مع نص", callback_data="ad_type_video")],
            [InlineKeyboardButton("📄 ملف مع نص", callback_data="ad_type_document")],
            [InlineKeyboardButton("📞 جهة اتصال (VCF)", callback_data="ad_type_contact")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_ads")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "📢 **إضافة إعلان جديد**\n\n"
            "اختر نوع الإعلان:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_ad_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة نوع الإعلان"""
        query = update.callback_query
        await query.answer()
        
        context.user_data['conversation_active'] = True
        ad_type = query.data.replace("ad_type_", "")
        context.user_data['ad_type'] = ad_type
        
        await query.edit_message_text(
            "📝 **إضافة نص الإعلان**\n\n"
            "يرجى إرسال نص الإعلان:\n\n"
            "أو أرسل /cancel للإلغاء",
            parse_mode='Markdown'
        )
        return ADD_AD_TEXT
    
    async def add_ad_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة نص الإعلان"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        context.user_data['ad_text'] = update.message.text
        ad_type = context.user_data['ad_type']
        admin_id = update.message.from_user.id
        
        if ad_type == 'text':
            self.db.add_ad('text', update.message.text, admin_id=admin_id)
            await update.message.reply_text("✅ تم إضافة الإعلان النصي بنجاح")
            context.user_data['conversation_active'] = False
            await self.start(update, context)
            return ConversationHandler.END
        else:
            file_type_text = {
                'photo': 'صورة',
                'video': 'فيديو', 
                'document': 'ملف',
                'contact': 'ملف جهة اتصال (VCF)'
            }
            await update.message.reply_text(
                f"📎 **إضافة {file_type_text.get(ad_type, 'ملف')}**\n\n"
                f"يرجى إرسال {file_type_text.get(ad_type, 'الملف')}:\n\n"
                f"أو أرسل /cancel للإلغاء"
            )
            return ADD_AD_MEDIA
    
    async def add_ad_media(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة ملف الإعلان"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        ad_type = context.user_data['ad_type']
        ad_text = context.user_data['ad_text']
        admin_id = update.message.from_user.id
        
        file_id = None
        file_type = None
        
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            file_type = 'photo'
        elif update.message.video:
            file_id = update.message.video.file_id
            file_type = 'video'
        elif update.message.document:
            file_id = update.message.document.file_id
            file_type = 'document'
        
        if file_id:
            file = await context.bot.get_file(file_id)
            file_path = os.path.join(ADS_DIR, f"{file_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            await file.download_to_drive(file_path)
            
            self.db.add_ad(ad_type, ad_text, file_path, file_type, admin_id)
            await update.message.reply_text(f"✅ تم إضافة الإعلان بنجاح")
        else:
            await update.message.reply_text("❌ لم يتم التعرف على الملف")
        
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def show_ads(self, query, context):
        """عرض الإعلانات"""
        admin_id = query.from_user.id
        ads = self.db.get_ads(admin_id)
        
        if not ads:
            await query.edit_message_text("❌ لا توجد إعلانات مضافة")
            return
        
        text = "📢 **الإعلانات المضافة:**\n\n"
        keyboard = []
        
        for ad in ads:
            ad_id, ad_type, ad_text, media_path, file_type, added_date, ad_admin_id = ad
            type_emoji = {"text": "📝", "photo": "🖼️", "video": "🎥", "document": "📄", "contact": "📞"}

            text += f"**#{ad_id}** - {type_emoji.get(ad_type, '📄')} {ad_type}\n"
            text += f"📋 {ad_text[:50]}...\n"
            text += "─" * 20 + "\n"
            
            keyboard.append([InlineKeyboardButton(f"🗑️ حذف #{ad_id}", callback_data=f"delete_ad_{ad_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_to_ads")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def delete_ad(self, query, context, ad_id):
        """حذف إعلان"""
        admin_id = query.from_user.id
        self.db.delete_ad(ad_id, admin_id)
        await query.edit_message_text(f"✅ تم حذف الإعلان #{ad_id}")
        await self.show_ads(query, context)
    
    # قسم إدارة المجموعات
    async def manage_groups(self, query, context):
        """إدارة المجموعات"""
        keyboard = [
            [InlineKeyboardButton("➕ إضافة مجموعة", callback_data="add_group")],
            [InlineKeyboardButton("📊 عرض المجموعات", callback_data="show_groups")],
            [InlineKeyboardButton("🚀 بدء النشر التلقائي", callback_data="start_publishing")],
            [InlineKeyboardButton("⏹️ إيقاف النشر", callback_data="stop_publishing")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "👥 **إدارة المجموعات**\n\n"
            "اختر الإجراء الذي تريد تنفيذه:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_group_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة مجموعة"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "👥 **إضافة مجموعة جديدة**\n\n"
                "يرجى إرسال رابط المجموعة:\n\n"
                "يمكنك إرسال رابط واحد أو عدة روابط مفصولة بمسافات\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "👥 **إضافة مجموعة جديدة**\n\n"
                "يرجى إرسال رابط المجموعة:\n\n"
                "يمكنك إرسال رابط واحد أو عدة روابط مفصولة بمسافات\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_GROUP
    
    async def add_group_link(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة رابط المجموعة"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        group_links = update.message.text.split()
        admin_id = update.message.from_user.id
        
        added_count = 0
        for link in group_links:
            if link.startswith('https://t.me/') or link.startswith('t.me/'):
                self.db.add_group(link, admin_id)
                added_count += 1
        
        if added_count > 0:
            asyncio.create_task(self.manager.join_groups(admin_id))
            await update.message.reply_text(f"✅ تم إضافة {added_count} مجموعة وبدأ عملية الانضمام")
        else:
            await update.message.reply_text("❌ لم يتم إضافة أي مجموعة، تأكد من صحة الروابط")
        
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def show_groups(self, query, context):
        """عرض المجموعات"""
        admin_id = query.from_user.id
        groups = self.db.get_groups(admin_id)
        
        if not groups:
            await query.edit_message_text("❌ لا توجد مجموعات مضافة")
            return
        
        text = "👥 **المجموعات المضافة:**\n\n"
        
        for group in groups:
            group_id, link, status, join_date, added_date, group_admin_id = group
            status_emoji = {"pending": "⏳", "joined": "✅", "failed": "❌"}
            
            text += f"**#{group_id}** - {link}\n"
            text += f"الحالة: {status_emoji.get(status, '❓')} {status}\n"
            
            if join_date:
                text += f"تاريخ الانضمام: {join_date}\n"
            
            text += "─" * 20 + "\n"
        
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_to_groups")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def start_publishing(self, query, context):
        """بدء النشر التلقائي"""
        admin_id = query.from_user.id
        if self.manager.start_publishing(admin_id):
            await query.edit_message_text("🚀 تم بدء النشر التلقائي في جميع الحسابات والمجموعات")
        else:
            await query.edit_message_text("⚠️ النشر التلقائي يعمل بالفعل")
    
    async def stop_publishing(self, query, context):
        """إيقاف النشر التلقائي"""
        if self.manager.stop_publishing():
            await query.edit_message_text("⏹️ تم إيقاف النشر التلقائي")
        else:
            await query.edit_message_text("⚠️ النشر التلقائي غير نشط")
    
    # قسم تجميع الروابط
    async def manage_link_collector(self, query, context):
        """إدارة تجميع روابط واتساب وتليجرام."""
        admin_id = query.from_user.id
        settings = self.db.get_link_collection_settings(admin_id)
        counts = self.db.get_collected_links_count(admin_id)
        telegram_count = counts.get('telegram', 0)
        whatsapp_count = counts.get('whatsapp', 0)
        status = "🟢 يعمل" if self.manager.link_collection_active else "🔴 متوقف"

        if settings:
            telegram_target, whatsapp_target, updated_date = settings
            targets_text = (
                f"📨 وجهة روابط تليجرام: `{telegram_target}`\n"
                f"📨 وجهة روابط واتساب: `{whatsapp_target}`\n"
                f"🕒 آخر تحديث: `{updated_date}`"
            )
        else:
            targets_text = "⚠️ لم يتم تحديد وجهات الإرسال بعد."

        text = (
            "🔗 **تجميع الروابط**\n\n"
            "هذه الخاصية تفحص رسائل المجموعات والقنوات الموجودة في الحسابات المضافة، "
            "وتستخرج روابط واتساب وروابط تليجرام بدون فتحها أو التحقق منها، مع تجاهل روابط تليجرام التي تنتهي بـ bot.\n\n"
            f"الحالة: {status}\n"
            f"روابط تليجرام المجمعة: `{telegram_count}`\n"
            f"روابط واتساب المجمعة: `{whatsapp_count}`\n\n"
            f"{targets_text}\n\n"
            "ملاحظة: الحسابات المضافة يجب أن تكون قادرة على الإرسال في وجهات التجميع."
        )

        keyboard = [
            [InlineKeyboardButton("🎯 تحديد وجهات التجميع", callback_data="set_link_targets")],
            [InlineKeyboardButton("🚀 بدء تجميع الروابط", callback_data="start_link_collection")],
            [InlineKeyboardButton("⏹️ إيقاف تجميع الروابط", callback_data="stop_link_collection")],
            [InlineKeyboardButton("♻️ إعادة ضبط ذاكرة التجميع", callback_data="confirm_reset_link_collection")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')

    async def set_link_targets_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء تحديد وجهات إرسال الروابط المجمعة."""
        context.user_data['conversation_active'] = True
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "🎯 **تحديد وجهات تجميع الروابط**\n\n"
                "أرسل وجهتين في رسالتين منفصلتين أو في رسالة واحدة من سطرين:\n\n"
                "السطر الأول: رابط/معرف القناة أو المجموعة التي تستقبل روابط تليجرام\n"
                "السطر الثاني: رابط/معرف القناة أو المجموعة التي تستقبل روابط واتساب\n\n"
                "مثال:\n"
                "`@telegram_links_channel`\n"
                "`@whatsapp_links_channel`\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "🎯 **تحديد وجهات تجميع الروابط**\n\n"
                "أرسل السطر الأول لوجهة روابط تليجرام، والسطر الثاني لوجهة روابط واتساب.\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return SET_LINK_TARGETS

    def _clean_target_line(self, line):
        """تنظيف بسيط لسطر الوجهة."""
        line = line.strip()
        prefixes = [
            'telegram:', 'tg:', 'تليجرام:', 'روابط تليجرام:',
            'whatsapp:', 'wa:', 'واتساب:', 'روابط واتساب:'
        ]
        lowered = line.lower()
        for prefix in prefixes:
            if lowered.startswith(prefix.lower()):
                return line[len(prefix):].strip()
        return line

    async def set_link_targets_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """حفظ وجهات تجميع الروابط."""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END

        text = update.message.text.strip()
        lines = [self._clean_target_line(line) for line in text.replace(',', '\n').splitlines() if line.strip()]

        if len(lines) == 1:
            # دعم الإدخال على مرحلتين لمن يرسل كل وجهة في رسالة منفصلة.
            if 'telegram_links_target' not in context.user_data:
                context.user_data['telegram_links_target'] = lines[0]
                await update.message.reply_text(
                    "✅ تم حفظ وجهة روابط تليجرام مؤقتاً.\n\n"
                    "الآن أرسل وجهة روابط واتساب:",
                    parse_mode='Markdown'
                )
                return SET_LINK_TARGETS
            lines = [context.user_data.pop('telegram_links_target'), lines[0]]

        if len(lines) < 2:
            await update.message.reply_text(
                "❌ أرسل وجهتين: الأولى لروابط تليجرام والثانية لروابط واتساب.\n"
                "يمكنك إرسال كل وجهة في سطر مستقل."
            )
            return SET_LINK_TARGETS

        telegram_target = lines[0].strip()
        whatsapp_target = lines[1].strip()

        if not telegram_target or not whatsapp_target:
            await update.message.reply_text("❌ لا يمكن ترك أي وجهة فارغة.")
            return SET_LINK_TARGETS

        admin_id = update.message.from_user.id
        self.db.set_link_collection_settings(admin_id, telegram_target, whatsapp_target)
        context.user_data['conversation_active'] = False
        context.user_data.pop('telegram_links_target', None)

        await update.message.reply_text(
            "✅ تم حفظ وجهات التجميع بنجاح\n\n"
            f"روابط تليجرام ➜ `{telegram_target}`\n"
            f"روابط واتساب ➜ `{whatsapp_target}`\n\n"
            "يمكنك الآن الضغط على زر بدء تجميع الروابط.",
            parse_mode='Markdown'
        )
        await self.start(update, context)
        return ConversationHandler.END

    async def start_link_collection(self, query, context):
        """بدء تجميع الروابط من الحسابات المضافة."""
        admin_id = query.from_user.id
        settings = self.db.get_link_collection_settings(admin_id)
        if not settings:
            await query.edit_message_text(
                "❌ يجب تحديد وجهات التجميع أولاً من زر: 🎯 تحديد وجهات التجميع."
            )
            return

        if self.manager.start_link_collection(admin_id):
            await query.edit_message_text(
                "🚀 تم بدء تجميع الروابط.\n\n"
                "سيتم فحص كل الرسائل المتاحة في المجموعات والقنوات أول مرة، ثم يستمر البوت في التقاط الروابط الجديدة فقط.\n\nلن يتم فتح الروابط أو التحقق منها، وسيتم تجاهل روابط تليجرام التي تنتهي بـ bot."
            )
        else:
            await query.edit_message_text("⚠️ تجميع الروابط يعمل بالفعل.")

    async def stop_link_collection(self, query, context):
        """إيقاف تجميع الروابط."""
        if self.manager.stop_link_collection():
            await query.edit_message_text("⏹️ تم إيقاف تجميع الروابط.")
        else:
            await query.edit_message_text("⚠️ تجميع الروابط غير نشط حالياً.")

    async def confirm_reset_link_collection(self, query, context):
        """تأكيد إعادة ضبط ذاكرة تجميع الروابط."""
        keyboard = [
            [InlineKeyboardButton("✅ نعم، امسح ذاكرة التجميع", callback_data="reset_link_collection")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="back_to_link_collector")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            "⚠️ **تأكيد إعادة الضبط**\n\n"
            "سيتم حذف كل الروابط التي تم تجميعها سابقاً، "
            "وسيبدأ البوت بتجميع الروابط من جديد وكأنها أول مرة.\n\n"
            "لن يتم حذف الحسابات أو وجهات التجميع.",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    async def reset_link_collection(self, query, context):
        """تنفيذ إعادة ضبط ذاكرة تجميع الروابط."""
        admin_id = query.from_user.id
        was_running = self.manager.link_collection_active

        if was_running:
            self.manager.stop_link_collection()

        self.db.reset_link_collection_memory(admin_id)
        self.manager.reset_link_collection_runtime_memory()

        if was_running:
            self.manager.start_link_collection(admin_id)

        await query.edit_message_text(
            "✅ تم إعادة ضبط ذاكرة التجميع بنجاح.\n\n"
            "تم حذف الروابط القديمة والذاكرة المؤقتة.\n"
            "الآن سيبدأ البوت بتجميع الروابط من جديد بدون اعتبار الروابط السابقة مكررة."
        )

    # قسم إدارة الردود
    async def manage_replies(self, query, context):
        """إدارة الردود"""
        keyboard = [
            [InlineKeyboardButton("💬 الردود في الخاص", callback_data="private_replies")],
            [InlineKeyboardButton("👥 الردود في القروبات", callback_data="group_replies")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "💬 **إدارة الردود**\n\n"
            "اختر نوع الردود التي تريد إدارتها:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def manage_private_replies(self, query, context):
        """إدارة الردود الخاصة"""
        admin_id = query.from_user.id
        replies = self.db.get_private_replies(admin_id)
        
        text = "💬 **الردود في الخاص:**\n\n"
        keyboard = []
        
        if replies:
            for reply in replies:
                reply_id, reply_text, is_active, added_date, reply_admin_id = reply
                status = "🟢 نشط" if is_active else "🔴 غير نشط"
                
                text += f"**#{reply_id}**\n"
                text += f"📝 {reply_text[:50]}...\n"
                text += f"الحالة: {status}\n"
                text += "─" * 20 + "\n"
        else:
            text += "❌ لا توجد ردود مضافة\n"
        
        keyboard.append([InlineKeyboardButton("➕ إضافة رد", callback_data="add_private_reply")])
        keyboard.append([InlineKeyboardButton("🚀 بدء الرد التلقائي", callback_data="start_private_reply")])
        keyboard.append([InlineKeyboardButton("⏹️ إيقاف الرد التلقائي", callback_data="stop_private_reply")])
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_to_replies")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def add_private_reply_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة رد خاص"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "💬 **إضافة رد في الخاص**\n\n"
                "يرجى إرسال نص الرد الذي سيتم إرساله للمستخدمين في الخاص:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "💬 **إضافة رد في الخاص**\n\n"
                "يرجى إرسال نص الرد الذي سيتم إرساله للمستخدمين في الخاص:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_PRIVATE_TEXT
    
    async def add_private_reply_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة نص الرد الخاص"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        reply_text = update.message.text
        admin_id = update.message.from_user.id
        
        self.db.add_private_reply(reply_text, admin_id=admin_id)
        await update.message.reply_text("✅ تم إضافة الرد في الخاص بنجاح")
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def start_private_reply(self, query, context):
        """بدء الرد التلقائي في الخاص"""
        admin_id = query.from_user.id
        if self.manager.start_private_reply(admin_id):
            await query.edit_message_text("🚀 تم بدء الرد التلقائي على الرسائل الخاصة")
        else:
            await query.edit_message_text("⚠️ الرد التلقائي على الرسائل الخاصة يعمل بالفعل")
    
    async def stop_private_reply(self, query, context):
        """إيقاف الرد التلقائي في الخاص"""
        if self.manager.stop_private_reply():
            await query.edit_message_text("⏹️ تم إيقاف الرد التلقائي على الرسائل الخاصة")
        else:
            await query.edit_message_text("⚠️ الرد التلقائي على الرسائل الخاصة غير نشط")
    
    async def manage_group_replies(self, query, context):
        """إدارة الردود في القروبات"""
        admin_id = query.from_user.id
        text_replies = self.db.get_group_text_replies(admin_id)
        photo_replies = self.db.get_group_photo_replies(admin_id)
        random_replies = self.db.get_group_random_replies(admin_id)
        
        text = "👥 **الردود في القروبات:**\n\n"
        
        text += "**الردود على رسائل محددة:**\n"
        if text_replies or photo_replies:
            if text_replies:
                for reply in text_replies:
                    reply_id, trigger, reply_text, is_active, added_date, reply_admin_id = reply
                    status = "🟢 نشط" if is_active else "🔴 غير نشط"
                    
                    text += f"**#{reply_id}** - {trigger}\n"
                    text += f"➡️ {reply_text[:30]}...\n"
                    text += f"الحالة: {status}\n"
                    text += "─" * 20 + "\n"
            
            if photo_replies:
                for reply in photo_replies:
                    reply_id, trigger, reply_text, media_path, is_active, added_date, reply_admin_id = reply
                    status = "🟢 نشط" if is_active else "🔴 غير نشط"
                    
                    text += f"**#{reply_id}** - {trigger}\n"
                    text += f"➡️ {reply_text[:30]}...\n"
                    text += f"الحالة: {status}\n"
                    text += "─" * 20 + "\n"
        else:
            text += "❌ لا توجد ردود مضافة\n"
        
        text += "\n**الردود العشوائية (100%):**\n"
        if random_replies:
            for reply in random_replies:
                reply_id, reply_text, is_active, added_date, reply_admin_id = reply
                status = "🟢 نشط" if is_active else "🔴 غير نشط"
                
                text += f"**#{reply_id}** - {reply_text[:50]}...\n"
                text += f"الحالة: {status}\n"
                text += "─" * 20 + "\n"
        else:
            text += "❌ لا توجد ردود عشوائية مضافة\n"
        
        keyboard = [
            [InlineKeyboardButton("➕ إضافة رد محدد", callback_data="add_group_text_reply")],
            [InlineKeyboardButton("➕ إضافة رد مع صورة", callback_data="add_group_photo_reply")],
            [InlineKeyboardButton("➕ إضافة رد عشوائي", callback_data="add_random_reply")],
            [InlineKeyboardButton("🚀 بدء الردود المحددة", callback_data="start_group_reply")],
            [InlineKeyboardButton("⏹️ إيقاف الردود المحددة", callback_data="stop_group_reply")],
            [InlineKeyboardButton("🚀 بدء الردود العشوائية", callback_data="start_random_reply")],
            [InlineKeyboardButton("⏹️ إيقاف الردود العشوائية", callback_data="stop_random_reply")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_replies")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def add_group_text_reply_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة رد نصي في القروبات"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "👥 **إضافة رد نصي في القروبات**\n\n"
                "يرجى إرسال النص الذي سيتم الرد عليه:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "👥 **إضافة رد نصي في القروبات**\n\n"
                "يرجى إرسال النص الذي سيتم الرد عليه:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_GROUP_TEXT
    
    async def add_group_text_reply_trigger(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة خطوات إضافة رد نصي في القروبات."""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END

        if 'group_text_trigger' not in context.user_data:
            context.user_data['group_text_trigger'] = update.message.text
            await update.message.reply_text(
                "👥 **إضافة رد نصي في القروبات**\n\n"
                "يرجى إرسال نص الرد:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
            return ADD_GROUP_TEXT

        trigger = context.user_data.pop('group_text_trigger')
        reply_text = update.message.text
        admin_id = update.message.from_user.id

        self.db.add_group_text_reply(trigger, reply_text, admin_id=admin_id)
        await update.message.reply_text("✅ تم إضافة الرد النصي في القروبات بنجاح")
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def add_group_text_reply_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """توافق قديم: يمرر للمعالج المرحلي الجديد."""
        return await self.add_group_text_reply_trigger(update, context)
    
    async def add_group_photo_reply_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة رد مع صورة في القروبات"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "👥 **إضافة رد مع صورة في القروبات**\n\n"
                "يرجى إرسال النص الذي سيتم الرد عليه:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "👥 **إضافة رد مع صورة في القروبات**\n\n"
                "يرجى إرسال النص الذي سيتم الرد عليه:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_GROUP_PHOTO
    
    async def add_group_photo_reply_trigger(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة خطوات إضافة رد مع صورة في القروبات."""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END

        if 'group_photo_trigger' not in context.user_data:
            context.user_data['group_photo_trigger'] = update.message.text
            await update.message.reply_text(
                "👥 **إضافة رد مع صورة في القروبات**\n\n"
                "يرجى إرسال نص الرد:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
            return ADD_GROUP_PHOTO

        if 'group_photo_text' not in context.user_data:
            context.user_data['group_photo_text'] = update.message.text
            await update.message.reply_text(
                "👥 **إضافة رد مع صورة في القروبات**\n\n"
                "يرجى إرسال الصورة:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
            return ADD_GROUP_PHOTO

        await update.message.reply_text("❌ يرجى إرسال صورة صالحة أو /cancel للإلغاء")
        return ADD_GROUP_PHOTO
    
    async def add_group_photo_reply_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """توافق قديم: يمرر للمعالج المرحلي الجديد."""
        return await self.add_group_photo_reply_trigger(update, context)
    
    async def add_group_photo_reply_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة صورة الرد"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        if update.message.photo:
            if 'group_photo_trigger' not in context.user_data or 'group_photo_text' not in context.user_data:
                await update.message.reply_text("❌ بيانات الرد غير مكتملة. ابدأ العملية من جديد.")
                context.user_data['conversation_active'] = False
                await self.start(update, context)
                return ConversationHandler.END

            trigger = context.user_data.pop('group_photo_trigger')
            reply_text = context.user_data.pop('group_photo_text')
            admin_id = update.message.from_user.id
            
            file_id = update.message.photo[-1].file_id
            file = await context.bot.get_file(file_id)
            file_path = os.path.join(GROUP_REPLIES_DIR, f"{file_id}.jpg")
            await file.download_to_drive(file_path)
            
            self.db.add_group_photo_reply(trigger, reply_text, file_path, admin_id=admin_id)
            await update.message.reply_text("✅ تم إضافة الرد مع الصورة في القروبات بنجاح")
        else:
            await update.message.reply_text("❌ يرجى إرسال صورة صالحة")
            return ADD_GROUP_PHOTO
        
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def add_random_reply_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة رد عشوائي"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "🎲 **إضافة رد عشوائي في القروبات**\n\n"
                "يرجى إرسال نص الرد العشوائي:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "🎲 **إضافة رد عشوائي في القروبات**\n\n"
                "يرجى إرسال نص الرد العشوائي:\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_RANDOM_REPLY
    
    async def add_random_reply_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة نص الرد العشوائي"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        reply_text = update.message.text
        admin_id = update.message.from_user.id
        
        self.db.add_group_random_reply(reply_text, admin_id=admin_id)
        await update.message.reply_text("✅ تم إضافة الرد العشوائي بنجاح")
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def start_group_reply(self, query, context):
        """بدء الرد التلقائي في القروبات"""
        admin_id = query.from_user.id
        if self.manager.start_group_reply(admin_id):
            await query.edit_message_text("🚀 تم بدء الرد التلقائي على الرسائل المحددة في القروبات")
        else:
            await query.edit_message_text("⚠️ الرد التلقائي على الرسائل المحددة في القروبات يعمل بالفعل")
    
    async def stop_group_reply(self, query, context):
        """إيقاف الرد التلقائي في القروبات"""
        if self.manager.stop_group_reply():
            await query.edit_message_text("⏹️ تم إيقاف الرد التلقائي على الرسائل المحددة في القروبات")
        else:
            await query.edit_message_text("⚠️ الرد التلقائي على الرسائل المحددة في القروبات غير نشط")
    
    async def start_random_reply(self, query, context):
        """بدء الردود العشوائية في القروبات"""
        admin_id = query.from_user.id
        if self.manager.start_random_reply(admin_id):
            await query.edit_message_text("🚀 تم بدء الردود العشوائية في القروبات (الرد على 100% من الرسائل)")
        else:
            await query.edit_message_text("⚠️ الردود العشوائية في القروبات تعمل بالفعل")
    
    async def stop_random_reply(self, query, context):
        """إيقاف الردود العشوائية في القروبات"""
        if self.manager.stop_random_reply():
            await query.edit_message_text("⏹️ تم إيقاف الردود العشوائية في القروبات")
        else:
            await query.edit_message_text("⚠️ الردود العشوائية في القروبات غير نشطة")
    
    # قسم إدارة المشرفين
    async def manage_admins(self, query, context):
        """إدارة المشرفين"""
        keyboard = [
            [InlineKeyboardButton("➕ إضافة مشرف", callback_data="add_admin")],
            [InlineKeyboardButton("👨‍💼 عرض المشرفين", callback_data="show_admins")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "👨‍💼 **إدارة المشرفين**\n\n"
            "اختر الإجراء الذي تريد تنفيذه:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    
    async def add_admin_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """بدء إضافة مشرف"""
        context.user_data['conversation_active'] = True
        
        if update.callback_query:
            query = update.callback_query
            await query.edit_message_text(
                "👨‍💼 **إضافة مشرف جديد**\n\n"
                "يرجى إرسال معرف المستخدم (User ID) للمشرف الجديد:\n\n"
                "يمكنك الحصول على الـ User ID من @userinfobot\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "👨‍💼 **إضافة مشرف جديد**\n\n"
                "يرجى إرسال معرف المستخدم (User ID) للمشرف الجديد:\n\n"
                "يمكنك الحصول على الـ User ID من @userinfobot\n\n"
                "أو أرسل /cancel للإلغاء",
                parse_mode='Markdown'
            )
        return ADD_ADMIN
    
    async def add_admin_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """معالجة معرف المشرف"""
        if not context.user_data.get('conversation_active'):
            await update.message.reply_text("❌ تم إلغاء العملية. استخدم /start للبدء من جديد.")
            return ConversationHandler.END
            
        try:
            user_id = int(update.message.text)
            
            username = "يتم إضافته"
            full_name = "مشرف جديد"
            
            result, message = self.db.add_admin(user_id, username, full_name, False)
            await update.message.reply_text(f"✅ {message}\n\nتم إضافة المستخدم {user_id} كمشرف")
                
        except ValueError:
            await update.message.reply_text("❌ معرف المستخدم يجب أن يكون رقماً")
        
        context.user_data['conversation_active'] = False
        await self.start(update, context)
        return ConversationHandler.END
    
    async def show_admins(self, query, context):
        """عرض المشرفين"""
        admins = self.db.get_admins()
        
        if not admins:
            await query.edit_message_text("❌ لا توجد مشرفين مضافة")
            return
        
        text = "👨‍💼 **المشرفين المضافين:**\n\n"
        keyboard = []
        
        for admin in admins:
            admin_id, user_id, username, full_name, added_date, is_super_admin = admin
            role = "🟢 مشرف رئيسي" if is_super_admin else "🔵 مشرف عادي"
            
            text += f"**#{admin_id}** - {full_name}\n"
            text += f"المعرف: {user_id} | {username}\n"
            text += f"الدور: {role}\n"
            text += "─" * 20 + "\n"
            
            keyboard.append([InlineKeyboardButton(f"🗑️ حذف #{admin_id}", callback_data=f"delete_admin_{admin_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_to_admins")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def delete_admin(self, query, context, admin_id):
        """حذف مشرف مع حماية المالك الرئيسي من الحذف."""
        for admin in self.db.get_admins():
            row_id, user_id, username, full_name, added_date, is_super_admin = admin
            if row_id == admin_id and int(user_id) == OWNER_ID:
                await query.edit_message_text("❌ لا يمكن حذف المالك الرئيسي من لوحة التحكم.")
                return

        self.db.delete_admin(admin_id)
        await query.edit_message_text(f"✅ تم حذف المشرف #{admin_id}")
        await self.show_admins(query, context)
    
    # قسم الإعدادات
    async def settings_menu(self, query, context):
        """قائمة الإعدادات"""
        keyboard = [
            [InlineKeyboardButton("📊 حالة البوت", callback_data="bot_status")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_to_main")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "⚙️ **إعدادات البوت**\n\n"
            "اختر الإعداد الذي تريد تعديله:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

    async def bot_status(self, query, context):
        """عرض حالة البوت والعمليات العاملة."""
        admin_id = query.from_user.id
        accounts_count = len(self.db.get_accounts(admin_id))
        ads_count = len(self.db.get_ads(admin_id))
        groups_count = len(self.db.get_groups(admin_id))
        admins_count = len(self.db.get_admins())

        publishing = "🟢 يعمل" if self.manager.publishing_active else "🔴 متوقف"
        private_reply = "🟢 يعمل" if self.manager.private_reply_active else "🔴 متوقف"
        group_reply = "🟢 يعمل" if self.manager.group_reply_active else "🔴 متوقف"
        random_reply = "🟢 يعمل" if self.manager.random_reply_active else "🔴 متوقف"
        link_collection = "🟢 يعمل" if self.manager.link_collection_active else "🔴 متوقف"

        text = (
            "📊 **حالة البوت**\n\n"
            f"👤 آيدي المالك: `{OWNER_ID}`\n"
            f"👥 الحسابات: `{accounts_count}`\n"
            f"📢 الإعلانات: `{ads_count}`\n"
            f"🔗 المجموعات: `{groups_count}`\n"
            f"👨‍💼 المشرفون: `{admins_count}`\n\n"
            f"🚀 النشر التلقائي: {publishing}\n"
            f"💬 ردود الخاص: {private_reply}\n"
            f"👥 ردود القروبات المحددة: {group_reply}\n"
            f"🎲 الردود العشوائية: {random_reply}\n"
            f"🔗 تجميع الروابط: {link_collection}"
        )

        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    
    def setup_handlers(self):
        """إعداد معالجات البوت"""
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("cancel", self.cancel))
        
        # معالجات المحادثة
        add_account_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_account_start, pattern="^add_account$")],
            states={
                ADD_ACCOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_account_session)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(add_account_conv)
        
        add_ad_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_ad_type, pattern="^ad_type_")],
            states={
                ADD_AD_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_ad_text)],
                ADD_AD_MEDIA: [MessageHandler(filters.PHOTO | filters.VIDEO | filters.Document.ALL, self.add_ad_media)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(add_ad_conv)
        
        add_group_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_group_start, pattern="^add_group$")],
            states={
                ADD_GROUP: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_group_link)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(add_group_conv)
        
        add_admin_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_admin_start, pattern="^add_admin$")],
            states={
                ADD_ADMIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_admin_id)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(add_admin_conv)
        
        private_reply_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_private_reply_start, pattern="^add_private_reply$")],
            states={
                ADD_PRIVATE_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_private_reply_text)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(private_reply_conv)
        
        group_text_reply_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_group_text_reply_start, pattern="^add_group_text_reply$")],
            states={
                ADD_GROUP_TEXT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_group_text_reply_trigger)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(group_text_reply_conv)
        
        group_photo_reply_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_group_photo_reply_start, pattern="^add_group_photo_reply$")],
            states={
                ADD_GROUP_PHOTO: [
                    MessageHandler(filters.PHOTO, self.add_group_photo_reply_photo),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_group_photo_reply_trigger)
                ]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(group_photo_reply_conv)
        
        random_reply_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.add_random_reply_start, pattern="^add_random_reply$")],
            states={
                ADD_RANDOM_REPLY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.add_random_reply_text)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(random_reply_conv)

        link_targets_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.set_link_targets_start, pattern="^set_link_targets$")],
            states={
                SET_LINK_TARGETS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.set_link_targets_text)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(link_targets_conv)
        
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))
    
    def run(self):
        """تشغيل البوت"""
        self.application = Application.builder().token(BOT_TOKEN).build()
        self.setup_handlers()
        
        self.db.add_admin(OWNER_ID, "@owner", "المالك الرئيسي", True)
        
        print("🤖 البوت يعمل الآن...")
        print(f"✅ تم إضافة الآيدي {OWNER_ID} كمشرف رئيسي")
        print("🎯 البوت جاهز: تجميع سريع مباشر عبر طابور إرسال بدون فحص أو فتح الروابط")
        
        self.application.run_polling()

if __name__ == "__main__":
    print(f"📁 مجلد بيانات هذا البوت: {DATA_DIR}")
    print(f"🗄️ قاعدة بيانات هذا البوت: {DB_NAME}")
    print(f"🔐 التوكن المستخدم يبدأ بـ: {BOT_TOKEN[:8]}... وينتهي بـ: ...{BOT_TOKEN[-6:]}")

    bot = BotHandler()
    bot.run()
