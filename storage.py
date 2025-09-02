# storage.py
# Хранилище: подписки, KV, КАСТОМНЫЕ индикаторы (пер-чат)
import aiosqlite
import os
import hashlib

DB_PATH = os.getenv("DB_PATH", "jobless.db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS subs(chat_id INTEGER PRIMARY KEY)")
        await conn.execute("CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, val TEXT)")
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_indicators(
            chat_id INTEGER,
            key TEXT,
            title TEXT,
            url TEXT,
            rule TEXT,        -- 'LT','GT','FOMC'
            UNIQUE(chat_id, key),
            UNIQUE(chat_id, title)
        )""")
        await conn.commit()

# ----- подписки -----
async def add_sub(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("INSERT OR IGNORE INTO subs(chat_id) VALUES(?)", (chat_id,))
        await conn.commit()

async def list_subs():
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT chat_id FROM subs")
        return [r[0] for r in await cur.fetchall()]

# ----- KV -----
async def set_state(key: str, val: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("INSERT OR REPLACE INTO kv(key,val) VALUES(?,?)", (key, val))
        await conn.commit()

async def get_state(key: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT val FROM kv WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None

# ----- кастомные индикаторы (пер-чат) -----
def _make_key(chat_id: int, title: str) -> str:
    raw = f"{chat_id}:{title}".encode("utf-8")
    h = hashlib.md5(raw).hexdigest()[:10].upper()
    return f"CUSTOM_{h}"

async def add_custom_indicator(chat_id: int, title: str, url: str, rule: str) -> str:
    """
    rule ∈ {'LT','GT','FOMC'}
    """
    key = _make_key(chat_id, title.strip())
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            INSERT OR REPLACE INTO custom_indicators(chat_id, key, title, url, rule)
            VALUES(?,?,?,?,?)
        """, (chat_id, key, title.strip(), url.strip(), rule.strip().upper()))
        await conn.commit()
    return key

async def delete_custom_indicator_by_title(chat_id: int, title: str) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            DELETE FROM custom_indicators WHERE chat_id=? AND title=?
        """, (chat_id, title.strip()))
        await conn.commit()
        return cur.rowcount

async def list_custom_indicators(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            SELECT key, title, url, rule
            FROM custom_indicators
            WHERE chat_id=?
            ORDER BY title
        """, (chat_id,))
        rows = await cur.fetchall()
        return [
            {"key": r[0], "title": r[1], "url": r[2], "rule": r[3]}
            for r in rows
        ]
