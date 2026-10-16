#!/usr/bin/env python3
"""
Telegram AI Assistant Bot (Gemini)
----------------------------------
Flow:  /start -> Join message (button) -> Welcome message -> user asks anything -> Gemini replies
Admin: send /tradevyx_adarsh_bot to open the admin panel (everything is editable there).

Environment variables (only these two are required):
    BOT_TOKEN       - token from @BotFather
    GEMINI_API_KEY  - key from aistudio.google.com
"""
import asyncio
import base64
import copy
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import defaultdict, deque

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # python-dotenv is optional
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot_data.db")
ADMIN_COMMAND = "tradevyx_adarsh_bot"

API = f"https://api.telegram.org/bot{BOT_TOKEN}"
FILE_API = f"https://api.telegram.org/file/bot{BOT_TOKEN}"
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")
logging.getLogger("httpx").setLevel(logging.WARNING)

client: httpx.AsyncClient = None  # created in main()

# ======================================================================
# Database
# ======================================================================
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_dblock = threading.Lock()


def db(sql, params=(), fetch=None):
    with _dblock:
        cur = _conn.execute(sql, params)
        _conn.commit()
        if fetch == "one":
            r = cur.fetchone()
            return dict(r) if r else None
        if fetch == "all":
            return [dict(r) for r in cur.fetchall()]
        return cur.rowcount


db("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
db(
    "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, first_name TEXT, username TEXT, "
    "joined_at INTEGER, last_seen INTEGER, blocked INTEGER DEFAULT 0)"
)
db(
    "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, "
    "role TEXT, text TEXT, ts INTEGER)"
)
db("CREATE INDEX IF NOT EXISTS idx_hist ON history(user_id, id)")


def kv_get(key, default=None):
    row = db("SELECT value FROM kv WHERE key=?", (key,), "one")
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except Exception:
        return default


def kv_set(key, value):
    db(
        "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value, ensure_ascii=False)),
    )
    schedule_backup()


_last_admin_chat = None
_backup_task = None


def schedule_backup():
    """After any panel edit, send a backup file to the admin chat (once, 30s after the first change)."""
    global _backup_task
    if not _last_admin_chat:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if _backup_task and not _backup_task.done():
        return
    _backup_task = loop.create_task(_backup_later())


async def _backup_later():
    await asyncio.sleep(30)
    await send_backup(_last_admin_chat, silent=True)


def export_data():
    kvs = {r["key"]: json.loads(r["value"]) for r in db("SELECT key, value FROM kv", fetch="all")}
    users = db("SELECT id, first_name, username, joined_at, last_seen, blocked FROM users", fetch="all")
    return {"version": 1, "kv": kvs, "users": users}


async def send_backup(chat_id, silent=False):
    data = json.dumps(export_data(), ensure_ascii=False).encode()
    name = time.strftime("bot_backup_%Y%m%d_%H%M.json")
    try:
        r = await client.post(
            f"{API}/sendDocument",
            data={
                "chat_id": str(chat_id),
                "caption": "💾 Backup. Keep this file safe. Restore it from Admin Panel > Backup & Restore.",
                "disable_notification": "true" if silent else "false",
            },
            files={"document": (name, data, "application/json")},
            timeout=120,
        )
        return r.json()
    except Exception as e:
        log.warning("backup failed: %s", e)
        return {"ok": False}


def import_data(obj):
    if not isinstance(obj, dict) or "kv" not in obj:
        raise ValueError("not a backup file")
    for k, v in obj["kv"].items():
        db(
            "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (k, json.dumps(v, ensure_ascii=False)),
        )
    for u in obj.get("users", []):
        db(
            "INSERT INTO users(id, first_name, username, joined_at, last_seen, blocked) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET first_name=excluded.first_name, username=excluded.username, "
            "joined_at=excluded.joined_at, last_seen=excluded.last_seen, blocked=excluded.blocked",
            (u["id"], u.get("first_name", ""), u.get("username", ""), u.get("joined_at", 0),
             u.get("last_seen", 0), u.get("blocked", 0)),
        )
    return len(obj["kv"]), len(obj.get("users", []))


# ======================================================================
# Editable content ("slots") and AI settings
# ======================================================================
def _btn(text, typ="url", url="", style="", emoji_id="", new_row=True):
    return {"text": text, "type": typ, "url": url, "style": style, "emoji_id": emoji_id, "new_row": new_row}


def _slot(text="", buttons=None):
    return {"text": text, "entities": [], "media_type": None, "media_id": None, "buttons": buttons or []}


DEFAULT_SLOTS = {
    "join": _slot(
        "👋 Welcome!\n\nPlease join our channels below, then tap the button.",
        [_btn("✅ Joined All Channels", "next", style="success")],
    ),
    "welcome": _slot("🎉 Welcome! Ask me anything, I'm ready to help."),
    "ai_header": _slot(),
    "ai_footer": _slot(),
    "err_generic": _slot("⚠️ Something went wrong. Please try again in a moment."),
    "err_rate": _slot("⏳ You're sending messages too fast. Please wait a minute and try again."),
    "err_unsupported": _slot("Please send a text, photo or voice message."),
}
SLOT_TITLES = {
    "join": "📨 Join Message",
    "welcome": "👋 Welcome Message",
    "ai_header": "🤖 AI Reply Header",
    "ai_footer": "🤖 AI Reply Footer",
    "err_generic": "⚠️ Error Message",
    "err_rate": "⏳ Rate-limit Message",
    "err_unsupported": "🚫 Unsupported-input Message",
}
BUTTON_SLOTS = {"join", "welcome", "ai_footer"}
ERR_SLOTS = ["err_generic", "err_rate", "err_unsupported"]

STYLES = ["", "primary", "success", "danger"]
STYLE_LABEL = {"": "Default", "primary": "🔵 Blue", "success": "🟢 Green", "danger": "🔴 Red"}


def get_slot(name):
    base = copy.deepcopy(DEFAULT_SLOTS[name])
    saved = kv_get(f"slot:{name}")
    if saved:
        base.update(saved)
    return base


def save_slot(name, slot):
    kv_set(f"slot:{name}", slot)


DEFAULT_SYSTEM_PROMPT = (
    "You are a friendly, smart AI assistant inside a Telegram bot. You can answer questions on any topic. "
    "Reply in the same language the user writes in (Hindi, English, Hinglish, etc.). "
    "Keep answers clear and reasonably short. "
    "Format using only Telegram-friendly Markdown: **bold**, *italic*, `inline code`, ``` code blocks ``` "
    "and simple '-' bullet lists. Do not use tables, '#' headings or LaTeX."
)
DEFAULT_AI = {
    "system_prompt": DEFAULT_SYSTEM_PROMPT,
    "model": "gemini-2.5-flash",
    "history_len": 10,
    "rate_limit": 6,
    "temperature": 0.8,
}


def ai_settings():
    s = dict(DEFAULT_AI)
    s.update(kv_get("ai", {}) or {})
    return s


# ======================================================================
# Telegram API helpers
# ======================================================================
async def tg(method, **params):
    timeout = params.pop("_timeout", 30)
    params = {k: v for k, v in params.items() if v is not None}
    try:
        r = await client.post(f"{API}/{method}", json=params, timeout=timeout)
        data = r.json()
    except Exception as e:
        log.warning("tg %s failed: %s", method, e)
        return {"ok": False, "description": str(e), "error_code": 0}
    if not data.get("ok"):
        log.warning("tg %s -> %s %s", method, data.get("error_code"), data.get("description"))
    return data


def u16(s):
    """Length in UTF-16 code units (what Telegram uses for entity offsets)."""
    return len(s.encode("utf-16-le")) // 2


def build_markup(buttons):
    rows = []
    for b in buttons or []:
        btn = {"text": b.get("text") or "•"}
        if b.get("style") in ("primary", "success", "danger"):
            btn["style"] = b["style"]
        if b.get("emoji_id"):
            btn["icon_custom_emoji_id"] = b["emoji_id"]
        if b.get("type") == "url":
            if not b.get("url"):
                continue
            btn["url"] = b["url"]
        else:
            btn["callback_data"] = "next"
        if b.get("new_row", True) or not rows:
            rows.append([btn])
        else:
            rows[-1].append(btn)
    return {"inline_keyboard": rows} if rows else None


def strip_extras(markup):
    """Remove color/premium-emoji fields (used as fallback if Telegram rejects them)."""
    if not markup:
        return markup
    m = copy.deepcopy(markup)
    for row in m["inline_keyboard"]:
        for b in row:
            b.pop("style", None)
            b.pop("icon_custom_emoji_id", None)
    return m


_AUTO = object()


async def _send_once(chat_id, text, ents, mt, mid, markup, preview):
    if mt and mid:
        method = {"photo": "sendPhoto", "video": "sendVideo", "animation": "sendAnimation"}[mt]
        if u16(text) <= 1024:
            p = {"chat_id": chat_id, mt: mid}
            if text:
                p["caption"] = text
                if ents:
                    p["caption_entities"] = ents
            if markup:
                p["reply_markup"] = markup
            return await tg(method, **p)
        r = await tg(method, chat_id=chat_id, **{mt: mid})  # caption too long: media first, then text
        if not r.get("ok"):
            return r
    if not text:
        return {"ok": True}
    p = {"chat_id": chat_id, "text": text}
    if ents:
        p["entities"] = ents
    if markup:
        p["reply_markup"] = markup
    if not preview:
        p["link_preview_options"] = {"is_disabled": True}
    return await tg("sendMessage", **p)


async def send_message_cfg(chat_id, cfg, markup=_AUTO, preview=True):
    """Send a stored message (text + entities + media + buttons) exactly as saved."""
    if markup is _AUTO:
        markup = build_markup(cfg.get("buttons", []))
    text, ents = cfg.get("text", ""), cfg.get("entities", [])
    mt, mid = cfg.get("media_type"), cfg.get("media_id")
    plain = strip_extras(markup)
    attempts = [(markup, ents, mt, mid)]
    if plain != markup:
        attempts.append((plain, ents, mt, mid))
    attempts.append((plain, [], mt, mid))
    if mt:
        attempts.append((plain, [], None, None))
    last = {"ok": False}
    for mk, en, t, i in attempts:
        last = await _send_once(chat_id, text, en, t, i, mk, preview)
        if last.get("ok"):
            return last
        if last.get("error_code") in (0, 403, 429):
            break
    return last


# ======================================================================
# Markdown (Gemini output) -> Telegram text + entities
# ======================================================================
_PATS = [
    ("code", re.compile(r"`([^`\n]+)`")),
    ("link", re.compile(r"\[([^\]\n]+)\]\((https?://[^\s)]+)\)")),
    ("bold", re.compile(r"\*\*(.+?)\*\*")),
    ("bold", re.compile(r"__(.+?)__")),
    ("strike", re.compile(r"~~(.+?)~~")),
    ("italic", re.compile(r"(?<![\*\w])\*(?![\s\*])(.+?)(?<![\s\*])\*(?![\*\w])")),
    ("italic", re.compile(r"(?<![_\w])_(?![\s_])(.+?)(?<![\s_])_(?![_\w])")),
]
_ENT_TYPE = {"bold": "bold", "italic": "italic", "strike": "strikethrough", "link": "text_link"}


def parse_inline(s):
    out, ents, pos = "", [], 0
    while pos < len(s):
        best = None
        for kind, rx in _PATS:
            m = rx.search(s, pos)
            if m and (best is None or m.start() < best[1].start()):
                best = (kind, m)
        if best is None:
            out += s[pos:]
            break
        kind, m = best
        out += s[pos : m.start()]
        start = u16(out)
        if kind == "code":
            inner = m.group(1)
            out += inner
            ents.append({"type": "code", "offset": start, "length": u16(inner)})
        else:
            inner, inner_ents = parse_inline(m.group(1))
            out += inner
            for e in inner_ents:
                e = dict(e)
                e["offset"] += start
                ents.append(e)
            if inner:
                ent = {"type": _ENT_TYPE[kind], "offset": start, "length": u16(inner)}
                if kind == "link":
                    ent["url"] = m.group(2)
                ents.append(ent)
        pos = m.end()
    return out, ents


def md_to_entities(md):
    lines = md.replace("\r\n", "\n").strip("\n").split("\n")
    parts, entities = [], []
    cur = 0

    def add(txt, ents):
        nonlocal cur
        for e in ents:
            e = dict(e)
            e["offset"] += cur
            entities.append(e)
        parts.append(txt)
        cur += u16(txt) + 1  # +1 for the joining newline

    def flush_code(buf, lang):
        code = "\n".join(buf)
        if code:
            ent = {"type": "pre", "offset": 0, "length": u16(code)}
            if lang:
                ent["language"] = lang
            add(code, [ent])

    in_code, lang, buf = False, "", []
    for line in lines:
        st = line.strip()
        if st.startswith("```"):
            if not in_code:
                in_code, lang, buf = True, st[3:].strip(), []
            else:
                in_code = False
                flush_code(buf, lang)
            continue
        if in_code:
            buf.append(line)
            continue
        m = re.match(r"^\s{0,3}#{1,6}\s+(.*)$", line)
        if m:
            inner, ents = parse_inline(m.group(1).strip())
            if inner:
                ents.append({"type": "bold", "offset": 0, "length": u16(inner)})
            add(inner, ents)
            continue
        if re.match(r"^\s*([-*_])\1{2,}\s*$", line):
            add("──────────", [])
            continue
        m = re.match(r"^(\s*)[-*+]\s+(.*)$", line)
        if m:
            prefix = "  " * (len(m.group(1)) // 2) + "• "
            inner, ents = parse_inline(m.group(2))
            shift = u16(prefix)
            ents = [dict(e, offset=e["offset"] + shift) for e in ents]
            add(prefix + inner, ents)
            continue
        m = re.match(r"^>\s?(.*)$", line)
        if m:
            inner, ents = parse_inline(m.group(1))
            shift = u16("▎ ")
            ents = [dict(e, offset=e["offset"] + shift) for e in ents]
            add("▎ " + inner, ents)
            continue
        inner, ents = parse_inline(line)
        add(inner, ents)
    if in_code:
        flush_code(buf, lang)
    return "\n".join(parts), entities


def split_markdown(md, limit=3000):
    if len(md) <= limit:
        return [md]
    lines = []
    for line in md.split("\n"):
        if len(line) > limit:
            lines.extend(line[k : k + limit] for k in range(0, len(line), limit))
        else:
            lines.append(line)
    chunks, cur, cur_len = [], [], 0
    in_fence, fence_open = False, ""
    for line in lines:
        ln = len(line) + 1
        flush = False
        if cur and cur_len + ln > limit:
            flush = True
        elif cur and not in_fence and line.strip() == "" and cur_len > limit * 0.6:
            flush = True
        if flush:
            if in_fence:
                cur.append("```")
            chunks.append("\n".join(cur))
            cur = [fence_open] if in_fence else []
            cur_len = sum(len(x) + 1 for x in cur)
        cur.append(line)
        cur_len += ln
        if line.strip().startswith("```"):
            in_fence = not in_fence
            fence_open = line.strip() if in_fence else ""
    if cur:
        chunks.append("\n".join(cur))
    return [c for c in chunks if c.strip()]


def combine(parts):
    """Join several (text, entities) pieces with a blank line between them."""
    text, ents = "", []
    for t, e in parts:
        if not t:
            continue
        if text:
            text += "\n\n"
        base = u16(text)
        for x in e:
            y = dict(x)
            y["offset"] += base
            ents.append(y)
        text += t
    return text, ents


async def send_ai_reply(chat_id, md):
    header, footer = get_slot("ai_header"), get_slot("ai_footer")
    h_sep = bool(header["media_id"])  # header with media = separate message
    f_sep = bool(footer["media_id"])
    if h_sep and (header["text"] or header["media_id"]):
        await send_message_cfg(chat_id, {**header, "buttons": []}, markup=None)
    chunks = split_markdown(md.strip()) or ["…"]
    for i, ch in enumerate(chunks):
        body = md_to_entities(ch)
        pieces = []
        if i == 0 and not h_sep and header["text"]:
            pieces.append((header["text"], header["entities"]))
        pieces.append(body)
        last = i == len(chunks) - 1
        if last and not f_sep and footer["text"]:
            pieces.append((footer["text"], footer["entities"]))
        text, ents = combine(pieces)
        markup = build_markup(footer["buttons"]) if (last and not f_sep) else None
        if u16(text) > 4096:
            text, ents = body
        cfg = {"text": text, "entities": ents, "media_type": None, "media_id": None}
        await send_message_cfg(chat_id, cfg, markup=markup, preview=False)
    if f_sep:
        await send_message_cfg(chat_id, footer, preview=False)


# ======================================================================
# Gemini
# ======================================================================
_rate = defaultdict(deque)
_user_locks = defaultdict(asyncio.Lock)


def rate_ok(uid):
    limit = int(ai_settings()["rate_limit"])
    now = time.time()
    dq = _rate[uid]
    while dq and now - dq[0] > 60:
        dq.popleft()
    if len(dq) >= limit:
        return False
    dq.append(now)
    return True


async def ask_gemini(uid, user_parts):
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY is not set")
        return None
    s = ai_settings()
    rows = []
    if int(s["history_len"]) > 0:
        rows = db(
            "SELECT role, text FROM history WHERE user_id=? ORDER BY id DESC LIMIT ?",
            (uid, int(s["history_len"])),
            "all",
        )
        rows.reverse()
        while rows and rows[0]["role"] != "user":
            rows.pop(0)
    contents = [{"role": r["role"], "parts": [{"text": r["text"]}]} for r in rows]
    contents.append({"role": "user", "parts": user_parts})
    payload = {
        "contents": contents,
        "systemInstruction": {"parts": [{"text": s["system_prompt"]}]},
        "generationConfig": {"temperature": float(s["temperature"])},
    }
    url = GEMINI_URL.format(model=s["model"])
    for attempt in range(3):
        try:
            r = await client.post(
                url, headers={"x-goog-api-key": GEMINI_API_KEY}, json=payload, timeout=90
            )
        except Exception as e:
            log.warning("gemini request failed: %s", e)
            await asyncio.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            log.warning("gemini status %s, retrying", r.status_code)
            await asyncio.sleep(2 * (attempt + 1))
            continue
        if r.status_code != 200:
            log.error("gemini error %s: %s", r.status_code, r.text[:400])
            return None
        data = r.json()
        try:
            parts = data["candidates"][0]["content"]["parts"]
        except Exception:
            log.warning("gemini returned no content: %s", json.dumps(data)[:400])
            return None
        out = "".join(p.get("text", "") for p in parts if not p.get("thought")).strip()
        return out or None
    return None


def save_history(uid, user_text, reply):
    if int(ai_settings()["history_len"]) <= 0:
        return
    now = int(time.time())
    db("INSERT INTO history(user_id, role, text, ts) VALUES(?,?,?,?)", (uid, "user", user_text[:4000], now))
    db("INSERT INTO history(user_id, role, text, ts) VALUES(?,?,?,?)", (uid, "model", reply[:4000], now))
    db(
        "DELETE FROM history WHERE user_id=? AND id NOT IN "
        "(SELECT id FROM history WHERE user_id=? ORDER BY id DESC LIMIT 100)",
        (uid, uid),
    )


async def download_file(file_id, max_bytes=15 * 1024 * 1024):
    r = await tg("getFile", file_id=file_id)
    if not r.get("ok"):
        return None
    f = r["result"]
    if f.get("file_size", 0) > max_bytes:
        return None
    try:
        resp = await client.get(f"{FILE_API}/{f['file_path']}", timeout=60)
        return resp.content if resp.status_code == 200 else None
    except Exception as e:
        log.warning("download failed: %s", e)
        return None


async def keep_typing(chat_id, stop):
    while not stop.is_set():
        await tg("sendChatAction", chat_id=chat_id, action="typing")
        try:
            await asyncio.wait_for(stop.wait(), 4)
        except asyncio.TimeoutError:
            pass


async def handle_ai(msg):
    chat_id, uid = msg["chat"]["id"], msg["from"]["id"]
    text = (msg.get("text") or msg.get("caption") or "").strip()
    if not (text or msg.get("photo") or msg.get("voice") or msg.get("audio")):
        await send_message_cfg(chat_id, get_slot("err_unsupported"))
        return
    if not rate_ok(uid):
        await send_message_cfg(chat_id, get_slot("err_rate"))
        return
    async with _user_locks[uid]:
        stop = asyncio.Event()
        typing = asyncio.create_task(keep_typing(chat_id, stop))
        try:
            parts, hist = [], text
            if msg.get("photo"):
                data = await download_file(msg["photo"][-1]["file_id"])
                if not data:
                    await send_message_cfg(chat_id, get_slot("err_generic"))
                    return
                parts.append({"inline_data": {"mime_type": "image/jpeg", "data": base64.b64encode(data).decode()}})
                hist = f"[photo] {text}".strip()
                if not text:
                    text = "Look at this image and respond helpfully."
            elif msg.get("voice") or msg.get("audio"):
                media = msg.get("voice") or msg.get("audio")
                data = await download_file(media["file_id"])
                if not data:
                    await send_message_cfg(chat_id, get_slot("err_generic"))
                    return
                mime = media.get("mime_type") or "audio/ogg"
                parts.append({"inline_data": {"mime_type": mime, "data": base64.b64encode(data).decode()}})
                hist = "[voice message]"
                if not text:
                    text = "Listen to this audio and respond to what the user says."
            parts.append({"text": text})
            reply = await ask_gemini(uid, parts)
        finally:
            stop.set()
            await typing
        if not reply:
            await send_message_cfg(chat_id, get_slot("err_generic"))
            return
        save_history(uid, hist or text, reply)
        await send_ai_reply(chat_id, reply)


# ======================================================================
# Users
# ======================================================================
def touch_user(u):
    now = int(time.time())
    db(
        "INSERT INTO users(id, first_name, username, joined_at, last_seen, blocked) VALUES(?,?,?,?,?,0) "
        "ON CONFLICT(id) DO UPDATE SET first_name=excluded.first_name, username=excluded.username, "
        "last_seen=excluded.last_seen, blocked=0",
        (u["id"], u.get("first_name", ""), u.get("username", ""), now, now),
    )


async def cmd_start(msg):
    uid, chat_id = msg["from"]["id"], msg["chat"]["id"]
    admin_state.pop(uid, None)
    db("DELETE FROM history WHERE user_id=?", (uid,))  # fresh chat every /start
    join = get_slot("join")
    if join["text"] or join["media_id"]:
        await send_message_cfg(chat_id, join)
    else:
        await send_message_cfg(chat_id, get_slot("welcome"))


_done_next = {}


async def on_next(cb):
    await tg("answerCallbackQuery", callback_query_id=cb["id"])
    msg = cb.get("message")
    if not msg:
        return
    key = (msg["chat"]["id"], msg["message_id"])
    if key in _done_next:
        return
    if len(_done_next) > 5000:
        _done_next.clear()
    _done_next[key] = time.time()
    await tg("deleteMessage", chat_id=key[0], message_id=key[1])
    await send_message_cfg(cb["from"]["id"], get_slot("welcome"))


# ======================================================================
# Admin panel
# ======================================================================
admin_state = {}  # uid -> {"mode": ..., ...}


def allowed(uid):
    lock = kv_get("lock", {"on": False, "admins": []})
    return (not lock.get("on")) or uid in lock.get("admins", [])


def kb(rows):
    return {"inline_keyboard": [[{"text": t, "callback_data": d} for t, d in row] for row in rows]}


async def show(chat_id, text, rows, mid=None):
    markup = kb(rows)
    if mid:
        r = await tg("editMessageText", chat_id=chat_id, message_id=mid, text=text, reply_markup=markup)
        if r.get("ok") or "not modified" in str(r.get("description", "")):
            return
    await tg("sendMessage", chat_id=chat_id, text=text, reply_markup=markup)


def extract_content(msg):
    if "text" in msg:
        return {"text": msg["text"], "entities": msg.get("entities", []), "media_type": None, "media_id": None}
    for mt in ("photo", "animation", "video"):
        if mt in msg:
            fid = msg[mt][-1]["file_id"] if mt == "photo" else msg[mt]["file_id"]
            return {
                "text": msg.get("caption", ""),
                "entities": msg.get("caption_entities", []),
                "media_type": mt,
                "media_id": fid,
            }
    return None


def split_button_text(text, ents):
    """If the text contains a premium emoji, use it as the button icon and strip it from the text."""
    emoji_id = ""
    for e in sorted(ents or [], key=lambda x: x["offset"]):
        if e["type"] == "custom_emoji":
            emoji_id = e["custom_emoji_id"]
            raw = text.encode("utf-16-le")
            a, b = e["offset"] * 2, (e["offset"] + e["length"]) * 2
            text = (raw[:a] + raw[b:]).decode("utf-16-le")
            break
    return text.strip(), emoji_id


def norm_url(u):
    u = u.strip()
    if u.startswith("@"):
        return "https://t.me/" + u[1:]
    if re.match(r"^(t\.me|telegram\.me)/", u):
        return "https://" + u
    if re.match(r"^(https?|tg)://", u):
        return u
    if re.match(r"^[\w.-]+\.[A-Za-z]{2,}(/.*)?$", u):
        return "https://" + u
    return None


async def main_panel(chat_id, mid=None):
    rows = [
        [(SLOT_TITLES["join"], "a:slot:join"), (SLOT_TITLES["welcome"], "a:slot:welcome")],
        [("🤖 AI Header", "a:slot:ai_header"), ("🤖 AI Footer", "a:slot:ai_footer")],
        [("⚠️ Error Messages", "a:errs"), ("🧠 AI Settings", "a:ai")],
        [("📊 Stats", "a:stats"), ("📢 Broadcast", "a:bc")],
        [("🔒 Lock", "a:lock"), ("💾 Backup & Restore", "a:backup")],
    ]
    await show(chat_id, "🛠 Admin Panel\n\nChoose what you want to edit.", rows, mid)


async def errs_menu(chat_id, mid=None):
    rows = [[(SLOT_TITLES[n], f"a:slot:{n}")] for n in ERR_SLOTS] + [[("⬅️ Back", "a:main")]]
    await show(chat_id, "⚠️ Bot messages for errors and limits", rows, mid)


async def slot_menu(chat_id, name, mid=None):
    s = get_slot(name)
    info = f"{SLOT_TITLES[name]}\n\n📝 Text: {len(s['text'])} chars\n🖼 Media: {s['media_type'] or 'none'}"
    if name in BUTTON_SLOTS:
        info += f"\n🔘 Buttons: {len(s['buttons'])}"
    if name in ("ai_header", "ai_footer"):
        info += "\n\nℹ️ Added around every AI reply. Leave the text empty to disable."
    rows = [
        [("✏️ Edit message", f"a:edit:{name}")],
        [("🖼 Change image/video", f"a:img:{name}"), ("🗑 Remove image", f"a:rmimg:{name}")],
    ]
    if name in BUTTON_SLOTS:
        rows.append([("🔘 Buttons", f"a:btns:{name}")])
    rows.append([("👁 Preview", f"a:prev:{name}"), ("♻️ Reset", f"a:resetq:{name}")])
    rows.append([("⬅️ Back", "a:errs" if name in ERR_SLOTS else "a:main")])
    await show(chat_id, info, rows, mid)


async def btns_menu(chat_id, name, mid=None):
    s = get_slot(name)
    rows = []
    for i, b in enumerate(s["buttons"]):
        kind = "link" if b["type"] == "url" else "next"
        rows.append([(f"{i + 1}. {b['text'][:28]} ({kind})", f"a:b:{name}:{i}")])
    rows.append([("➕ Add button", f"a:badd:{name}")])
    rows.append([("⬅️ Back", f"a:slot:{name}")])
    await show(chat_id, f"🔘 Buttons of {SLOT_TITLES[name]}", rows, mid)


async def btn_menu(chat_id, name, i, mid=None):
    s = get_slot(name)
    if i >= len(s["buttons"]):
        return await btns_menu(chat_id, name, mid)
    b = s["buttons"][i]
    text = (
        f"🔘 Button {i + 1}\n\n"
        f"Text: {b['text']}\n"
        f"Type: {'Link' if b['type'] == 'url' else 'Next (deletes message, shows Welcome)'}\n"
        f"Link: {b['url'] or '-'}\n"
        f"Color: {STYLE_LABEL[b['style']]}\n"
        f"Premium emoji: {('✅ ' + b['emoji_id']) if b['emoji_id'] else 'none'}\n"
        f"Layout: {'own row' if b['new_row'] else 'same row as previous'}"
    )
    first = [("✏️ Text", f"a:bt:{name}:{i}")]
    if b["type"] == "url":
        first.append(("🔗 Link", f"a:bu:{name}:{i}"))
    rows = [
        first,
        [("😀 Premium emoji", f"a:be:{name}:{i}"), ("🧹 Clear emoji", f"a:bec:{name}:{i}")],
        [(f"🎨 Color: {STYLE_LABEL[b['style']]} (tap to change)", f"a:bs:{name}:{i}")],
    ]
    if name == "join":
        rows.append([("🔁 Switch type (Link / Next)", f"a:bk:{name}:{i}")])
    rows.append([("↔️ Toggle row layout", f"a:br:{name}:{i}")])
    rows.append([("⬆️ Up", f"a:bm:{name}:{i}:u"), ("⬇️ Down", f"a:bm:{name}:{i}:d")])
    rows.append([("🗑 Delete", f"a:bd:{name}:{i}"), ("⬅️ Back", f"a:btns:{name}")])
    await show(chat_id, text, rows, mid)


async def ai_menu(chat_id, mid=None):
    s = ai_settings()
    text = (
        "🧠 AI Settings\n\n"
        f"Model: {s['model']}\n"
        f"Memory: last {s['history_len']} messages\n"
        f"Rate limit: {s['rate_limit']} messages/min per user\n"
        f"Temperature: {s['temperature']}\n\n"
        f"System prompt:\n{s['system_prompt'][:900]}"
    )
    rows = [
        [("✏️ System prompt", "a:aip"), ("♻️ Reset prompt", "a:aipr")],
        [("🤖 Model", "a:aim"), ("🧠 Memory", "a:aih")],
        [("⏱ Rate limit", "a:air"), ("🌡 Temperature", "a:ait")],
        [("⬅️ Back", "a:main")],
    ]
    await show(chat_id, text, rows, mid)


AI_KEYS = {
    "aip": ("system_prompt", "Send the new system prompt (tone, language, format rules for the AI)."),
    "aim": ("model", "Send the Gemini model name, e.g. gemini-2.5-flash"),
    "aih": ("history_len", "Send how many past messages to remember (0 to 40). 0 = no memory."),
    "air": ("rate_limit", "Send max messages per minute per user (1 to 60)."),
    "ait": ("temperature", "Send temperature between 0 and 2 (e.g. 0.8)."),
}


async def lock_menu(chat_id, mid=None):
    lock = kv_get("lock", {"on": False, "admins": []})
    text = (
        f"🔒 Lock is {'ON' if lock['on'] else 'OFF'}\n\n"
        "OFF: anyone who sends the admin command can open this panel.\n"
        "ON: only the Telegram IDs below can open it.\n\n"
        f"Allowed IDs: {', '.join(map(str, lock['admins'])) or 'none'}"
    )
    rows = [
        [(("🔓 Turn OFF" if lock["on"] else "🔒 Turn ON (adds you)"), "a:locktog")],
        [("➕ Add admin ID", "a:lockadd"), ("🧹 Keep only me", "a:lockclr")],
        [("⬅️ Back", "a:main")],
    ]
    await show(chat_id, text, rows, mid)


async def stats_menu(chat_id, mid=None):
    now = int(time.time())
    total = db("SELECT COUNT(*) c FROM users", fetch="one")["c"]
    active = db("SELECT COUNT(*) c FROM users WHERE last_seen>?", (now - 86400,), "one")["c"]
    new = db("SELECT COUNT(*) c FROM users WHERE joined_at>?", (now - 86400,), "one")["c"]
    blocked = db("SELECT COUNT(*) c FROM users WHERE blocked=1", fetch="one")["c"]
    text = (
        f"📊 Stats\n\n👥 Total users: {total}\n🟢 Active (24h): {active}\n"
        f"🆕 New (24h): {new}\n🚫 Blocked the bot: {blocked}"
    )
    await show(chat_id, text, [[("⬅️ Back", "a:main")]], mid)


def cancel_row(target):
    return [[("❌ Cancel", target)]]


async def admin_callback(cb):
    uid = cb["from"]["id"]
    msg = cb.get("message")
    if not msg:
        await tg("answerCallbackQuery", callback_query_id=cb["id"])
        return
    chat_id, mid = msg["chat"]["id"], msg["message_id"]
    if not allowed(uid):
        await tg("answerCallbackQuery", callback_query_id=cb["id"], text="🔒 Locked", show_alert=True)
        return
    await tg("answerCallbackQuery", callback_query_id=cb["id"])
    global _last_admin_chat
    _last_admin_chat = chat_id
    p = cb["data"].split(":")
    act = p[1]
    pending = admin_state.pop(uid, None)

    if act == "main":
        return await main_panel(chat_id, mid)
    if act == "errs":
        return await errs_menu(chat_id, mid)
    if act == "slot":
        return await slot_menu(chat_id, p[2], mid)
    if act == "edit":
        admin_state[uid] = {"mode": "edit_slot", "slot": p[2]}
        return await show(
            chat_id,
            "✏️ Send the new message now.\n\nYou can send text, or a photo / video / GIF with a caption. "
            "Formatting, links, spoilers and premium emojis are saved exactly as you send them.",
            cancel_row(f"a:slot:{p[2]}"),
            mid,
        )
    if act == "img":
        admin_state[uid] = {"mode": "img_slot", "slot": p[2]}
        return await show(
            chat_id, "🖼 Send a photo, video or GIF. (Only the media changes, the text stays.)",
            cancel_row(f"a:slot:{p[2]}"), mid,
        )
    if act == "rmimg":
        s = get_slot(p[2])
        s["media_type"], s["media_id"] = None, None
        save_slot(p[2], s)
        return await slot_menu(chat_id, p[2], mid)
    if act == "resetq":
        return await show(
            chat_id, f"♻️ Reset {SLOT_TITLES[p[2]]} to default? Your edits will be lost.",
            [[("✅ Yes, reset", f"a:reset:{p[2]}"), ("❌ No", f"a:slot:{p[2]}")]], mid,
        )
    if act == "reset":
        kv_set(f"slot:{p[2]}", copy.deepcopy(DEFAULT_SLOTS[p[2]]))
        return await slot_menu(chat_id, p[2], mid)
    if act == "prev":
        await send_message_cfg(chat_id, get_slot(p[2]), preview=True)
        return await slot_menu(chat_id, p[2])

    # ---- buttons ----
    if act == "btns":
        return await btns_menu(chat_id, p[2], mid)
    if act == "badd":
        admin_state[uid] = {"mode": "btn_add_text", "slot": p[2]}
        return await show(
            chat_id,
            "➕ Send the button text.\n\nTip: if you include a premium emoji, it becomes the button icon.",
            cancel_row(f"a:btns:{p[2]}"), mid,
        )
    if act in ("b", "bt", "bu", "be", "bec", "bs", "bk", "br", "bd", "bm"):
        name, i = p[2], int(p[3])
        s = get_slot(name)
        if i >= len(s["buttons"]):
            return await btns_menu(chat_id, name, mid)
        b = s["buttons"][i]
        if act == "b":
            return await btn_menu(chat_id, name, i, mid)
        if act == "bt":
            admin_state[uid] = {"mode": "btn_text", "slot": name, "i": i}
            return await show(chat_id, "✏️ Send the new button text (premium emoji allowed).",
                              cancel_row(f"a:b:{name}:{i}"), mid)
        if act == "bu":
            admin_state[uid] = {"mode": "btn_url", "slot": name, "i": i}
            return await show(chat_id, "🔗 Send the new link (https://..., t.me/... or @username).",
                              cancel_row(f"a:b:{name}:{i}"), mid)
        if act == "be":
            admin_state[uid] = {"mode": "btn_emoji", "slot": name, "i": i}
            return await show(chat_id, "😀 Send a message containing the premium emoji you want on this button.",
                              cancel_row(f"a:b:{name}:{i}"), mid)
        if act == "bec":
            b["emoji_id"] = ""
        elif act == "bs":
            b["style"] = STYLES[(STYLES.index(b["style"]) + 1) % len(STYLES)]
        elif act == "bk" and name == "join":
            if b["type"] == "next":
                b["type"] = "url"
            else:
                b["type"], b["url"] = "next", ""
        elif act == "br":
            b["new_row"] = not b["new_row"]
        elif act == "bd":
            s["buttons"].pop(i)
            save_slot(name, s)
            return await btns_menu(chat_id, name, mid)
        elif act == "bm":
            j = i - 1 if p[4] == "u" else i + 1
            if 0 <= j < len(s["buttons"]):
                s["buttons"][i], s["buttons"][j] = s["buttons"][j], s["buttons"][i]
                i = j
        save_slot(name, s)
        return await btn_menu(chat_id, name, i, mid)

    # ---- AI settings ----
    if act == "ai":
        return await ai_menu(chat_id, mid)
    if act == "aipr":
        kv_set("ai", {**ai_settings(), "system_prompt": DEFAULT_SYSTEM_PROMPT})
        return await ai_menu(chat_id, mid)
    if act in AI_KEYS:
        admin_state[uid] = {"mode": "ai_edit", "key": act}
        return await show(chat_id, "✏️ " + AI_KEYS[act][1], cancel_row("a:ai"), mid)

    # ---- stats / broadcast / lock ----
    if act == "stats":
        return await stats_menu(chat_id, mid)
    if act == "bc":
        admin_state[uid] = {"mode": "bc_wait"}
        return await show(
            chat_id,
            "📢 Send the broadcast message now (text, photo, video, GIF... premium emojis and formatting are kept).",
            cancel_row("a:main"), mid,
        )
    if act == "bcgo":
        if not pending or pending.get("mode") != "bc_confirm":
            return await main_panel(chat_id, mid)
        await show(chat_id, "📢 Broadcast started...", [], mid)
        task = asyncio.create_task(run_broadcast(chat_id, pending["chat"], pending["mid"], mid))
        _tasks.add(task)
        task.add_done_callback(_tasks.discard)
        return
    if act == "backup":
        return await show(
            chat_id,
            "💾 Backup & Restore\n\nAll your edits (messages, buttons, AI settings, users) are saved in a file. "
            "A backup is also sent to you automatically after every edit.\n\n"
            "If you ever lose your settings, tap Restore and send the backup file.",
            [[("📤 Send backup now", "a:bkpnow")], [("📥 Restore from file", "a:restore")], [("⬅️ Back", "a:main")]],
            mid,
        )
    if act == "bkpnow":
        await send_backup(chat_id)
        return
    if act == "restore":
        admin_state[uid] = {"mode": "restore_wait"}
        return await show(chat_id, "📥 Send the backup .json file now.", cancel_row("a:backup"), mid)
    if act == "lock":
        return await lock_menu(chat_id, mid)
    if act == "locktog":
        lock = kv_get("lock", {"on": False, "admins": []})
        lock["on"] = not lock["on"]
        if lock["on"] and uid not in lock["admins"]:
            lock["admins"].append(uid)
        kv_set("lock", lock)
        return await lock_menu(chat_id, mid)
    if act == "lockadd":
        admin_state[uid] = {"mode": "lock_add"}
        return await show(chat_id, "➕ Send the numeric Telegram user ID to allow.", cancel_row("a:lock"), mid)
    if act == "lockclr":
        kv_set("lock", {"on": kv_get("lock", {"on": False})["on"], "admins": [uid]})
        return await lock_menu(chat_id, mid)


async def admin_input(msg, st):
    uid, chat_id = msg["from"]["id"], msg["chat"]["id"]
    mode = st["mode"]
    text, ents = msg.get("text", ""), msg.get("entities", [])

    async def done(note, then):
        admin_state.pop(uid, None)
        await tg("sendMessage", chat_id=chat_id, text=note)
        await then

    if mode == "edit_slot":
        c = extract_content(msg)
        if not c:
            return await tg("sendMessage", chat_id=chat_id, text="Please send text, or a photo/video/GIF with caption.")
        name = st["slot"]
        s = get_slot(name)
        s["text"], s["entities"] = c["text"], c["entities"]
        if c["media_id"]:
            s["media_type"], s["media_id"] = c["media_type"], c["media_id"]
        save_slot(name, s)
        await done("✅ Saved. Here is how it looks:", send_message_cfg(chat_id, s))
        return await slot_menu(chat_id, name)

    if mode == "img_slot":
        c = extract_content(msg)
        if not c or not c["media_id"]:
            return await tg("sendMessage", chat_id=chat_id, text="Please send a photo, video or GIF.")
        name = st["slot"]
        s = get_slot(name)
        s["media_type"], s["media_id"] = c["media_type"], c["media_id"]
        save_slot(name, s)
        await done("✅ Media saved.", send_message_cfg(chat_id, s))
        return await slot_menu(chat_id, name)

    if mode == "btn_add_text":
        if not text:
            return await tg("sendMessage", chat_id=chat_id, text="Please send the button text.")
        btxt, emoji = split_button_text(text, ents)
        if not btxt:
            btxt = "•"
        admin_state[uid] = {"mode": "btn_add_url", "slot": st["slot"], "text": btxt, "emoji": emoji}
        extra = "\nSend `-` to make it a 'Next' button (deletes this message and shows Welcome)." if st["slot"] == "join" else ""
        return await tg(
            "sendMessage", chat_id=chat_id,
            text="🔗 Now send the link for this button (https://..., t.me/... or @username)." + extra.replace("`", ""),
        )

    if mode == "btn_add_url":
        name = st["slot"]
        s = get_slot(name)
        if text.strip() == "-" and name == "join":
            b = _btn(st["text"], "next", emoji_id=st["emoji"])
        else:
            url = norm_url(text) if text else None
            if not url:
                return await tg("sendMessage", chat_id=chat_id, text="That doesn't look like a valid link. Try again.")
            b = _btn(st["text"], "url", url, emoji_id=st["emoji"])
        s["buttons"].append(b)
        save_slot(name, s)
        await done("✅ Button added.", asyncio.sleep(0))
        return await btn_menu(chat_id, name, len(s["buttons"]) - 1)

    if mode in ("btn_text", "btn_url", "btn_emoji"):
        name, i = st["slot"], st["i"]
        s = get_slot(name)
        if i >= len(s["buttons"]):
            admin_state.pop(uid, None)
            return await btns_menu(chat_id, name)
        b = s["buttons"][i]
        if mode == "btn_text":
            if not text:
                return await tg("sendMessage", chat_id=chat_id, text="Please send text.")
            btxt, emoji = split_button_text(text, ents)
            b["text"] = btxt or "•"
            if emoji:
                b["emoji_id"] = emoji
        elif mode == "btn_url":
            url = norm_url(text) if text else None
            if not url:
                return await tg("sendMessage", chat_id=chat_id, text="That doesn't look like a valid link. Try again.")
            b["url"] = url
        else:
            emoji = next((e["custom_emoji_id"] for e in ents if e["type"] == "custom_emoji"), "")
            if not emoji and text.strip().isdigit():
                emoji = text.strip()
            if not emoji:
                return await tg("sendMessage", chat_id=chat_id, text="No premium emoji found. Send a message with one.")
            b["emoji_id"] = emoji
        save_slot(name, s)
        await done("✅ Updated.", asyncio.sleep(0))
        return await btn_menu(chat_id, name, i)

    if mode == "ai_edit":
        field, _ = AI_KEYS[st["key"]]
        val = text.strip()
        try:
            if field == "system_prompt":
                if not val:
                    raise ValueError
            elif field == "model":
                if not re.match(r"^[\w.\-]+$", val):
                    raise ValueError
            elif field == "history_len":
                val = int(val)
                if not 0 <= val <= 40:
                    raise ValueError
            elif field == "rate_limit":
                val = int(val)
                if not 1 <= val <= 60:
                    raise ValueError
            elif field == "temperature":
                val = float(val)
                if not 0 <= val <= 2:
                    raise ValueError
        except ValueError:
            return await tg("sendMessage", chat_id=chat_id, text="Invalid value, please try again.")
        kv_set("ai", {**ai_settings(), field: val})
        await done("✅ Saved.", asyncio.sleep(0))
        return await ai_menu(chat_id)

    if mode == "bc_wait":
        total = db("SELECT COUNT(*) c FROM users WHERE blocked=0", fetch="one")["c"]
        admin_state[uid] = {"mode": "bc_confirm", "chat": chat_id, "mid": msg["message_id"]}
        await tg("sendMessage", chat_id=chat_id, text="👆 This is what users will receive.")
        return await tg(
            "sendMessage", chat_id=chat_id, text=f"Send this to {total} users?",
            reply_markup=kb([[("✅ Send", "a:bcgo"), ("❌ Cancel", "a:main")]]),
        )

    if mode == "restore_wait":
        doc = msg.get("document")
        if not doc:
            return await tg("sendMessage", chat_id=chat_id, text="Please send the backup .json file.")
        data = await download_file(doc["file_id"], max_bytes=19 * 1024 * 1024)
        try:
            kvn, usern = import_data(json.loads(data.decode("utf-8")))
        except Exception as e:
            log.warning("restore failed: %s", e)
            return await tg("sendMessage", chat_id=chat_id, text="❌ This is not a valid backup file.")
        await done(f"✅ Restored {kvn} settings and {usern} users.", main_panel(chat_id))
        return

    if mode == "lock_add":
        if not text.strip().isdigit():
            return await tg("sendMessage", chat_id=chat_id, text="Send a numeric ID.")
        lock = kv_get("lock", {"on": False, "admins": []})
        if int(text) not in lock["admins"]:
            lock["admins"].append(int(text))
        kv_set("lock", lock)
        await done("✅ Added.", asyncio.sleep(0))
        return await lock_menu(chat_id)

    admin_state.pop(uid, None)


_tasks = set()


async def run_broadcast(admin_chat, src_chat, src_mid, status_mid):
    users = db("SELECT id FROM users WHERE blocked=0", fetch="all")
    sent = failed = 0
    for n, u in enumerate(users, 1):
        r = await tg("copyMessage", chat_id=u["id"], from_chat_id=src_chat, message_id=src_mid)
        if not r.get("ok") and r.get("error_code") == 429:
            await asyncio.sleep(r.get("parameters", {}).get("retry_after", 5) + 1)
            r = await tg("copyMessage", chat_id=u["id"], from_chat_id=src_chat, message_id=src_mid)
        if r.get("ok"):
            sent += 1
        else:
            failed += 1
            if r.get("error_code") == 403:
                db("UPDATE users SET blocked=1 WHERE id=?", (u["id"],))
        if n % 50 == 0:
            await tg("editMessageText", chat_id=admin_chat, message_id=status_mid,
                     text=f"📢 Sending... {n}/{len(users)}")
        await asyncio.sleep(0.05)
    await tg("editMessageText", chat_id=admin_chat, message_id=status_mid,
             text=f"✅ Broadcast finished.\n\nSent: {sent}\nFailed: {failed}",
             reply_markup=kb([[("⬅️ Admin Panel", "a:main")]]))


# ======================================================================
# Update routing
# ======================================================================
def parse_command(text):
    if text and text.startswith("/"):
        return text.split()[0][1:].split("@")[0].lower()
    return None


async def handle_message(msg):
    chat = msg["chat"]
    if chat["type"] != "private" or "from" not in msg or msg["from"].get("is_bot"):
        return
    uid = msg["from"]["id"]
    touch_user(msg["from"])
    cmd = parse_command(msg.get("text", ""))
    if cmd == ADMIN_COMMAND:
        if not allowed(uid):
            await tg("sendMessage", chat_id=chat["id"], text="🔒 Not available.")
            return
        global _last_admin_chat
        _last_admin_chat = chat["id"]
        admin_state.pop(uid, None)
        return await main_panel(chat["id"])
    if cmd == "start":
        return await cmd_start(msg)
    st = admin_state.get(uid)
    if st:
        return await admin_input(msg, st)
    await handle_ai(msg)


async def handle_callback(cb):
    data = cb.get("data", "")
    if data == "next":
        return await on_next(cb)
    if data.startswith("a:"):
        return await admin_callback(cb)
    await tg("answerCallbackQuery", callback_query_id=cb["id"])


async def handle_update(u):
    if "message" in u:
        await handle_message(u["message"])
    elif "callback_query" in u:
        await handle_callback(u["callback_query"])
    elif "my_chat_member" in u:
        m = u["my_chat_member"]
        if m["chat"]["type"] == "private":
            blocked = 1 if m["new_chat_member"]["status"] == "kicked" else 0
            db("UPDATE users SET blocked=? WHERE id=?", (blocked, m["chat"]["id"]))


async def main():
    global client
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing. Add it in the environment variables.")
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY is missing - AI replies will fail until you add it.")
    client = httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))
    me = await tg("getMe")
    if not me.get("ok"):
        raise SystemExit(f"Invalid BOT_TOKEN: {me.get('description')}")
    log.info("Bot started as @%s", me["result"]["username"])
    await tg("deleteWebhook", drop_pending_updates=False)

    sem = asyncio.Semaphore(30)

    async def run(u):
        async with sem:
            try:
                await handle_update(u)
            except Exception:
                log.exception("update failed")

    offset = None
    while True:
        res = await tg(
            "getUpdates", offset=offset, timeout=50, _timeout=70,
            allowed_updates=["message", "callback_query", "my_chat_member"],
        )
        if not res.get("ok"):
            await asyncio.sleep(3)
            continue
        for u in res["result"]:
            offset = u["update_id"] + 1
            t = asyncio.create_task(run(u))
            _tasks.add(t)
            t.add_done_callback(_tasks.discard)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
