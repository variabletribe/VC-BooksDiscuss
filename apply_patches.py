#!/usr/bin/env python3
"""
apply_patches.py - upgrades bot.py and db.py in place.

Usage (run from the folder that contains bot.py and db.py):

    python apply_patches.py            # patch both files
    python apply_patches.py --dry-run  # only check that every edit point is found

Safe by design:
  * every edit point is located first; if ANY is missing, nothing is changed
  * the patched code is syntax-checked before anything is written
  * originals are kept as bot.py.bak / db.py.bak
  * running it twice is harmless (it detects the files are already patched)
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "# --- Per-user cooldown for heavy"
MARKER_DB = "# Welcome messages (/setwelcome, /welcome, /resetwelcome)"

# ----------------------------------------------------------------------------
# New code blocks
# ----------------------------------------------------------------------------

DB_APPEND = r'''

# =============================================================================
# Welcome messages (/setwelcome, /welcome, /resetwelcome)
# Stored in chat_settings like filters: {"type", "text", "entities", "file_id"}
# =============================================================================


def get_welcome(chat_id: int) -> tuple[bool, dict | None]:
    """(enabled, welcome_data_or_None)."""
    doc = _coll("chat_settings").find_one({"_id": chat_id}) or {}
    return bool(doc.get("welcome_enabled", False)), doc.get("welcome")


def set_welcome(chat_id: int, data: dict) -> None:
    """Save the welcome message and switch it on."""
    _coll("chat_settings").update_one(
        {"_id": chat_id},
        {
            "$set": {"welcome": data, "welcome_enabled": True},
            "$setOnInsert": {"monthly_reports": True},
        },
        upsert=True,
    )


def set_welcome_enabled(chat_id: int, enabled: bool) -> None:
    _coll("chat_settings").update_one(
        {"_id": chat_id},
        {"$set": {"welcome_enabled": enabled}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def clear_welcome(chat_id: int) -> bool:
    """Delete the saved welcome message and switch it off. True if one existed."""
    result = _coll("chat_settings").update_one(
        {"_id": chat_id, "welcome": {"$exists": True}},
        {"$unset": {"welcome": ""}, "$set": {"welcome_enabled": False}},
    )
    return result.modified_count > 0


# =============================================================================
# Backup / restore (the bot DMs a gzipped JSON dump to the bot admins on a schedule)
# =============================================================================


def build_backup() -> tuple[bytes, dict[str, int]]:
    """Dump every collection to gzipped JSON. Returns (bytes, {collection: doc_count})."""
    import gzip

    from bson import json_util

    assert _db is not None, "init_db() must be called first"
    collections: dict[str, list] = {}
    counts: dict[str, int] = {}
    for name in sorted(_db.list_collection_names()):
        if name.startswith("system."):
            continue
        docs = list(_db[name].find({}))
        collections[name] = docs
        counts[name] = len(docs)
    payload = {
        "format": "vc_bot_backup",
        "version": 1,
        "created_at": datetime.now(timezone.utc),
        "db": _db.name,
        "collections": collections,
    }
    raw = json_util.dumps(payload).encode("utf-8")
    return gzip.compress(raw, compresslevel=6), counts


def parse_backup(blob: bytes) -> dict:
    """Decode a backup file (gzipped or plain JSON) and sanity-check it."""
    import gzip

    from bson import json_util

    try:
        raw = gzip.decompress(blob)
    except (OSError, EOFError):
        raw = blob
    payload = json_util.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict) or payload.get("format") != "vc_bot_backup":
        raise ValueError("This file is not a vc_bot backup.")
    return payload


def restore_backup(blob: bytes, only: list[str] | None = None) -> dict[str, int]:
    """Upsert every document from a backup by _id. Never deletes anything that is
    already in the database. Returns {collection: docs_restored}."""
    payload = parse_backup(blob)
    restored: dict[str, int] = {}
    for name, docs in (payload.get("collections") or {}).items():
        if only and name not in only:
            continue
        coll = _coll(name)
        count = 0
        for d in docs:
            if "_id" in d:
                coll.replace_one({"_id": d["_id"]}, d, upsert=True)
                count += 1
        restored[name] = count
    return restored


def get_last_backup_at() -> datetime | None:
    doc = _coll("meta").find_one({"_id": "last_backup"})
    when = doc.get("at") if doc else None
    if when is not None and when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when


def set_last_backup_at(when: datetime) -> None:
    _coll("meta").update_one({"_id": "last_backup"}, {"$set": {"at": when}}, upsert=True)
'''

BOT_BLOCK = r'''# =============================================================================
# Welcome messages: /setwelcome, /welcome, /resetwelcome
#
# Stored like filters ({"type", "text", "entities", "file_id"}) so bold/italic and
# media survive. Placeholders: {first} {last} {fullname} {username} {mention} {id}
# {chatname} {count}
# =============================================================================

_PLACEHOLDER_RE = re.compile(
    r"\{(first|last|fullname|username|mention|id|chatname|count)\}", re.IGNORECASE
)
_WELCOME_HELP = "Placeholders: {first} {last} {fullname} {username} {mention} {id} {chatname} {count}"


def _utf16_len(s: str) -> int:
    """Telegram entity offsets/lengths count UTF-16 code units, not Python characters."""
    return len(s.encode("utf-16-le")) // 2


def _render_placeholders(
    text: str, entities: list[dict], values: dict[str, str], mention_id: int | None
) -> tuple[str, list[dict]]:
    """Replace {placeholders} in `text` while keeping every formatting entity (bold,
    italic, links...) correctly positioned. {mention} becomes a clickable link to the user.

    Works right-to-left so offsets to the left of the current match are still valid, and
    shifts/stretches each entity by the length difference of the replacement."""
    ents = [dict(e) for e in entities]
    for m in reversed(list(_PLACEHOLDER_RE.finditer(text))):
        key = m.group(1).lower()
        token_len = _utf16_len(m.group(0))
        start = _utf16_len(text[: m.start()])
        end = start + token_len
        replacement = values.get(key, "")
        new_len = _utf16_len(replacement)
        delta = new_len - token_len
        for e in ents:
            if e["offset"] >= end:
                e["offset"] += delta
            elif e["offset"] <= start and e["offset"] + e["length"] >= end:
                e["length"] += delta
        if key == "mention" and mention_id is not None and new_len > 0:
            ents.append(
                {"type": "text_link", "offset": start, "length": new_len,
                 "url": f"tg://user?id={mention_id}"}
            )
        text = text[: m.start()] + replacement + text[m.end():]
    return text, [e for e in ents if e["length"] > 0]


def _extract_command_body(msg) -> tuple[str, list[dict]] | None:
    """(body_text, entity_dicts) for everything after the command token, keeping the
    exact formatting. Same offset logic as _extract_filter_command_body, minus the keyword."""
    text = msg.text or ""
    parts = text.split(None, 1)
    if len(parts) < 2:
        return None
    idx = len(parts[0])
    while idx < len(text) and text[idx].isspace():
        idx += 1
    body = text[idx:]
    if not body.strip():
        return None
    ents: list[dict] = []
    for e in (msg.entities or []):
        if e.offset >= idx:
            d = _entity_to_dict(e)
            d["offset"] = e.offset - idx
            ents.append(d)
    return body, ents


async def _send_welcome(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user, chat_title: str = "") -> None:
    """Post this group's welcome message for `user`, if one is saved and switched on.
    Never raises: a broken welcome must not break joins."""
    try:
        enabled, data = await asyncio.to_thread(dbmod.get_welcome, chat_id)
        if not enabled or not data:
            return
        first = user.first_name or "there"
        last = user.last_name or ""
        values = {
            "first": first,
            "last": last,
            "fullname": f"{first} {last}".strip(),
            "username": f"@{user.username}" if user.username else first,
            "mention": first,
            "id": str(user.id),
            "chatname": chat_title or "this group",
            "count": "",
        }
        text = data.get("text") or ""
        if re.search(r"\{count\}", text, re.IGNORECASE):
            try:
                values["count"] = str(await context.bot.get_chat_member_count(chat_id))
            except Exception:
                values["count"] = ""
        entities = [e for e in (data.get("entities") or []) if e.get("type") != "text_mention"]
        if text:
            text, entities = _render_placeholders(text, entities, values, user.id)
        rendered = {**data, "text": text if text else data.get("text"), "entities": entities}
        await _send_filter_response(context, chat_id, None, rendered)
    except Exception:
        logger.exception("Welcome message failed chat_id=%s", chat_id)


async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Records when someone joins the group (for /mystats's "Joined group" field) and
    posts the welcome message if one is set.

    Two known limits, both unavoidable without Telegram giving us historical data:
    - No join date exists for anyone who was already in the group before this shipped.
    - If the group has "Hide join/leave messages" turned on, this event never fires for
      them either; /mystats will show "Unknown" in both cases, which is accurate."""
    msg = update.message
    if not msg or not msg.chat or not msg.new_chat_members:
        return
    chat = msg.chat
    if chat.type not in ("group", "supergroup"):
        return
    when = msg.date or datetime.now(timezone.utc)

    welcomed = 0  # cap welcomes per join event so a mass-join can't flood the chat
    for user in msg.new_chat_members:
        if user.is_bot:
            continue
        await asyncio.to_thread(dbmod.record_group_join, chat.id, user.id, _user_label(user), when)
        if welcomed < 5:
            welcomed += 1
            await _send_welcome(context, chat.id, user, chat.title or "")


async def cmd_setwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: /setwelcome <text>  - or reply to any message with /setwelcome."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can set the welcome message.")
        return

    reply = update.message.reply_to_message
    if reply is not None:
        data = _filter_data_from_message(reply)
        if data is None:
            await _reply_autodelete(update, context, "I can't use that message type as a welcome message.")
            return
    else:
        parsed = _extract_command_body(update.message)
        if parsed is None:
            await _reply_autodelete(
                update, context,
                "Usage: /setwelcome <text>\n"
                "Or reply to any message (text, photo, video, sticker, ...) with /setwelcome\n\n"
                + _WELCOME_HELP,
            )
            return
        body, ents = parsed
        data = {"type": "text", "file_id": None, "text": body, "entities": ents}

    await asyncio.to_thread(dbmod.set_welcome, chat.id, data)
    await _reply_autodelete(update, context, "Welcome message saved and switched on. Preview below:")
    await _send_welcome(context, chat.id, update.effective_user, chat.title or "")


async def cmd_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone can view the status; group admins can switch it: /welcome [on|off]."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    enabled, data = await asyncio.to_thread(dbmod.get_welcome, chat.id)
    if not context.args:
        if data is None:
            text = "No welcome message set yet.\nSet one with /setwelcome <text>\n\n" + _WELCOME_HELP
        else:
            text = (
                f"Welcome message is {'on' if enabled else 'off'}.\n"
                "Change it with /setwelcome, switch it with /welcome on|off, "
                "or remove it with /resetwelcome."
            )
        await _reply_autodelete(update, context, text)
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this.")
        return
    arg = context.args[0].lower()
    if arg not in ("on", "off"):
        await _reply_autodelete(update, context, "Usage: /welcome on|off")
        return
    if arg == "on" and data is None:
        await _reply_autodelete(update, context, "Set a welcome message first with /setwelcome <text>.")
        return
    await asyncio.to_thread(dbmod.set_welcome_enabled, chat.id, arg == "on")
    await _reply_autodelete(update, context, f"Welcome message turned {arg}.")


async def cmd_resetwelcome(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: delete the saved welcome message."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can reset the welcome message.")
        return
    removed = await asyncio.to_thread(dbmod.clear_welcome, chat.id)
    await _reply_autodelete(
        update, context,
        "Welcome message deleted and switched off." if removed else "There was no welcome message to delete.",
    )


# =============================================================================
# /purge, /pin, /unpin
# =============================================================================

_PURGE_MAX_MESSAGES = 500


async def cmd_purge(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: reply to a message with /purge to delete it and everything after it,
    down to the command itself (max 500 messages)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can purge messages.")
        return
    reply = update.message.reply_to_message
    if reply is None:
        await _reply_autodelete(
            update, context,
            "Reply to the first message you want deleted. Everything from there down to your "
            "/purge command will be removed.",
        )
        return

    first_id, last_id = reply.message_id, update.message.message_id
    count = last_id - first_id + 1
    if count > _PURGE_MAX_MESSAGES:
        await _reply_autodelete(
            update, context,
            f"That would delete {count} messages; the limit is {_PURGE_MAX_MESSAGES} per purge. "
            "Reply to a more recent message.",
        )
        return

    ids = list(range(first_id, last_id + 1))
    chunks = failed = 0
    for i in range(0, len(ids), 100):
        chunks += 1
        try:
            await context.bot.delete_messages(chat.id, ids[i : i + 100])
        except Exception:
            failed += 1
            logger.exception("Purge chunk failed chat_id=%s", chat.id)
    if failed == chunks:
        await _reply_autodelete(
            update, context,
            "Couldn't delete the messages. Make sure I'm an admin with the 'Delete messages' right.",
        )
        return

    actor = update.effective_user
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "purge", 0, "messages", actor.id, _user_label(actor),
        f"{count} message ids ({first_id}-{last_id})",
    )
    try:
        notice = await context.bot.send_message(chat.id, "Purge complete.")
    except Exception:
        return
    jq = context.job_queue
    if jq is not None:
        jq.run_once(
            _delete_messages_later,
            when=5,
            data={"chat_id": chat.id, "message_ids": [notice.message_id]},
            name=f"purge-notice-{chat.id}-{notice.message_id}",
        )


async def cmd_pin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: reply to a message with /pin [loud]. Silent unless 'loud' is given."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can pin messages.")
        return
    reply = update.message.reply_to_message
    if reply is None:
        await _reply_autodelete(update, context, "Reply to the message you want to pin.")
        return
    loud = bool(context.args) and context.args[0].lower() in ("loud", "notify")
    try:
        await context.bot.pin_chat_message(chat.id, reply.message_id, disable_notification=not loud)
    except Exception:
        logger.exception("pin failed chat_id=%s", chat.id)
        await _reply_autodelete(update, context, "Couldn't pin. I need to be an admin with the 'Pin messages' right.")
        return
    author = reply.from_user
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "pin", author.id if author else 0,
        _user_label(author) if author else "message", update.effective_user.id,
        _user_label(update.effective_user), f"message {reply.message_id}",
    )
    await _reply_autodelete(
        update, context,
        "Pinned (members notified)." if loud else "Pinned silently. Add 'loud' to notify members.",
    )


async def cmd_unpin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: /unpin (reply to a message to unpin that one, otherwise the latest pin)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can unpin messages.")
        return
    reply = update.message.reply_to_message
    try:
        if reply is not None:
            await context.bot.unpin_chat_message(chat.id, message_id=reply.message_id)
        else:
            await context.bot.unpin_chat_message(chat.id)
    except Exception:
        logger.exception("unpin failed chat_id=%s", chat.id)
        await _reply_autodelete(update, context, "Couldn't unpin. I need to be an admin with the 'Pin messages' right.")
        return
    author = reply.from_user if reply is not None else None
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "unpin", author.id if author else 0,
        _user_label(author) if author else "latest pin", update.effective_user.id,
        _user_label(update.effective_user), f"message {reply.message_id}" if reply else "",
    )
    await _reply_autodelete(update, context, "Unpinned.")


# =============================================================================
# Database backup: /backup (bot admins, DM only) + automatic scheduled backups.
# Atlas's free tier (M0) has no automatic backups, so the bot sends a gzipped JSON dump
# to every bot admin. Restore with restore_backup.py.
#   BACKUP_INTERVAL_DAYS (default 7)   BACKUP_HOUR_UTC (default 21)
# =============================================================================

_BACKUP_MAX_BYTES = 49 * 1024 * 1024  # Telegram bots can upload up to 50 MB


def _backup_hour_utc() -> int:
    try:
        return min(23, max(0, int(os.getenv("BACKUP_HOUR_UTC", "21"))))
    except ValueError:
        return 21


def _backup_interval_days() -> int:
    try:
        return max(1, int(os.getenv("BACKUP_INTERVAL_DAYS", "7")))
    except ValueError:
        return 7


async def _deliver_backup(context: ContextTypes.DEFAULT_TYPE) -> tuple[int, str]:
    """Build a backup and DM it to every bot admin (falling back to the admin relay
    group if no DM goes through). Returns (chats_delivered_to, summary)."""
    blob, counts = await asyncio.to_thread(dbmod.build_backup)
    summary = f"{sum(counts.values())} documents in {len(counts)} collections, {len(blob) / 1024:.0f} KB"
    if len(blob) > _BACKUP_MAX_BYTES:
        raise ValueError(f"backup is {len(blob) / 1048576:.1f} MB, over Telegram's 50 MB upload limit")
    filename = f"vc_bot_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json.gz"
    caption = f"Database backup: {summary}\nRestore with restore_backup.py"

    delivered = 0
    for uid in sorted(app_state.parse_admin_user_ids()):
        try:
            await context.bot.send_document(
                uid, document=InputFile(io.BytesIO(blob), filename=filename), caption=caption
            )
            delivered += 1
        except Exception:
            logger.warning("Backup DM failed for admin %s (they may need to /start the bot)", uid)
    relay = app_state.admin_relay_chat_id()
    if delivered == 0 and relay:
        try:
            await context.bot.send_document(
                relay, document=InputFile(io.BytesIO(blob), filename=filename), caption=caption
            )
            delivered += 1
        except Exception:
            logger.exception("Backup to admin relay chat failed")
    if delivered:
        await asyncio.to_thread(dbmod.set_last_backup_at, datetime.now(timezone.utc))
    return delivered, summary


async def daily_backup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs daily; actually backs up only when BACKUP_INTERVAL_DAYS have passed since the
    last successful one (state lives in MongoDB, so restarts don't cause repeats or gaps)."""
    try:
        last = await asyncio.to_thread(dbmod.get_last_backup_at)
        if last is not None:
            due_after = last + timedelta(days=_backup_interval_days()) - timedelta(hours=1)
            if datetime.now(timezone.utc) < due_after:
                return
        delivered, summary = await _deliver_backup(context)
        if delivered:
            logger.info("Scheduled backup sent to %s chat(s): %s", delivered, summary)
        else:
            logger.error("Scheduled backup was built but could not be delivered to any admin")
    except Exception as exc:
        logger.exception("Scheduled backup failed")
        for uid in sorted(app_state.parse_admin_user_ids()):
            try:
                await context.bot.send_message(uid, f"Scheduled database backup failed: {str(exc)[:300]}")
            except Exception:
                pass


async def cmd_backup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot admin, DM only: /backup - build and send a backup right now."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command only works in a private chat with me.")
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return
    status = await update.message.reply_text("Building backup...")
    try:
        delivered, summary = await _deliver_backup(context)
    except Exception as exc:
        logger.exception("Manual backup failed")
        await status.edit_text(f"Backup failed: {str(exc)[:300]}")
        return
    if delivered:
        await status.edit_text(f"Backup sent to {delivered} chat(s): {summary}")
    else:
        await status.edit_text("Backup built, but I couldn't deliver it. Admins must /start the bot first.")


'''

BOT_COOLDOWN = r'''# --- Per-user cooldown for heavy (database-aggregating) commands ------------

_COMMAND_COOLDOWNS = {
    "vcreport": 30,
    "monthreport": 30,
    "weekly": 30,
    "attendance": 20,
    "xpleaderboard": 15,
}
_cooldown_last: dict[tuple[int, int, str], float] = {}  # (chat, user, command) -> monotonic time


async def _cooldown_blocked(update: Update, context: ContextTypes.DEFAULT_TYPE, command: str) -> bool:
    """True (after telling the user to wait) if this user ran `command` too recently in
    this chat. Group admins are exempt. Costs nothing on the normal path: the admin lookup
    only happens when someone is actually inside the cooldown window."""
    seconds = _COMMAND_COOLDOWNS.get(command, 0)
    user, chat = update.effective_user, update.effective_chat
    if seconds <= 0 or not user or not chat:
        return False
    now = time.monotonic()
    key = (chat.id, user.id, command)
    last = _cooldown_last.get(key)
    if last is not None and now - last < seconds:
        if not await _is_group_admin(update, context):
            wait = int(seconds - (now - last)) + 1
            await _reply_autodelete(update, context, f"Please wait {wait}s before using /{command} again.")
            return True
    _cooldown_last[key] = now
    if len(_cooldown_last) > 5000:  # keep memory bounded
        for k in [k for k, t in _cooldown_last.items() if now - t > 3600]:
            _cooldown_last.pop(k, None)
    return False


'''

# ----------------------------------------------------------------------------
# Patch definitions. ("sub", name, anchor, replacement) replaces text that must occur
# exactly once (trailing spaces / blank-line whitespace in the anchor are tolerated).
# ----------------------------------------------------------------------------

COOLDOWN_HEAD = (
    "    if not update.message or not update.effective_chat:\n"
    "        return\n"
    "    chat = update.effective_chat\n"
    '    if chat.type not in ("group", "supergroup"):\n'
    '        await _reply_autodelete(update, context, "Use this command in a group.")\n'
    "        return"
)

DB_OPS = [
    ("sub", "db: /finduser treats the search text literally",
     r'''    term = query.strip().lstrip("@")
    if not term:
        return []''',
     r'''    term = query.strip().lstrip("@")
    if not term:
        return []
    # Match the text literally: input like "c++" or "(" is an invalid regex and used to crash.
    term = re.escape(term)'''),
    ("sub", "db: filter docstring",
     r'''    """Case-insensitive substring match against saved keywords; returns (keyword,''',
     r'''    """Case-insensitive whole-word match against saved keywords; returns (keyword,'''),
    ("sub", "db: filters match whole words only",
     r'''    for keyword, filter_data in filters_map.items():
        if keyword in lowered:
            return keyword, filter_data''',
     r'''    # Longest keyword first, whole words only: the filter "hi" no longer fires inside
    # "this" or "which".
    for keyword in sorted(filters_map, key=len, reverse=True):
        if re.search(rf"(?<!\w){re.escape(keyword)}(?!\w)", lowered):
            return keyword, filters_map[keyword]'''),
    ("append", "db: welcome + backup functions", DB_APPEND),
]

BOT_OPS = [
    # ---- captcha removal ----------------------------------------------------
    ("remove_func", "bot: remove old on_new_chat_members (replaced below)", "on_new_chat_members"),
    ("remove_func", "bot: remove _captcha_timeout_kick", "_captcha_timeout_kick"),
    ("remove_func", "bot: remove on_captcha_callback", "on_captcha_callback"),
    ("remove_func", "bot: remove cmd_captcha", "cmd_captcha"),
    ("delete_line", "bot: captcha constant", "_CAPTCHA_TIMEOUT_SECONDS = 300"),
    ("delete_line", "bot: captcha pending dict", "_pending_captchas: dict"),
    ("delete_line", "bot: captcha help entry", '"captcha": ("mod",'),
    ("delete_line", "bot: captcha command handler", 'CommandHandler("captcha", cmd_captcha)'),
    ("delete_line", "bot: captcha button handler", "CallbackQueryHandler(on_captcha_callback"),
    ("sub", "bot: callbacks comment",
     r'''    # Inline-button callbacks: generic confirm/cancel (ban, removeuser, broadcast) and
    # new-member captcha verification. Matched by callback_data prefix via `pattern`.''',
     r'''    # Inline-button callbacks: generic confirm/cancel (ban, removeuser, broadcast) and the
    # /start help menu. Matched by callback_data prefix via `pattern`.'''),

    # ---- /start menu --------------------------------------------------------
    ("sub", "bot: help category title escaping (fixes the 'Stats & Progress' button)",
     r'''    label = HELP_CATEGORIES.get(cat_key, cat_key)
    return f"{label}\n\nTap a command to see what it does and how to use it."''',
     r'''    label = html.escape(HELP_CATEGORIES.get(cat_key, cat_key), quote=False)
    return f"{label}\n\nTap a command to see what it does and how to use it."'''),
    ("sub", "bot: /help alias",
     r'''app.add_handler(CommandHandler("start", cmd_start))''',
     r'''app.add_handler(CommandHandler(["start", "help"], cmd_start))'''),
    ("sub", "bot: reserve 'help' so custom commands can't shadow it",
     r'''return set(HELP_COMMANDS.keys()) | {"start", "gmeet_rec"}''',
     r'''return set(HELP_COMMANDS.keys()) | {"start", "help", "gmeet_rec"}'''),
    ("sub", "bot: /attendance wording matches the real once-per-day rule",
     r'''minutes in one call = +1 present day (once per call).</i>''',
     r'''minutes in a call = +1 present day (max once per day).</i>'''),
    ("sub", "bot: new help entries",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "setwelcome": ("groupadmin", "/setwelcome <text>  -  or reply to any message with /setwelcome", "Set the message new members get (text, photo, video, sticker, ...). Placeholders: {first} {last} {fullname} {username} {mention} {id} {chatname} {count}. Sends a preview and switches the welcome on.", "Group admin"),
    "welcome": ("groupadmin", "/welcome [on|off]", "Show whether the welcome message is on, or switch it on/off.", "Anyone can view; group admin to change"),
    "resetwelcome": ("groupadmin", "/resetwelcome", "Delete the saved welcome message and switch it off.", "Group admin"),
    "purge": ("groupadmin", "/purge  (reply to the first message to delete)", "Delete everything from the replied-to message down to your command, up to 500 messages. Logged in /modlog.", "Group admin"),
    "pin": ("groupadmin", "/pin [loud]  (reply to a message)", "Pin the replied-to message. Silent by default; add 'loud' to notify members.", "Group admin"),
    "unpin": ("groupadmin", "/unpin  (reply to a message, or alone for the latest pin)", "Unpin the replied-to message, or the most recent pin if you don't reply.", "Group admin"),
    "backup": ("botadmin", "/backup", "Build a full database backup and DM it to every bot admin. It also runs automatically (BACKUP_INTERVAL_DAYS, BACKUP_HOUR_UTC). DM only.", "Bot admin, DM only"),'''),

    # ---- anonymous-admin handling + caching ----------------------------------
    ("sub", "bot: admin-status cache constants",
     r'''async def _message_sender_is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:''',
     r'''_ADMIN_CACHE_TTL_SECONDS = 60
_admin_status_cache: dict[tuple[int, int], tuple[float, bool]] = {}  # (chat, user) -> (checked_at, is_admin)


async def _message_sender_is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:'''),
    ("sub", "bot: _message_sender_is_admin also works for edited messages",
     r'''    msg = update.message
    chat = update.effective_chat
    if not msg or not chat:
        return False''',
     r'''    msg = update.effective_message  # also covers edited messages
    chat = update.effective_chat
    if not msg or not chat:
        return False'''),
    ("sub", "bot: _message_sender_is_admin caches lookups for a minute",
     r'''    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
    except Exception:
        return False
    return member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)''',
     r'''    key = (chat.id, msg.from_user.id)
    now = time.monotonic()
    cached = _admin_status_cache.get(key)
    if cached is not None and now - cached[0] < _ADMIN_CACHE_TTL_SECONDS:
        return cached[1]
    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
    except Exception:
        return False
    is_admin = member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)
    if len(_admin_status_cache) > 5000:
        _admin_status_cache.clear()
    _admin_status_cache[key] = (now, is_admin)
    return is_admin'''),

    # ---- blocklist: edits, captions, anonymous admins -------------------------
    ("sub", "bot: blocklist also sees edited messages and captions",
     r'''    msg = update.message
    if not msg or not msg.text or not msg.from_user or not update.effective_chat:''',
     r'''    msg = update.effective_message  # also fires for edited messages
    text = ((msg.text or msg.caption) if msg else None) or ""
    if not msg or not text or not msg.from_user or not update.effective_chat:'''),
    ("sub", "bot: blocklist exempts anonymous admins, checks admin only after a match",
     r'''a bad blocklist word must not be able to gag the mods.
    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return
    except Exception:
        pass

    matched = await asyncio.to_thread(dbmod.find_blocked_word, chat.id, msg.text)
    if not matched:
        return''',
     r'''a bad blocklist word must not be able to gag the mods.
    # (Checked only after a word matched, so ordinary messages cost no Telegram API call.)
    matched = await asyncio.to_thread(dbmod.find_blocked_word, chat.id, text)
    if not matched:
        return
    if await _message_sender_is_admin(update, context):  # includes anonymous admins
        return'''),
    ("sub", "bot: blocklist handler also listens to captions",
     r'''MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_text_check_blocklist),''',
     r'''MessageHandler(filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION) & ~filters.COMMAND, on_text_check_blocklist),'''),

    # ---- flood: anonymous admins ---------------------------------------------
    ("sub", "bot: flood control exempts anonymous admins too",
     r'''    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return
    except Exception:
        pass

    window = settings["flood_window_seconds"]''',
     r'''    # Exempt admins/owners, including anonymous "send as group" admins (cached for a minute).
    if await _message_sender_is_admin(update, context):
        return

    window = settings["flood_window_seconds"]'''),

    # ---- link lock: edited messages -------------------------------------------
    ("sub", "bot: link lock also sees edited messages",
     r'''    Registered in its own handler group (group=5)."""
    msg = update.message''',
     r'''    Registered in its own handler group (group=5). Also runs on edited messages, so a
    link can't be smuggled in by editing a clean message."""
    msg = update.effective_message'''),

    # ---- /warn in the mod log --------------------------------------------------
    ("sub", "bot: /warn is recorded in /modlog",
     r'''    # Now get the ACTIVE warning count (after the watermark)''',
     r'''    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "warn", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), reason,
    )

    # Now get the ACTIVE warning count (after the watermark)'''),
    ("sub", "bot: /modlog labels for new actions",
     r'''_MODLOG_ACTION_LABELS = {''',
     r'''_MODLOG_ACTION_LABELS = {
    "warn": "⚠️ Warn", "setstat": "✏️ Stat edit", "purge": "🧹 Purge",
    "pin": "📌 Pin", "unpin": "📌 Unpin",'''),

    # ---- filters ---------------------------------------------------------------
    ("sub", "bot: reject filter keywords that would corrupt the Mongo document",
     r'''    await asyncio.to_thread(dbmod.add_filter, chat.id, keyword, filter_data)''',
     r'''    if "." in keyword or keyword.startswith("$"):
        await _reply_autodelete(
            update, context,
            "Filter keywords can't contain a dot (.) or start with $. Use a plain word instead.",
        )
        return
    await asyncio.to_thread(dbmod.add_filter, chat.id, keyword, filter_data)'''),
    ("sub", "bot: skip text_mention entities that Telegram would reject when re-sent",
     r'''    entities = [_dict_to_entity(d) for d in (filter_data.get("entities") or [])] or None''',
     r'''    entities = [
        _dict_to_entity(d) for d in (filter_data.get("entities") or []) if d.get("type") != "text_mention"
    ] or None'''),

    # ---- /vcreport truncation note ---------------------------------------------
    ("sub", "bot: /vcreport remembers the real row count",
     r'''        truncated = len(rows) > MAX_ROWS
        if truncated:
            rows = rows[:MAX_ROWS]''',
     r'''        total_rows = len(rows)
        truncated = total_rows > MAX_ROWS
        if truncated:
            rows = rows[:MAX_ROWS]'''),
    ("sub", "bot: /vcreport 'more users' count was always 0",
     r'''{len(rows) - MAX_ROWS} more users not shown.''',
     r'''{total_rows - MAX_ROWS} more users not shown.'''),

    # ---- cooldowns --------------------------------------------------------------
    ("sub", "bot: cooldown helper",
     r'''TELEGRAM_SAFE_LEN = 3800''',
     BOT_COOLDOWN + "TELEGRAM_SAFE_LEN = 3800"),
] + [
    ("sub", f"bot: cooldown on /{cmd}",
     f"async def cmd_{cmd}(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:\n" + COOLDOWN_HEAD,
     f"async def cmd_{cmd}(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:\n" + COOLDOWN_HEAD
     + f'\n    if await _cooldown_blocked(update, context, "{cmd}"):\n        return')
    for cmd in ("vcreport", "attendance", "monthreport", "weekly", "xpleaderboard")
] + [
    # ---- new features -----------------------------------------------------------
    ("sub", "bot: welcome / purge / pin / backup code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_BLOCK + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: schedule the backup job",
     r'''    jq.run_daily(
        daily_streak_reset_job,
        time=dt_time(hour=0, minute=5, tzinfo=timezone.utc),
        name="daily_streak_reset_job",
    )''',
     r'''    jq.run_daily(
        daily_streak_reset_job,
        time=dt_time(hour=0, minute=5, tzinfo=timezone.utc),
        name="daily_streak_reset_job",
    )
    jq.run_daily(
        daily_backup_job,
        time=dt_time(hour=_backup_hour_utc(), minute=30, tzinfo=timezone.utc),
        name="daily_backup_job",
    )
    logger.info(
        "Scheduled database backup check daily at %02d:30 UTC (every %s day(s))",
        _backup_hour_utc(), _backup_interval_days(),
    )'''),
    ("sub", "bot: register new commands",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("setwelcome", cmd_setwelcome))
    app.add_handler(CommandHandler("welcome", cmd_welcome))
    app.add_handler(CommandHandler("resetwelcome", cmd_resetwelcome))
    app.add_handler(CommandHandler("purge", cmd_purge))
    app.add_handler(CommandHandler("pin", cmd_pin))
    app.add_handler(CommandHandler("unpin", cmd_unpin))
    app.add_handler(CommandHandler("backup", cmd_backup))'''),
    ("sub", "bot: /health shows the last backup",
     r'''    groq_key = (os.environ.get("GROQ_API_KEY") or "").strip()''',
     r'''    try:
        last_backup = await asyncio.to_thread(dbmod.get_last_backup_at)
        if last_backup:
            hours = int((datetime.now(timezone.utc) - last_backup).total_seconds() // 3600)
            lines.append(f"✅ Backup: last sent {hours}h ago")
        else:
            lines.append("⚠️ Backup: none sent yet (automatic, or run /backup)")
    except Exception as exc:
        lines.append(f"❌ Backup status: {html.escape(str(exc)[:200], quote=False)}")

    groq_key = (os.environ.get("GROQ_API_KEY") or "").strip()'''),
]

# ----------------------------------------------------------------------------
# Patch engine
# ----------------------------------------------------------------------------


def anchor_regex(anchor: str) -> re.Pattern:
    """Exact text, except trailing spaces on each line and whitespace-only lines may differ."""
    lines = anchor.split("\n")
    pieces = [re.escape(l.rstrip()) if l.strip() else "" for l in lines]
    return re.compile("[ \\t]*\n".join(pieces))


def _func_span(text: str, name: str) -> tuple[int, int] | None:
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            return start, node.end_lineno
    return None


def apply_ops(text: str, ops: list, label: str) -> tuple[str | None, list[str]]:
    """Returns (new_text, problems). new_text is None if any problem was found."""
    problems: list[str] = []
    original = text

    # Pass 1: every edit point must exist exactly once in the ORIGINAL file.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            n = len(list(anchor_regex(op[2]).finditer(original)))
            if n != 1:
                problems.append(f"{label}: '{name}' - edit point found {n} times (expected 1)")
        elif kind == "delete_line":
            n = sum(1 for l in original.split("\n") if op[2] in l)
            if n != 1:
                problems.append(f"{label}: '{name}' - line found {n} times (expected 1)")
        elif kind == "remove_func":
            if _func_span(original, op[2]) is None:
                problems.append(f"{label}: '{name}' - function {op[2]} not found")
    if problems:
        return None, problems

    # Pass 2: apply in order.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            pat, new = anchor_regex(op[2]), op[3]
            matches = list(pat.finditer(text))
            if len(matches) != 1:
                return None, [f"{label}: '{name}' - edit point changed during patching ({len(matches)} matches)"]
            m = matches[0]
            text = text[: m.start()] + new + text[m.end():]
        elif kind == "append":
            text = text.rstrip("\n") + "\n" + op[2]
        elif kind == "delete_line":
            lines = text.split("\n")
            lines = [l for l in lines if op[2] not in l]
            text = "\n".join(lines)
        elif kind == "remove_func":
            span = _func_span(text, op[2])
            if span is None:
                return None, [f"{label}: '{name}' - function vanished during patching"]
            start, end = span
            lines = text.split("\n")
            while end < len(lines) and not lines[end].strip():
                end += 1  # also swallow the blank lines after it
            text = "\n".join(lines[: start - 1] + lines[end:])
    try:
        compile(text, label, "exec")
    except SyntaxError as exc:
        return None, [f"{label}: patched code has a syntax error at line {exc.lineno}: {exc.msg}"]
    return text, []


def patch_file(path: Path, ops: list, marker: str, dry: bool) -> bool:
    raw = path.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    if marker in text:
        print(f"- {path.name}: already patched, skipping")
        return True
    new_text, problems = apply_ops(text, ops, path.name)
    if new_text is None:
        print(f"X {path.name}: NOT patched")
        for p in problems:
            print("    " + p)
        return False
    if dry:
        print(f"OK {path.name}: all {len(ops)} edit points found (dry run, nothing written)")
        return True
    shutil.copy2(path, path.with_name(path.name + ".bak"))
    if crlf:
        new_text = new_text.replace("\n", "\r\n")
    path.write_bytes(new_text.encode("utf-8"))
    print(f"OK {path.name}: patched ({len(ops)} edits), original saved as {path.name}.bak")
    return True


def main() -> int:
    dry = "--dry-run" in sys.argv
    here = Path(".")
    missing = [n for n in ("bot.py", "db.py") if not (here / n).exists()]
    if missing:
        print(f"Run this from the folder that contains bot.py and db.py (missing: {', '.join(missing)})")
        return 1

    # Check both files first so we never leave the project half-patched.
    results = []
    for name, ops, marker in (("db.py", DB_OPS, MARKER_DB), ("bot.py", BOT_OPS, MARKER_BOT)):
        raw = (here / name).read_bytes().decode("utf-8").replace("\r\n", "\n")
        if marker in raw:
            results.append(True)
            continue
        new_text, problems = apply_ops(raw, ops, name)
        if new_text is None:
            print(f"X {name}: cannot be patched")
            for p in problems:
                print("    " + p)
            results.append(False)
        else:
            results.append(True)
    if not all(results):
        print("\nNothing was changed. Send me the lines above and I'll adjust the script.")
        return 1

    for name, ops, marker in (("db.py", DB_OPS, MARKER_DB), ("bot.py", BOT_OPS, MARKER_BOT)):
        if not patch_file(here / name, ops, marker, dry):
            return 1
    if not dry:
        print("\nDone. Also copy restore_backup.py next to bot.py, then commit and redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
