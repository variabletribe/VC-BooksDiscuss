#!/usr/bin/env python3
"""
apply_rnr.py - adds the Read & Record (/rnr) feature to bot.py and db.py.

Run it AFTER apply_patches.py, from the folder that contains bot.py and db.py:

    python apply_rnr.py --dry-run   # only check that every edit point is found
    python apply_rnr.py             # patch both files

Same safety as before: all edit points are checked first (nothing changes if one is
missing), the result is syntax-checked, originals are kept as bot.py.bak / db.py.bak
(overwriting the earlier .bak), and running it twice is harmless.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

PREREQ_BOT = "# --- Per-user cooldown for heavy"
PREREQ_DB = "# Welcome messages (/setwelcome, /welcome, /resetwelcome)"
MARKER_BOT = "# Read & Record: /rnr, /myrnr"
MARKER_DB = "# Read & Record (/rnr)"

DB_RNR = r'''

# =============================================================================
# Read & Record (/rnr)
#
# One document per recorded audio in `rnr_records`:
#   _id "chat_id:message_id", serial (permanent per-chat number), user_id, display_name,
#   kind (voice|audio), duration, audio_at (when the audio was SENT), day ("YYYY-MM-DD" in
#   the R&R timezone), recorded_at (when /rnr was used), file_unique_id,
#   storage_chat_id / storage_msg_id (the copy in the private storage group), deleted.
# Rules: only the sender's own, non-forwarded audio; 30s to 4min; max 3 per user per
# rolling 24h; the same audio can't be recorded twice. Nothing is removed when a member
# leaves or is removed from the group, and /delrnr only soft-deletes.
# Days/streaks use RNR_TZ_OFFSET_MINUTES (default 330 = IST).
# =============================================================================

RNR_MIN_SECONDS = 30
RNR_MAX_SECONDS = 240
RNR_DAILY_LIMIT = 3

BADGES.update(
    {
        "rnr_5": {"label": "🎙️ Voice Starter", "desc": "Recorded 5 Read & Records"},
        "rnr_25": {"label": "🎧 Voice Regular", "desc": "Recorded 25 Read & Records"},
        "rnr_100": {"label": "🏆 Voice Legend", "desc": "Recorded 100 Read & Records"},
        "rnr_streak7": {"label": "📖 Reader's Week", "desc": "Recorded on 7 days in a row"},
        "rnr_streak30": {"label": "🌟 Reader's Month", "desc": "Recorded on 30 days in a row"},
    }
)
_RNR_TOTAL_BADGES = {5: "rnr_5", 25: "rnr_25", 100: "rnr_100"}
_RNR_STREAK_BADGES = {7: "rnr_streak7", 30: "rnr_streak30"}

_rnr_indexes_ready = False


def _rnr():
    global _rnr_indexes_ready
    coll = _coll("rnr_records")
    if not _rnr_indexes_ready:
        coll.create_index([("chat_id", ASCENDING), ("serial", ASCENDING)], unique=True)
        coll.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)])
        coll.create_index([("chat_id", ASCENDING), ("file_unique_id", ASCENDING)])
        coll.create_index([("user_id", ASCENDING), ("recorded_at", DESCENDING)])
        _rnr_indexes_ready = True
    return coll


def _utc(dt: datetime | None) -> datetime | None:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def rnr_tz_offset() -> timedelta:
    try:
        return timedelta(minutes=int(os.getenv("RNR_TZ_OFFSET_MINUTES", "330")))
    except ValueError:
        return timedelta(minutes=330)


def rnr_day_of(dt: datetime) -> str:
    """'YYYY-MM-DD' of this moment in the R&R timezone."""
    return (_utc(dt) + rnr_tz_offset()).strftime("%Y-%m-%d")


def rnr_today() -> str:
    return rnr_day_of(datetime.now(timezone.utc))


def rnr_date_label(dt: datetime | None) -> str:
    """'14 Oct 2026' in the R&R timezone."""
    if dt is None:
        return "-"
    return (_utc(dt) + rnr_tz_offset()).strftime("%d %b %Y")


def rnr_streaks(days, today: str | None = None) -> tuple[int, int]:
    """(current_streak, longest_streak) from 'YYYY-MM-DD' strings. A streak is still
    'current' if the last recording was today or yesterday."""
    if not days:
        return 0, 0
    ordinals = sorted({datetime.strptime(d, "%Y-%m-%d").date().toordinal() for d in days})
    longest = run = 0
    prev = None
    for o in ordinals:
        run = run + 1 if prev is not None and o == prev + 1 else 1
        longest = max(longest, run)
        prev = o
    today_o = datetime.strptime(today or rnr_today(), "%Y-%m-%d").date().toordinal()
    current = run if prev is not None and today_o - prev <= 1 else 0
    return current, longest


def _rnr_next_serial(chat_id: int) -> int:
    doc = _coll("rnr_counters").find_one_and_update(
        {"_id": chat_id}, {"$inc": {"seq": 1}}, upsert=True, return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])


def rnr_user_progress(chat_id: int, user_id: int) -> tuple[int, int, int]:
    """(total, current_streak, longest_streak) for one user in one chat."""
    docs = list(
        _rnr().find({"chat_id": chat_id, "user_id": user_id, "deleted": {"$ne": True}}, {"day": 1})
    )
    cur, lng = rnr_streaks({d["day"] for d in docs})
    return len(docs), cur, lng


def rnr_precheck(chat_id: int, user_id: int, msg_id: int, file_unique_id: str, now: datetime):
    """('ok', None) | ('duplicate', existing_doc) | ('limit', {'free_at': datetime})."""
    coll = _rnr()
    dup = coll.find_one(
        {
            "chat_id": chat_id,
            "deleted": {"$ne": True},
            "$or": [{"_id": f"{chat_id}:{msg_id}"}, {"file_unique_id": file_unique_id}],
        }
    )
    if dup:
        return "duplicate", dup
    recent = list(
        coll.find(
            {
                "user_id": user_id,
                "deleted": {"$ne": True},
                "recorded_at": {"$gte": _utc(now) - timedelta(hours=24)},
            }
        ).sort("recorded_at", ASCENDING)
    )
    if len(recent) >= RNR_DAILY_LIMIT:
        oldest_in_window = recent[len(recent) - RNR_DAILY_LIMIT]
        return "limit", {"free_at": _utc(oldest_in_window["recorded_at"]) + timedelta(hours=24)}
    return "ok", None


def rnr_insert(
    chat_id: int,
    user_id: int,
    display_name: str,
    msg_id: int,
    file_unique_id: str,
    kind: str,
    duration: int,
    audio_at: datetime,
    storage_chat_id: int,
    storage_msg_id: int,
    now: datetime,
) -> dict:
    """Save the record and return {serial, total, current_streak, longest_streak, badges}."""
    coll = _rnr()
    before_total, before_streak, _ = rnr_user_progress(chat_id, user_id)
    fields = {
        "chat_id": chat_id,
        "user_id": user_id,
        "display_name": display_name[:512],
        "kind": kind,
        "duration": int(duration),
        "audio_at": _utc(audio_at),
        "day": rnr_day_of(audio_at),
        "recorded_at": _utc(now),
        "file_unique_id": file_unique_id,
        "storage_chat_id": storage_chat_id,
        "storage_msg_id": storage_msg_id,
        "deleted": False,
    }
    doc_id = f"{chat_id}:{msg_id}"
    existing = coll.find_one({"_id": doc_id})
    if existing:  # soft-deleted earlier by an admin: bring it back under its old number
        serial = int(existing["serial"])
        coll.update_one({"_id": doc_id}, {"$set": fields})
    else:
        serial = _rnr_next_serial(chat_id)
        coll.insert_one({"_id": doc_id, "serial": serial, "msg_id": msg_id, **fields})

    total, cur, lng = rnr_user_progress(chat_id, user_id)
    earned: list[BadgeEarned] = []
    for n, bid in _RNR_TOTAL_BADGES.items():
        if before_total < n <= total:
            meta = BADGES[bid]
            earned.append(BadgeEarned(user_id, display_name, bid, meta["label"], meta["desc"],
                                      award_badge(chat_id, user_id, display_name, bid)))
    for n, bid in _RNR_STREAK_BADGES.items():
        if before_streak < n <= cur:
            meta = BADGES[bid]
            earned.append(BadgeEarned(user_id, display_name, bid, meta["label"], meta["desc"],
                                      award_badge(chat_id, user_id, display_name, bid)))
    return {"serial": serial, "total": total, "current_streak": cur, "longest_streak": lng, "badges": earned}


def rnr_user_summary(chat_id: int, user_id: int) -> dict:
    docs = list(
        _rnr().find(
            {"chat_id": chat_id, "user_id": user_id, "deleted": {"$ne": True}}, {"day": 1, "audio_at": 1}
        )
    )
    if not docs:
        return {"total": 0, "first_at": None, "last_at": None, "current_streak": 0, "longest_streak": 0}
    times = [_utc(d["audio_at"]) for d in docs]
    cur, lng = rnr_streaks({d["day"] for d in docs})
    return {
        "total": len(docs),
        "first_at": min(times),
        "last_at": max(times),
        "current_streak": cur,
        "longest_streak": lng,
    }


def format_rnr_stats_lines(s: dict) -> str:
    """Extra lines for /mystats (HTML)."""
    if not s["total"]:
        return "🎙️ Read &amp; Record: <b>0</b> (reply /rnr to your voice note to start)"
    return (
        f"🎙️ Read &amp; Record: <b>{s['total']}</b> recording(s)\n"
        f"📅 First: <b>{rnr_date_label(s['first_at'])}</b> · Latest: <b>{rnr_date_label(s['last_at'])}</b>\n"
        f"🔥 R&amp;R streak: <b>{s['current_streak']}</b> day(s) · best <b>{s['longest_streak']}</b>"
    )


def rnr_list_for_user(user_id: int) -> list[dict]:
    """All of a user's records (any chat), newest audio first."""
    return list(_rnr().find({"user_id": user_id, "deleted": {"$ne": True}}).sort("audio_at", DESCENDING))


def rnr_get(chat_id: int, serial: int) -> dict | None:
    return _rnr().find_one({"chat_id": chat_id, "serial": serial, "deleted": {"$ne": True}})


def rnr_soft_delete(chat_id: int, serial: int, by_id: int) -> dict | None:
    """Hide a record (and free its audio to be recorded again). The storage copy is kept."""
    return _rnr().find_one_and_update(
        {"chat_id": chat_id, "serial": serial, "deleted": {"$ne": True}},
        {"$set": {"deleted": True, "deleted_by": by_id, "deleted_at": datetime.now(timezone.utc)}},
    )


def _rnr_table(chat_id: int) -> dict[int, dict]:
    table: dict[int, dict] = {}
    cursor = _rnr().find(
        {"chat_id": chat_id, "deleted": {"$ne": True}}, {"user_id": 1, "display_name": 1, "day": 1}
    )
    for d in cursor:
        u = table.setdefault(
            int(d["user_id"]),
            {"user_id": int(d["user_id"]), "name": "", "total": 0, "day_counts": {}},
        )
        u["total"] += 1
        u["name"] = d.get("display_name") or u["name"]
        u["day_counts"][d["day"]] = u["day_counts"].get(d["day"], 0) + 1
    for u in table.values():
        u["current_streak"], u["longest_streak"] = rnr_streaks(set(u["day_counts"]))
    return table


def rnr_leaderboard(chat_id: int, limit: int = 20) -> list[dict]:
    rows = list(_rnr_table(chat_id).values())
    rows.sort(key=lambda u: (-u["total"], -u["current_streak"], u["name"].lower()))
    return rows[:limit]


def rnr_recorders_on_day(chat_id: int, day: str) -> list[dict]:
    """Everyone who recorded (audio sent) on `day`, with that day's count, streak and total."""
    rows = []
    for u in _rnr_table(chat_id).values():
        if day in u["day_counts"]:
            rows.append({**u, "count": u["day_counts"][day]})
    rows.sort(key=lambda u: (-u["count"], -u["current_streak"], -u["total"], u["name"].lower()))
    return rows


def rnr_recorders_in_days(chat_id: int, days: list[str]) -> list[dict]:
    rows = []
    for u in _rnr_table(chat_id).values():
        count = sum(u["day_counts"].get(d, 0) for d in days)
        if count:
            rows.append({**u, "count": count})
    rows.sort(key=lambda u: (-u["count"], -u["current_streak"], -u["total"], u["name"].lower()))
    return rows


def rnr_chat_ids() -> list[int]:
    return [int(c) for c in _rnr().distinct("chat_id")]


def get_rnr_reports_enabled(chat_id: int) -> bool:
    doc = _coll("chat_settings").find_one({"_id": chat_id}) or {}
    return bool(doc.get("rnr_reports", True))


def set_rnr_reports(chat_id: int, enabled: bool) -> None:
    _coll("chat_settings").update_one(
        {"_id": chat_id},
        {"$set": {"rnr_reports": enabled}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def rnr_posted(key: str) -> str | None:
    """The day string a daily/weekly R&R post was last sent for (dedupes restarts)."""
    doc = _coll("meta").find_one({"_id": key})
    return doc.get("day") if doc else None


def rnr_set_posted(key: str, day: str) -> None:
    _coll("meta").update_one({"_id": key}, {"$set": {"day": day}}, upsert=True)
'''

BOT_RNR = r'''# =============================================================================
# Read & Record: /rnr, /myrnr, /rnrboard, /delrnr, /rnrreports
#
# A member replies /rnr to THEIR OWN voice note / audio (30s - 4min). The bot copies it
# into the private storage group (RNR_STORAGE_CHAT_ID), numbers it (RNR #n) and records
# who/when. Members fetch their recordings later with /myrnr in a private chat with the bot.
#   RNR_STORAGE_CHAT_ID      private group that stores the audio copies
#   RNR_TZ_OFFSET_MINUTES    day boundary for streaks/reports (default 330 = IST)
#   RNR_MORNING_HOUR         local hour the daily post goes out (default 8)
# =============================================================================

_RNR_STORAGE_DEFAULT = -5469935473
_COMMAND_COOLDOWNS["rnrboard"] = 20
_rnr_lock = asyncio.Lock()  # one /rnr at a time, so a double-tap can't save an audio twice
_RNR_PAGE_SIZE = 8


def _rnr_storage_chat_id() -> int:
    raw = (os.environ.get("RNR_STORAGE_CHAT_ID") or "").strip()
    try:
        return int(raw) if raw else _RNR_STORAGE_DEFAULT
    except ValueError:
        return _RNR_STORAGE_DEFAULT


def _rnr_morning_hour() -> int:
    try:
        return min(17, max(0, int(os.getenv("RNR_MORNING_HOUR", "8"))))
    except ValueError:
        return 8


async def cmd_rnr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reply to your own voice note / audio with /rnr to record it."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat, user = update.effective_chat, update.effective_user
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use /rnr in the group, as a reply to your voice note.")
        return
    reply = update.message.reply_to_message
    media = (reply.voice or reply.audio) if reply is not None else None
    if reply is None or media is None:
        await _reply_autodelete(update, context, "Reply to your own voice note or audio with /rnr.")
        return
    if getattr(reply, "forward_origin", None) is not None:
        await _reply_autodelete(update, context, "Forwarded audio can't be recorded. Record it yourself and reply /rnr to it.")
        return
    if not reply.from_user or reply.from_user.id != user.id:
        await _reply_autodelete(update, context, "You can only record your own audio. Reply /rnr to a voice note you sent.")
        return

    duration = int(media.duration or 0)
    if duration < dbmod.RNR_MIN_SECONDS:
        await _reply_autodelete(
            update, context,
            f"Too short ({_format_duration(duration)}). A Read & Record must be at least "
            f"{dbmod.RNR_MIN_SECONDS}s.",
        )
        return
    if duration > dbmod.RNR_MAX_SECONDS:
        await _reply_autodelete(
            update, context,
            f"Too long ({_format_duration(duration)}). A Read & Record can be at most "
            f"{dbmod.RNR_MAX_SECONDS // 60} minutes.",
        )
        return

    kind = "voice" if reply.voice else "audio"
    label = _user_label(user)
    now = datetime.now(timezone.utc)
    audio_at = reply.date or now

    async with _rnr_lock:
        status, info = await asyncio.to_thread(
            dbmod.rnr_precheck, chat.id, user.id, reply.message_id, media.file_unique_id, now
        )
        if status == "duplicate":
            await _reply_autodelete(
                update, context,
                f"Already recorded as RNR #{info['serial']} ({dbmod.rnr_date_label(info.get('audio_at'))}).",
            )
            return
        if status == "limit":
            wait = max(1, int((info["free_at"] - now).total_seconds()))
            await _reply_autodelete(
                update, context,
                f"Limit reached: {dbmod.RNR_DAILY_LIMIT} recordings per 24 hours. "
                f"Try again in {_format_duration(wait)}.",
            )
            return

        storage = _rnr_storage_chat_id()
        caption = (
            f"RNR from {label} (id {user.id}) | sent {audio_at.strftime('%d %b %Y %H:%M')} UTC"
            f" | {chat.title or chat.id}"
        )[:1000]
        try:
            copied = await context.bot.copy_message(storage, chat.id, reply.message_id, caption=caption)
        except Exception:
            logger.exception("RNR: copying to storage chat %s failed", storage)
            await _reply_autodelete(
                update, context,
                "I couldn't save your audio to the storage group, so nothing was recorded. "
                "An admin needs to check that I'm a member of it.",
            )
            return
        result = await asyncio.to_thread(
            dbmod.rnr_insert, chat.id, user.id, label, reply.message_id, media.file_unique_id,
            kind, duration, audio_at, storage, copied.message_id, now,
        )

    safe = html.escape(label, quote=False)
    await _reply_autodelete(
        update, context,
        f"🎙️ <b>Recorded as RNR #{result['serial']}</b> — {safe}\n"
        f"📅 Audio sent: {dbmod.rnr_date_label(audio_at)} · ⏱️ {_format_duration(duration)}\n"
        f"🔥 Streak: <b>{result['current_streak']}</b> day(s) · Total: <b>{result['total']}</b>",
        parse_mode="HTML",
    )
    if result["badges"]:
        try:  # badge announcements stay (they are not auto-deleted like command replies)
            await context.bot.send_message(chat.id, _format_badges_earned_html(result["badges"]), parse_mode="HTML")
        except Exception:
            logger.debug("RNR badge announcement failed chat_id=%s", chat.id)


def _rnr_list_view(docs: list[dict], page: int) -> tuple[str, InlineKeyboardMarkup]:
    pages = max(1, (len(docs) + _RNR_PAGE_SIZE - 1) // _RNR_PAGE_SIZE)
    page = min(max(page, 0), pages - 1)
    chunk = docs[page * _RNR_PAGE_SIZE : (page + 1) * _RNR_PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(
            f"#{d['serial']} · {dbmod.rnr_date_label(d.get('audio_at'))} · {_format_duration(int(d.get('duration') or 0))}",
            callback_data=f"rnr:get:{d['chat_id']}:{d['serial']}",
        )]
        for d in chunk
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀ Newer", callback_data=f"rnr:pg:{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("Older ▶", callback_data=f"rnr:pg:{page + 1}"))
    if nav:
        rows.append(nav)
    text = (
        f"🎙️ <b>Your Read &amp; Record</b> — {len(docs)} recording(s)\n"
        f"Page {page + 1}/{pages}. Tap one and I'll send you the audio."
    )
    return text, InlineKeyboardMarkup(rows)


async def cmd_myrnr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """In a private chat with the bot: list your recordings with buttons to fetch each."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        uname = context.bot.username or "the bot"
        await _reply_autodelete(update, context, f"Send /myrnr to me in a private chat: @{uname}")
        return
    docs = await asyncio.to_thread(dbmod.rnr_list_for_user, update.effective_user.id)
    if not docs:
        await update.message.reply_text(
            "You have no Read & Record yet. In the group, reply /rnr to your voice note."
        )
        return
    text, keyboard = _rnr_list_view(docs, 0)
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=keyboard)


async def on_rnr_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return
    parts = query.data.split(":")
    try:
        if len(parts) == 3 and parts[1] == "pg":
            docs = await asyncio.to_thread(dbmod.rnr_list_for_user, query.from_user.id)
            if not docs:
                await query.answer("No recordings found.", show_alert=True)
                return
            text, keyboard = _rnr_list_view(docs, int(parts[2]))
            await query.answer()
            try:
                await query.edit_message_text(text, parse_mode="HTML", reply_markup=keyboard)
            except Exception:
                pass
            return
        if len(parts) == 4 and parts[1] == "get":
            chat_id, serial = int(parts[2]), int(parts[3])
            doc = await asyncio.to_thread(dbmod.rnr_get, chat_id, serial)
            if not doc or (doc["user_id"] != query.from_user.id and not _is_admin_user(query.from_user.id)):
                await query.answer("That recording wasn't found.", show_alert=True)
                return
            await context.bot.copy_message(
                chat_id=query.from_user.id,
                from_chat_id=doc["storage_chat_id"],
                message_id=doc["storage_msg_id"],
                caption=f"RNR #{serial} · {dbmod.rnr_date_label(doc.get('audio_at'))}",
            )
            await query.answer("Sending your audio...")
            return
    except Exception:
        logger.exception("RNR callback failed data=%s", query.data)
        await query.answer("Couldn't fetch that audio. Please tell an admin.", show_alert=True)
        return
    await query.answer()


def _rnr_row_line(i: int, u: dict, count_label: str, show_total: bool = True) -> str:
    medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
    safe = html.escape(u["name"] or str(u["user_id"]), quote=False)
    line = f"{medal} {safe} — <b>{u['count']}</b> {count_label} · 🔥 {u['current_streak']}"
    return line + (f" · total {u['total']}" if show_total else "")


async def cmd_rnrboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """All-time Read & Record leaderboard."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if await _cooldown_blocked(update, context, "rnrboard"):
        return
    rows = await asyncio.to_thread(dbmod.rnr_leaderboard, chat.id, 20)
    if not rows:
        await _reply_autodelete(update, context, "No Read & Record yet. Reply /rnr to your voice note to start.")
        return
    lines = ["🎙️ <b>Read &amp; Record leaderboard</b>", "<i>🔥 = current streak</i>", ""]
    for i, u in enumerate(rows, start=1):
        lines.append(_rnr_row_line(i, {**u, "count": u["total"]}, "recordings", show_total=False))
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_delrnr(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: /delrnr <number> - hide a wrong record (the audio copy is kept)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can delete a record.")
        return
    try:
        serial = int((context.args or [""])[0].lstrip("#"))
    except ValueError:
        await _reply_autodelete(update, context, "Usage: /delrnr <number>   e.g. /delrnr 12")
        return
    doc = await asyncio.to_thread(dbmod.rnr_soft_delete, chat.id, serial, update.effective_user.id)
    if doc is None:
        await _reply_autodelete(update, context, f"No active record RNR #{serial}.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "delrnr", int(doc["user_id"]), str(doc.get("display_name", "")),
        update.effective_user.id, _user_label(update.effective_user), f"RNR #{serial}",
    )
    await _reply_autodelete(
        update, context,
        f"RNR #{serial} removed from the records. The saved audio copy is kept, and the "
        "member can record that audio again.",
    )


async def cmd_rnrreports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: /rnrreports on|off - the daily/weekly Read & Record posts."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    enabled = await asyncio.to_thread(dbmod.get_rnr_reports_enabled, chat.id)
    if not context.args:
        await _reply_autodelete(
            update, context,
            f"Daily/weekly Read & Record posts are {'on' if enabled else 'off'}.\nUsage: /rnrreports on|off",
        )
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this.")
        return
    arg = context.args[0].lower()
    if arg not in ("on", "off"):
        await _reply_autodelete(update, context, "Usage: /rnrreports on|off")
        return
    await asyncio.to_thread(dbmod.set_rnr_reports, chat.id, arg == "on")
    await _reply_autodelete(update, context, f"Daily/weekly Read & Record posts turned {arg}.")


def _format_rnr_daily(day: str, rows: list[dict]) -> str:
    label = datetime.strptime(day, "%Y-%m-%d").strftime("%d %b")
    lines = [
        f"🌅 <b>Read &amp; Record — {label}</b>",
        f"<i>{len(rows)} member(s) recorded · 🔥 = current streak</i>",
        "",
    ]
    for i, u in enumerate(rows, start=1):
        lines.append(_rnr_row_line(i, u, "recording" if u["count"] == 1 else "recordings"))
    lines += ["", "<i>Reply /rnr to your voice note to join today's list.</i>"]
    return "\n".join(lines)


def _format_rnr_weekly(days: list[str], rows: list[dict]) -> str:
    start = datetime.strptime(days[0], "%Y-%m-%d").strftime("%d %b")
    end = datetime.strptime(days[-1], "%Y-%m-%d").strftime("%d %b")
    total = sum(u["count"] for u in rows)
    lines = [
        f"📅 <b>Weekly Read &amp; Record — {start} to {end}</b>",
        f"<i>{len(rows)} member(s) recorded · {total} recording(s) in total</i>",
        "",
    ]
    for i, u in enumerate(rows, start=1):
        lines.append(_rnr_row_line(i, u, "this week"))
    return "\n".join(lines)


async def rnr_morning_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Checked every 30 minutes. Each morning (local time) posts yesterday's recorders with
    streak and total; on Mondays also the full list of everyone who recorded last week.
    Progress is stored in MongoDB, so restarts never double-post, and a late restart still
    catches up the same morning."""
    try:
        local = datetime.now(timezone.utc) + dbmod.rnr_tz_offset()
        start_hour = _rnr_morning_hour()
        if not (start_hour <= local.hour < start_hour + 6):
            return
        today = local.strftime("%Y-%m-%d")
        yesterday = (local - timedelta(days=1)).strftime("%Y-%m-%d")
        week_days = [(local - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, 0, -1)]
        chat_ids = await asyncio.to_thread(dbmod.rnr_chat_ids)
    except Exception:
        logger.exception("RNR morning job setup failed")
        return

    for chat_id in chat_ids:
        try:
            if not await asyncio.to_thread(dbmod.get_rnr_reports_enabled, chat_id):
                continue
            daily_key = f"rnr_daily:{chat_id}"
            if await asyncio.to_thread(dbmod.rnr_posted, daily_key) != today:
                rows = await asyncio.to_thread(dbmod.rnr_recorders_on_day, chat_id, yesterday)
                if rows:
                    for chunk in _split_for_telegram(_format_rnr_daily(yesterday, rows)):
                        await context.bot.send_message(chat_id, chunk, parse_mode="HTML")
                await asyncio.to_thread(dbmod.rnr_set_posted, daily_key, today)
            weekly_key = f"rnr_weekly:{chat_id}"
            if local.weekday() == 0 and await asyncio.to_thread(dbmod.rnr_posted, weekly_key) != today:
                rows = await asyncio.to_thread(dbmod.rnr_recorders_in_days, chat_id, week_days)
                if rows:
                    for chunk in _split_for_telegram(_format_rnr_weekly(week_days, rows)):
                        await context.bot.send_message(chat_id, chunk, parse_mode="HTML")
                await asyncio.to_thread(dbmod.rnr_set_posted, weekly_key, today)
        except Exception:
            logger.exception("RNR morning post failed chat_id=%s", chat_id)


'''

DB_OPS = [("append", "db: Read & Record functions", DB_RNR)]

BOT_OPS = [
    ("sub", "bot: Read & Record help category",
     r'''HELP_CATEGORIES: dict[str, str] = {''',
     r'''HELP_CATEGORIES: dict[str, str] = {
    "rnr": "🎙️ Read & Record",'''),
    ("sub", "bot: Read & Record help entries",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "rnr": ("rnr", "/rnr  (reply to your own voice note or audio)", "Record a Read & Record. Your audio (30s to 4 min) is saved with the date it was sent. Each audio counts once, max 3 recordings per 24 hours, forwarded audio doesn't count.", "Anyone"),
    "myrnr": ("rnr", "/myrnr  (in a private chat with the bot)", "List all your recordings with dates. Tap one to get the audio back.", "Anyone"),
    "rnrboard": ("rnr", "/rnrboard", "Read & Record leaderboard with streaks.", "Anyone"),
    "delrnr": ("rnr", "/delrnr <number>", "Remove a wrong record. The saved audio copy is kept.", "Group admin"),
    "rnrreports": ("rnr", "/rnrreports on|off", "Switch the morning post and the Monday weekly list on or off.", "Anyone can view; group admin to change"),'''),
    ("sub", "bot: /mystats shows Read & Record",
     r'''    await _reply_autodelete(update, context, dbmod.format_my_stats_message(stats), parse_mode="HTML")''',
     r'''    text = dbmod.format_my_stats_message(stats)
    try:
        rnr = await asyncio.to_thread(dbmod.rnr_user_summary, chat.id, user.id)
        text += "\n\n" + dbmod.format_rnr_stats_lines(rnr)
    except Exception:
        logger.exception("R&R stats failed for /mystats chat_id=%s", chat.id)
    await _reply_autodelete(update, context, text, parse_mode="HTML")'''),
    ("sub", "bot: Read & Record code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_RNR + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: register Read & Record commands",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("rnr", cmd_rnr))
    app.add_handler(CommandHandler("myrnr", cmd_myrnr))
    app.add_handler(CommandHandler("rnrboard", cmd_rnrboard))
    app.add_handler(CommandHandler("delrnr", cmd_delrnr))
    app.add_handler(CommandHandler("rnrreports", cmd_rnrreports))
    app.add_handler(CallbackQueryHandler(on_rnr_callback, pattern=r"^rnr:"))'''),
    ("sub", "bot: schedule the morning Read & Record post",
     r'''        name="daily_backup_job",
    )''',
     r'''        name="daily_backup_job",
    )
    jq.run_repeating(rnr_morning_job, interval=1800, first=90, name="rnr_morning_job")'''),
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
    texts = {n: (here / n).read_bytes().decode("utf-8").replace("\r\n", "\n") for n in ("bot.py", "db.py")}
    if PREREQ_BOT not in texts["bot.py"] or PREREQ_DB not in texts["db.py"]:
        print("bot.py / db.py don't have the earlier upgrade yet. Run apply_patches.py first, then this script.")
        return 1

    jobs = (("db.py", DB_OPS, MARKER_DB), ("bot.py", BOT_OPS, MARKER_BOT))
    ok = True
    for name, ops, marker in jobs:
        if marker in texts[name]:
            continue
        new_text, problems = apply_ops(texts[name], ops, name)
        if new_text is None:
            ok = False
            print(f"X {name}: cannot be patched")
            for p in problems:
                print("    " + p)
    if not ok:
        print("\nNothing was changed. Send me the lines above and I'll adjust the script.")
        return 1
    for name, ops, marker in jobs:
        if not patch_file(here / name, ops, marker, dry):
            return 1
    if not dry:
        print("\nDone. Commit and push bot.py and db.py, then let Render redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())