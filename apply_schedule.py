#!/usr/bin/env python3
"""
apply_schedule.py - schedule posts for later: /schedule, /scheduled, /unschedule.

Run from the folder that contains bot.py and db.py:

    python apply_schedule.py --dry-run
    python apply_schedule.py

A bot admin sends (or forwards) a post to the bot in a private chat and replies to it with
    /schedule 3/10/2026 8am
and the bot publishes it to the group at that time. Same safety as the other scripts.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "# Scheduled posts: a bot admin replies to a post"
MARKER_DB = "# Scheduled posts (/schedule, /scheduled, /unschedule)"

DB_SCHED = r'''

# =============================================================================
# Scheduled posts (/schedule, /scheduled, /unschedule)
# One document per queued post: the content is stored like filters
# ({"type", "text", "entities", "file_id"}), so it survives the admin deleting the original.
# status: scheduled -> sending -> sent | failed, or cancelled.
# =============================================================================

_sched_ready = False


def _sched():
    global _sched_ready
    coll = _coll("scheduled_posts")
    if not _sched_ready:
        coll.create_index([("status", ASCENDING), ("send_at", ASCENDING)])
        _sched_ready = True
    return coll


def sched_create(chat_id: int, chat_title: str, data: dict, send_at: datetime, by_id: int, by_name: str) -> int:
    counter = _coll("sched_counters").find_one_and_update(
        {"_id": "posts"}, {"$inc": {"seq": 1}}, upsert=True, return_document=ReturnDocument.AFTER
    )
    serial = int(counter["seq"])
    if send_at.tzinfo is None:
        send_at = send_at.replace(tzinfo=timezone.utc)
    _sched().insert_one(
        {
            "_id": serial,
            "serial": serial,
            "chat_id": chat_id,
            "chat_title": (chat_title or "")[:200],
            "data": data,
            "send_at": send_at,
            "by_id": by_id,
            "by_name": by_name[:512],
            "status": "scheduled",
            "created_at": datetime.now(timezone.utc),
        }
    )
    return serial


def sched_pending() -> list[dict]:
    return list(_sched().find({"status": "scheduled"}).sort("send_at", ASCENDING))


def sched_cancel(serial: int) -> dict | None:
    return _sched().find_one_and_update(
        {"_id": serial, "status": "scheduled"}, {"$set": {"status": "cancelled"}}
    )


def sched_due(now: datetime) -> list[dict]:
    return list(_sched().find({"status": "scheduled", "send_at": {"$lte": now}}).sort("send_at", ASCENDING).limit(20))


def sched_claim(serial: int) -> bool:
    """Scheduled -> sending, atomically, so a post can never go out twice."""
    return _sched().find_one_and_update({"_id": serial, "status": "scheduled"}, {"$set": {"status": "sending"}}) is not None


def sched_finish(serial: int, status: str, error: str = "") -> None:
    _sched().update_one(
        {"_id": serial},
        {"$set": {"status": status, "error": error[:300], "finished_at": datetime.now(timezone.utc)}},
    )
'''

BOT_SCHED = r'''# =============================================================================
# Scheduled posts: a bot admin replies to a post in a PRIVATE chat with the bot:
#     /schedule 3/10/2026 8am          (day/month/year, group time)
# and the bot publishes that post to the group at that moment. Queue as many as you like.
#     /scheduled   lists what is waiting      /unschedule 3   cancels #3
# Group time is RNR_TZ_OFFSET_MINUTES (default 330 = IST). Stored in MongoDB, so restarts
# don't lose anything; a post that came due while the bot was down goes out when it is back.
# =============================================================================

_SCHED_MAX_DAYS = 366
_SCHED_DATE_RE = re.compile(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})")
_SCHED_ISO_RE = re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})")
_SCHED_TIME_RE = re.compile(r"(?<![\d/\-.])(\d{1,2})(?:[:.](\d{2}))?\s*(am|pm)?(?![\d/\-.])", re.IGNORECASE)


def _sched_offset() -> timedelta:
    try:
        return timedelta(minutes=int(os.getenv("RNR_TZ_OFFSET_MINUTES", "330")))
    except ValueError:
        return timedelta(minutes=330)


def _sched_tz_label() -> str:
    minutes = int(_sched_offset().total_seconds() // 60)
    if minutes == 330:
        return "IST"
    sign = "+" if minutes >= 0 else "-"
    return f"UTC{sign}{abs(minutes) // 60:02d}:{abs(minutes) % 60:02d}"


def _parse_schedule_when(text: str, now: datetime) -> tuple[datetime | None, str]:
    """(send_time_in_UTC, error). Accepts '3/10/2026 8am', '03-10-2026 20:30', '2026-10-03 8.30pm',
    'tomorrow 7pm', 'today 9pm'. Dates are day/month/year, times are in group time."""
    t = text.strip().lower()
    local_now = now + _sched_offset()
    day = None
    m = _SCHED_ISO_RE.search(t)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        t = t[: m.start()] + " " + t[m.end():]
    else:
        m = _SCHED_DATE_RE.search(t)
        if m:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            t = t[: m.start()] + " " + t[m.end():]
        elif "tomorrow" in t:
            day = (local_now + timedelta(days=1)).date()
            t = t.replace("tomorrow", " ")
        elif "today" in t:
            day = local_now.date()
            t = t.replace("today", " ")
        else:
            return None, "I need a date, like 3/10/2026 (day/month/year), or the word tomorrow."
    if day is None:
        try:
            day = datetime(y, mo, d).date()
        except ValueError:
            return None, "That date doesn't exist. Use day/month/year, like 3/10/2026."

    tm = _SCHED_TIME_RE.search(t)
    if not tm:
        return None, "I need a time too, like 8am, 8:30pm or 20:00."
    hour, minute, ampm = int(tm.group(1)), int(tm.group(2) or 0), (tm.group(3) or "").lower()
    if minute > 59 or (ampm and not 1 <= hour <= 12) or (not ampm and hour > 23):
        return None, "That time doesn't look right. Use 8am, 8:30pm or 20:00."
    if ampm == "pm" and hour != 12:
        hour += 12
    if ampm == "am" and hour == 12:
        hour = 0

    local_dt = datetime(day.year, day.month, day.day, hour, minute)
    when = (local_dt - _sched_offset()).replace(tzinfo=timezone.utc)
    if when <= now + timedelta(seconds=30):
        return None, f"That time has already passed (it is {local_now.strftime('%d %b %Y, %H:%M')} {_sched_tz_label()} now)."
    if when > now + timedelta(days=_SCHED_MAX_DAYS):
        return None, "That is more than a year away. Pick a closer date."
    return when, ""


async def _send_data_or_raise(bot, chat_id: int, data: dict):
    """Send stored post content (same shape as filters). Unlike _send_filter_response this
    raises on failure, so the scheduler can tell the admin."""
    ftype = data.get("type", "text")
    text = data.get("text")
    entities = [
        _dict_to_entity(d) for d in (data.get("entities") or []) if d.get("type") != "text_mention"
    ] or None
    file_id = data.get("file_id")
    if ftype == "text":
        return await bot.send_message(chat_id, text or "", entities=entities)
    senders = {
        "photo": ("send_photo", "photo"), "video": ("send_video", "video"),
        "animation": ("send_animation", "animation"), "document": ("send_document", "document"),
        "voice": ("send_voice", "voice"), "audio": ("send_audio", "audio"),
    }
    if ftype in senders:
        method, arg = senders[ftype]
        return await getattr(bot, method)(chat_id, **{arg: file_id}, caption=text, caption_entities=entities)
    if ftype == "sticker":
        return await bot.send_sticker(chat_id, sticker=file_id)
    if ftype == "video_note":
        return await bot.send_video_note(chat_id, video_note=file_id)
    raise ValueError(f"unsupported post type {ftype}")


def _sched_when_label(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return f"{(dt + _sched_offset()).strftime('%d %b %Y, %H:%M')} {_sched_tz_label()}"


def _sched_in_label(seconds: int) -> str:
    """'12 days 14 h', '3 h 20 min', 'under a minute' - friendlier than 302h 30m."""
    days, rem = divmod(max(0, int(seconds)), 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    parts = []
    if days:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours:
        parts.append(f"{hours} h")
    if minutes and not days:
        parts.append(f"{minutes} min")
    return " ".join(parts) or "under a minute"


def _sched_preview(data: dict) -> str:
    kind = data.get("type", "text")
    text = (data.get("text") or "").strip().replace("\n", " ")
    snippet = (text[:40] + "...") if len(text) > 40 else text
    return f"{kind}: {snippet}" if snippet else kind


async def cmd_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot admin, private chat: reply to a post with /schedule <date> <time> [chat_id]."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("Schedule posts in a private chat with me.")
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Only bot admins can schedule posts.")
        return
    usage = (
        "To schedule a post: send (or forward) it to me here, then REPLY to it with\n"
        "/schedule 3/10/2026 8am\n\n"
        "Date is day/month/year, time is in group time. Also works: tomorrow 7pm, 03-10-2026 20:30.\n"
        "By default it goes to the main group; add a chat id at the end to pick another.\n"
        "/scheduled lists waiting posts, /unschedule <number> cancels one."
    )
    reply = update.message.reply_to_message
    if reply is None:
        await update.message.reply_text(usage)
        return
    data = _filter_data_from_message(reply)
    if data is None:
        await update.message.reply_text(
            "I can schedule text, photos, videos, GIFs, documents, stickers, voice notes, audio and "
            "video notes. That kind of message isn't supported."
        )
        return

    args = list(context.args or [])
    target = None
    if args and re.fullmatch(r"-\d{5,}", args[-1]):
        target = int(args.pop())
    when, error = _parse_schedule_when(" ".join(args), datetime.now(timezone.utc))
    if when is None:
        await update.message.reply_text(f"{error}\n\n{usage}")
        return
    if target is None:
        target = _broadcast_target_chat_id()
    if target is None:
        await update.message.reply_text("No main group is configured. Add a chat id: /schedule 3/10/2026 8am -100123456789")
        return
    try:
        chat = await context.bot.get_chat(target)
        title = chat.title or str(target)
    except Exception:
        await update.message.reply_text(f"I can't see chat {target}. Is the bot a member of it?")
        return

    user = update.effective_user
    serial = await asyncio.to_thread(
        dbmod.sched_create, target, title, data, when, user.id, _user_label(user)
    )
    delta = when - datetime.now(timezone.utc)
    await update.message.reply_text(
        f"Scheduled #{serial} for {title}\n"
        f"{_sched_when_label(when)} (in {_sched_in_label(int(delta.total_seconds()))})\n"
        f"{_sched_preview(data)}\n\n"
        f"/scheduled shows the queue, /unschedule {serial} cancels it."
    )


async def cmd_scheduled(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot admin, private chat: list posts waiting to go out."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private" or not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Bot admins only, in a private chat with me.")
        return
    rows = await asyncio.to_thread(dbmod.sched_pending)
    if not rows:
        await update.message.reply_text("Nothing is scheduled. Reply to a post with /schedule 3/10/2026 8am")
        return
    lines = [f"Scheduled posts ({len(rows)}):", ""]
    for r in rows[:30]:
        lines.append(f"#{r['serial']}  {_sched_when_label(r['send_at'])}")
        lines.append(f"    {_sched_preview(r['data'])} -> {r.get('chat_title') or r['chat_id']} (by {r.get('by_name', '?')})")
    if len(rows) > 30:
        lines.append(f"... and {len(rows) - 30} more")
    for chunk in _split_for_telegram("\n".join(lines)):
        await update.message.reply_text(chunk)


async def cmd_unschedule(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot admin, private chat: /unschedule <number>."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private" or not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Bot admins only, in a private chat with me.")
        return
    try:
        serial = int((context.args or [""])[0].lstrip("#"))
    except ValueError:
        await update.message.reply_text("Usage: /unschedule <number>   (see /scheduled)")
        return
    doc = await asyncio.to_thread(dbmod.sched_cancel, serial)
    if doc is None:
        await update.message.reply_text(f"No waiting post #{serial}. It may already be sent or cancelled.")
        return
    await update.message.reply_text(f"Cancelled #{serial} ({_sched_preview(doc['data'])}).")


async def scheduled_post_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Every 30s: publish due posts and tell the admin who scheduled each one how it went."""
    try:
        due = await asyncio.to_thread(dbmod.sched_due, datetime.now(timezone.utc))
    except Exception:
        logger.exception("Scheduled-post worker could not read the queue")
        return
    for d in due:
        serial = d["serial"]
        try:
            if not await asyncio.to_thread(dbmod.sched_claim, serial):
                continue
            title = d.get("chat_title") or d["chat_id"]
            try:
                await _send_data_or_raise(context.bot, int(d["chat_id"]), d["data"])
            except Exception as exc:
                logger.exception("Scheduled post #%s failed", serial)
                await asyncio.to_thread(dbmod.sched_finish, serial, "failed", str(exc))
                note = f"Scheduled post #{serial} could NOT be posted to {title}: {str(exc)[:200]}"
            else:
                await asyncio.to_thread(dbmod.sched_finish, serial, "sent")
                note = f"Posted #{serial} to {title}."
            try:
                await context.bot.send_message(int(d["by_id"]), note)
            except Exception:
                logger.debug("Could not notify admin %s about scheduled post #%s", d.get("by_id"), serial)
        except Exception:
            logger.exception("Scheduled-post worker failed on #%s", serial)


'''

DB_OPS = [("append", "db: scheduled posts", DB_SCHED)]

BOT_OPS = [
    ("sub", "bot: scheduler code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_SCHED + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: scheduler help entries",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "schedule": ("botadmin", "/schedule 3/10/2026 8am  (reply to a post, in a private chat with the bot)", "Publish that post to the group at the given date and time (day/month/year, group time). Queue as many as you like.", "Bot admin, DM only"),
    "scheduled": ("botadmin", "/scheduled", "List the posts waiting to go out.", "Bot admin, DM only"),
    "unschedule": ("botadmin", "/unschedule <number>", "Cancel a waiting scheduled post.", "Bot admin, DM only"),'''),
    ("sub", "bot: register scheduler commands",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("schedule", cmd_schedule))
    app.add_handler(CommandHandler("scheduled", cmd_scheduled))
    app.add_handler(CommandHandler("unschedule", cmd_unschedule))'''),
    ("sub", "bot: schedule the post worker",
     r'''        name="daily_streak_reset_job",
    )''',
     r'''        name="daily_streak_reset_job",
    )
    jq.run_repeating(scheduled_post_worker, interval=30, first=50, name="scheduled_post_worker")'''),
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
        elif kind in ("remove_func", "replace_func"):
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
        elif kind == "replace_func":
            span = _func_span(text, op[2])
            if span is None:
                return None, [f"{label}: '{name}' - function vanished during patching"]
            start, end = span
            lines = text.split("\n")
            text = "\n".join(lines[: start - 1] + op[3].rstrip("\n").split("\n") + lines[end:])
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

