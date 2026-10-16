#!/usr/bin/env python3
"""
apply_vchost.py - after a voice chat ends, the bot asks "what was the topic and who was the
host?" and shows the answer (with the date) in /topics and /alltopics. It also shows who
added each topic and when.

Run from the folder that contains bot.py and db.py, after the earlier scripts:

    python apply_vchost.py --dry-run
    python apply_vchost.py

Same safety as the other scripts: nothing changes if an edit point is missing, the result
is syntax-checked, .bak copies are kept, and running it twice is harmless.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "# VC host/topic question: asked when a call ends"
MARKER_DB = "# Topic discussions: host + date, asked right after a voice chat ends"

DB_VCQ = r'''

# =============================================================================
# Topic discussions: host + date, asked right after a voice chat ends
#
# When a call of 20+ minutes ends, a question is stored ("scheduled"), posted ~25s later
# ("pending", open for 1 hour) and either answered by a group admin ("answered") or closed
# ("expired") and saved as "Topic not recorded / Host: Don't know". All state is in MongoDB,
# so a restart never loses a question. Discussed topics become done topics with
# discussed_at (the day the call ended) and host.
# =============================================================================

_vcq_ready = False


def _vcq():
    global _vcq_ready
    coll = _coll("vc_questions")
    if not _vcq_ready:
        coll.create_index([("status", ASCENDING)])
        coll.create_index([("chat_id", ASCENDING), ("message_id", ASCENDING)])
        coll.create_index("created_at", expireAfterSeconds=30 * 24 * 3600)
        _vcq_ready = True
    return coll


def _aware(dt: datetime | None) -> datetime | None:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def topic_date_label(dt: datetime | None) -> str:
    """'14 Oct 2026' in the group's timezone (RNR_TZ_OFFSET_MINUTES, default 330 = IST)."""
    if dt is None:
        return "-"
    try:
        offset = timedelta(minutes=int(os.getenv("RNR_TZ_OFFSET_MINUTES", "330")))
    except ValueError:
        offset = timedelta(minutes=330)
    return (_aware(dt) + offset).strftime("%d %b %Y")


def _topic_row(d: dict) -> TopicRow:
    return TopicRow(
        int(d["serial"]),
        str(d["text"]),
        str(d["state"]),
        int(d.get("votes", 0)),
        str(d.get("added_by_name") or ""),
        d.get("added_at"),
        d.get("discussed_at"),
        str(d.get("host") or ""),
    )


def get_recent_discussed(chat_id: int, limit: int = 5) -> list[TopicRow]:
    cursor = (
        _coll("topics")
        .find({"chat_id": chat_id, "state": "done", "discussed_at": {"$exists": True}})
        .sort("discussed_at", DESCENDING)
        .limit(limit)
    )
    return [_topic_row(d) for d in cursor]


def vc_question_schedule(
    chat_id: int, ended_msg_id: int, ended_at: datetime, duration: int, post_at: datetime
) -> bool:
    """Queue a question for one ended call. False if this call is already queued."""
    try:
        _vcq().insert_one(
            {
                "_id": f"{chat_id}:{ended_msg_id}",
                "chat_id": chat_id,
                "ended_at": _aware(ended_at),
                "duration": int(duration),
                "status": "scheduled",
                "post_at": _aware(post_at),
                "created_at": datetime.now(timezone.utc),
            }
        )
        return True
    except DuplicateKeyError:
        return False


def vc_questions_open() -> list[dict]:
    return list(_vcq().find({"status": {"$in": ["scheduled", "pending"]}}))


def vc_question_claim(doc_id: str, from_status: str, to_status: str) -> dict | None:
    """Atomically move a question between states; None if someone else got there first."""
    return _vcq().find_one_and_update(
        {"_id": doc_id, "status": from_status}, {"$set": {"status": to_status}}
    )


def vc_question_set_posted(doc_id: str, message_id: int, expires_at: datetime) -> None:
    _vcq().update_one(
        {"_id": doc_id},
        {"$set": {"status": "pending", "message_id": message_id, "expires_at": _aware(expires_at)}},
    )


def vc_question_claim_answer(chat_id: int, message_id: int) -> dict | None:
    """First valid answer wins: pending -> answered, atomically."""
    return _vcq().find_one_and_update(
        {"chat_id": chat_id, "message_id": message_id, "status": "pending"},
        {"$set": {"status": "answered", "answered_at": datetime.now(timezone.utc)}},
    )


def vc_resolve_topic(chat_id: int, text: str) -> tuple[str, int | None, str]:
    """('existing', serial, text) | ('new', None, text) | ('badnumber', serial, text).
    A bare number means "open topic #n"; otherwise an exact (case-insensitive) match against
    open topics; otherwise it is a new topic."""
    text = text.strip()
    if text.isdigit():
        topic = get_topic(chat_id, int(text))
        if topic and topic.get("state") == "active":
            return "existing", int(text), str(topic["text"])
        return "badnumber", int(text), text
    dup = find_active_topic_by_text(chat_id, text)
    if dup is not None:
        return "existing", dup.serial, dup.text
    return "new", None, text


def _create_done_topic(chat_id, text, host, discussed_at, by_id, by_name, source) -> int:
    now = datetime.now(timezone.utc)
    serial = _next_topic_serial(chat_id)
    _coll("topics").insert_one(
        {
            "_id": f"{chat_id}:{serial}",
            "chat_id": chat_id,
            "serial": serial,
            "text": text[:1000],
            "state": "done",
            "added_by_id": by_id,
            "added_by_name": by_name[:512],
            "added_at": now,
            "votes": 0,
            "voter_ids": [],
            "done_by_id": by_id,
            "done_by_name": by_name[:512],
            "done_at": now,
            "discussed_at": _aware(discussed_at),
            "host": host[:100],
            "source": source,
        }
    )
    return serial


def vc_save_discussion(
    chat_id: int, serial: int | None, text: str, host: str,
    discussed_at: datetime, by_id: int, by_name: str,
) -> dict:
    """Record what was discussed. An open topic is marked done (with date and host);
    anything else becomes a new topic that is already done."""
    if serial is not None:
        result = _coll("topics").update_one(
            {"_id": f"{chat_id}:{serial}", "state": "active"},
            {
                "$set": {
                    "state": "done",
                    "done_by_id": by_id,
                    "done_by_name": by_name[:512],
                    "done_at": datetime.now(timezone.utc),
                    "discussed_at": _aware(discussed_at),
                    "host": host[:100],
                    "source": "vc",
                }
            },
        )
        if result.modified_count:
            return {"serial": serial, "created": False, "text": text}
    new_serial = _create_done_topic(chat_id, text, host, discussed_at, by_id, by_name, "vc")
    return {"serial": new_serial, "created": True, "text": text}


def vc_save_unrecorded(chat_id: int, discussed_at: datetime) -> int:
    """Nobody answered in time: keep a record that the call happened."""
    return _create_done_topic(chat_id, "Topic not recorded", "Don't know", discussed_at, 0, "Bot", "vc_unrecorded")


def topic_set_host(chat_id: int, serial: int, host: str) -> bool:
    """Fix the host of a topic that is already done."""
    result = _coll("topics").update_one(
        {"_id": f"{chat_id}:{serial}", "state": "done"}, {"$set": {"host": host[:100]}}
    )
    return result.matched_count > 0
'''

BOT_VCQ = r'''# =============================================================================
# VC host/topic question: asked when a call ends, answered by a group admin
#
# Reply format:  (topic name)(host)     e.g.  (Atomic Habits)(Anna)   (also: Atomic Habits | Anna)
# A bare number means an open topic:  (12)(Anna)  marks topic #12 as discussed.
# The question stays open for 1 hour; after that the call is saved as
# "Topic not recorded / Host: Don't know". Everything is stored in MongoDB (see db.py).
# =============================================================================

_VC_QUESTION_DELAY_SECONDS = 25  # let the call summary post first
_VC_QUESTION_OPEN_HOURS = 1
_VC_ANSWER_RE = re.compile(r"^\s*\(([^()]+)\)\s*\(([^()]+)\)\s*$", re.DOTALL)
_vc_pending_ids: set[tuple[int, int]] = set()  # (chat_id, question message id): cheap filter for replies


def _parse_vc_answer(text: str) -> tuple[str, str] | None:
    """(topic, host) from '(topic)(host)' or 'topic | host'; None if it doesn't fit."""
    m = _VC_ANSWER_RE.match(text or "")
    if m:
        topic, host = m.group(1).strip(), m.group(2).strip()
    elif "|" in (text or "") and not text.lstrip().startswith("/"):
        topic, host = [p.strip() for p in text.split("|", 1)]
    else:
        return None
    if not topic or not host:
        return None
    return topic[:1000], host[:100]


async def _schedule_vc_host_question(chat_id: int, ended_msg_id: int, ended_at: datetime, duration: int) -> None:
    """Called when a call ends. Never raises; short calls are skipped."""
    try:
        if duration < dbmod.present_threshold_sec():
            return
        post_at = datetime.now(timezone.utc) + timedelta(seconds=_VC_QUESTION_DELAY_SECONDS)
        await asyncio.to_thread(dbmod.vc_question_schedule, chat_id, ended_msg_id, ended_at, duration, post_at)
    except Exception:
        logger.exception("Could not schedule the VC host question chat_id=%s", chat_id)


def _vc_question_text(duration: int) -> str:
    return (
        f"🎙️ <b>Voice chat ended</b> ({_format_duration(int(duration))}).\n"
        "What was the topic name and who was the host?\n\n"
        "Reply to this message with:\n"
        "<code>(topic name)(host)</code>\n"
        "Example: <code>(Atomic Habits)(Anna)</code>\n\n"
        "<i>Group admins only. This question closes in 1 hour.</i>"
    )


async def vc_question_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Every 30s: post questions that are due, close the ones open for over an hour (saving
    'Topic not recorded / Don't know'), and refresh the set of open question messages."""
    try:
        docs = await asyncio.to_thread(dbmod.vc_questions_open)
    except Exception:
        logger.exception("VC question worker could not read the queue")
        return
    now = datetime.now(timezone.utc)
    open_ids: set[tuple[int, int]] = set()
    for d in docs:
        try:
            chat_id = int(d["chat_id"])
            if d["status"] == "scheduled":
                post_at = dbmod._aware(d.get("post_at")) or now
                if post_at > now:
                    continue
                if await asyncio.to_thread(dbmod.vc_question_claim, d["_id"], "scheduled", "posting") is None:
                    continue
                try:
                    sent = await context.bot.send_message(
                        chat_id, _vc_question_text(d.get("duration", 0)), parse_mode="HTML"
                    )
                except Exception:
                    logger.exception("Could not post the VC host question chat_id=%s", chat_id)
                    await asyncio.to_thread(dbmod.vc_question_claim, d["_id"], "posting", "failed")
                    continue
                expires = now + timedelta(hours=_VC_QUESTION_OPEN_HOURS)
                await asyncio.to_thread(dbmod.vc_question_set_posted, d["_id"], sent.message_id, expires)
                open_ids.add((chat_id, sent.message_id))
            elif d["status"] == "pending":
                expires = dbmod._aware(d.get("expires_at")) or now
                if expires > now:
                    open_ids.add((chat_id, int(d["message_id"])))
                    continue
                if await asyncio.to_thread(dbmod.vc_question_claim, d["_id"], "pending", "expired") is None:
                    continue
                ended_at = dbmod._aware(d.get("ended_at")) or now
                serial = await asyncio.to_thread(dbmod.vc_save_unrecorded, chat_id, ended_at)
                try:
                    await context.bot.edit_message_text(
                        f"Question closed: nobody answered within 1 hour, so this call was saved as "
                        f"topic #{serial} (topic not recorded, host unknown).",
                        chat_id=chat_id, message_id=int(d["message_id"]),
                    )
                except Exception:
                    logger.debug("Could not edit the closed VC question chat_id=%s", chat_id)
        except Exception:
            logger.exception("VC question worker failed on %s", d.get("_id"))
    _vc_pending_ids.clear()
    _vc_pending_ids.update(open_ids)


async def on_vc_question_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """A group admin replies to the question with (topic)(host). Runs in its own handler
    group, so it never competes with other reply handlers."""
    msg, chat = update.message, update.effective_chat
    if not msg or not chat or not msg.reply_to_message or not msg.text:
        return
    qid = msg.reply_to_message.message_id
    if (chat.id, qid) not in _vc_pending_ids:
        return
    if not await _message_sender_is_admin(update, context):
        return  # only group admins can answer; everyone else is ignored quietly

    parsed = _parse_vc_answer(msg.text)
    if parsed is None:
        await _reply_autodelete(
            update, context,
            "Please reply in this format: (topic name)(host)\nExample: (Atomic Habits)(Anna)",
        )
        return
    topic_text, host = parsed
    kind, serial, resolved_text = await asyncio.to_thread(dbmod.vc_resolve_topic, chat.id, topic_text)
    if kind == "badnumber":
        await _reply_autodelete(
            update, context, f"Topic #{serial} isn't an open topic. Use its name instead, e.g. (Atomic Habits)(Anna)."
        )
        return

    doc = await asyncio.to_thread(dbmod.vc_question_claim_answer, chat.id, qid)
    if doc is None:
        return  # someone else answered first
    _vc_pending_ids.discard((chat.id, qid))

    by = update.effective_user
    by_name = _user_label(by) if by else "Admin"
    ended_at = dbmod._aware(doc.get("ended_at")) or datetime.now(timezone.utc)
    result = await asyncio.to_thread(
        dbmod.vc_save_discussion, chat.id, serial if kind == "existing" else None,
        resolved_text, host, ended_at, by.id if by else 0, by_name,
    )
    how = "marked as done" if not result["created"] else "added as a new topic"
    await msg.reply_text(
        f"✅ Saved: <b>{html.escape(resolved_text, quote=False)}</b> · {dbmod.topic_date_label(ended_at)} · "
        f"Host: <b>{html.escape(host, quote=False)}</b>\n<i>Topic #{result['serial']} {how}.</i>",
        parse_mode="HTML",
    )


async def cmd_sethost(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group admin: /sethost <topic number> <host>  - fix the host of a discussed topic."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can set a host.")
        return
    args = context.args or []
    try:
        serial = int(args[0].lstrip("#"))
        host = " ".join(args[1:]).strip()[:100]
        if not host:
            raise ValueError
    except (ValueError, IndexError):
        await _reply_autodelete(update, context, "Usage: /sethost <topic number> <host>   e.g. /sethost 12 Anna")
        return
    ok = await asyncio.to_thread(dbmod.topic_set_host, chat.id, serial, host)
    if not ok:
        await _reply_autodelete(update, context, f"Topic #{serial} isn't a discussed (done) topic.")
        return
    await _reply_autodelete(
        update, context, f"Host of topic #{serial} set to {html.escape(host, quote=False)}.", parse_mode="HTML"
    )


'''

NEW_CMD_TOPICS = r'''async def cmd_topics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: open topics ranked by votes (with who added them and when), then the most
    recently discussed topics with their date and host."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.get_active_topics, chat.id)
    recent = await asyncio.to_thread(dbmod.get_recent_discussed, chat.id, 5)
    if not rows and not recent:
        await _reply_autodelete(update, context, "No active topics yet.\n\nAdd one with /addtopic <topic>")
        return
    lines = ["📋 <b>Active Topics</b>", ""]
    if not rows:
        lines.append("<i>No open topics right now.</i>")
    for r in rows[:_MAX_TOPIC_ROWS]:
        safe = html.escape(r.text, quote=False)
        vote_badge = f" — 👍 {r.votes}" if r.votes else ""
        lines.append(f"{r.serial}. {safe}{vote_badge}")
        if r.added_by_name:
            lines.append(
                f"    <i>added by {html.escape(r.added_by_name, quote=False)} · {dbmod.topic_date_label(r.added_at)}</i>"
            )
    if len(rows) > _MAX_TOPIC_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - _MAX_TOPIC_ROWS} more not shown.</i>")
    if recent:
        lines += ["", "✅ <b>Recently discussed</b>"]
        for r in recent:
            lines.append(
                f"{r.serial}. {html.escape(r.text, quote=False)} · {dbmod.topic_date_label(r.discussed_at)}"
                f" · Host: {html.escape(r.host or '-', quote=False)}"
            )
    lines.append("")
    lines.append("<i>Use the number shown with /topicdone, /deletetopic, or /upvote.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")
'''

NEW_CMD_ALLTOPICS = r'''async def cmd_alltopics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: every topic ever added, sorted by number. Open = plain, discussed = ✅ with the
    date it was discussed and its host, deleted = struck through. Each shows who added it."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.get_all_topics, chat.id)
    if not rows:
        await _reply_autodelete(update, context, "No topics yet.\n\nAdd one with /addtopic <topic>")
        return
    shown = rows[:_MAX_TOPIC_ROWS]
    lines = ["📚 <b>All Topics</b>", ""]
    for r in shown:
        safe = html.escape(r.text, quote=False)
        if r.state == "done":
            lines.append(
                f"{r.serial}. {safe} ✅ · {dbmod.topic_date_label(r.discussed_at)}"
                f" · Host: {html.escape(r.host or '-', quote=False)}"
            )
        elif r.state == "deleted":
            lines.append(f"<s>{r.serial}. {safe}</s>")
        else:
            lines.append(f"{r.serial}. {safe}")
        if r.added_by_name:
            lines.append(
                f"    <i>added by {html.escape(r.added_by_name, quote=False)} · {dbmod.topic_date_label(r.added_at)}</i>"
            )
    if len(rows) > _MAX_TOPIC_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - _MAX_TOPIC_ROWS} more not shown.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")
'''

NEW_GET_ACTIVE = r'''def get_active_topics(chat_id: int) -> list[TopicRow]:
    """Sorted by votes (highest first), then serial ascending as a tiebreak, so the
    topic the group most wants to discuss next surfaces at the top of /topics."""
    coll = _coll("topics")
    cursor = coll.find({"chat_id": chat_id, "state": "active"}).sort(
        [("votes", DESCENDING), ("serial", ASCENDING)]
    )
    return [_topic_row(d) for d in cursor]
'''

NEW_GET_ALL = r'''def get_all_topics(chat_id: int) -> list[TopicRow]:
    """Sorted by serial (chronological record), unlike get_active_topics which sorts by votes."""
    coll = _coll("topics")
    cursor = coll.find({"chat_id": chat_id}).sort("serial", ASCENDING)
    return [_topic_row(d) for d in cursor]
'''

DB_OPS = [
    ("sub", "db: topics carry who added them, when, the discussion date and the host",
     r'''    state: str
    votes: int = 0''',
     r'''    state: str
    votes: int = 0
    added_by_name: str = ""
    added_at: datetime | None = None
    discussed_at: datetime | None = None
    host: str = ""'''),
    ("replace_func", "db: get_active_topics returns the richer rows", "get_active_topics", NEW_GET_ACTIVE),
    ("replace_func", "db: get_all_topics returns the richer rows", "get_all_topics", NEW_GET_ALL),
    ("append", "db: VC question + discussion functions", DB_VCQ),
]

BOT_OPS = [
    ("sub", "bot: ask the host question when a call ends",
     r'''    now = msg.date
    configured = app_state.configured_assistant_groups()''',
     r'''    now = msg.date
    if msg.video_chat_ended:
        await _schedule_vc_host_question(chat_id, msg.message_id, now, msg.video_chat_ended.duration)
    configured = app_state.configured_assistant_groups()'''),
    ("replace_func", "bot: /topics shows who added a topic and what was discussed", "cmd_topics", NEW_CMD_TOPICS),
    ("replace_func", "bot: /alltopics shows date discussed, host and who added", "cmd_alltopics", NEW_CMD_ALLTOPICS),
    ("sub", "bot: host question code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_VCQ + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: /sethost help entry",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "sethost": ("topics", "/sethost <topic number> <host>", "Fix or set the host of a discussed topic.", "Group admin"),'''),
    ("sub", "bot: register /sethost and the answer handler",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("sethost", cmd_sethost))
    # Answers to the "who was the host?" question; own handler group so it never shadows other reply handlers.
    app.add_handler(
        MessageHandler(
            filters.REPLY & filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND & filters.UpdateType.MESSAGE,
            on_vc_question_reply,
        ),
        group=8,
    )'''),
    ("sub", "bot: schedule the question worker",
     r'''        name="daily_streak_reset_job",
    )''',
     r'''        name="daily_streak_reset_job",
    )
    jq.run_repeating(vc_question_worker, interval=30, first=45, name="vc_question_worker")'''),
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