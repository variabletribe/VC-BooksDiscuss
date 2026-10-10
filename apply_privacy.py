#!/usr/bin/env python3
"""
apply_privacy.py - adds /privacy and /mydata to bot.py and db.py.

Run from the folder that contains bot.py and db.py (after the earlier scripts):

    python apply_privacy.py --dry-run   # only check that every edit point is found
    python apply_privacy.py             # patch both files

Same safety as before: all edit points are checked first (nothing changes if one is
missing), the result is syntax-checked, originals are kept as bot.py.bak / db.py.bak,
and running it twice is harmless.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "# Privacy: /privacy and /mydata"
MARKER_DB = "# Privacy: data export for /mydata"

DB_PRIVACY = r'''

# =============================================================================
# Privacy: data export for /mydata
# =============================================================================


def _export_json_default(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    return str(value)


def export_user_data(user_id: int) -> dict:
    """Everything this bot stores about one user across all chats. Read-only. Never
    includes anyone else's data and leaves out admin names on moderation entries."""
    out: dict = {
        "about": (
            "Everything this bot stores about you. Times are UTC. The audio files of your "
            "Read & Record are not in this file; use /myrnr in a private chat to get them back."
        ),
        "user_id": user_id,
        "exported_at": datetime.now(timezone.utc),
    }

    stats = []
    for d in _coll("user_attendance").find({"user_id": user_id}):
        row = {k: v for k, v in d.items() if k != "_id"}
        row["level"] = _level_for_xp(int(d.get("xp", 0)))[0]
        stats.append(row)
    out["stats"] = stats

    sessions = []
    cursor = _coll("vc_sessions").find(
        {"participants.user_id": user_id},
        {"participants": 1, "chat_id": 1, "started_at": 1, "ended_at": 1, "duration_sec": 1},
    ).sort("ended_at", ASCENDING)
    for s in cursor:
        p = next((x for x in (s.get("participants") or []) if x.get("user_id") == user_id), {})
        sessions.append(
            {
                "chat_id": s.get("chat_id"),
                "started_at": s.get("started_at"),
                "ended_at": s.get("ended_at"),
                "call_length_sec": s.get("duration_sec"),
                "your_estimated_sec": p.get("estimated_seconds"),
                "name_shown": p.get("display_name"),
            }
        )
    out["voice_chat_sessions"] = sessions

    out["recordings"] = [
        {
            "chat_id": d.get("chat_id"),
            "number": d.get("serial"),
            "kind": d.get("kind"),
            "duration_sec": d.get("duration"),
            "audio_sent_at": d.get("audio_at"),
            "recorded_at": d.get("recorded_at"),
            "removed_by_admin": bool(d.get("deleted")),
        }
        for d in _coll("rnr_records").find({"user_id": user_id}).sort("audio_at", ASCENDING)
    ]

    warnings = []
    for d in _coll("warnings").find({"user_id": user_id}):
        cleared = d.get("cleared_before")
        warnings.append(
            {
                "chat_id": d.get("chat_id"),
                "warnings": [
                    {
                        "reason": w.get("reason"),
                        "at": w.get("at"),
                        "active": (not cleared or w["at"] > cleared),
                    }
                    for w in d.get("warns", [])
                ],
            }
        )
    out["warnings"] = warnings

    out["moderation_actions"] = [
        {"chat_id": d.get("chat_id"), "action": d.get("action"), "reason": d.get("reason"), "at": d.get("at")}
        for d in _coll("mod_log").find({"target_id": user_id}).sort("at", ASCENDING)
    ]

    out["topics_added"] = [
        {
            "chat_id": d.get("chat_id"),
            "number": d.get("serial"),
            "text": d.get("text"),
            "state": d.get("state"),
            "votes": d.get("votes", 0),
            "added_at": d.get("added_at"),
        }
        for d in _coll("topics").find({"added_by_id": user_id}).sort("added_at", ASCENDING)
    ]
    out["topics_voted_for"] = [
        {"chat_id": d.get("chat_id"), "number": d.get("serial")}
        for d in _coll("topics").find({"voter_ids": user_id}, {"chat_id": 1, "serial": 1})
    ]

    out["codes"] = [
        {"chat_id": d.get("chat_id"), "telegram_code": d.get("code")}
        for d in _coll("tg_codes").find({"user_id": user_id})
    ] + [
        {"chat_id": d.get("chat_id"), "linked_meet_code": d.get("code")}
        for d in _coll("meet_codes").find({"linked_tg_user_id": user_id})
    ]

    out["seen_as"] = [
        {
            "chat_id": d.get("chat_id"),
            "username": d.get("username"),
            "display_name": d.get("display_name"),
            "last_seen": d.get("updated_at"),
        }
        for d in _coll("known_users").find({"user_id": user_id})
    ]

    out["allowed_to_post_links_in"] = [
        d["_id"] for d in _coll("chat_settings").find({"link_allowlist": user_id}, {"_id": 1})
    ]
    return out


def export_user_data_json(user_id: int) -> tuple[bytes, dict[str, int]]:
    """(pretty JSON bytes, {section: item_count}) for /mydata."""
    import json

    data = export_user_data(user_id)
    counts = {k: len(v) for k, v in data.items() if isinstance(v, list)}
    raw = json.dumps(data, indent=2, ensure_ascii=False, default=_export_json_default)
    return raw.encode("utf-8"), counts
'''

BOT_PRIVACY = r'''# =============================================================================
# Privacy: /privacy and /mydata
# /privacy explains what is stored and who can see it. /mydata sends a member a file with
# everything stored about THEM. Both open in a private chat (also via t.me/<bot>?start=...),
# so personal data is never posted in the group.
# =============================================================================

_PRIVACY_TEXT = (
    "🔒 <b>Privacy notice</b>\n\n"
    "<b>What I store about you</b>\n"
    "• Your Telegram ID, name and @username, to show your stats and to find you for admin commands.\n"
    "• Voice-chat activity: when you join calls, estimated minutes, present days, streaks, XP and badges.\n"
    "• The date I first saw you join the group.\n"
    "• Read &amp; Record: a copy of each voice note or audio you record with /rnr (kept in a private "
    "storage group), plus its date and length.\n"
    "• Topics you suggest and vote for, and your member codes.\n"
    "• Moderation: warnings, mutes, bans and kicks given to you, with the reason.\n\n"
    "<b>What I don't store</b>\n"
    "• The text of group messages. I read messages only to apply group rules (blocked words, links, "
    "flooding, filters) and to remember @usernames, then forget the text.\n"
    "• Messages you send me privately are forwarded to the group admins so they can reply.\n\n"
    "<b>Who can see it</b>\n"
    "• Everyone in the group sees the leaderboards (name, hours, days, streaks).\n"
    "• Bot admins and group admins can look up your stats and warnings.\n"
    "• Everyone in the Read &amp; Record storage group can hear the saved audio.\n"
    "• If AI recaps are switched on, names and call minutes are sent to an AI service (Groq) to write "
    "the post-call summary.\n"
    "• Bot admins receive regular database backups that contain this data.\n\n"
    "<b>How long</b>\n"
    "Until an admin deletes it. Leaving the group does not delete your data.\n\n"
    "<b>Your options</b>\n"
    "• /mydata: get a file with everything stored about you.\n"
    "• /myrnr: get your recordings back.\n"
    "• To have your data corrected or deleted, ask a group admin."
)

_MYDATA_COOLDOWN_SECONDS = 60
_mydata_last: dict[int, float] = {}  # user_id -> monotonic time of last export


async def _redirect_to_private(update: Update, context: ContextTypes.DEFAULT_TYPE, topic: str, text: str) -> None:
    """In a group: point the user to a private chat with a one-tap button; tidy up after 60s."""
    uname = context.bot.username
    keyboard = None
    if uname:
        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Open private chat", url=f"https://t.me/{uname}?start={topic}")]]
        )
    sent = await update.message.reply_text(text, reply_markup=keyboard)
    jq = context.job_queue
    if jq is not None and update.effective_chat:
        jq.run_once(
            _delete_messages_later,
            when=60,
            data={"chat_id": update.effective_chat.id, "message_ids": [update.message.message_id, sent.message_id]},
            name=f"{topic}-redirect-{update.effective_chat.id}-{sent.message_id}",
        )


async def cmd_privacy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: what the bot stores, who can see it, and how to get/delete your data."""
    if not update.message or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        await _redirect_to_private(
            update, context, "privacy", "The privacy notice is long, so I'll show it in a private chat."
        )
        return
    await update.message.reply_text(_PRIVACY_TEXT, parse_mode="HTML")


async def cmd_mydata(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone, private chat only: send the member a JSON file of everything stored about them."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        await _redirect_to_private(
            update, context, "mydata", "Your data is personal, so I only send it in a private chat."
        )
        return

    user = update.effective_user
    now = time.monotonic()
    last = _mydata_last.get(user.id)
    if last is not None and now - last < _MYDATA_COOLDOWN_SECONDS:
        wait = int(_MYDATA_COOLDOWN_SECONDS - (now - last)) + 1
        await update.message.reply_text(f"Please wait {wait}s before asking again.")
        return
    _mydata_last[user.id] = now

    try:
        blob, counts = await asyncio.to_thread(dbmod.export_user_data_json, user.id)
    except Exception:
        logger.exception("/mydata export failed user_id=%s", user.id)
        await update.message.reply_text("Sorry, I couldn't prepare your data right now. Please try again later.")
        return

    found = {k: v for k, v in counts.items() if v}
    if not found:
        await update.message.reply_text("I have no data stored about you.")
        return
    names = {
        "stats": "group stats",
        "voice_chat_sessions": "voice-chat sessions",
        "recordings": "Read & Record recordings",
        "warnings": "warning records",
        "moderation_actions": "moderation actions",
        "topics_added": "topics added",
        "topics_voted_for": "topic votes",
        "codes": "codes",
        "seen_as": "name records",
        "allowed_to_post_links_in": "link permissions",
    }
    summary = ", ".join(f"{v} {names.get(k, k)}" for k, v in found.items())
    filename = f"my_data_{user.id}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.json"
    await update.message.reply_document(
        document=InputFile(io.BytesIO(blob), filename=filename),
        caption=f"Everything I have stored about you: {summary}.\nSee /privacy for how it is used."[:1000],
    )


'''

DB_OPS = [("append", "db: data export", DB_PRIVACY)]

BOT_OPS = [
    ("sub", "bot: /start deep links for the privacy notice and data export",
     r'''    await update.message.reply_text(
        _help_main_menu_text(), parse_mode="HTML", reply_markup=_help_main_menu_keyboard()
    )''',
     r'''    arg = context.args[0].lower() if context.args else ""
    if update.effective_chat and update.effective_chat.type == "private":
        if arg == "privacy":
            await cmd_privacy(update, context)
            return
        if arg == "mydata":
            await cmd_mydata(update, context)
            return
    await update.message.reply_text(
        _help_main_menu_text(), parse_mode="HTML", reply_markup=_help_main_menu_keyboard()
    )'''),
    ("sub", "bot: help entries",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "privacy": ("everyone", "/privacy", "What the bot stores about you, who can see it, and how to get or delete your data.", "Anyone"),
    "mydata": ("everyone", "/mydata  (in a private chat with the bot)", "Get a file with everything the bot has stored about you: stats, voice-chat sessions, recordings, warnings, topics.", "Anyone"),'''),
    ("sub", "bot: privacy code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_PRIVACY + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: register commands",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("privacy", cmd_privacy))
    app.add_handler(CommandHandler("mydata", cmd_mydata))'''),
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