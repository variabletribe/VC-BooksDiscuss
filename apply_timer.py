#!/usr/bin/env python3
"""
apply_timer.py - /timer now shows a live countdown message in the group.

Run from the folder that contains bot.py:

    python apply_timer.py --dry-run
    python apply_timer.py

The bot posts one message (hourglass, progress bar, mm:ss) and edits it every 10 seconds
(TIMER_TICK_SECONDS on Render changes that; minimum 5), then sends a fresh "Time's up!"
message so members get a notification. /canceltimer stops it and marks the message cancelled.
Same safety as the other scripts.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

MARKER_BOT = "_TIMER_BAR_CELLS = 12"

NEW_TIMER = r'''_TIMER_BAR_CELLS = 12


def _timer_tick_seconds() -> int:
    """How often the live countdown message is edited. Telegram allows only about 20 messages
    per minute per group (edits count), so never go below 5s; default 10s = 6 edits/minute."""
    try:
        return max(5, int(os.getenv("TIMER_TICK_SECONDS", "10")))
    except ValueError:
        return 10


def _timer_render(remaining: int, total: int, minutes: int, started_by: str, frame: int, state: str = "running") -> str:
    """The live timer message: hourglass that flips every update, a progress bar and mm:ss."""
    remaining = max(0, min(remaining, total))
    elapsed = 1 - (remaining / total if total else 0)
    filled = round(elapsed * _TIMER_BAR_CELLS)
    bar = "▰" * filled + "▱" * (_TIMER_BAR_CELLS - filled)
    mm, ss = divmod(remaining, 60)
    if state == "done":
        title = "⏰ <b>Time's up!</b>"
        bar, pct = "▰" * _TIMER_BAR_CELLS, 100
    elif state == "cancelled":
        title = "⏹️ <b>Timer cancelled</b>"
        pct = round(elapsed * 100)
    else:
        title = f"{'⏳' if frame % 2 == 0 else '⌛'} <b>{mm:02d}:{ss:02d}</b> left"
        pct = round(elapsed * 100)
    return f"{title}\n{bar} {pct}%\n<i>{minutes} min timer · started by {html.escape(started_by, quote=False)}</i>"


async def _timer_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Repeating job: edit the live timer message. Never raises; backs off if Telegram asks."""
    from telegram.error import BadRequest, RetryAfter

    chat_id = (context.job.data or {}).get("chat_id")
    entry = _active_timers.get(chat_id)
    if entry is None or entry.get("message_id") is None:
        context.job.schedule_removal()
        return
    if time.monotonic() < entry.get("pause_until", 0.0):
        return
    total = entry["minutes"] * 60
    remaining = int((entry["end_at"] - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        return  # _timer_fire finishes it
    entry["frame"] += 1
    try:
        await context.bot.edit_message_text(
            _timer_render(remaining, total, entry["minutes"], entry["started_by"], entry["frame"]),
            chat_id=chat_id, message_id=entry["message_id"], parse_mode="HTML",
        )
    except RetryAfter as exc:
        wait = exc.retry_after.total_seconds() if hasattr(exc.retry_after, "total_seconds") else float(exc.retry_after)
        entry["pause_until"] = time.monotonic() + wait + 1
        logger.warning("Timer: Telegram asked to slow down, pausing updates for %ss chat_id=%s", wait, chat_id)
    except BadRequest as exc:
        if "not modified" in str(exc).lower():
            return
        logger.info("Timer: live message can't be edited any more (%s); stopping updates chat_id=%s", exc, chat_id)
        entry["message_id"] = None
        context.job.schedule_removal()
    except Exception:
        logger.debug("Timer tick failed chat_id=%s", chat_id)


async def _timer_finish_live_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, entry: dict, state: str) -> None:
    """Turn the live message into its final 'Time's up' / 'cancelled' form."""
    if entry.get("message_id") is None:
        return
    total = entry["minutes"] * 60
    remaining = int((entry["end_at"] - datetime.now(timezone.utc)).total_seconds())
    try:
        await context.bot.edit_message_text(
            _timer_render(remaining, total, entry["minutes"], entry["started_by"], entry.get("frame", 0), state),
            chat_id=chat_id, message_id=entry["message_id"], parse_mode="HTML",
        )
    except Exception:
        logger.debug("Timer: couldn't finalise the live message chat_id=%s", chat_id)


async def cmd_timer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /timer <Nm> - e.g. /timer 5m. Minutes only, max 20m, one timer per chat. The bot
    posts a live countdown (progress bar + mm:ss) that it edits every few seconds, then pings
    the chat when time is up."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not context.args:
        await _reply_autodelete(
            update, context,
            f"Usage: /timer &lt;Nm&gt; (minutes only, max {TIMER_MAX_MINUTES}m)\nExample: /timer 5m",
            parse_mode="HTML",
        )
        return
    m = _TIMER_RE.match(context.args[0].strip())
    if not m:
        await _reply_autodelete(
            update, context,
            f"Invalid format - minutes only, e.g. /timer 5m (max {TIMER_MAX_MINUTES}m).",
        )
        return
    minutes = int(m.group(1))
    if minutes < 1 or minutes > TIMER_MAX_MINUTES:
        await _reply_autodelete(update, context, f"Timer must be between 1m and {TIMER_MAX_MINUTES}m.")
        return

    existing = _active_timers.get(chat.id)
    if existing is not None:
        remaining = existing["end_at"] - datetime.now(timezone.utc)
        remaining_min = max(0, int(remaining.total_seconds() // 60) + 1)
        safe_by = html.escape(existing["started_by"], quote=False)
        await _reply_autodelete(
            update, context,
            f"A timer is already running (~{remaining_min}m left, started by {safe_by}).\n"
            f"Use /canceltimer to cancel it first.",
            parse_mode="HTML",
        )
        return

    jq = context.job_queue
    if jq is None:
        await _reply_autodelete(update, context, "Timers aren't available right now.")
        return

    started_at = datetime.now(timezone.utc)
    end_at = started_at + timedelta(minutes=minutes)
    started_by = _user_label(update.effective_user)
    entry = {
        "end_at": end_at, "minutes": minutes, "started_by": started_by, "started_at": started_at,
        "message_id": None, "frame": 0, "pause_until": 0.0,
    }
    _active_timers[chat.id] = entry
    jq.run_once(_timer_fire, when=minutes * 60, data={"chat_id": chat.id}, name=f"timer-{chat.id}")

    try:
        live = await update.message.reply_text(
            _timer_render(minutes * 60, minutes * 60, minutes, started_by, 0), parse_mode="HTML"
        )
        entry["message_id"] = live.message_id
    except Exception:
        logger.exception("Timer: couldn't post the live message chat_id=%s", chat.id)

    if entry["message_id"] is not None:
        tick = _timer_tick_seconds()
        jq.run_repeating(_timer_tick, interval=tick, first=tick, data={"chat_id": chat.id}, name=f"timer-tick-{chat.id}")
        # keep the chat tidy: the live message stays, the command message goes
        jq.run_once(
            _delete_messages_later, when=30,
            data={"chat_id": chat.id, "message_ids": [update.message.message_id]},
            name=f"timer-cmd-{chat.id}-{update.message.message_id}",
        )
    else:
        await _reply_autodelete(
            update, context, f"⏳ Timer set for {minutes}m - I'll ping this chat at {end_at.strftime('%H:%M UTC')}."
        )
'''

NEW_FIRE = r'''async def _timer_fire(context: ContextTypes.DEFAULT_TYPE) -> None:
    """job_queue callback: fires once when a /timer's duration elapses."""
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    entry = _active_timers.pop(chat_id, None)
    if entry is None:
        return  # cancelled before firing
    if context.job_queue is not None:
        for job in context.job_queue.get_jobs_by_name(f"timer-tick-{chat_id}"):
            job.schedule_removal()
    await _timer_finish_live_message(context, chat_id, entry, "done")
    start_str = entry["started_at"].strftime("%H:%M")
    end_str = entry["end_at"].strftime("%H:%M")
    minutes = entry["minutes"]
    safe_by = html.escape(entry["started_by"], quote=False)
    try:
        # a new message (not an edit) so members actually get a notification
        await context.bot.send_message(
            chat_id,
            f"⏰ <b>Time's up!</b> {minutes} minute(s) are up.\n"
            f"<i>{start_str} → {end_str} UTC · started by {safe_by}</i>",
            parse_mode="HTML",
        )
    except Exception:
        logger.debug("Timer fire: send failed chat_id=%s", chat_id)
'''

NEW_CANCEL = r'''async def cmd_canceltimer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /canceltimer - cancels the chat's active timer, if any."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    entry = _active_timers.get(chat.id)
    if entry is None:
        await _reply_autodelete(update, context, "No timer is running.")
        return
    jq = context.job_queue
    if jq is not None:
        for name in (f"timer-{chat.id}", f"timer-tick-{chat.id}"):
            for job in jq.get_jobs_by_name(name):
                job.schedule_removal()
    _active_timers.pop(chat.id, None)
    await _timer_finish_live_message(context, chat.id, entry, "cancelled")
    await _reply_autodelete(update, context, "⏹️ Timer cancelled.")
'''

BOT_OPS = [
    ("replace_func", "bot: /timer with a live countdown", "cmd_timer", NEW_TIMER),
    ("replace_func", "bot: timer finish edits the live message", "_timer_fire", NEW_FIRE),
    ("replace_func", "bot: /canceltimer stops the live countdown", "cmd_canceltimer", NEW_CANCEL),
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
    path = Path("bot.py")
    if not path.exists():
        print("Run this from the folder that contains bot.py")
        return 1
    text = path.read_bytes().decode("utf-8").replace("\r\n", "\n")
    if MARKER_BOT not in text:
        new_text, problems = apply_ops(text, BOT_OPS, "bot.py")
        if new_text is None:
            print("X bot.py: cannot be patched")
            for p in problems:
                print("    " + p)
            print("\nNothing was changed. Send me the lines above and I'll adjust the script.")
            return 1
    if not patch_file(path, BOT_OPS, MARKER_BOT, dry):
        return 1
    if not dry:
        print("\nDone. Commit and push bot.py, then let Render redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())