#!/usr/bin/env python3
"""
apply_limits.py - private-chat limits and overload protection.

Run AFTER apply_rnr.py, from the folder that contains bot.py and db.py:

    python apply_limits.py --dry-run
    python apply_limits.py

Adds: 10 commands and 5 audio downloads per member per 24 hours in the private chat
(DM_COMMAND_LIMIT / DM_AUDIO_LIMIT on Render change them), /mylimits, a burst guard,
a cap on forwarded DMs, a global overload breaker and a command-spam guard in groups.
Same safety as the other scripts.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

PREREQ_BOT = "# Read & Record: /rnr, /myrnr"
MARKER_BOT = "# Private-chat limits and overload protection"
MARKER_DB = "# Private-chat usage limits (command and audio quotas"

DB_LIMITS = r'''

# =============================================================================
# Private-chat usage limits (command and audio quotas, /mylimits)
# Stored per user in `dm_usage` as lists of timestamps, so a restart can't reset them.
# =============================================================================

_dm_usage_ready = False


def _dm_usage():
    global _dm_usage_ready
    coll = _coll("dm_usage")
    if not _dm_usage_ready:
        coll.create_index("updated_at", expireAfterSeconds=3 * 24 * 3600)  # tidy up idle users
        _dm_usage_ready = True
    return coll


def _dm_window_times(doc: dict, kind: str, now: datetime, hours: int) -> list[datetime]:
    cutoff = now - timedelta(hours=hours)
    out = []
    for t in doc.get(kind, []):
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        if t > cutoff:
            out.append(t)
    return sorted(out)


def dm_quota_status(user_id: int, kind: str, now: datetime, hours: int = 24) -> tuple[int, datetime | None]:
    """(uses in the last `hours`, when the oldest of them drops out and a slot opens)."""
    doc = _dm_usage().find_one({"_id": user_id}) or {}
    times = _dm_window_times(doc, kind, now, hours)
    return len(times), (times[0] + timedelta(hours=hours) if times else None)


def dm_quota_consume(user_id: int, kind: str, now: datetime, hours: int = 24) -> None:
    """Record one use (and drop uses older than the window)."""
    coll = _dm_usage()
    doc = coll.find_one({"_id": user_id}) or {}
    times = _dm_window_times(doc, kind, now, hours)
    times.append(now.replace(microsecond=(now.microsecond // 1000) * 1000))  # Mongo keeps milliseconds
    coll.update_one({"_id": user_id}, {"$set": {kind: times, "updated_at": now}}, upsert=True)
'''

BOT_LIMITS = r'''# =============================================================================
# Private-chat limits and overload protection
#
#  * Each member gets DM_COMMAND_LIMIT commands (default 10) and DM_AUDIO_LIMIT audio
#    downloads (default 5) per rolling 24 hours. Stored in MongoDB, so restarts don't reset it.
#    /start, /help and /mylimits are free; menu buttons are not commands. Bot admins are exempt.
#  * Burst guard: too many messages or button taps in a few seconds are ignored for a while.
#  * Relay cap: non-command DMs are forwarded to the admin group, so they are capped per hour.
#  * Global breaker: if far too many DMs arrive per minute, the excess is dropped.
#  * Command-spam guard in groups (admins exempt).
# Every gate fails OPEN: if a check itself breaks, the message goes through.
# =============================================================================

from collections import deque


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


_DM_COMMAND_LIMIT = _env_int("DM_COMMAND_LIMIT", 10)
_DM_AUDIO_LIMIT = _env_int("DM_AUDIO_LIMIT", 5)
_DM_FREE_COMMANDS = {"start", "help", "mylimits"}
_DM_BURST_MESSAGES, _DM_BURST_WINDOW, _DM_BURST_PENALTY = 6, 10, 60      # msgs / seconds / block seconds
_DM_TAP_LIMIT, _DM_TAP_WINDOW, _DM_TAP_PENALTY = 8, 10, 20               # button taps
_DM_RELAY_PER_HOUR = 30
_DM_GLOBAL_PER_MINUTE = _env_int("DM_GLOBAL_PER_MINUTE", 300)
_GRP_CMD_LIMIT, _GRP_CMD_WINDOW, _GRP_CMD_PENALTY = 8, 20, 60

_dm_burst: dict[int, dict] = {}
_dm_taps: dict[int, dict] = {}
_dm_relay: dict[int, dict] = {}
_dm_global: deque = deque()
_dm_global_warned = float("-inf")
_grp_cmd_state: dict[tuple[int, int], dict] = {}
_dm_quota_lock = asyncio.Lock()


def _prune_state(state: dict, now: float, idle_seconds: float) -> None:
    """Keep these in-memory tables from growing forever."""
    if len(state) > 2000:
        for key in [k for k, v in state.items() if now - v.get("last", 0.0) > idle_seconds]:
            state.pop(key, None)


def _burst_blocked(state: dict, key, now: float, limit: int, window: int, penalty: int) -> tuple[bool, bool]:
    """Sliding-window burst check. Returns (blocked, just_started_blocking)."""
    st = state.setdefault(key, {"t": deque(), "until": 0.0, "last": now})
    st["last"] = now
    if now < st["until"]:
        return True, False
    times = st["t"]
    while times and now - times[0] > window:
        times.popleft()
    times.append(now)
    if len(times) > limit:
        st["until"] = now + penalty
        times.clear()
        return True, True
    return False, False


def _global_overloaded(now: float) -> bool:
    global _dm_global_warned
    while _dm_global and now - _dm_global[0] > 60:
        _dm_global.popleft()
    _dm_global.append(now)
    if len(_dm_global) > _DM_GLOBAL_PER_MINUTE:
        if now - _dm_global_warned > 60:
            _dm_global_warned = now
            logger.warning("DM overload: more than %s private updates/minute, dropping the excess", _DM_GLOBAL_PER_MINUTE)
        return True
    return False


async def _dm_check_message(msg, user) -> tuple[bool, str | None]:
    """(blocked, notice_to_send). Silent block = (True, None)."""
    now = time.monotonic()
    if _global_overloaded(now):
        return True, None

    blocked, started = _burst_blocked(_dm_burst, user.id, now, _DM_BURST_MESSAGES, _DM_BURST_WINDOW, _DM_BURST_PENALTY)
    if blocked:
        return True, ("You're sending messages too fast. I'll listen again in a minute." if started else None)
    _prune_state(_dm_burst, now, 3600)

    text = msg.text or ""
    if not text.startswith("/"):
        st = _dm_relay.setdefault(user.id, {"t": deque(), "told": float("-inf"), "last": now})
        st["last"] = now
        while st["t"] and now - st["t"][0] > 3600:
            st["t"].popleft()
        if len(st["t"]) >= _DM_RELAY_PER_HOUR:
            told = now - st["told"] < 3600
            st["told"] = now if not told else st["told"]
            return True, (None if told else "You've sent a lot of messages this hour. Please try again later.")
        st["t"].append(now)
        _prune_state(_dm_relay, now, 7200)
        return False, None

    parts = text.split()
    name = parts[0][1:].split("@")[0].lower()
    if name in _DM_FREE_COMMANDS and not (name == "start" and len(parts) > 1):  # "/start privacy" does real work
        return False, None

    async with _dm_quota_lock:
        when = datetime.now(timezone.utc)
        used, opens_at = await asyncio.to_thread(dbmod.dm_quota_status, user.id, "commands", when)
        if used >= _DM_COMMAND_LIMIT:
            wait = max(60, int((opens_at - when).total_seconds())) if opens_at else 60
            return True, (
                f"Your limit is over: {_DM_COMMAND_LIMIT} commands per 24 hours. "
                f"Come back in about {_format_duration(wait)}."
            )
        await asyncio.to_thread(dbmod.dm_quota_consume, user.id, "commands", when)
    return False, None


async def dm_message_gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs before every other handler (group -1) for private messages."""
    from telegram.ext import ApplicationHandlerStop

    msg, user = update.message, update.effective_user
    if not msg or not user or user.is_bot or _is_admin_user(user.id):
        return
    try:
        blocked, notice = await _dm_check_message(msg, user)
    except Exception:
        logger.exception("DM gate failed; letting the message through")
        return
    if blocked:
        if notice:
            try:
                await msg.reply_text(notice)
            except Exception:
                pass
        raise ApplicationHandlerStop


async def dm_callback_gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Button taps: ignore rapid-fire tapping (and shed load if the whole bot is swamped)."""
    from telegram.ext import ApplicationHandlerStop

    query = update.callback_query
    user = query.from_user if query else None
    if not user or user.is_bot or _is_admin_user(user.id):
        return
    try:
        now = time.monotonic()
        overloaded = _global_overloaded(now)
        blocked, started = _burst_blocked(_dm_taps, user.id, now, _DM_TAP_LIMIT, _DM_TAP_WINDOW, _DM_TAP_PENALTY)
        _prune_state(_dm_taps, now, 3600)
    except Exception:
        logger.exception("Callback gate failed; letting the tap through")
        return
    if overloaded or blocked:
        try:
            await query.answer("Slow down a little, then try again." if started else None)
        except Exception:
            pass
        raise ApplicationHandlerStop


async def group_command_gate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Group command spam guard: more than 8 commands in 20s from one member are ignored
    for a minute. Admins are never throttled."""
    from telegram.ext import ApplicationHandlerStop

    msg, user, chat = update.message, update.effective_user, update.effective_chat
    if not msg or not user or user.is_bot or not chat:
        return
    try:
        now = time.monotonic()
        blocked, started = _burst_blocked(
            _grp_cmd_state, (chat.id, user.id), now, _GRP_CMD_LIMIT, _GRP_CMD_WINDOW, _GRP_CMD_PENALTY
        )
        _prune_state(_grp_cmd_state, now, 3600)
        if not blocked:
            return
        if await _is_group_admin(update, context):
            _grp_cmd_state[(chat.id, user.id)]["until"] = 0.0
            return
    except Exception:
        logger.exception("Group command gate failed; letting the command through")
        return
    if started:
        try:
            await _reply_autodelete(update, context, "Too many commands too quickly. Please wait a minute.")
        except Exception:
            pass
    raise ApplicationHandlerStop


async def cmd_mylimits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Private chat: how many commands and audio downloads you have left (rolling 24h)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        await _reply_autodelete(update, context, "Send /mylimits to me in a private chat.")
        return
    user = update.effective_user
    if _is_admin_user(user.id):
        await update.message.reply_text("Bot admins have no limits.")
        return
    now = datetime.now(timezone.utc)
    lines = ["Your limits (rolling 24 hours)", ""]
    for kind, label, limit in (("commands", "Commands", _DM_COMMAND_LIMIT), ("audios", "Audios", _DM_AUDIO_LIMIT)):
        used, opens_at = await asyncio.to_thread(dbmod.dm_quota_status, user.id, kind, now)
        line = f"{label}: {used} of {limit} used, {max(0, limit - used)} left"
        if used >= limit and opens_at:
            line += f" (next slot in {_format_duration(max(60, int((opens_at - now).total_seconds())))})"
        lines.append(line)
    lines += ["", "/start, /help and /mylimits are free."]
    await update.message.reply_text("\n".join(lines))


'''

DB_OPS = [("append", "db: usage quota functions", DB_LIMITS)]

BOT_OPS = [
    ("sub", "bot: /mylimits help entry",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "mylimits": ("everyone", "/mylimits  (in a private chat with the bot)", "See how many commands and audio downloads you have left today.", "Anyone"),'''),
    ("sub", "bot: limits code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_LIMITS + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: register /mylimits and the gates",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("mylimits", cmd_mylimits))
    # Gates run first (group -1) and stop the update when a limit is hit.
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & filters.UpdateType.MESSAGE, dm_message_gate), group=-1
    )
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.COMMAND & filters.UpdateType.MESSAGE, group_command_gate),
        group=-1,
    )
    app.add_handler(CallbackQueryHandler(dm_callback_gate), group=-1)'''),
    ("sub", "bot: audio download limit (check before sending)",
     r'''            await context.bot.copy_message(
                chat_id=query.from_user.id,''',
     r'''            is_admin = _is_admin_user(query.from_user.id)
            if not is_admin:
                used, opens_at = await asyncio.to_thread(
                    dbmod.dm_quota_status, query.from_user.id, "audios", datetime.now(timezone.utc)
                )
                if used >= _DM_AUDIO_LIMIT:
                    wait = int((opens_at - datetime.now(timezone.utc)).total_seconds()) if opens_at else 0
                    await query.answer(
                        f"Your limit is over ({_DM_AUDIO_LIMIT} audios per day). Come back after 24 hours "
                        f"to get other audios. Next one unlocks in {_format_duration(max(60, wait))}.",
                        show_alert=True,
                    )
                    return
            await context.bot.copy_message(
                chat_id=query.from_user.id,'''),
    ("sub", "bot: audio download limit (count after a successful send)",
     r'''await query.answer("Sending your audio...")''',
     r'''if not is_admin:
                await asyncio.to_thread(
                    dbmod.dm_quota_consume, query.from_user.id, "audios", datetime.now(timezone.utc)
                )
            await query.answer("Sending your audio...")'''),
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
    if PREREQ_BOT not in texts["bot.py"]:
        print("The Read & Record feature isn't in bot.py yet. Run apply_rnr.py first, then this script.")
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
