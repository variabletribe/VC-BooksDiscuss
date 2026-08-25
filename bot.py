"""
Group voice/video chat tracker for Telegram.

Telegram's Bot API does not expose "everyone who joined the VC" or real per-user
durations. It only offers invite-style participant hints plus total call duration.
This bot uses:
- video_chat_participants_invited (subset of people, not all joiners)
- video_chat_ended.duration (official call length in seconds)

Data is stored in SQLite (local) or PostgreSQL (DATABASE_URL, e.g. Render).

Env: BOT_TOKEN, optional DATABASE_URL, MONTHLY_REPORT_HOUR_UTC (default 9).
On Render Web Services (RENDER_EXTERNAL_URL + PORT), the bot uses a webhook instead of getUpdates,
which avoids Telegram Conflict when only one public URL receives updates. Locally, use polling
(no public URL). Optional: WEBHOOK_URL + USE_WEBHOOK=1 + PORT for tunnels; FORCE_POLLING=1 to
disable webhook on Render. TELEGRAM_WEBHOOK_PATH / TELEGRAM_WEBHOOK_SECRET optional.
If PORT is set but webhook mode is off, a tiny HTTP stub is started for health checks.

If /start never works on free Render (521 / webhook errors), set FORCE_POLLING=1 with exactly
one running service so Telegram uses getUpdates instead of pushing to your URL. WEBHOOK_DEBUG=1
enables verbose telegram.ext logs (incoming webhook POSTs).

On Render free Web services, idle spin-down yields HTTP 521 and Telegram webhook backlog.
While the process runs, a background keep-alive GETs RENDER_EXTERNAL_URL every
KEEP_ALIVE_INTERVAL_SECONDS (default 300). Disable with KEEP_ALIVE_DISABLE=1.
For long cold starts, use an external uptime monitor or a paid instance.

Privacy: @BotFather -> /setprivacy -> Disable if service messages are missing.

--- Admin DM relay + broadcast --------------------------------------------
ADMIN_RELAY_CHAT_ID = chat id of a private admin group (bot must be a member).
    Any private DM sent to the bot gets copied into this group with a small
    header. Admins reply (native Telegram reply, long-press -> Reply) to that
    copied message inside the admin group, and the bot delivers the reply back
    to the original sender. A plain new message in the admin group does NOT
    get routed anywhere — only replies to a relayed message do.
ADMIN_USER_IDS = comma-separated Telegram user ids allowed to use /message,
    /broadcast, and reply-relay in the admin group.
BROADCAST_CHAT_ID (optional) = which tracked group's VC participant list
    /broadcast targets. Defaults to the first id in ASSISTANT_GROUP_IDS.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import io
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict

import httpx
from dotenv import load_dotenv
from telegram import ChatPermissions, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, MessageEntity, Update
from telegram.constants import ChatMemberStatus
from telegram.error import Conflict, InvalidToken
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    filters,
)
from telegram.ext.filters import MessageFilter

import db as dbmod
import state as app_state

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
# Avoid logging full Telegram URLs (they embed the bot token).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def _configure_debug_loggers() -> None:
    if _env_truthy("WEBHOOK_DEBUG"):
        logging.getLogger("telegram.ext").setLevel(logging.DEBUG)
        logging.getLogger("telegram").setLevel(logging.DEBUG)
        logger.info("WEBHOOK_DEBUG=1: telegram.ext logging at DEBUG")


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def _webhook_public_base() -> str | None:
    base = (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("WEBHOOK_URL") or "").strip().rstrip("/")
    return base or None


def _webhook_path_segment() -> str:
    custom = (os.environ.get("TELEGRAM_WEBHOOK_PATH") or "").strip().strip("/")
    if custom:
        return custom
    token = (os.environ.get("BOT_TOKEN") or "").encode()
    return hashlib.sha256(token).hexdigest()[:20]


def _use_webhook() -> bool:
    if _env_truthy("FORCE_POLLING"):
        return False
    base = _webhook_public_base()
    port = os.environ.get("PORT")
    if not base or not port:
        return False
    if _env_truthy("USE_WEBHOOK"):
        return True
    # Render web/static sets RENDER_EXTERNAL_URL; workers leave it empty.
    if (os.environ.get("RENDER_EXTERNAL_URL") or "").strip():
        return True
    return False


def _log_webhook_info(token: str) -> None:
    """Log current Bot API webhook state (helps debug Conflict / wrong URL)."""
    try:
        r = httpx.get(
            f"https://api.telegram.org/bot{token}/getWebhookInfo",
            timeout=20.0,
        )
        r.raise_for_status()
        body = r.json()
        res = body.get("result") if isinstance(body, dict) else None
        if not isinstance(res, dict):
            logger.warning("getWebhookInfo: unexpected response shape")
            return
        pending_raw = res.get("pending_update_count")
        last_err = res.get("last_error_message") or ""
        try:
            pending_n = int(pending_raw or 0)
        except (TypeError, ValueError):
            pending_n = 0
        logger.info(
            "getWebhookInfo: url=%r pending_updates=%s last_error_date=%s last_error=%r",
            res.get("url"),
            pending_raw,
            res.get("last_error_date"),
            last_err,
        )
        if pending_n > 0:
            logger.warning(
                "Telegram has %s webhook update(s) still queued—earlier deliveries failed "
                "(often 521 when Render was asleep). After this deploy they should drain; "
                "keep-alive reduces sleep. External ping or paid tier is most reliable.",
                pending_n,
            )
        err_l = str(last_err).lower()
        if "521" in err_l or "503" in err_l or "wrong response" in err_l or "timeout" in err_l:
            logger.warning(
                "Recent webhook error from Telegram: %r — origin unreachable or bad response. "
                "Typical on free Render when idle.",
                (last_err[:240] + "…") if len(str(last_err)) > 240 else last_err,
            )
        if "403" in err_l or "forbidden" in err_l or "secret" in err_l:
            logger.warning(
                "Webhook may be rejecting requests (403/forbidden/secret). If you set "
                "TELEGRAM_WEBHOOK_SECRET in Render, it must match what Telegram has; easiest fix "
                "is to remove TELEGRAM_WEBHOOK_SECRET from the dashboard and redeploy so PTB sets "
                "a clean webhook without a secret, unless you need that header for security."
            )
    except Exception:
        logger.warning("getWebhookInfo failed (non-fatal)", exc_info=True)


def _log_bot_identity(token: str) -> None:
    """Confirm BOT_TOKEN is valid; does not prove webhooks are delivered."""
    try:
        r = httpx.get(f"https://api.telegram.org/bot{token}/getMe", timeout=20.0)
        r.raise_for_status()
        body = r.json()
        res = body.get("result") if isinstance(body, dict) else None
        if not isinstance(res, dict):
            logger.warning("getMe: unexpected response shape")
            return
        uname = res.get("username")
        bid = res.get("id")
        logger.info(
            "getMe OK: bot id=%s @%s — token is valid; silent /start means updates are not "
            "reaching the app (webhook 521/403, wrong URL, or a second process using this token).",
            bid,
            uname,
        )
    except httpx.HTTPStatusError as exc:
        logger.error(
            "getMe HTTP %s: %s",
            exc.response.status_code,
            (exc.response.text or "")[:400],
        )
    except Exception:
        logger.exception("getMe request failed")


def _start_render_keepalive_thread() -> None:
    """GET the public service URL on an interval so Render's idle timer resets (free Web tier)."""
    base = (os.environ.get("RENDER_EXTERNAL_URL") or "").strip().rstrip("/")
    if not base:
        return
    try:
        interval = max(60, int(os.getenv("KEEP_ALIVE_INTERVAL_SECONDS", "300")))
    except ValueError:
        interval = 300
    try:
        start_delay = max(20, int(os.getenv("KEEP_ALIVE_START_DELAY_SECONDS", "90")))
    except ValueError:
        start_delay = 90
    path = (os.getenv("KEEP_ALIVE_PATH") or "/").strip()
    if not path.startswith("/"):
        path = "/" + path
    url = f"{base}{path}"

    def _run() -> None:
        time.sleep(start_delay)
        logger.info(
            "Keep-alive: GET %s every %ss (KEEP_ALIVE_INTERVAL_SECONDS; disable KEEP_ALIVE_DISABLE=1)",
            url,
            interval,
        )
        while True:
            try:
                r = httpx.get(url, timeout=45.0, follow_redirects=True)
                if r.status_code >= 500:
                    logger.warning("Keep-alive GET %s returned HTTP %s", url, r.status_code)
            except Exception as exc:
                logger.warning("Keep-alive request failed: %s", exc)
            time.sleep(interval)

    threading.Thread(target=_run, name="render-keepalive", daemon=True).start()


def _start_http_on_port_for_render() -> None:
    """Render Web Services require a bound PORT; polling bots otherwise fail the port scan."""
    raw = os.environ.get("PORT")
    if not raw:
        return
    try:
        port = int(raw)
    except ValueError:
        logger.warning("PORT is not an integer (%r); skipping HTTP stub", raw)
        return

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.write(b"ok")

    def _run():
        HTTPServer(("0.0.0.0", port), _Handler).serve_forever()

    threading.Thread(target=_run, name="http-port", daemon=True).start()
    logger.info("HTTP stub listening on 0.0.0.0:%s (Render PORT check)", port)


def _utc_ts(dt: datetime) -> float:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


@dataclass
class VCSession:
    started_at: datetime | None = None
    participants: dict[int, tuple[str, datetime]] = field(default_factory=dict)


_sessions: Dict[int, VCSession] = {}


def _user_label(user) -> str:
    parts = []
    if user.first_name:
        parts.append(user.first_name)
    if user.last_name:
        parts.append(user.last_name)
    name = " ".join(parts).strip()
    if user.username:
        name = f"{name} (@{user.username})" if name else f"@{user.username}"
    return name or str(user.id)


def _format_duration(seconds: int) -> str:
    if seconds <= 0:
        return "0s"
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s" if s else f"{m}m"
    return f"{s}s"


def _month_name(month: int) -> str:
    return datetime(2000, month, 1, tzinfo=timezone.utc).strftime("%B")


def _format_duration_hours(seconds: int) -> str:
    if seconds <= 0:
        return "0h"
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}h {m}m" if m else f"{h}h"
    if m:
        return f"{m}m {s}s" if s else f"{m}m"
    return f"{s}s"


def _format_date_utc(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d %b %Y")


def _format_vc_stats_html(
    title: str,
    subtitle: str,
    rows: list[dbmod.VCStatsRow],
) -> str:
    lines = [
        f"📊 <b>{html.escape(title)}</b>",
        f"<i>{html.escape(subtitle)}</i>",
        "",
    ]
    for i, row in enumerate(rows, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        vc_word = "VC" if row.vc_count == 1 else "VCs"
        lines.append(
            f"{medal} {safe} — <b>{row.vc_count}</b> {vc_word}, "
            f"<b>{_format_duration_hours(row.total_seconds)}</b>"
        )
    lines.append("")
    lines.append("<i>VC count = calls joined · time = total minutes/hours in calls.</i>")
    return "\n".join(lines)


def _format_attendance_html(rows: list[dbmod.AttendanceRow]) -> str:
    threshold_min = dbmod.present_threshold_sec() // 60
    lines = [
        "📋 <b>Present attendance</b>",
        " <i>Counts from 4 August 2026</i>",
        f"<i>More than {threshold_min} minutes in one call = +1 present day (once per call).</i>",
        "",
    ]
    for i, row in enumerate(rows, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        day_word = "day" if row.present_days == 1 else "days"
        lines.append(f"{medal} {safe} — <b>{row.present_days}</b> {day_word}")
    return "\n".join(lines)


COMMAND_AUTODELETE_SECONDS = 30


async def _delete_messages_later(context: ContextTypes.DEFAULT_TYPE) -> None:
    """job_queue callback: deletes a batch of message ids in one chat.

    Only ever scheduled by _reply_autodelete (i.e. only for command replies). The bot's
    own automatic posts — VC summaries, attendance, badges, AI recap, monthly/weekly
    reports, admin-relay messages — always go straight through reply_text/send_message
    and never touch this function, so they're never auto-deleted.
    """
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    message_ids = data.get("message_ids") or []
    for mid in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=mid)
        except Exception:
            logger.debug(
                "Autodelete: could not delete message_id=%s in chat_id=%s (already gone, "
                "or the bot isn't a group admin with 'Delete messages' rights)",
                mid,
                chat_id,
            )


async def _reply_autodelete(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    parse_mode: str | None = None,
):
    """Reply to a command, then — only in groups — schedule both that reply and the
    person's own /command message for deletion after COMMAND_AUTODELETE_SECONDS.

    Every cmd_* handler below uses this instead of update.message.reply_text directly.
    Nothing the bot posts on its own initiative goes through this helper, so this only
    ever affects command-and-response pairs, never the bot's automatic messages.

    Requires the bot to be a group admin with "Delete messages" permission — without it,
    Telegram just silently refuses the delete (logged at debug level); nothing breaks,
    the messages simply stay visible.
    """
    if not update.message or not update.effective_chat:
        return None
    sent = await update.message.reply_text(text, parse_mode=parse_mode)
    if update.effective_chat.type not in ("group", "supergroup"):
        return sent
    jq = context.job_queue
    if jq is None:
        return sent
    jq.run_once(
        _delete_messages_later,
        when=COMMAND_AUTODELETE_SECONDS,
        data={
            "chat_id": update.effective_chat.id,
            "message_ids": [update.message.message_id, sent.message_id],
        },
        name=f"autodelete-{update.effective_chat.id}-{sent.message_id}",
    )
    return sent


# =============================================================================
# Interactive /start menu: category buttons -> command buttons -> per-command detail.
# Replaces one giant wall of text with a tappable menu, since the old /start had grown
# to 50+ commands worth of text in a single message.
#
# HELP_COMMANDS[key] = (category_key, usage, description, access_note)
# Kept as the single source of truth — the README's command tables are generated
# from (a copy of) this same data, so the two can't drift apart silently.
# =============================================================================

HELP_CATEGORIES: dict[str, str] = {
    "stats": "📊 Stats & Progress",
    "topics": "🗂️ VC Topics",
    "mod": "🛡️ Moderation",
    "everyone": "🔔 Everyone",
    "groupadmin": "🛠️ Group Admin Tools",
    "botadmin": "👑 Bot Owner Tools",
}

HELP_COMMANDS: dict[str, tuple[str, str, str, str]] = {
    # --- Stats & Progress (anyone) ---
    "vcreport": ("stats", "/vcreport", "All-time leaderboard: VCs joined and total hours, per user.", "Anyone"),
    "attendance": ("stats", "/attendance", "Present-day leaderboard — people who crossed the attendance threshold.", "Anyone"),
    "monthreport": ("stats", "/monthreport", "Previous calendar month's leaderboard.", "Anyone"),
    "weekly": ("stats", "/weekly", "Last 7 days: top hours and streak leaders.", "Anyone"),
    "vcstatus": ("stats", "/vcstatus", "This group's chat id, and whether the Telethon assistant is actively tracking it.", "Anyone"),
    "mystats": ("stats", "/mystats", "Your full profile: attendance, VCs, hours, join dates, streak, XP, level.", "Anyone"),
    "level": ("stats", "/level", "Your XP and level.", "Anyone"),
    "xpleaderboard": ("stats", "/xpleaderboard", "Top 10 XP earners in the group.", "Anyone"),
    "streak": ("stats", "/streak", "Your current and longest attendance streak.", "Anyone"),
    "badges": ("stats", "/badges", "Your earned badges.", "Anyone"),

    # --- VC Topics ---
    "addtopic": ("topics", "/addtopic <topic text>", "Suggest a discussion topic. Gets a permanent serial number that's never reused, even after deletion.", "Anyone"),
    "topics": ("topics", "/topics", "Active topics only, ranked by votes (see /upvote) so the group's real priority shows.", "Anyone"),
    "alltopics": ("topics", "/alltopics", "Every topic ever added, sorted by serial. Done shows ✅, deleted shows struck through.", "Anyone"),
    "deletedtopics": ("topics", "/deletedtopics", "Deleted topics, with their original serial numbers.", "Anyone"),
    "upvote": ("topics", "/upvote <serial>", "Support an active topic so it ranks higher in /topics. One vote per person per topic; +2 XP the first time.", "Anyone"),
    "topicdone": ("topics", "/topicdone <serial>", "Mark an active topic as Done.", "Group admin"),
    "deletetopic": ("topics", "/deletetopic <serial>", "Delete a topic. The serial number is never reused for a future topic.", "Group admin"),

    # --- Moderation ---
    "warn": ("mod", "/warn [reply / id / @user] [reason]", "Warn a user. At the warn limit (default 3), auto-applies the configured punishment (default: kick) and resets the count.", "Group admin"),
    "warns": ("mod", "/warns [reply / id / @user]", "Check a user's warnings — defaults to your own if no target is given.", "Group admin"),
    "resetwarn": ("mod", "/resetwarn [reply / id / @user]", "Clear a user's warnings back to zero.", "Group admin"),
    "warnlimit": ("mod", "/warnlimit [n]", "View, or set, how many warnings trigger auto-punishment (default 3).", "Group admin"),
    "warnmode": ("mod", "/warnmode [ban/mute/kick]", "View, or set, what happens at the warn limit (default kick).", "Group admin"),
    "ban": ("mod", "/ban [reply / id / @user] [reason]", "Permanently ban a user. Shows a confirm/cancel button first — nothing happens until you tap Confirm.", "Group admin"),
    "tban": ("mod", "/tban [reply / id / @user] <time> [reason]", "Temporary ban — time as 30m, 2h, 1d, or 1w. Telegram itself lifts it automatically, even across a bot restart.", "Group admin"),
    "unban": ("mod", "/unban [id / @user]", "Lift a ban early.", "Group admin"),
    "kick": ("mod", "/kick [reply / id / @user] [reason]", "Remove someone from the group — they CAN rejoin (this is a ban immediately followed by an unban).", "Group admin"),
    "mute": ("mod", "/mute [reply / id / @user] [reason]", "Restrict a user from sending anything, indefinitely.", "Group admin"),
    "tmute": ("mod", "/tmute [reply / id / @user] <time> [reason]", "Temporary mute — same time syntax as /tban, also enforced natively by Telegram.", "Group admin"),
    "unmute": ("mod", "/unmute [reply / id / @user]", "Lift a mute early, restoring the group's normal permissions.", "Group admin"),
    "blocklist": ("mod", "/blocklist", "View the current blocklisted words and the action mode.", "Group admin"),
    "addblocklist": ("mod", "/addblocklist word1 word2 ...", "Add one or more words to the blocklist.", "Group admin"),
    "unblocklist": ("mod", "/unblocklist word1 word2 ...", "Remove words from the blocklist.", "Group admin"),
    "blocklistmode": ("mod", "/blocklistmode [delete/warn/mute/kick/ban]", "View, or set, what happens when someone posts a blocklisted word. The message is always deleted; this controls what (if anything) also happens to the sender.", "Group admin"),
    "filter": ("mod", "/filter <keyword> <reply text>  —  or reply to any message with /filter <keyword>", "Save an auto-reply for a keyword. Typed text keeps its exact formatting; replying to a message saves it verbatim (photo, video, sticker, document, etc.).", "Group admin"),
    "filters": ("mod", "/filters", "List every saved filter keyword.", "Group admin"),
    "stop": ("mod", "/stop <keyword>", "Remove a saved filter.", "Group admin"),
    "lock": ("mod", "/lock links", "From now on, delete any non-admin message containing a link.", "Group admin"),
    "unlock": ("mod", "/unlock links", "Turn link-locking back off.", "Group admin"),
    "locks": ("mod", "/locks", "View which content types are currently locked.", "Group admin"),
    "captcha": ("mod", "/captcha on|off", "New-member verification: joiners are muted and must tap a button within 5 minutes, or they're auto-kicked (and can rejoin to retry).", "Anyone can view; group admin to change"),
    "setflood": ("mod", "/setflood <count> [window_seconds]  or  /setflood off", "Auto-punish anyone posting too many messages too fast. Off by default.", "Anyone can view; group admin to change"),
    "floodmode": ("mod", "/floodmode [mute/kick/ban]", "What happens when flood control triggers.", "Anyone can view; group admin to change"),
    "modlog": ("mod", "/modlog [count]", "The last moderation actions in this group: who did what, to whom, when, and why.", "Group admin"),
    "allowlink": ("mod", "/allowlink [reply / id / @user]", "Allow a user to send links even when link lock is on.", "Group admin"),
    "disallowlink": ("mod", "/disallowlink [reply / id / @user]", "Remove a user from the link allowlist.", "Group admin"),
    "allowlist": ("mod", "/allowlist", "View the list of users allowed to send links.", "Group admin"),

    # --- Everyone (no admin needed at all) ---
    "timer": ("everyone", "/timer <N>m", "One-shot reminder timer — minutes only, max 20m, one running per group at a time.", "Anyone"),
    "canceltimer": ("everyone", "/canceltimer", "Cancel the currently running timer early.", "Anyone"),
    "mywarns": ("everyone", "/mywarns", "See your own full warning history — active and cleared, with dates and reasons.", "Anyone"),

    # --- Group Admin Tools (Telegram group admin/owner status) ---
    "streakboard": ("groupadmin", "/streakboard", "Every member's current and best attendance streak, ranked.", "Group admin"),
    "reports": ("groupadmin", "/reports on|off", "Toggle the automatic monthly report for this group.", "Group admin"),
    "removeuser": ("groupadmin", "/removeuser USER_ID", "Wipe a user's VC stats and attendance. Shows a preview and asks for confirmation first.", "Group admin"),
    "finduser": ("groupadmin", "/finduser NAME", "Look up a user's numeric id by name or an old @username.", "Group admin"),

    # --- Bot Owner Tools (ADMIN_USER_IDS — cross-group, not tied to any one group) ---
    "message": ("botadmin", "/message ID [ID2 ID3 ...] text  —  or reply to any message with /message ID [ID2 ...]", "DM one or more users directly by numeric id. Reply to any message (text, photo, audio, video, etc.) to copy it verbatim instead of typing text.", "Bot admin"),
    "broadcast": ("botadmin", "/broadcast text  (or reply to a message with /broadcast)", "Message everyone who has ever joined a tracked VC. Shows the audience size and asks for confirmation before sending.", "Bot admin"),
    "exportdata": ("botadmin", "/exportdata [chat_id]", "CSV of every user who's joined a VC — hours, present days, streaks, XP, level, join dates. Works only in a DM with the bot.", "Bot admin, DM only"),
    "user": ("botadmin", "/user USER_ID [chat_id]", "Full stats for any one user by id, without needing them to run /mystats themselves. DM only.", "Bot admin, DM only"),
    "health": ("botadmin", "/health", "Checks MongoDB, the Telegram Bot API, the Telethon assistant, and Groq — catches a silent failure before it's noticed the hard way. DM only.", "Bot admin, DM only"),
}


def _help_main_menu_text() -> str:
    return (
        "🎙️ <b>BooksDiscuss VC Tracker Bot</b>\n\n"
        "Tracks VC attendance, XP &amp; levels, VC topic suggestions, and full "
        "group moderation — all in one bot.\n\n"
        "Tap a category below, then tap any command to see exactly what it does "
        "and how to use it."
    )


def _help_main_menu_keyboard() -> InlineKeyboardMarkup:
    items = list(HELP_CATEGORIES.items())
    rows = [
        [InlineKeyboardButton(items[i][1], callback_data=f"menu:cat:{items[i][0]}")]
        + ([InlineKeyboardButton(items[i + 1][1], callback_data=f"menu:cat:{items[i + 1][0]}")] if i + 1 < len(items) else [])
        for i in range(0, len(items), 2)
    ]
    return InlineKeyboardMarkup(rows)


def _help_category_text(cat_key: str) -> str:
    label = HELP_CATEGORIES.get(cat_key, cat_key)
    return f"{label}\n\nTap a command to see what it does and how to use it."


def _help_category_keyboard(cat_key: str) -> InlineKeyboardMarkup:
    cmd_keys = [k for k, v in HELP_COMMANDS.items() if v[0] == cat_key]
    rows = []
    for i in range(0, len(cmd_keys), 2):
        row = [InlineKeyboardButton(f"/{cmd_keys[i]}", callback_data=f"menu:cmd:{cmd_keys[i]}")]
        if i + 1 < len(cmd_keys):
            row.append(InlineKeyboardButton(f"/{cmd_keys[i + 1]}", callback_data=f"menu:cmd:{cmd_keys[i + 1]}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back to categories", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _help_command_text(cmd_key: str) -> str | None:
    entry = HELP_COMMANDS.get(cmd_key)
    if entry is None:
        return None
    _cat_key, usage, desc, access = entry
    return (
        f"<b>/{cmd_key}</b>\n\n"
        f"{html.escape(desc, quote=False)}\n\n"
        f"<b>Usage:</b> <code>{html.escape(usage, quote=False)}</code>\n"
        f"<b>Who can use it:</b> {html.escape(access, quote=False)}"
    )


def _help_command_keyboard(cmd_key: str) -> InlineKeyboardMarkup:
    cat_key = HELP_COMMANDS[cmd_key][0]
    cat_label = HELP_CATEGORIES.get(cat_key, cat_key)
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🔙 Back to {cat_label}", callback_data=f"menu:cat:{cat_key}")],
            [InlineKeyboardButton("🏠 Main menu", callback_data="menu:main")],
        ]
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Shows the tappable category menu. Deliberately NOT sent through
    _reply_autodelete — this is meant to be browsed, not auto-cleaned after 30s
    like a normal command response."""
    if not update.message:
        return
    await update.message.reply_text(
        _help_main_menu_text(), parse_mode="HTML", reply_markup=_help_main_menu_keyboard()
    )


async def on_help_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles every menu:... callback from the /start button tree."""
    query = update.callback_query
    if not query or not query.data:
        return
    _prefix, _, rest = query.data.partition(":")
    action, _, key = rest.partition(":")

    if action == "main":
        await query.answer()
        try:
            await query.edit_message_text(
                _help_main_menu_text(), parse_mode="HTML", reply_markup=_help_main_menu_keyboard()
            )
        except Exception:
            pass
        return

    if action == "cat" and key in HELP_CATEGORIES:
        await query.answer()
        try:
            await query.edit_message_text(
                _help_category_text(key), parse_mode="HTML", reply_markup=_help_category_keyboard(key)
            )
        except Exception:
            pass
        return

    if action == "cmd":
        text = _help_command_text(key)
        if text is None:
            await query.answer("That command's details couldn't be found.", show_alert=True)
            return
        await query.answer()
        try:
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=_help_command_keyboard(key))
        except Exception:
            pass
        return

    await query.answer()


async def _is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_chat:
        return False

    # Anonymous admins ("send as group") show up with sender_chat == the group
    # itself, and update.effective_user is Telegram's GroupAnonymousBot
    # placeholder (state.GROUP_ANONYMOUS_BOT_ID) — not a real member, so
    # get_chat_member() below always fails for them even though they ARE an
    # admin. Telegram only allows group admins to post anonymously in the
    # first place, so this is a safe short-circuit.
    msg = update.message
    if msg and msg.sender_chat and msg.sender_chat.id == update.effective_chat.id:
        return True

    if not update.effective_user:
        return False
    try:
        m = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    except Exception:
        return False
    return m.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)


async def _message_sender_is_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """True if the actual sender of update.message is a real group owner/admin — covers
    both a normal admin's own account AND an admin posting anonymously ("send as group",
    where update.effective_user/msg.from_user is Telegram's GroupAnonymousBot placeholder,
    not the real admin). Passive message-scanner handlers (link-lock today; blocklist/flood
    can adopt this too) must use this instead of a raw get_chat_member lookup on
    msg.from_user.id — that placeholder id is not itself a member with admin rights, so a
    naive lookup treats a real admin's anonymous post as an ordinary member's and lets
    enforcement fire on it."""
    msg = update.message
    chat = update.effective_chat
    if not msg or not chat:
        return False
    if msg.sender_chat and msg.sender_chat.id == chat.id:
        return True
    if not msg.from_user:
        return False
    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
    except Exception:
        return False
    return member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)


# =============================================================================
# Moderation: warn / ban / tban / kick / mute / tmute / blocklist
# (command names and behavior modeled on Rose bot)
# =============================================================================

WARN_MODES = ("ban", "mute", "kick")
BLOCKLIST_MODES = ("delete", "warn", "mute", "kick", "ban")
FLOOD_MODES = ("mute", "kick", "ban")

# Telegram's ban_chat_member/restrict_chat_member until_date must be at least
# 30s and at most 366 days out, or Telegram treats it as a permanent action.
_MIN_TIMED_ACTION_SECONDS = 30
_MAX_TIMED_ACTION_SECONDS = 366 * 24 * 3600

_DURATION_RE = re.compile(r"^(\d+)([mhdw])$", re.IGNORECASE)


def _parse_duration(text: str) -> timedelta | None:
    """Rose-style duration shorthand: 30m, 2h, 1d, 1w. Returns None if invalid."""
    m = _DURATION_RE.match(text.strip())
    if not m:
        return None
    n = int(m.group(1))
    if n <= 0:
        return None
    unit = m.group(2).lower()
    if unit == "m":
        return timedelta(minutes=n)
    if unit == "h":
        return timedelta(hours=n)
    if unit == "d":
        return timedelta(days=n)
    if unit == "w":
        return timedelta(weeks=n)
    return None


def _clamp_until(delta: timedelta) -> datetime:
    total = max(_MIN_TIMED_ACTION_SECONDS, min(delta.total_seconds(), _MAX_TIMED_ACTION_SECONDS))
    return datetime.now(timezone.utc) + timedelta(seconds=total)


async def _resolve_user_ref(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, ref: str
) -> tuple[int, str] | None:
    """Resolve a bare numeric user id or @username into (user_id, label).
    Falls back to (id, id-as-string) for a numeric id that isn't currently a
    chat member (still lets admins act on someone who already left), but
    returns None for an @username Telegram can't resolve at all."""
    if ref.lstrip("-").isdigit():
        try:
            member = await context.bot.get_chat_member(chat_id, int(ref))
            return member.user.id, _user_label(member.user)
        except Exception:
            return int(ref), ref
    if ref.startswith("@"):
        # Our own username -> id memory first — built passively from every message the
        # bot has seen in this group (see on_track_known_user). Telegram's own getChat
        # only resolves a user's @username if the bot has already been introduced to
        # them some other way (e.g. they DMed the bot), which normally fails for an
        # arbitrary group member, so it's only used as a last-resort fallback here.
        known = await asyncio.to_thread(dbmod.resolve_username, chat_id, ref)
        if known is not None:
            return known
        try:
            chat = await context.bot.get_chat(ref)
            label = f"@{chat.username}" if chat.username else (chat.first_name or ref)
            return chat.id, label
        except Exception:
            return None
    return None


async def _resolve_target_and_reason(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> tuple[int, str, str] | None:
    """(user_id, label, reason) from a reply, or from args[0] (id/@username) +
    the rest of args as the reason. Replies with a usage hint and returns None
    if nothing usable was given."""
    msg = update.message
    args = context.args or []

    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        return u.id, _user_label(u), " ".join(args).strip()

    if not args:
        await msg.reply_text(
            "Reply to a user's message, or give their user id / @username as the first argument."
        )
        return None

    resolved = await _resolve_user_ref(context, update.effective_chat.id, args[0])
    if resolved is None:
        await msg.reply_text(
            f"Couldn't resolve {html.escape(args[0], quote=False)} — try replying to their message instead."
        )
        return None
    uid, label = resolved
    return uid, label, " ".join(args[1:]).strip()


async def _resolve_target_time_reason(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> tuple[int, str, timedelta, str] | None:
    """(user_id, label, duration, reason) for /tban and /tmute. Replies with a
    usage hint and returns None if the target or duration couldn't be parsed."""
    msg = update.message
    args = context.args or []

    if msg.reply_to_message and msg.reply_to_message.from_user:
        u = msg.reply_to_message.from_user
        if not args:
            await msg.reply_text(
                "Usage (replying to a user): <command> <time> [reason]  e.g. 2h spamming"
            )
            return None
        duration = _parse_duration(args[0])
        if duration is None:
            await msg.reply_text("Invalid time — use a number + m/h/d/w, e.g. 30m, 2h, 1d, 1w.")
            return None
        return u.id, _user_label(u), duration, " ".join(args[1:]).strip()

    if len(args) < 2:
        await msg.reply_text(
            "Usage: <command> <user_id or @username> <time> [reason]  e.g. 12345 2h spamming"
        )
        return None

    resolved = await _resolve_user_ref(context, update.effective_chat.id, args[0])
    if resolved is None:
        await msg.reply_text(
            f"Couldn't resolve {html.escape(args[0], quote=False)} — try replying to their message instead."
        )
        return None
    duration = _parse_duration(args[1])
    if duration is None:
        await msg.reply_text("Invalid time — use a number + m/h/d/w, e.g. 30m, 2h, 1d, 1w.")
        return None
    uid, label = resolved
    return uid, label, duration, " ".join(args[2:]).strip()


async def _target_is_protected(update: Update, context: ContextTypes.DEFAULT_TYPE, target_id: int) -> bool:
    """True for the bot itself or any owner/admin — these commands must never act on them,
    so a misfired blocklist word or a mistaken command can't lock mods out of their own group."""
    if target_id == context.bot.id:
        return True
    try:
        m = await context.bot.get_chat_member(update.effective_chat.id, target_id)
    except Exception:
        return False
    return m.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)


_MUTE_PERMISSIONS = ChatPermissions(
    can_send_messages=False,
    can_send_audios=False,
    can_send_documents=False,
    can_send_photos=False,
    can_send_videos=False,
    can_send_video_notes=False,
    can_send_voice_notes=False,
    can_send_polls=False,
    can_send_other_messages=False,
    can_add_web_page_previews=False,
)

_UNMUTE_FALLBACK_PERMISSIONS = ChatPermissions(
    can_send_messages=True,
    can_send_audios=True,
    can_send_documents=True,
    can_send_photos=True,
    can_send_videos=True,
    can_send_video_notes=True,
    can_send_voice_notes=True,
    can_send_polls=True,
    can_send_other_messages=True,
    can_add_web_page_previews=True,
)


# =============================================================================
# Generic confirmation flow — used by /ban, /removeuser, /broadcast: irreversible or
# broad-blast-radius actions that shouldn't fire from a single fat-fingered command.
# In-memory only (not Mongo): a pending confirmation is meant to be acted on within
# seconds by the same admin who's mid-conversation with the bot, so losing it on a
# redeploy just means re-running the command — not worth the persistence overhead.
# =============================================================================

_PENDING_CONFIRMATION_TTL_SECONDS = 120
_pending_confirmations: dict[str, dict] = {}  # token -> {kind, chat_id, issuer_id, payload, expires_mono}


def _register_pending_confirmation(kind: str, chat_id: int, issuer_id: int, payload: dict) -> str:
    token = uuid.uuid4().hex[:12]
    _pending_confirmations[token] = {
        "kind": kind,
        "chat_id": chat_id,
        "issuer_id": issuer_id,
        "payload": payload,
        "expires_mono": time.monotonic() + _PENDING_CONFIRMATION_TTL_SECONDS,
    }
    return token


def _pop_valid_confirmation(token: str, clicker_id: int) -> dict | None:
    """Returns the pending confirmation dict if the token exists, hasn't expired, and the
    clicker is the same admin who issued the original command — otherwise None. Always
    removes the token (one-shot; a stale/foreign click can't be replayed)."""
    entry = _pending_confirmations.pop(token, None)
    if entry is None:
        return None
    if time.monotonic() > entry["expires_mono"]:
        return None
    if entry["issuer_id"] != clicker_id:
        # Put it back — a different admin clicking shouldn't consume/invalidate it for
        # the person who actually needs to confirm.
        _pending_confirmations[token] = entry
        return "forbidden"
    return entry


def _confirm_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"confirm:{token}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel:{token}"),
            ]
        ]
    )


async def on_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return
    action, _, token = query.data.partition(":")
    if action not in ("confirm", "cancel") or not token:
        return

    if action == "cancel":
        entry = _pending_confirmations.pop(token, None)
        if entry is None:
            await query.answer("This confirmation has expired.")
            return
        if entry["issuer_id"] != query.from_user.id:
            await query.answer("Only the admin who ran this command can cancel it.", show_alert=True)
            _pending_confirmations[token] = entry
            return
        await query.answer("Cancelled.")
        try:
            await query.edit_message_text("❌ Cancelled — no action taken.")
        except Exception:
            pass
        return

    entry = _pop_valid_confirmation(token, query.from_user.id)
    if entry == "forbidden":
        await query.answer("Only the admin who ran this command can confirm it.", show_alert=True)
        return
    if entry is None:
        await query.answer("This confirmation has expired — please re-run the command.", show_alert=True)
        try:
            await query.edit_message_text("⌛ This confirmation expired. Please re-run the command.")
        except Exception:
            pass
        return

    await query.answer("Working on it…")
    kind = entry["kind"]
    payload = entry["payload"]
    chat_id = entry["chat_id"]
    actor = query.from_user

    if kind == "ban":
        target_id = payload["target_id"]
        target_label = payload["target_label"]
        reason = payload["reason"]
        try:
            await context.bot.ban_chat_member(chat_id, target_id)
        except Exception:
            logger.exception("Confirmed ban failed chat_id=%s target=%s", chat_id, target_id)
            await _safe_edit(query, "❌ Ban failed — check that I'm an admin with ban rights.")
            return
        await asyncio.to_thread(
            dbmod.log_mod_action, chat_id, "ban", target_id, target_label, actor.id, _user_label(actor), reason
        )
        safe = html.escape(target_label, quote=False)
        safe_reason = html.escape(reason, quote=False) if reason else "No reason given"
        await _safe_edit(query, f"🚫 Banned {safe}\nReason: {safe_reason}")

    elif kind == "removeuser":
        target_id = payload["target_id"]
        result = await asyncio.to_thread(dbmod.remove_user_from_chat, chat_id, target_id)
        label = html.escape(result.display_name or str(target_id), quote=False)
        await _safe_edit(
            query,
            f"Removed <b>{label}</b> (<code>{target_id}</code>) from this group's stats:\n"
            f"• VC call records deleted: <b>{result.vc_rows_deleted}</b>\n"
            f"• Attendance records deleted: <b>{result.attendance_rows_deleted}</b>",
        )

    elif kind == "broadcast":
        users: list[tuple[int, str]] = payload["users"]
        source_chat_id = payload["source_chat_id"]
        source_message_id = payload.get("source_message_id")
        text_arg = payload.get("text_arg")
        sent, failed = 0, 0
        for uid, _label in users:
            try:
                if source_message_id:
                    await context.bot.copy_message(chat_id=uid, from_chat_id=source_chat_id, message_id=source_message_id)
                else:
                    await context.bot.send_message(uid, text_arg)
                sent += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
        await _safe_edit(query, f"📣 Broadcast done: {sent} sent, {failed} failed (out of {len(users)}).")


async def _safe_edit(query, text: str) -> None:
    try:
        await query.edit_message_text(text, parse_mode="HTML")
    except Exception:
        logger.debug("Confirmation: couldn't edit message after action")


async def _apply_punishment(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    target_id: int,
    target_label: str,
    mode: str,
    by_id: int | None = None,
    by_name: str = "",
    reason: str = "",
) -> str:
    """Executes ban/mute/kick on target_id and returns a short description for the reply.
    Shared by /warn (once the warn limit is hit) and blocklist enforcement. Records the
    action to the mod log; by_id/by_name default to the bot itself for automatic
    enforcement (blocklist), or should be passed explicitly for admin-triggered actions
    (e.g. /warn hitting its limit) so /modlog attributes it correctly."""
    chat_id = update.effective_chat.id
    safe = html.escape(target_label, quote=False)
    actor_id = by_id if by_id is not None else context.bot.id
    actor_name = by_name or "Bot (automatic)"
    try:
        if mode == "ban":
            await context.bot.ban_chat_member(chat_id, target_id)
            result = f"{safe} banned"
        elif mode == "mute":
            await context.bot.restrict_chat_member(chat_id, target_id, permissions=_MUTE_PERMISSIONS)
            result = f"{safe} muted"
        elif mode == "kick":
            await context.bot.ban_chat_member(chat_id, target_id)
            await context.bot.unban_chat_member(chat_id, target_id)
            result = f"{safe} kicked"
        else:
            return f"no action taken for {safe} (unknown mode {mode})"
    except Exception:
        logger.exception("Punishment action failed mode=%s chat_id=%s target=%s", mode, chat_id, target_id)
        return f"couldn't act on {safe} (check my admin rights)"

    await asyncio.to_thread(
        dbmod.log_mod_action, chat_id, mode, target_id, target_label, actor_id, actor_name, reason
    )
    return result


# --- /warn, /warns, /resetwarn, /warnlimit, /warnmode -----------------------


async def cmd_warn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can warn users.")
        return

    resolved = await _resolve_target_and_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't warn an admin.")
        return

    settings = await asyncio.to_thread(dbmod.get_chat_mod_settings, chat.id)

    # Always add the warning first (this increments the total history)
    await asyncio.to_thread(
        dbmod.add_warning,
        chat.id,
        target_id,
        target_label,
        reason,
        update.effective_user.id,
        _user_label(update.effective_user),
    )

    # Now get the ACTIVE warning count (after the watermark)
    active_count, active_entries, stored_name = await asyncio.to_thread(
        dbmod.get_warnings, chat.id, target_id
    )
    # Use active_count for the limit check
    limit = settings["warn_limit"]

    safe_target = html.escape(target_label, quote=False)
    safe_reason = html.escape(reason, quote=False) if reason else "No reason given"

    if active_count >= limit:
        action_text = await _apply_punishment(
            update, context, target_id, target_label, settings["warn_mode"],
            by_id=update.effective_user.id,
            by_name=_user_label(update.effective_user),
            reason="warn limit reached",
        )
        # Reset the active warnings (sets the watermark to now)
        await asyncio.to_thread(dbmod.reset_warnings, chat.id, target_id)
        lines = [
            f"⚠️ Warned {safe_target} ({active_count}/{limit})",
            f"Reason: {safe_reason}",
            "",
            f"🚫 Warn limit reached — {action_text}. Warnings reset."
        ]
    else:
        mode_word = {"ban": "banned", "mute": "muted", "kick": "kicked"}.get(
            settings["warn_mode"], settings["warn_mode"]
        )
        lines = [
            f"⚠️ Warned {safe_target} ({active_count}/{limit})",
            f"Reason: {safe_reason}",
            "",
            f"<i>Reaching {limit}/{limit} warnings will get you automatically {mode_word}.</i>"
        ]

    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML") 
    

async def cmd_warns(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: check a user's warnings (their own, or any member's)."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can check warnings.")
        return

    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    elif update.effective_user:
        target_id, target_label = update.effective_user.id, _user_label(update.effective_user)
    else:
        return

    count, entries, stored_name = await asyncio.to_thread(dbmod.get_warnings, chat.id, target_id)
    label = stored_name or target_label
    safe = html.escape(label, quote=False)
    if count == 0:
        await _reply_autodelete(update, context, f"{safe} has no warnings.", parse_mode="HTML")
        return

    settings = await asyncio.to_thread(dbmod.get_chat_mod_settings, chat.id)
    lines = [f"⚠️ <b>{safe}</b> — {count}/{settings['warn_limit']} warning(s)", ""]
    for i, e in enumerate(entries[-10:], start=1):
        reason = html.escape(e.get("reason") or "No reason given", quote=False)
        lines.append(f"{i}. {reason}")
    if count > 10:
        lines.append("")
        lines.append(f"<i>+ {count - 10} more not shown.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_resetwarn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can reset warnings.")
        return

    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    else:
        await _reply_autodelete(update, context, "Reply to a user, or give their user id / @username.")
        return

    cleared = await asyncio.to_thread(dbmod.reset_warnings, chat.id, target_id)
    if cleared:
        await asyncio.to_thread(
            dbmod.log_mod_action, chat.id, "resetwarn", target_id, target_label,
            update.effective_user.id, _user_label(update.effective_user), "",
        )
    safe = html.escape(target_label, quote=False)
    text = f"Warnings cleared for {safe}." if cleared else f"{safe} had no warnings."
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def cmd_warnlimit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view or change this.")
        return
    settings = await asyncio.to_thread(dbmod.get_chat_mod_settings, chat.id)
    if not context.args:
        await _reply_autodelete(update, context, f"Current warn limit: {settings['warn_limit']}")
        return
    try:
        limit = int(context.args[0])
    except ValueError:
        await _reply_autodelete(update, context, "Usage: /warnlimit <number>")
        return
    if limit < 1:
        await _reply_autodelete(update, context, "Warn limit must be at least 1.")
        return
    await asyncio.to_thread(dbmod.set_warn_limit, chat.id, limit)
    await _reply_autodelete(update, context, f"Warn limit set to {limit}.")


async def cmd_warnmode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view or change this.")
        return
    settings = await asyncio.to_thread(dbmod.get_chat_mod_settings, chat.id)
    if not context.args:
        await _reply_autodelete(
            update, context,
            f"Current warn mode: {settings['warn_mode']}\nOptions: {', '.join(WARN_MODES)}",
        )
        return
    mode = context.args[0].lower()
    if mode not in WARN_MODES:
        await _reply_autodelete(update, context, f"Invalid mode. Options: {', '.join(WARN_MODES)}")
        return
    await asyncio.to_thread(dbmod.set_warn_mode, chat.id, mode)
    await _reply_autodelete(update, context, f"Warn mode set to {mode}.")


# --- /ban, /tban, /unban, /kick ----------------------------------------------


async def cmd_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can ban users.")
        return
    resolved = await _resolve_target_and_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't ban an admin.")
        return

    # Irreversible + broad-impact action: require an inline confirmation before executing,
    # so a fat-fingered id/@username can't permanently ban the wrong person by accident.
    token = _register_pending_confirmation(
        "ban", chat.id, update.effective_user.id,
        {"target_id": target_id, "target_label": target_label, "reason": reason},
    )
    safe_target = html.escape(target_label, quote=False)
    await update.message.reply_text(
        f"⚠️ Permanently ban {safe_target}?",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard(token),
    )


async def cmd_tban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can ban users.")
        return
    resolved = await _resolve_target_time_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, duration, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't ban an admin.")
        return
    # Telegram itself lifts the ban at until_date — no scheduler/job needed on our side,
    # and it survives bot restarts/redeploys since Telegram enforces it server-side.
    until = _clamp_until(duration)
    try:
        await context.bot.ban_chat_member(chat.id, target_id, until_date=until)
    except Exception:
        logger.exception("tban failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't ban — check that I'm an admin with ban rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "tban", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), reason,
    )
    safe = html.escape(target_label, quote=False)
    safe_reason = html.escape(reason, quote=False) if reason else "No reason given"
    await _reply_autodelete(
        update, context,
        f"🚫 Banned {safe} for {_format_duration(int(duration.total_seconds()))}\n"
        f"Auto-unbanned at: {until.strftime('%d %b %Y %H:%M UTC')}\n"
        f"Reason: {safe_reason}",
        parse_mode="HTML",
    )


async def cmd_unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can unban users.")
        return
    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    else:
        await _reply_autodelete(update, context, "Usage: /unban <user_id or @username>")
        return
    try:
        await context.bot.unban_chat_member(chat.id, target_id, only_if_banned=True)
    except Exception:
        logger.exception("unban failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't unban — check that I'm an admin with ban rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "unban", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), "",
    )
    safe = html.escape(target_label, quote=False)
    await _reply_autodelete(update, context, f"✅ Unbanned {safe}.", parse_mode="HTML")


async def cmd_kick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can kick users.")
        return
    resolved = await _resolve_target_and_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't kick an admin.")
        return
    try:
        # ban immediately followed by unban = removed from the group but free to rejoin
        await context.bot.ban_chat_member(chat.id, target_id)
        await context.bot.unban_chat_member(chat.id, target_id)
    except Exception:
        logger.exception("kick failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't kick — check that I'm an admin with ban rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "kick", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), reason,
    )
    safe = html.escape(target_label, quote=False)
    safe_reason = html.escape(reason, quote=False) if reason else "No reason given"
    await _reply_autodelete(update, context, f"👢 Kicked {safe}\nReason: {safe_reason}", parse_mode="HTML")


# --- /mute, /tmute, /unmute --------------------------------------------------


async def cmd_mute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can mute users.")
        return
    resolved = await _resolve_target_and_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't mute an admin.")
        return
    try:
        await context.bot.restrict_chat_member(chat.id, target_id, permissions=_MUTE_PERMISSIONS)
    except Exception:
        logger.exception("mute failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't mute — check that I'm an admin with restrict rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "mute", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), reason,
    )
    safe = html.escape(target_label, quote=False)
    safe_reason = html.escape(reason, quote=False) if reason else "No reason given"
    await _reply_autodelete(update, context, f"🔇 Muted {safe}\nReason: {safe_reason}", parse_mode="HTML")


async def cmd_tmute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can mute users.")
        return
    resolved = await _resolve_target_time_reason(update, context)
    if resolved is None:
        return
    target_id, target_label, duration, reason = resolved
    if await _target_is_protected(update, context, target_id):
        await _reply_autodelete(update, context, "I can't mute an admin.")
        return
    until = _clamp_until(duration)
    try:
        await context.bot.restrict_chat_member(
            chat.id, target_id, permissions=_MUTE_PERMISSIONS, until_date=until
        )
    except Exception:
        logger.exception("tmute failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't mute — check that I'm an admin with restrict rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "tmute", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), reason,
    )
    safe = html.escape(target_label, quote=False)
    safe_reason = html.escape(reason, quote=False) if reason else "No reason given"
    await _reply_autodelete(
        update, context,
        f"🔇 Muted {safe} for {_format_duration(int(duration.total_seconds()))}\n"
        f"Auto-unmuted at: {until.strftime('%d %b %Y %H:%M UTC')}\n"
        f"Reason: {safe_reason}",
        parse_mode="HTML",
    )


async def cmd_unmute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can unmute users.")
        return
    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    else:
        await _reply_autodelete(update, context, "Reply to a user, or give their user id / @username.")
        return

    try:
        # Restore this group's actual default permissions if we can read them,
        # rather than always granting the maximal permission set.
        restore = _UNMUTE_FALLBACK_PERMISSIONS
        try:
            group_chat = await context.bot.get_chat(chat.id)
            if group_chat.permissions:
                restore = group_chat.permissions
        except Exception:
            pass
        await context.bot.restrict_chat_member(chat.id, target_id, permissions=restore)
    except Exception:
        logger.exception("unmute failed chat_id=%s target=%s", chat.id, target_id)
        await _reply_autodelete(update, context, "Couldn't unmute — check that I'm an admin with restrict rights.")
        return
    await asyncio.to_thread(
        dbmod.log_mod_action, chat.id, "unmute", target_id, target_label,
        update.effective_user.id, _user_label(update.effective_user), "",
    )
    safe = html.escape(target_label, quote=False)
    await _reply_autodelete(update, context, f"🔊 Unmuted {safe}.", parse_mode="HTML")


# --- Blocklist: /blocklist, /addblocklist, /unblocklist, /blocklistmode -----


async def cmd_blocklist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: view the current blocklisted words."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view the blocklist.")
        return
    words, mode = await asyncio.to_thread(dbmod.get_blocklist, chat.id)
    if not words:
        await _reply_autodelete(
            update, context,
            f"No blocklisted words yet.\nMode: {mode}\n\nAdd some with /addblocklist word1 word2 ...",
        )
        return
    safe_words = "\n".join(f"• {html.escape(w, quote=False)}" for w in words)
    await _reply_autodelete(
        update, context,
        f"🚫 <b>Blocklisted words</b> ({len(words)})\nMode: <b>{html.escape(mode, quote=False)}</b>\n\n{safe_words}",
        parse_mode="HTML",
    )


async def cmd_addblocklist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can edit the blocklist.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /addblocklist word1 word2 ...")
        return
    added = await asyncio.to_thread(dbmod.add_blocklist_words, chat.id, context.args)
    if not added:
        await _reply_autodelete(update, context, "Those word(s) are already blocklisted.")
        return
    safe = ", ".join(html.escape(w, quote=False) for w in added)
    await _reply_autodelete(update, context, f"Added to blocklist: {safe}", parse_mode="HTML")


async def cmd_unblocklist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can edit the blocklist.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /unblocklist word1 word2 ...")
        return
    removed = await asyncio.to_thread(dbmod.remove_blocklist_words, chat.id, context.args)
    if not removed:
        await _reply_autodelete(update, context, "None of those word(s) were on the blocklist.")
        return
    safe = ", ".join(html.escape(w, quote=False) for w in removed)
    await _reply_autodelete(update, context, f"Removed from blocklist: {safe}", parse_mode="HTML")


async def cmd_blocklistmode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view or change this.")
        return
    _words, mode = await asyncio.to_thread(dbmod.get_blocklist, chat.id)
    if not context.args:
        await _reply_autodelete(
            update, context,
            f"Current blocklist mode: {mode}\nOptions: {', '.join(BLOCKLIST_MODES)}",
        )
        return
    new_mode = context.args[0].lower()
    if new_mode not in BLOCKLIST_MODES:
        await _reply_autodelete(update, context, f"Invalid mode. Options: {', '.join(BLOCKLIST_MODES)}")
        return
    await asyncio.to_thread(dbmod.set_blocklist_mode, chat.id, new_mode)
    await _reply_autodelete(
        update, context,
        f"Blocklist mode set to {new_mode}.\n"
        f"<i>'delete' just removes the message; 'warn' also posts a notice (auto-deleted after 30s, "
        f"not a formal /warn); 'mute'/'kick'/'ban' further punish the sender.</i>",
        parse_mode="HTML",
    )


async def on_text_check_blocklist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs on every plain-text group message; deletes it (and optionally punishes the
    sender) if it contains a blocklisted word. Registered in a separate handler group
    (group=1) so it always runs alongside whatever fires in the default group (group=0),
    instead of competing with other MessageHandlers for "first match wins"."""
    msg = update.message
    if not msg or not msg.text or not msg.from_user or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return

    # Never act on admins/owners — a bad blocklist word must not be able to gag the mods.
    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return
    except Exception:
        pass

    matched = await asyncio.to_thread(dbmod.find_blocked_word, chat.id, msg.text)
    if not matched:
        return

    try:
        await msg.delete()
    except Exception:
        logger.debug(
            "Blocklist: couldn't delete message chat_id=%s (bot may lack delete rights)", chat.id
        )

    _words, mode = await asyncio.to_thread(dbmod.get_blocklist, chat.id)
    if mode == "delete":
        return

    target_id = msg.from_user.id
    target_label = _user_label(msg.from_user)

    if mode == "warn":
        # Just an informational notice — does NOT call dbmod.add_warning, so this never
        # counts toward /warnlimit or triggers /warnmode's auto-punishment. The notice
        # itself is cleaned up automatically after 30s so it doesn't clutter the chat.
        safe = html.escape(target_label, quote=False)
        text = f"⚠️ {safe}, your message was deleted — it contained a blocklisted word."
        try:
            notice = await context.bot.send_message(chat.id, text, parse_mode="HTML")
        except Exception:
            logger.debug("Blocklist warn notice failed chat_id=%s", chat.id)
            return
        jq = context.job_queue
        if jq is not None:
            jq.run_once(
                _delete_messages_later,
                when=30,
                data={"chat_id": chat.id, "message_ids": [notice.message_id]},
                name=f"blocklist-warn-notice-{chat.id}-{notice.message_id}",
            )
        return

    action_text = await _apply_punishment(
        update, context, target_id, target_label, mode, reason=f"blocklisted word ({matched})"
    )
    await context.bot.send_message(
        chat.id,
        f"🚫 Message deleted — it contained a blocklisted word. {action_text}.",
        parse_mode="HTML",
    )


# --- Filters: /filter, /filters, /stop --------------------------------------
#
# Filters store the exact MessageEntity list (bold/italic/spacing/etc.) instead of
# plain joined text, and can save any message type (photo/video/sticker/document/
# animation/voice/audio/video_note), not just text — matching Rose's real /filter,
# which lets you reply to any message with /filter <keyword> to save it verbatim.


def _entity_to_dict(e: MessageEntity) -> dict:
    d: dict = {"type": e.type, "offset": e.offset, "length": e.length}
    if e.url:
        d["url"] = e.url
    if e.language:
        d["language"] = e.language
    if e.custom_emoji_id:
        d["custom_emoji_id"] = e.custom_emoji_id
    # text_mention (entity.user) isn't fully reconstructable without re-fetching the
    # user, so it's intentionally dropped here — the visible text is kept, only the
    # clickable-mention behavior of that one entity type is lost.
    return d


def _dict_to_entity(d: dict) -> MessageEntity:
    return MessageEntity(
        type=d["type"],
        offset=d["offset"],
        length=d["length"],
        url=d.get("url"),
        language=d.get("language"),
        custom_emoji_id=d.get("custom_emoji_id"),
    )


def _filter_data_from_message(msg) -> dict | None:
    """Builds filter storage data from any message type. Caption/text entities are
    kept as-is (already correctly offset for that message)."""
    def _ents(entities) -> list[dict]:
        return [_entity_to_dict(e) for e in (entities or [])]

    if msg.photo:
        return {"type": "photo", "file_id": msg.photo[-1].file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.video:
        return {"type": "video", "file_id": msg.video.file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.animation:
        return {"type": "animation", "file_id": msg.animation.file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.document:
        return {"type": "document", "file_id": msg.document.file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.sticker:
        return {"type": "sticker", "file_id": msg.sticker.file_id, "text": None, "entities": []}
    if msg.voice:
        return {"type": "voice", "file_id": msg.voice.file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.audio:
        return {"type": "audio", "file_id": msg.audio.file_id, "text": msg.caption, "entities": _ents(msg.caption_entities)}
    if msg.video_note:
        return {"type": "video_note", "file_id": msg.video_note.file_id, "text": None, "entities": []}
    if msg.text:
        return {"type": "text", "file_id": None, "text": msg.text, "entities": _ents(msg.entities)}
    return None


_FILTER_BODY_RE = re.compile(r"^(\S+)(\s+)(.*)$", re.DOTALL)


def _extract_filter_command_body(msg) -> tuple[str, str, list] | None:
    """(keyword, body_text, body_entities) parsed straight from the raw message text,
    preserving exact whitespace/newlines and formatting in body_text — unlike
    context.args, which collapses runs of whitespace and discards entity offsets
    entirely (that's what made bold/spacing get lost on /filter before this fix).

    Entity offsets are Telegram's own UTF-16 code-unit offsets into msg.text; slicing
    at a purely-ASCII prefix ("/filter <keyword> ") keeps that offset arithmetic exact
    (each ASCII char = 1 UTF-16 unit), which covers the normal case. A keyword itself
    containing astral characters (rare — most emoji are fine, some are astral) could
    shift things very slightly; not worth the extra complexity for that edge case."""
    text = msg.text or ""
    if not text.startswith("/"):
        return None
    parts = text.split(None, 1)
    if len(parts) < 2:
        return None
    command_token_end = len(parts[0])
    idx = command_token_end
    while idx < len(text) and text[idx].isspace():
        idx += 1
    rest = text[idx:]
    rest_start = idx

    m = _FILTER_BODY_RE.match(rest)
    if not m:
        return None
    keyword = m.group(1)
    body_start = rest_start + m.start(3)
    body_text = text[body_start:]

    body_entities = []
    for e in (msg.entities or []):
        if e.offset >= body_start:
            body_entities.append(
                MessageEntity(
                    type=e.type,
                    offset=e.offset - body_start,
                    length=e.length,
                    url=e.url,
                    language=e.language,
                    custom_emoji_id=e.custom_emoji_id,
                )
            )
    return keyword, body_text, body_entities


async def cmd_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only. Two forms:
    - /filter <keyword> <reply text>  — saves formatted text exactly as typed.
    - Reply to any message with /filter <keyword>  — saves that message verbatim
      (photo, video, sticker, document, animation, voice, audio, video note, or text)."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can add filters.")
        return

    reply = update.message.reply_to_message
    if reply is not None:
        if not context.args:
            await _reply_autodelete(update, context, "Usage (replying to a message): /filter <keyword>")
            return
        keyword = context.args[0]
        filter_data = _filter_data_from_message(reply)
        if filter_data is None:
            await _reply_autodelete(update, context, "I can't save that message type as a filter.")
            return
    else:
        parsed = _extract_filter_command_body(update.message)
        if parsed is None:
            await _reply_autodelete(
                update, context,
                "Usage: /filter <keyword> <reply text>\n"
                "Or reply to any message (text, photo, video, sticker, document, ...) with /filter <keyword>",
            )
            return
        keyword, body_text, body_entities = parsed
        filter_data = {
            "type": "text",
            "file_id": None,
            "text": body_text,
            "entities": [_entity_to_dict(e) for e in body_entities],
        }

    await asyncio.to_thread(dbmod.add_filter, chat.id, keyword, filter_data)
    safe_kw = html.escape(keyword.lower(), quote=False)
    await _reply_autodelete(update, context, f"Filter saved for \"{safe_kw}\".", parse_mode="HTML")


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: list saved filter keywords."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view filters.")
        return
    filters_map = await asyncio.to_thread(dbmod.get_filters, chat.id)
    if not filters_map:
        await _reply_autodelete(update, context, "No filters saved yet.\n\nAdd one with /filter keyword reply text")
        return
    safe_words = "\n".join(f"• {html.escape(k, quote=False)}" for k in sorted(filters_map))
    await _reply_autodelete(
        update, context,
        f"🔎 <b>Filters</b> ({len(filters_map)})\n\n{safe_words}",
        parse_mode="HTML",
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /stop <keyword> — removes a saved filter."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can remove filters.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /stop <keyword>")
        return
    keyword = context.args[0]
    removed = await asyncio.to_thread(dbmod.remove_filter, chat.id, keyword)
    safe_kw = html.escape(keyword.lower(), quote=False)
    text = f"Filter \"{safe_kw}\" removed." if removed else f"No filter found for \"{safe_kw}\"."
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def _send_filter_response(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, reply_to_message_id: int, filter_data: dict
) -> None:
    ftype = filter_data.get("type", "text")
    text = filter_data.get("text")
    entities = [_dict_to_entity(d) for d in (filter_data.get("entities") or [])] or None
    file_id = filter_data.get("file_id")
    try:
        if ftype == "text":
            await context.bot.send_message(
                chat_id, text or "", entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "photo":
            await context.bot.send_photo(
                chat_id, photo=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "video":
            await context.bot.send_video(
                chat_id, video=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "animation":
            await context.bot.send_animation(
                chat_id, animation=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "document":
            await context.bot.send_document(
                chat_id, document=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "sticker":
            await context.bot.send_sticker(chat_id, sticker=file_id, reply_to_message_id=reply_to_message_id)
        elif ftype == "voice":
            await context.bot.send_voice(
                chat_id, voice=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "audio":
            await context.bot.send_audio(
                chat_id, audio=file_id, caption=text, caption_entities=entities, reply_to_message_id=reply_to_message_id
            )
        elif ftype == "video_note":
            await context.bot.send_video_note(chat_id, video_note=file_id, reply_to_message_id=reply_to_message_id)
    except Exception:
        logger.exception("Filter response failed chat_id=%s type=%s", chat_id, ftype)


async def on_text_check_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs on every plain-text group message; sends the saved response if the text
    contains a filter keyword. Registered in its own handler group (group=2) so it
    always runs independently of on_text_check_blocklist (group=1) and the default
    command dispatch (group=0)."""
    msg = update.message
    if not msg or not msg.text or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return
    match = await asyncio.to_thread(dbmod.find_filter_match, chat.id, msg.text)
    if not match:
        return
    _keyword, filter_data = match
    await _send_filter_response(context, chat.id, msg.message_id, filter_data)


async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Passive, silent: remembers username -> user_id for every message seen in a tracked
    group (any message type, not just text/commands), so /warn, /ban, /mute etc. can later
    resolve a bare @username. Registered in its own handler group (group=3) so it always
    runs alongside command dispatch and the other text scanners, never competing for
    "first match" with any of them."""
    msg = update.message
    if not msg or not msg.from_user or msg.from_user.is_bot or not update.effective_chat:
        return
    if update.effective_chat.type not in ("group", "supergroup"):
        return
    await asyncio.to_thread(
        dbmod.record_known_user,
        update.effective_chat.id,
        msg.from_user.id,
        msg.from_user.username,
        _user_label(msg.from_user),
    )


# --- @admin tagging ----------------------------------------------------------

_ADMIN_TAG_RE = re.compile(r"(?<!\w)@admin(?!\w)", re.IGNORECASE)
_ADMIN_TAG_COOLDOWN_SECONDS = 60
_admin_tag_cooldown: dict[int, float] = {}  # chat_id -> monotonic time of last tag


async def on_text_admin_tag(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Whenever someone writes "@admin" in a group message, pings every current admin
    with a clickable mention (tg://user link, so it works even for admins with no
    @username). Rate-limited per chat so it can't be spammed. Registered in its own
    handler group (group=4)."""
    msg = update.message
    if not msg or not msg.text or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return
    if not _ADMIN_TAG_RE.search(msg.text):
        return

    now = time.monotonic()
    last = _admin_tag_cooldown.get(chat.id, 0.0)
    if now - last < _ADMIN_TAG_COOLDOWN_SECONDS:
        return
    _admin_tag_cooldown[chat.id] = now

    try:
        admins = await context.bot.get_chat_administrators(chat.id)
    except Exception:
        logger.exception("Admin tag: get_chat_administrators failed chat_id=%s", chat.id)
        return

    mentions = []
    for a in admins:
        u = a.user
        # Skip real bots AND the GroupAnonymousBot placeholder explicitly (belt-and-braces
        # alongside is_bot — this placeholder represents "an admin posting anonymously",
        # not a person, so it must never be pinged even if a client library ever reports
        # its is_bot flag inconsistently).
        if u.is_bot or u.id == app_state.GROUP_ANONYMOUS_BOT_ID:
            continue
        label = html.escape(u.first_name or "Admin", quote=False)
        mentions.append(f'<a href="tg://user?id={u.id}">{label}</a>')
    if not mentions:
        return

    try:
        await msg.reply_text("🔔 " + " ".join(mentions) + " — attention needed here.", parse_mode="HTML")
    except Exception:
        logger.debug("Admin tag: reply failed chat_id=%s", chat.id)


# --- Link lock: /lock, /unlock, /locks ---------------------------------------

_LOCK_ALIASES = {"link": "links", "links": "links", "url": "links", "urls": "links"}
_LOCK_TYPE_NAMES = sorted(set(_LOCK_ALIASES.values()))
_URL_RE = re.compile(r"(https?://|www\.\w|t\.me/)", re.IGNORECASE)


async def cmd_lock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /lock links — non-admin messages containing a link get deleted from
    then on. Only "links" is supported today; the alias table exists so more lock
    types (photos, forwards, etc.) can be added later without changing the command."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can lock content types.")
        return
    if not context.args:
        await _reply_autodelete(update, context, f"Usage: /lock <type>\nSupported: {', '.join(_LOCK_TYPE_NAMES)}")
        return
    lock_name = _LOCK_ALIASES.get(context.args[0].lower())
    if lock_name is None:
        await _reply_autodelete(update, context, f"Unknown lock type. Supported: {', '.join(_LOCK_TYPE_NAMES)}")
        return
    await asyncio.to_thread(dbmod.lock_type, chat.id, lock_name)
    await _reply_autodelete(
        update, context,
        f"🔒 Locked: {lock_name}. Non-admin messages containing a link will now be deleted automatically.",
    )


async def cmd_unlock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can unlock content types.")
        return
    if not context.args:
        await _reply_autodelete(update, context, f"Usage: /unlock <type>\nSupported: {', '.join(_LOCK_TYPE_NAMES)}")
        return
    lock_name = _LOCK_ALIASES.get(context.args[0].lower())
    if lock_name is None:
        await _reply_autodelete(update, context, f"Unknown lock type. Supported: {', '.join(_LOCK_TYPE_NAMES)}")
        return
    removed = await asyncio.to_thread(dbmod.unlock_type, chat.id, lock_name)
    text = f"🔓 Unlocked: {lock_name}." if removed else f"{lock_name} wasn't locked."
    await _reply_autodelete(update, context, text)


async def cmd_locks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: view which content types are currently locked."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view locks.")
        return
    locked = await asyncio.to_thread(dbmod.get_locked_types, chat.id)
    text = "🔒 Locked: " + ", ".join(locked) if locked else "No content types are locked."
    await _reply_autodelete(update, context, text)


_MODLOG_ACTION_LABELS = {
    "ban": "🚫 Ban", "tban": "🚫 Temp ban", "unban": "✅ Unban",
    "kick": "👢 Kick", "mute": "🔇 Mute", "tmute": "🔇 Temp mute", "unmute": "🔊 Unmute",
    "resetwarn": "♻️ Reset warnings",
}


async def cmd_modlog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /modlog [n] — the last n moderation actions (default 20, max 50) taken
    in this group: who did what, to whom, when, and why. Covers ban/tban/unban/kick/mute/
    tmute/unmute/resetwarn and automatic punishments from /warn's limit or blocklist
    enforcement — everything /ban etc. and _apply_punishment record via dbmod.log_mod_action."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view the mod log.")
        return

    limit = 20
    if context.args:
        try:
            limit = max(1, min(50, int(context.args[0])))
        except ValueError:
            await _reply_autodelete(update, context, "Usage: /modlog [count]")
            return

    entries = await asyncio.to_thread(dbmod.fetch_mod_log, chat.id, limit)
    if not entries:
        await _reply_autodelete(update, context, "No moderation actions logged yet.")
        return

    lines = ["📜 <b>Mod log</b>", ""]
    for e in entries:
        label = _MODLOG_ACTION_LABELS.get(e.action, e.action)
        target = html.escape(e.target_name or str(e.target_id), quote=False)
        by = html.escape(e.by_name or str(e.by_id), quote=False)
        when = e.at.strftime("%d %b %H:%M UTC") if e.at else "?"
        line = f"{label} — {target} — by {by} — {when}"
        if e.reason:
            line += f"\n   <i>{html.escape(e.reason, quote=False)}</i>"
        lines.append(line)
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def on_text_check_links(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Deletes messages containing a link if "links" is locked for this chat and the
    sender isn't an admin or on the link allowlist. Checks both Telegram's own url/text_link
    entities (catches formatted/masked links) and a plain-text regex fallback.
    Registered in its own handler group (group=5)."""
    msg = update.message
    if not msg or not update.effective_chat or not msg.from_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return

    locked = await asyncio.to_thread(dbmod.get_locked_types, chat.id)
    if "links" not in locked:
        return

    text = msg.text or msg.caption or ""
    entities = list(msg.entities or []) + list(msg.caption_entities or [])
    has_link = any(e.type in ("url", "text_link") for e in entities) or bool(_URL_RE.search(text))
    if not has_link:
        return

    # Check if sender is admin (covers anonymous admins too)
    if await _message_sender_is_admin(update, context):
        return

    # Check if sender is on the link allowlist
    if await asyncio.to_thread(dbmod.is_link_allowed, chat.id, msg.from_user.id):
        return

    try:
        await msg.delete()
    except Exception:
        logger.debug("Link lock: couldn't delete message chat_id=%s (bot may lack delete rights)", chat.id)
        return

    try:
        notice = await context.bot.send_message(chat.id, "🔗 Links aren't allowed here — message deleted.")
        jq = context.job_queue
        if jq is not None:
            jq.run_once(
                _delete_messages_later,
                when=15,
                data={"chat_id": chat.id, "message_ids": [notice.message_id]},
                name=f"link-lock-notice-{chat.id}-{notice.message_id}",
            )
    except Exception:
        logger.debug("Link lock: couldn't send notice chat_id=%s", chat.id) 
        


# =============================================================================
# VC Topic Management: /addtopic, /topics, /deletetopic, /deletedtopics,
# /topicdone, /alltopics
#
# Access model per spec: anyone can add or view topics; only group admins can
# mark done or delete.
# =============================================================================

_MAX_TOPIC_ROWS = 100  # safety cap so a chat with hundreds of topics can't blow past
                        # Telegram's 4096-char message limit (see the /streakboard fix
                        # earlier in this file for the exact failure mode this avoids)


def _raw_command_arg_text(msg) -> str:
    """Everything after the command token, preserving exact whitespace/newlines —
    unlike ' '.join(context.args), which collapses runs of spaces and strips them."""
    text = msg.text or ""
    parts = text.split(None, 1)
    return parts[1] if len(parts) > 1 else ""


async def cmd_addtopic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /addtopic <topic text> — assigns the next permanent serial number.
    Blocks exact-duplicate submissions against currently-Active topics (case/whitespace
    insensitive) and points the person at /upvote instead of letting the same topic pile
    up under multiple serials."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    topic_text = _raw_command_arg_text(update.message).strip()
    if not topic_text:
        await _reply_autodelete(update, context, "Usage: /addtopic <topic>")
        return

    dup = await asyncio.to_thread(dbmod.find_active_topic_by_text, chat.id, topic_text)
    if dup is not None:
        safe = html.escape(dup.text, quote=False)
        await _reply_autodelete(
            update, context,
            f"This topic already exists as #{dup.serial}: {safe}\n"
            f"Use /upvote {dup.serial} to support it instead of adding a duplicate.",
            parse_mode="HTML",
        )
        return

    serial = await asyncio.to_thread(
        dbmod.add_topic, chat.id, topic_text, update.effective_user.id, _user_label(update.effective_user)
    )
    safe = html.escape(topic_text, quote=False)
    await _reply_autodelete(update, context, f"✅ Topic #{serial} added: {safe}", parse_mode="HTML")


async def cmd_topics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: shows only Active topics, ranked by votes (see /upvote) so the group's
    actual priority order is visible, not just insertion order."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.get_active_topics, chat.id)
    if not rows:
        await _reply_autodelete(update, context, "No active topics yet.\n\nAdd one with /addtopic <topic>")
        return
    shown = rows[:_MAX_TOPIC_ROWS]
    lines = ["📋 <b>Active Topics</b>", ""]
    for r in shown:
        safe = html.escape(r.text, quote=False)
        vote_badge = f" — 👍 {r.votes}" if r.votes else ""
        lines.append(f"{r.serial}. {safe}{vote_badge}")
    if len(rows) > _MAX_TOPIC_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - _MAX_TOPIC_ROWS} more not shown.</i>")
    lines.append("")
    lines.append("<i>Use the number shown with /topicdone, /deletetopic, or /upvote.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_upvote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /upvote <serial> — supports an active topic so it ranks higher in /topics.
    One vote per person per topic; the first time you vote on a given topic, you get a
    small XP grant (feeds the same XP/level system VC time does)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /upvote <serial_number>")
        return
    try:
        serial = int(context.args[0])
    except ValueError:
        await _reply_autodelete(update, context, "Serial number must be a number.")
        return

    topic = await asyncio.to_thread(dbmod.get_topic, chat.id, serial)
    if topic is None or topic.get("state") != "active":
        await _reply_autodelete(update, context, f"Topic #{serial} isn't active (or doesn't exist).")
        return

    is_new, votes = await asyncio.to_thread(dbmod.upvote_topic, chat.id, serial, update.effective_user.id)
    safe = html.escape(str(topic.get("text", "")), quote=False)
    if not is_new:
        await _reply_autodelete(
            update, context, f"You've already upvoted #{serial}: {safe} ({votes} 👍 total).", parse_mode="HTML"
        )
        return

    await asyncio.to_thread(
        dbmod.award_engagement_xp, chat.id, update.effective_user.id, _user_label(update.effective_user), 2
    )
    await _reply_autodelete(
        update, context, f"👍 Upvoted #{serial}: {safe} ({votes} total) — +2 XP", parse_mode="HTML"
    )


async def cmd_deletetopic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /deletetopic <serial_number> — moves a topic to Deleted. The serial
    is never reused (see db._next_topic_serial)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can delete topics.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /deletetopic <serial_number>")
        return
    try:
        serial = int(context.args[0])
    except ValueError:
        await _reply_autodelete(update, context, "Serial number must be a number.")
        return

    topic = await asyncio.to_thread(dbmod.get_topic, chat.id, serial)
    if topic is None:
        await _reply_autodelete(update, context, f"No topic #{serial} found.")
        return
    ok = await asyncio.to_thread(
        dbmod.delete_topic, chat.id, serial, update.effective_user.id, _user_label(update.effective_user)
    )
    if not ok:
        await _reply_autodelete(update, context, f"Topic #{serial} is already deleted.")
        return
    safe = html.escape(str(topic.get("text", "")), quote=False)
    await _reply_autodelete(update, context, f"🗑️ Topic #{serial} deleted: {safe}", parse_mode="HTML")


async def cmd_deletedtopics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: shows all deleted topics with their original serial numbers."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.get_deleted_topics, chat.id)
    if not rows:
        await _reply_autodelete(update, context, "No deleted topics.")
        return
    shown = rows[:_MAX_TOPIC_ROWS]
    lines = ["🗑️ <b>Deleted Topics</b>", ""]
    lines.extend(f"{r.serial}. {html.escape(r.text, quote=False)}" for r in shown)
    if len(rows) > _MAX_TOPIC_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - _MAX_TOPIC_ROWS} more not shown.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_topicdone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /topicdone <serial_number> — marks an Active topic Done. Fails
    cleanly (no state change) if the topic isn't currently active."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can mark topics done.")
        return
    if not context.args:
        await _reply_autodelete(update, context, "Usage: /topicdone <serial_number>")
        return
    try:
        serial = int(context.args[0])
    except ValueError:
        await _reply_autodelete(update, context, "Serial number must be a number.")
        return

    topic = await asyncio.to_thread(dbmod.get_topic, chat.id, serial)
    if topic is None:
        await _reply_autodelete(update, context, f"No topic #{serial} found.")
        return
    ok = await asyncio.to_thread(
        dbmod.mark_topic_done, chat.id, serial, update.effective_user.id, _user_label(update.effective_user)
    )
    if not ok:
        state = topic.get("state")
        await _reply_autodelete(update, context, f"Topic #{serial} is already {state}, not active.")
        return
    safe = html.escape(str(topic.get("text", "")), quote=False)
    await _reply_autodelete(update, context, f"✅ Topic #{serial} marked done: {safe}", parse_mode="HTML")


async def cmd_alltopics(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: every topic ever added, sorted by serial. Active = plain, Done = + ✅,
    Deleted = struck through."""
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
            lines.append(f"{r.serial}. {safe} ✅")
        elif r.state == "deleted":
            lines.append(f"<s>{r.serial}. {safe}</s>")
        else:
            lines.append(f"{r.serial}. {safe}")
    if len(rows) > _MAX_TOPIC_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - _MAX_TOPIC_ROWS} more not shown.</i>")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


# =============================================================================
# Timer: /timer <Nm>, /canceltimer — minutes only, max 20m, one active timer per chat.
# In-memory only (like the flood tracker and link-lock notices elsewhere in this file):
# losing a running timer on a redeploy is an acceptable tradeoff for a max-20-minute
# utility feature, not worth persisting to Mongo.
# =============================================================================

TIMER_MAX_MINUTES = 20
_TIMER_RE = re.compile(r"^(\d{1,2})m$", re.IGNORECASE)
_active_timers: dict[int, dict] = {}  # chat_id -> {end_at, minutes, started_by, started_at}


async def cmd_timer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /timer <Nm> — e.g. /timer 5m. Minutes only (no h/d/w — deliberately
    different from /tban's duration syntax, since this is meant for short in-chat things
    like a VC break, not long moderation actions). Only one timer can run per chat at a
    time; starting another while one is active is rejected with the remaining time shown."""
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
            f"Invalid format — minutes only, e.g. /timer 5m (max {TIMER_MAX_MINUTES}m).",
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
    _active_timers[chat.id] = {
        "end_at": end_at, "minutes": minutes, "started_by": started_by, "started_at": started_at,
    }
    jq.run_once(_timer_fire, when=minutes * 60, data={"chat_id": chat.id}, name=f"timer-{chat.id}")
    await _reply_autodelete(
        update, context, f"⏳ Timer set for {minutes}m — I'll ping this chat at {end_at.strftime('%H:%M UTC')}."
    )


async def _timer_fire(context: ContextTypes.DEFAULT_TYPE) -> None:
    """job_queue callback: fires once when a /timer's duration elapses."""
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    entry = _active_timers.pop(chat_id, None)
    if entry is None:
        return  # cancelled before firing
    start_str = entry["started_at"].strftime("%H:%M")
    end_str = entry["end_at"].strftime("%H:%M")
    minutes = entry["minutes"]
    safe_by = html.escape(entry["started_by"], quote=False)
    try:
        await context.bot.send_message(
            chat_id,
            f"⏰ <b>Time's up!</b> {minutes} minute(s) are up.\n"
            f"<i>{start_str} → {end_str} UTC · started by {safe_by}</i>",
            parse_mode="HTML",
        )
    except Exception:
        logger.debug("Timer fire: send failed chat_id=%s", chat_id)


async def cmd_canceltimer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /canceltimer — cancels the chat's active timer, if any."""
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
        for job in jq.get_jobs_by_name(f"timer-{chat.id}"):
            job.schedule_removal()
    _active_timers.pop(chat.id, None)
    await _reply_autodelete(update, context, "⏹️ Timer cancelled.")


async def cmd_vcreport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return

    try:
        rows, start, end = await asyncio.to_thread(dbmod.fetch_alltime_vc_stats, chat.id)
        if not rows:
            await _reply_autodelete(update, context, "No recorded VC data in this group yet.")
            return
        subtitle = f"{_format_date_utc(start)} → {_format_date_utc(end)} (UTC)"
        text = _format_vc_stats_html("All-time VC report", subtitle, rows)
        await _reply_autodelete(update, context, text, parse_mode="HTML")
    except Exception as e:
        logger.exception("vcreport failed for chat_id=%s", chat.id)
        await _reply_autodelete(update, context, f"⚠️ Error generating report: {html.escape(str(e), quote=False)}")
 
async def cmd_attendance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return

    rows = await asyncio.to_thread(dbmod.fetch_all_attendance, chat.id)
    if not rows:
        threshold_min = dbmod.present_threshold_sec() // 60
        await _reply_autodelete(
            update,
            context,
            f"No present attendance recorded in this group yet.\n\n"
            f"Stay more than {threshold_min} minutes in a voice/video call to earn +1 present day.",
        )
        return

    await _reply_autodelete(update, context, _format_attendance_html(rows), parse_mode="HTML")


async def cmd_monthreport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return

    now = datetime.now(timezone.utc)
    y, m = dbmod.previous_calendar_month(now.year, now.month)
    rows, start, end = await asyncio.to_thread(dbmod.fetch_month_vc_stats, chat.id, y, m)
    if not rows:
        await _reply_autodelete(
            update, context, f"No recorded VC data for {_month_name(m)} {y} in this group."
        )
        return
    if start and end:
        subtitle = f"{_month_name(m)} {y}: {_format_date_utc(start)} → {_format_date_utc(end)} (UTC)"
    else:
        subtitle = f"{_month_name(m)} {y} (UTC)"
    text = _format_vc_stats_html(f"Monthly VC report — {_month_name(m)} {y}", subtitle, rows)
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def cmd_vcstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return

    configured = app_state.configured_assistant_groups()
    in_config = chat.id in configured
    in_runtime = chat.id in app_state.assistant_chat_ids
    has_session = bool((os.environ.get("TELEGRAM_SESSION_STRING") or "").strip())
    has_api = bool(
        (os.environ.get("TELEGRAM_API_ID") or "").strip()
        and (os.environ.get("TELEGRAM_API_HASH") or "").strip()
    )

    if app_state.assistant_running and in_runtime:
        tracking = "✅ Assistant is running and tracking this group."
    elif in_config and not app_state.assistant_running:
        tracking = (
            "⚠️ This group is in ASSISTANT_GROUP_IDS but the assistant is NOT running. "
            "Re-run session_login.py and update TELEGRAM_SESSION_STRING on Render."
        )
    elif not in_config and configured:
        tracking = (
            f"⚠️ This group is NOT in ASSISTANT_GROUP_IDS.\n"
            f"Configured ids: {sorted(configured)}\n"
            f"Add this group: <code>{chat.id}</code>"
        )
    elif not configured:
        tracking = (
            "⚠️ ASSISTANT_GROUP_IDS is not set — only limited Bot API tracking (often no names)."
        )
    else:
        tracking = "⚠️ Assistant not active for this group."

    lines = [
        "🔧 <b>VC tracking status</b>",
        "",
        f"<b>This group chat id:</b> <code>{chat.id}</code>",
        f"<b>Title:</b> {html.escape(chat.title or '—', quote=False)}",
        "",
        tracking,
        "",
        f"TELEGRAM_SESSION_STRING set: {'yes' if has_session else 'no'}",
        f"TELEGRAM_API_ID/HASH set: {'yes' if has_api else 'no'}",
        f"ASSISTANT_GROUP_IDS: <code>{html.escape(os.environ.get('ASSISTANT_GROUP_IDS', '') or '(not set)', quote=False)}</code>",
    ]
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_finduser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can search the database.")
        return

    query = " ".join(context.args).strip().lstrip("@")
    if not query:
        await _reply_autodelete(
            update,
            context,
            "Usage: /finduser NAME_OR_USERNAME\n\n"
            "Examples:\n"
            "/finduser udvega\n"
            "/finduser Palxp1\n"
            "/finduser Kumar",
        )
        return

    rows = await asyncio.to_thread(dbmod.find_users_in_chat, chat.id, query)
    if not rows:
        await _reply_autodelete(
            update,
            context,
            f"No database records match <code>{html.escape(query, quote=False)}</code> in this group.",
            parse_mode="HTML",
        )
        return

    lines = [f"Found <b>{len(rows)}</b> match(es) for <code>{html.escape(query, quote=False)}</code>:", ""]
    for i, row in enumerate(rows, start=1):
        safe_name = html.escape(row.display_name, quote=False)
        lines.append(f"{i}. <code>{row.user_id}</code> — {safe_name}")
        lines.append(
            f"   {row.vc_count} VC(s), {_format_duration(row.total_seconds)}"
            + (f" | {row.present_days} present day(s)" if row.present_days else "")
        )
    lines.append("")

    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_removeuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can remove users from the database.")
        return

    if not context.args:
        await _reply_autodelete(
            update,
            context,
            "Usage: /removeuser &lt;telegram_user_id&gt;\n\n"
            "Example: /removeuser 1087968824\n\n"
            "Run /finduser NAME to look up a user's numeric id.",
            parse_mode="HTML",
        )
        return

    raw = context.args[0].strip()
    try:
        user_id = int(raw)
    except ValueError:
        await _reply_autodelete(update, context, "User id must be a number, e.g. /removeuser 1087968824")
        return
    if user_id <= 0:
        await _reply_autodelete(update, context, "User id must be a positive Telegram user id.")
        return

    preview = await asyncio.to_thread(dbmod.preview_remove_user, chat.id, user_id)
    if preview.vc_rows_deleted == 0 and preview.attendance_rows_deleted == 0:
        await _reply_autodelete(
            update,
            context,
            f"No database records found for user id <code>{user_id}</code> in this group.",
            parse_mode="HTML",
        )
        return

    # Destructive and irreversible: confirm before actually deleting.
    token = _register_pending_confirmation(
        "removeuser", chat.id, update.effective_user.id, {"target_id": user_id}
    )
    label = html.escape(preview.display_name or str(user_id), quote=False)
    lines = [
        f"⚠️ Remove <b>{label}</b> (<code>{user_id}</code>) from this group's stats?",
        f"• VC call records to delete: <b>{preview.vc_rows_deleted}</b>",
        f"• Attendance records to delete: <b>{preview.attendance_rows_deleted}</b>",
    ]
    await update.message.reply_text(
        "\n".join(lines), parse_mode="HTML", reply_markup=_confirm_keyboard(token)
    )


async def cmd_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this setting.")
        return

    arg = (context.args[0].lower() if context.args else "").strip()
    if arg not in ("on", "off"):
        await _reply_autodelete(update, context, "Usage: /reports on  or  /reports off")
        return
    enabled = arg == "on"
    await asyncio.to_thread(dbmod.set_monthly_reports, chat.id, enabled)
    await _reply_autodelete(
        update,
        context,
        "Monthly auto-reports are now " + ("enabled" if enabled else "disabled") + " for this group.",
    )


_CAPTCHA_TIMEOUT_SECONDS = 300  # 5 minutes to click "I'm not a bot" before being kicked
_pending_captchas: dict[tuple[int, int], dict] = {}  # (chat_id, user_id) -> {message_id}


async def on_new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Records when someone joins the group, for /mystats's "Joined group" field, and — if
    /captcha is enabled for this chat — mutes the new member and posts a button they must
    click within 5 minutes to be unmuted, or they're auto-kicked. This is the actual
    security gap the bot had versus Rose: without it, moderation is entirely reactive (an
    admin has to notice and act), so a spam-bot wave posting immediately after joining
    goes completely unchecked until someone happens to be watching.

    Fires off Telegram's "X joined the group" service message. Two known limits, both
    unavoidable without Telegram giving us historical data:
    - No join date exists for anyone who was already in the group before this shipped.
    - If the group has "Hide join/leave messages" turned on, this event never fires for
      them either — /mystats will show "Unknown" in both cases, which is accurate, not a bug.
    """
    msg = update.message
    if not msg or not msg.chat or not msg.new_chat_members:
        return
    chat = msg.chat
    if chat.type not in ("group", "supergroup"):
        return
    when = msg.date or datetime.now(timezone.utc)

    settings = await asyncio.to_thread(dbmod.get_antispam_settings, chat.id)
    captcha_on = settings["captcha_enabled"]

    for user in msg.new_chat_members:
        if user.is_bot:
            continue
        label = _user_label(user)
        await asyncio.to_thread(dbmod.record_group_join, chat.id, user.id, label, when)

        if not captcha_on:
            continue
        try:
            await context.bot.restrict_chat_member(chat.id, user.id, permissions=_MUTE_PERMISSIONS)
            safe = html.escape(label, quote=False)
            sent = await context.bot.send_message(
                chat.id,
                f"👋 Welcome, {safe}! Tap the button below within 5 minutes to unlock chat.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("✅ I'm not a bot", callback_data=f"captcha:{chat.id}:{user.id}")]]
                ),
            )
        except Exception:
            logger.exception("Captcha setup failed chat_id=%s user_id=%s", chat.id, user.id)
            continue

        _pending_captchas[(chat.id, user.id)] = {"message_id": sent.message_id, "label": label}
        jq = context.job_queue
        if jq is not None:
            jq.run_once(
                _captcha_timeout_kick,
                when=_CAPTCHA_TIMEOUT_SECONDS,
                data={"chat_id": chat.id, "user_id": user.id},
                name=f"captcha-timeout-{chat.id}-{user.id}",
            )


async def _captcha_timeout_kick(context: ContextTypes.DEFAULT_TYPE) -> None:
    """job_queue callback: if a new member never clicked the captcha button in time,
    kick them (they can rejoin and try again) and clean up the prompt message."""
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    entry = _pending_captchas.pop((chat_id, user_id), None)
    if entry is None:
        return  # already verified, or already handled
    try:
        await context.bot.ban_chat_member(chat_id, user_id)
        await context.bot.unban_chat_member(chat_id, user_id)
    except Exception:
        logger.exception("Captcha timeout kick failed chat_id=%s user_id=%s", chat_id, user_id)
    try:
        await context.bot.delete_message(chat_id, entry["message_id"])
    except Exception:
        pass


async def on_captcha_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user:
        return
    _prefix, _, rest = query.data.partition(":")
    try:
        chat_id_s, user_id_s = rest.split(":")
        chat_id, target_user_id = int(chat_id_s), int(user_id_s)
    except ValueError:
        return

    if query.from_user.id != target_user_id:
        await query.answer("This verification isn't for you.", show_alert=True)
        return

    entry = _pending_captchas.pop((chat_id, target_user_id), None)
    if entry is None:
        await query.answer("Already verified (or this expired).")
        return

    try:
        restore = _UNMUTE_FALLBACK_PERMISSIONS
        try:
            group_chat = await context.bot.get_chat(chat_id)
            if group_chat.permissions:
                restore = group_chat.permissions
        except Exception:
            pass
        await context.bot.restrict_chat_member(chat_id, target_user_id, permissions=restore)
    except Exception:
        logger.exception("Captcha verify unmute failed chat_id=%s user_id=%s", chat_id, target_user_id)
        await query.answer("Verified, but I couldn't unmute you — ask an admin.", show_alert=True)
        return

    await query.answer("Verified — welcome!")
    try:
        await query.edit_message_text(f"✅ {html.escape(entry.get('label', 'User'), quote=False)} verified.")
    except Exception:
        pass


async def cmd_captcha(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /captcha on|off — new-member verification for this group."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    settings = await asyncio.to_thread(dbmod.get_antispam_settings, chat.id)
    if not context.args:
        state = "on" if settings["captcha_enabled"] else "off"
        await _reply_autodelete(update, context, f"Captcha is currently {state}.\nUsage: /captcha on|off")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this.")
        return
    arg = context.args[0].lower()
    if arg not in ("on", "off"):
        await _reply_autodelete(update, context, "Usage: /captcha on|off")
        return
    await asyncio.to_thread(dbmod.set_captcha_enabled, chat.id, arg == "on")
    await _reply_autodelete(update, context, f"Captcha turned {arg}.")


# --- Flood control -------------------------------------------------------

# In-memory sliding window per (chat_id, user_id) -> list of monotonic timestamps.
# Not persisted: flood detection is about the last few seconds, so losing this on a
# redeploy is a non-issue (worst case: one burst right after restart goes unflagged).
_flood_tracker: dict[tuple[int, int], list[float]] = {}


async def on_text_check_flood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Auto-mutes (default)/kicks/bans anyone who posts too many messages too fast.
    This is the other half of the anti-spam gap: /captcha stops bot accounts from being
    able to post at all; this stops a human (or an already-verified bot) from flooding
    the chat, without needing an admin to notice and act manually. Off by default
    (flood_limit=0) — opt-in via /setflood so it can't surprise an already-chatty group."""
    msg = update.message
    if not msg or not update.effective_chat or not msg.from_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        return

    settings = await asyncio.to_thread(dbmod.get_antispam_settings, chat.id)
    limit = settings["flood_limit"]
    if limit <= 0:
        return

    try:
        member = await context.bot.get_chat_member(chat.id, msg.from_user.id)
        if member.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR):
            return
    except Exception:
        pass

    window = settings["flood_window_seconds"]
    key = (chat.id, msg.from_user.id)
    now = time.monotonic()
    times = _flood_tracker.setdefault(key, [])
    times.append(now)
    cutoff = now - window
    while times and times[0] < cutoff:
        times.pop(0)

    if len(times) < limit:
        return

    _flood_tracker.pop(key, None)
    target_label = _user_label(msg.from_user)
    action_text = await _apply_punishment(
        update, context, msg.from_user.id, target_label, settings["flood_mode"],
        reason=f"flood: {limit}+ messages in {window}s",
    )
    try:
        await context.bot.send_message(chat.id, f"🌊 Flood detected — {action_text}.")
    except Exception:
        logger.debug("Flood notice failed chat_id=%s", chat.id)


async def cmd_setflood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /setflood <count> [window_seconds]  or  /setflood off."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    settings = await asyncio.to_thread(dbmod.get_antispam_settings, chat.id)
    if not context.args:
        state = (
            f"{settings['flood_limit']} messages / {settings['flood_window_seconds']}s"
            if settings["flood_limit"] > 0 else "off"
        )
        await _reply_autodelete(
            update, context,
            f"Current flood limit: {state}\nUsage: /setflood <count> [window_seconds]  or  /setflood off",
        )
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this.")
        return
    arg0 = context.args[0].lower()
    if arg0 == "off":
        await asyncio.to_thread(dbmod.set_flood_limit, chat.id, 0, settings["flood_window_seconds"])
        await _reply_autodelete(update, context, "Flood control turned off.")
        return
    try:
        count = int(arg0)
    except ValueError:
        await _reply_autodelete(update, context, "Usage: /setflood <count> [window_seconds]  or  /setflood off")
        return
    if count < 2:
        await _reply_autodelete(update, context, "Flood count must be at least 2.")
        return
    window = settings["flood_window_seconds"]
    if len(context.args) >= 2:
        try:
            window = max(2, int(context.args[1]))
        except ValueError:
            pass
    await asyncio.to_thread(dbmod.set_flood_limit, chat.id, count, window)
    await _reply_autodelete(update, context, f"Flood limit set: {count} messages / {window}s.")


async def cmd_floodmode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    settings = await asyncio.to_thread(dbmod.get_antispam_settings, chat.id)
    if not context.args:
        await _reply_autodelete(
            update, context,
            f"Current flood mode: {settings['flood_mode']}\nOptions: {', '.join(FLOOD_MODES)}",
        )
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can change this.")
        return
    mode = context.args[0].lower()
    if mode not in FLOOD_MODES:
        await _reply_autodelete(update, context, f"Invalid mode. Options: {', '.join(FLOOD_MODES)}")
        return
    await asyncio.to_thread(dbmod.set_flood_mode, chat.id, mode)
    await _reply_autodelete(update, context, f"Flood mode set to {mode}.")


# --- Admin DM relay + direct message + broadcast ----------------------------


def _is_admin_user(user_id: int) -> bool:
    return user_id in app_state.parse_admin_user_ids()


async def on_private_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Any non-command DM sent to the bot gets copied into the private admin relay group.

    Does NOT go to the bot's Saved Messages or anywhere else — only into
    ADMIN_RELAY_CHAT_ID, exactly as configured.
    """
    msg = update.message
    if not msg or not update.effective_user:
        return

    relay_chat_id = app_state.admin_relay_chat_id()
    if not relay_chat_id:
        logger.warning("Got a DM but ADMIN_RELAY_CHAT_ID is not set; nothing to relay to.")
        return

    user = update.effective_user
    label = _user_label(user)
    header = f"📩 <b>New message</b> from {html.escape(label, quote=False)} (<code>{user.id}</code>)"

    try:
        info_msg = await context.bot.send_message(relay_chat_id, header, parse_mode="HTML")
        copied = await context.bot.copy_message(
            chat_id=relay_chat_id,
            from_chat_id=user.id,
            message_id=msg.message_id,
        )
        # Map both the header line and the copied content — replying to either
        # one in the admin group will correctly route back to this user.
        await asyncio.to_thread(dbmod.save_relay_mapping, info_msg.message_id, user.id, label)
        await asyncio.to_thread(dbmod.save_relay_mapping, copied.message_id, user.id, label)
    except Exception:
        logger.exception("Failed to relay DM from user_id=%s", user.id)


async def on_admin_relay_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """A reply inside the admin relay group gets delivered back to the original DM sender.

    Only fires for messages that are (a) inside the configured admin relay group,
    (b) sent by an allow-listed admin, and (c) an actual Telegram reply to a
    previously relayed message. A fresh, non-reply message in the admin group is
    left alone (so normal chat there doesn't get misrouted).
    """
    msg = update.message
    if not msg or not msg.reply_to_message or not update.effective_user or not update.effective_chat:
        return

    relay_chat_id = app_state.admin_relay_chat_id()
    if not relay_chat_id or update.effective_chat.id != relay_chat_id:
        return
    if not _is_admin_user(update.effective_user.id):
        return

    mapping = await asyncio.to_thread(dbmod.get_relay_mapping, msg.reply_to_message.message_id)
    if not mapping:
        await msg.reply_text(
            "⚠️ Can't find who this belongs to — reply directly to the forwarded "
            "message (long-press → Reply), not a fresh message."
        )
        return

    target_user_id = mapping["user_chat_id"]
    try:
        await context.bot.copy_message(
            chat_id=target_user_id,
            from_chat_id=relay_chat_id,
            message_id=msg.message_id,
        )
        await msg.reply_text(f"✅ Delivered to {mapping.get('display_name') or target_user_id}")
    except Exception:
        logger.exception("Failed to deliver relay reply to user_id=%s", target_user_id)
        await msg.reply_text("❌ Failed to deliver — the user may have blocked the bot.")


async def cmd_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only. Two forms, both supporting multiple recipient ids:
    - /message ID [ID2 ID3 ...] text here  — sends the given text to each id.
    - Reply to any message (text, photo, audio, video, sticker, document, etc.) with
      /message ID [ID2 ID3 ...]  — copies that message verbatim to each id.

    Recipient ids are parsed as consecutive numeric tokens at the start of the args:
    in the reply form every arg must be a numeric id; in the text form, leading numeric
    tokens are ids and everything after the first non-numeric token is the message text.
    This means a text message that is itself entirely digits can't be sent this way —
    use the reply form instead for that edge case."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if not _is_admin_user(update.effective_user.id):
        await _reply_autodelete(update, context, "Admins only.")
        return

    args = context.args or []
    reply = update.message.reply_to_message

    if reply is not None:
        if not args:
            await _reply_autodelete(
                update, context, "Usage (replying to a message): /message ID [ID2 ID3 ...]"
            )
            return
        ids_raw = args
        text = None
    else:
        i = 0
        while i < len(args) and args[i].lstrip("-").isdigit():
            i += 1
        if i == 0 or i >= len(args):
            await _reply_autodelete(
                update, context,
                "Usage: /message ID [ID2 ID3 ...] your text here\n"
                "Or reply to any message (text, photo, audio, video, etc.) with "
                "/message ID [ID2 ID3 ...]",
            )
            return
        ids_raw = args[:i]
        text = " ".join(args[i:])

    target_ids: list[int] = []
    for raw in ids_raw:
        try:
            target_ids.append(int(raw))
        except ValueError:
            await _reply_autodelete(update, context, f"Invalid user id: {html.escape(raw, quote=False)}")
            return

    sent, failed = 0, 0
    for target_id in target_ids:
        try:
            if reply is not None:
                await context.bot.copy_message(
                    chat_id=target_id,
                    from_chat_id=update.effective_chat.id,
                    message_id=reply.message_id,
                )
            else:
                await context.bot.send_message(target_id, text)
            sent += 1
        except Exception:
            logger.exception("cmd_message failed target=%s", target_id)
            failed += 1

    if len(target_ids) == 1:
        text_result = (
            "✅ Sent."
            if failed == 0
            else "❌ Failed to send — user may have blocked the bot or never messaged it before."
        )
        await _reply_autodelete(update, context, text_result)
    else:
        summary = f"📨 Sent to {sent}/{len(target_ids)} user(s)."
        if failed:
            summary += f" {failed} failed (blocked the bot, or never messaged it before)."
        await _reply_autodelete(update, context, summary)


def _broadcast_target_chat_id() -> int | None:
    raw = (os.environ.get("BROADCAST_CHAT_ID") or "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    configured = app_state.configured_assistant_groups()
    return sorted(configured)[0] if configured else None


async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only: /broadcast text — or reply to any message with /broadcast to
    resend that exact content (photo, poll, text, whatever) to everyone who has
    ever joined a tracked VC in the target group. Shows a confirmation with the
    audience size before actually sending — a fat-fingered /broadcast can otherwise
    message hundreds of people instantly with no undo."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if not _is_admin_user(update.effective_user.id):
        await _reply_autodelete(update, context, "Admins only.")
        return

    target_chat_id = _broadcast_target_chat_id()
    if target_chat_id is None:
        await _reply_autodelete(
            update,
            context,
            "No target group configured. Set BROADCAST_CHAT_ID, or make sure ASSISTANT_GROUP_IDS is set.",
        )
        return

    users = await asyncio.to_thread(dbmod.fetch_all_known_user_ids, target_chat_id)
    if not users:
        await _reply_autodelete(update, context, "No VC participants recorded yet to broadcast to.")
        return

    source_msg = update.message.reply_to_message
    text_arg = " ".join(context.args) if context.args else None
    if not source_msg and not text_arg:
        await _reply_autodelete(
            update, context, "Usage: /broadcast your text  — or reply to a message with /broadcast"
        )
        return

    token = _register_pending_confirmation(
        "broadcast",
        update.effective_chat.id,
        update.effective_user.id,
        {
            "users": users,
            "source_chat_id": update.effective_chat.id,
            "source_message_id": source_msg.message_id if source_msg else None,
            "text_arg": text_arg,
        },
    )
    await update.message.reply_text(
        f"⚠️ This will message <b>{len(users)}</b> people. Send it?",
        parse_mode="HTML",
        reply_markup=_confirm_keyboard(token),
    )


async def cmd_exportdata(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only, DM-only: /exportdata [chat_id] — CSV of every user who has
    joined at least one VC in the given group (or the default tracked group
    if chat_id is omitted). Sent as a document, not posted to any group."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command only works in a private chat with me.")
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return

    if context.args:
        try:
            chat_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("chat_id must be a number.\nUsage: /exportdata [chat_id]")
            return
    else:
        chat_id = _broadcast_target_chat_id()
        if chat_id is None:
            await update.message.reply_text(
                "No chat_id given and no default group configured "
                "(set BROADCAST_CHAT_ID or ASSISTANT_GROUP_IDS).\nUsage: /exportdata <chat_id>"
            )
            return

    rows = await asyncio.to_thread(dbmod.fetch_export_data, chat_id)
    if not rows:
        await update.message.reply_text(f"No VC data found for chat_id <code>{chat_id}</code>.", parse_mode="HTML")
        return

    csv_bytes = await asyncio.to_thread(dbmod.export_rows_to_csv, rows)
    filename = f"vc_export_{chat_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=InputFile(io.BytesIO(csv_bytes), filename=filename),
        caption=f"📊 VC data export for chat_id <code>{chat_id}</code> — {len(rows)} user(s).",
        parse_mode="HTML",
    )


async def cmd_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only, DM-only: /user <user_id> [chat_id] — full stats for one user
    in the given group (or the default tracked group if chat_id is omitted).
    Use /finduser NAME in the group first if you don't know their numeric id."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command only works in a private chat with me.")
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return

    if not context.args:
        await update.message.reply_text(
            "Usage: /user <user_id> [chat_id]\n\n"
            "Omit chat_id to use the default tracked group.\n"
            "Don't know their id? Run /finduser NAME in the group first."
        )
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("user_id must be a number.")
        return

    if len(context.args) >= 2:
        try:
            chat_id = int(context.args[1])
        except ValueError:
            await update.message.reply_text("chat_id must be a number.")
            return
    else:
        chat_id = _broadcast_target_chat_id()
        if chat_id is None:
            await update.message.reply_text(
                "No chat_id given and no default group configured "
                "(set BROADCAST_CHAT_ID or ASSISTANT_GROUP_IDS).\nUsage: /user <user_id> <chat_id>"
            )
            return

    exists = await asyncio.to_thread(dbmod.has_any_data, chat_id, user_id)
    if not exists:
        await update.message.reply_text(
            f"No data found for user_id <code>{user_id}</code> in chat_id <code>{chat_id}</code>.",
            parse_mode="HTML",
        )
        return

    stats = await asyncio.to_thread(dbmod.get_my_stats, chat_id, user_id)
    history, stored_name = await asyncio.to_thread(dbmod.get_warning_history, chat_id, user_id)
    label = stored_name or stats.display_name or str(user_id)

    text = (
        dbmod.format_my_stats_message(stats)
        + f"\n\n<code>user_id: {user_id}</code>\n<code>chat_id: {chat_id}</code>"
    )
    if history:
        text += "\n\n" + dbmod.format_warning_history_html(label, history)

    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_mywarns(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Anyone: /mywarns — your own full warning history in this group, including warnings
    cleared by a past /resetwarn (shown, but marked "Cleared" rather than counted). Unlike
    /warns (admin-only, active count toward the warn limit), this is always scoped to the
    caller's own record and always shows the complete history, active and cleared alike."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return

    user = update.effective_user
    history, stored_name = await asyncio.to_thread(dbmod.get_warning_history, chat.id, user.id)
    label = stored_name or _user_label(user)

    note = ""
    if len(history) > 30:
        history = history[-30:]
        note = f"\n<i>Showing the 30 most recent.</i>"

    text = dbmod.format_warning_history_html(label, history) + note
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin-only, DM-only: /health — checks MongoDB, the Telegram Bot API, the Telethon
    assistant, and Groq (if configured), so a silent failure (dead assistant session,
    dropped Mongo connection) surfaces on demand instead of only being noticed once a
    feature has quietly stopped working."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if update.effective_chat.type != "private":
        await update.message.reply_text("This command only works in a private chat with me.")
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return

    lines = ["🩺 <b>Bot health check</b>", ""]

    try:
        ok = await asyncio.to_thread(dbmod.ping)
        lines.append("✅ MongoDB: reachable" if ok else "❌ MongoDB: ping failed")
    except Exception as exc:
        lines.append(f"❌ MongoDB: {html.escape(str(exc)[:200], quote=False)}")

    try:
        me = await context.bot.get_me()
        lines.append(f"✅ Telegram Bot API: OK (@{me.username})")
    except Exception as exc:
        lines.append(f"❌ Telegram Bot API: {html.escape(str(exc)[:200], quote=False)}")

    if app_state.assistant_running:
        tracked = sorted(app_state.assistant_chat_ids)
        lines.append(f"✅ Telethon assistant: running, tracking {len(tracked)} group(s)")
    else:
        configured = app_state.configured_assistant_groups()
        if configured:
            lines.append(
                "❌ Telethon assistant: configured but NOT running — "
                "check TELEGRAM_SESSION_STRING and Render logs"
            )
        else:
            lines.append("⚠️ Telethon assistant: not configured (ASSISTANT_GROUP_IDS unset)")

    groq_key = (os.environ.get("GROQ_API_KEY") or "").strip()
    if not groq_key:
        lines.append("⚠️ Groq (AI recap): not configured")
    else:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.get(
                    "https://api.groq.com/openai/v1/models",
                    headers={"Authorization": f"Bearer {groq_key}"},
                )
            lines.append("✅ Groq: reachable" if r.status_code == 200 else f"❌ Groq: HTTP {r.status_code}")
        except Exception as exc:
            lines.append(f"❌ Groq: {html.escape(str(exc)[:200], quote=False)}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")




async def cmd_allowlink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a user to the link allowlist."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can manage link permissions.")
        return

    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    else:
        await _reply_autodelete(update, context, "Reply to a user, or give their user id / @username.")
        return

    added = await asyncio.to_thread(dbmod.add_link_allow, chat.id, target_id)
    if added:
        safe = html.escape(target_label, quote=False)
        await _reply_autodelete(update, context, f"✅ {safe} can now send links even when link lock is on.", parse_mode="HTML")
    else:
        await _reply_autodelete(update, context, "That user is already on the allowlist.")

async def cmd_disallowlink(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a user from the link allowlist."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can manage link permissions.")
        return

    args = context.args or []
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        u = update.message.reply_to_message.from_user
        target_id, target_label = u.id, _user_label(u)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.")
            return
        target_id, target_label = resolved
    else:
        await _reply_autodelete(update, context, "Reply to a user, or give their user id / @username.")
        return

    removed = await asyncio.to_thread(dbmod.remove_link_allow, chat.id, target_id)
    if removed:
        safe = html.escape(target_label, quote=False)
        await _reply_autodelete(update, context, f"✅ {safe} removed from link allowlist.", parse_mode="HTML")
    else:
        await _reply_autodelete(update, context, "That user is not on the allowlist.")

async def cmd_allowlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the list of users allowed to send links."""
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view the link allowlist.")
        return

    allowlist = await asyncio.to_thread(dbmod.get_link_allowlist, chat.id)
    if not allowlist:
        await _reply_autodelete(update, context, "No users are on the link allowlist.")
        return

    lines = ["🔗 <b>Link Allowlist</b>", ""]
    for uid in allowlist:
        try:
            member = await context.bot.get_chat_member(chat.id, uid)
            label = _user_label(member.user)
        except Exception:
            label = str(uid)
        lines.append(f"• {html.escape(label, quote=False)} (<code>{uid}</code>)")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")





async def _http_bot_send_message(chat_id: int, text: str) -> bool:
    """Direct Bot API HTTP (works even when python-telegram-bot polling hits Conflict)."""
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.post(
                url,
                data={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"},
            )
        if r.status_code != 200:
            logger.error("HTTP sendMessage failed: %s %s", r.status_code, r.text[:400])
            return False
        return True
    except Exception:
        logger.exception("HTTP sendMessage exception chat_id=%s", chat_id)
        return False


async def _assistant_vc_fallback_report(
    chat_id: int,
    duration_sec: int,
) -> None:
    """If assistant never saw the call (short VC between polls), still post Telegram duration.

    Uses app_state.try_claim_vc_finalize() so this fallback and the assistant's own
    _finalize_call (assistant.py) never both record the same VC end to the database.
    Whichever path calls try_claim_vc_finalize() first for this chat_id wins and
    proceeds; the other silently skips. This must stay a claim taken up-front (not a
    timestamp comparison after the fact) or a slow assistant post (e.g. resolving many
    usernames) can lose the race and get double-recorded.
    """
    wait = float(os.getenv("ASSISTANT_FALLBACK_WAIT_SECONDS", "8"))
    await asyncio.sleep(wait)
    if not app_state.try_claim_vc_finalize(chat_id):
        logger.info(
            "Fallback: VC end for chat_id=%s already claimed (assistant got there first); skipping duplicate record",
            chat_id,
        )
        return
    ended = datetime.now(timezone.utc)
    hint = app_state.take_bot_vc_hint(chat_id)
    end_ts = _utc_ts(ended)

    parts: list[tuple[int, str, int]] = []
    started_at = hint.started_at if hint else None
    if hint and hint.participants:
        for uid, (label, first_seen) in hint.participants.items():
            span = max(0.0, end_ts - _utc_ts(first_seen))
            est_sec = int(min(span, float(duration_sec)))
            parts.append((uid, label, est_sec))

    await asyncio.to_thread(
        dbmod.record_vc_session,
        chat_id,
        ended,
        duration_sec,
        started_at,
        parts,
    )
    await asyncio.to_thread(dbmod.ensure_chat, chat_id, None)

    lines = [
        "📞 <b>Voice/video chat ended</b>",
        "",
        f"<b>Call length (tracked):</b> {duration_sec // 60} min {duration_sec % 60} s",
        "",
    ]
    if parts:
        rows = sorted(parts, key=lambda x: -x[2])
        lines.append(f"<b>People in VC:</b> {len(rows)}")
        lines.append("")
        for rank, (_uid, label, est_sec) in enumerate(rows, start=1):
            mp, sp = est_sec // 60, est_sec % 60
            safe = html.escape(label, quote=False)
            lines.append(f"{rank}. {safe}: <b>{mp} min {sp} s</b>")
        lines.append("")
        lines.append(
            "<i>Names from Bot API hints (assistant poll missed this short call). "
            "For full tracking, keep the assistant session active.</i>"
        )
    else:
        lines.append(
            "<i>No per-person breakdown: the assistant never saw an active group call in "
            "Telegram's channel state, and the Bot API did not report who joined. "
            "Check TELEGRAM_SESSION_STRING, ASSISTANT_GROUP_IDS, and that the assistant "
            "user is in this supergroup. Add ASSISTANT_DEBUG=1 and check Render logs.</i>"
        )
    text = "\n".join(lines)
    if not await _http_bot_send_message(chat_id, text):
        logger.error("Assistant fallback could not send chat_id=%s", chat_id)
        return

    earned = await asyncio.to_thread(dbmod.record_present_attendance, chat_id, parts)
    attendance_text = dbmod.format_attendance_message(earned)
    await _http_bot_send_message(chat_id, attendance_text)

    badges = await asyncio.to_thread(dbmod.check_and_award_session_badges, chat_id, parts)
    if badges:
        badge_text = _format_badges_earned_html(badges)
        await _http_bot_send_message(chat_id, badge_text)

    ai_summary = await generate_ai_vc_summary("this group", duration_sec, parts)
    if ai_summary:
        safe_summary = html.escape(ai_summary, quote=False)
        await _http_bot_send_message(chat_id, f"🤖 <i>{safe_summary}</i>")


class VideoChatServiceFilter(MessageFilter):
    def filter(self, message) -> bool:
        return bool(
            message.video_chat_started
            or message.video_chat_ended
            or message.video_chat_participants_invited
        )


def _vc_starter_from_message(msg) -> tuple[int | None, str | None]:
    """Best-effort who started the VC from a Bot API service message."""
    if msg.from_user:
        return msg.from_user.id, _user_label(msg.from_user)
    sender = getattr(msg, "sender_chat", None)
    if sender is not None and getattr(sender, "type", None) == "channel":
        title = getattr(sender, "title", None) or str(sender.id)
        return None, title
    return None, None


async def on_video_chat_service(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.chat:
        return

    chat = msg.chat
    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    now = msg.date
    configured = app_state.configured_assistant_groups()
    assistant_group = chat_id in configured
    use_assistant = app_state.assistant_running and chat_id in app_state.assistant_chat_ids

    if assistant_group:
        if msg.video_chat_started:
            starter_id, starter_label = _vc_starter_from_message(msg)
            app_state.note_bot_vc_started(chat_id, now, starter_id, starter_label)
            logger.info(
                "VC started chat_id=%s starter=%s assistant_running=%s",
                chat_id,
                starter_id,
                app_state.assistant_running,
            )
            if use_assistant:
                return
        if msg.video_chat_participants_invited and msg.video_chat_participants_invited.users:
            invited = [
                (u.id, _user_label(u)) for u in msg.video_chat_participants_invited.users
            ]
            app_state.note_bot_vc_invited(chat_id, now, invited)
            logger.info("VC invited chat_id=%s count=%s", chat_id, len(invited))
            if use_assistant:
                return
        if msg.video_chat_ended:
            _sessions.pop(chat_id, None)
            duration_sec = msg.video_chat_ended.duration
            if not app_state.assistant_running:
                logger.warning(
                    "VC ended chat_id=%s — assistant configured but not running; using hint fallback",
                    chat_id,
                )
            asyncio.create_task(
                _assistant_vc_fallback_report(chat_id, duration_sec),
                name=f"vc-fallback-{chat_id}",
            )
            return

    if msg.video_chat_ended:
        logger.warning(
            "VC ended chat_id=%s — not in ASSISTANT_GROUP_IDS (%s); limited Bot API tracking. "
            "Run /vcstatus in the group to get the correct chat id.",
            chat_id,
            sorted(configured) if configured else "none configured",
        )

    await asyncio.to_thread(dbmod.ensure_chat, chat_id, chat.title or None)

    if msg.video_chat_started:
        session = VCSession(started_at=now, participants={})
        starter_id, starter_label = _vc_starter_from_message(msg)
        if starter_id is not None and starter_label and app_state.is_vc_participant(starter_id):
            session.participants[starter_id] = (starter_label, now)
        hint = app_state.peek_bot_vc_hint(chat_id)
        if hint:
            for uid, (label, first_seen) in hint.participants.items():
                if app_state.is_vc_participant(uid) and uid not in session.participants:
                    session.participants[uid] = (label, first_seen)
        _sessions[chat_id] = session
        logger.info("VC started chat_id=%s starter=%s", chat_id, starter_id)
        return

    if msg.video_chat_participants_invited and msg.video_chat_participants_invited.users:
        logger.info(
            "VC participants invited chat_id=%s count=%s (not counted until they join)",
            chat_id,
            len(msg.video_chat_participants_invited.users),
        )
        return

    if msg.video_chat_ended:
        duration_sec = msg.video_chat_ended.duration
        session = _sessions.pop(chat_id, None)
        end_ts = _utc_ts(now)

        parts: list[tuple[int, str, int]] = []
        if session and session.participants:
            for uid, (label, first_seen) in session.participants.items():
                span = max(0.0, end_ts - _utc_ts(first_seen))
                est_sec = int(min(span, float(duration_sec)))
                parts.append((uid, label, est_sec))
        if not parts:
            hint = app_state.take_bot_vc_hint(chat_id)
            if hint and hint.participants:
                for uid, (label, first_seen) in hint.participants.items():
                    span = max(0.0, end_ts - _utc_ts(first_seen))
                    est_sec = int(min(span, float(duration_sec)))
                    parts.append((uid, label, est_sec))
        else:
            app_state.take_bot_vc_hint(chat_id)

        await asyncio.to_thread(
            dbmod.record_vc_session,
            chat_id,
            now,
            duration_sec,
            session.started_at if session else None,
            parts,
        )

        lines = [
            "📞 <b>Voice/video chat ended</b>",
            "",
            f"<b>Call length:</b> {duration_sec // 60} min {duration_sec % 60} s "
            f"({duration_sec} s total)",
        ]

        if not parts:
            lines.append("")
            lines.append(
                "<i>No names recorded. Add this group to ASSISTANT_GROUP_IDS and set up the "
                "Telethon assistant (TELEGRAM_SESSION_STRING). Run /vcstatus here for your "
                "exact chat id and setup checklist.</i>"
            )
        else:
            rows = sorted(parts, key=lambda x: -x[2])
            lines.append("")
            lines.append(
                f"<b>People listed (Telegram invite updates only):</b> {len(rows)} "
                f"<i>— not everyone who joined</i>"
            )
            lines.append("")
            for rank, (_uid, label, est_sec) in enumerate(rows, start=1):
                m_part = est_sec // 60
                s_part = est_sec % 60
                safe = html.escape(label, quote=False)
                lines.append(f"{rank}. {safe}: ~{m_part} min {s_part} s")

        lines.append("")
        lines.append(
            "<i>Telegram does not expose real per-person VC time for bots. "
            "These minutes are rough estimates from invite events, capped by call length.</i>"
        )

        text = "\n".join(lines)
        await msg.reply_text(text, parse_mode="HTML")

        earned = await asyncio.to_thread(dbmod.record_present_attendance, chat_id, parts)
        attendance_text = dbmod.format_attendance_message(earned)
        await msg.reply_text(attendance_text, parse_mode="HTML")

        badges = await asyncio.to_thread(dbmod.check_and_award_session_badges, chat_id, parts)
        if badges:
            badge_text = _format_badges_earned_html(badges)
            await msg.reply_text(badge_text, parse_mode="HTML")

        ai_summary = await generate_ai_vc_summary(chat.title or "this group", duration_sec, parts)
        if ai_summary:
            safe_summary = html.escape(ai_summary, quote=False)
            await msg.reply_text(f"🤖 <i>{safe_summary}</i>", parse_mode="HTML")

        logger.info(
            "VC ended chat_id=%s duration=%s participants=%s",
            chat_id,
            duration_sec,
            len(session.participants) if session else 0,
        )


def _format_badges_earned_html(badges: list) -> str:
    lines = ["🏅 <b>New badge(s) unlocked!</b>", ""]
    by_user: dict[int, list] = {}
    for b in badges:
        by_user.setdefault(b.user_id, []).append(b)
    for uid, blist in by_user.items():
        safe = html.escape(blist[0].display_name, quote=False)
        badge_labels = ", ".join(b.badge_label for b in blist)
        lines.append(f"{safe}: {badge_labels}")
    return "\n".join(lines)


async def generate_ai_vc_summary(
    chat_title: str,
    duration_sec: int,
    parts: list[tuple[int, str, int]],
    vc_topic: str | None = None,
) -> str | None:
    """Ask Groq (free tier, Llama 3.3 70B) for a short natural-language VC recap.
    Returns None if GROQ_API_KEY is missing or the call fails — caller should skip silently.

    vc_topic: the voice chat's title/topic, if one was set (e.g. renamed via Telegram's
    "Set chat title" option on the group call). Only the Telethon assistant path can supply
    this today (it comes from phone.GetGroupCallRequest, which needs a user account, not a
    bot). Bot-API-only fallback paths pass None, and the recap simply omits it."""
    api_key = (os.environ.get("GROQ_API_KEY") or "").strip()
    if not api_key:
        return None
    if not parts:
        return None

    rows = sorted(parts, key=lambda x: -x[2])
    roster_lines = []
    for _uid, label, sec in rows:
        m = sec // 60
        roster_lines.append(f"- {label}: {m} min")
    roster_text = "\n".join(roster_lines)
    total_min = duration_sec // 60

    topic_line = f' The voice chat was titled "{vc_topic}".' if vc_topic else ""
    topic_instruction = (
        " and naturally mention the chat's topic/title in the recap" if vc_topic else ""
    )

    prompt = (
        f"Write a short, upbeat 2-3 sentence recap of a voice chat that just ended in the "
        f"Telegram group \"{chat_title}\".{topic_line} The call lasted {total_min} minutes total. "
        f"Participants and their approximate time in the call:\n{roster_text}\n\n"
        f"Mention who stayed longest and roughly how many people joined{topic_instruction}. "
        f"Keep it casual and friendly, like a group chat bot, not formal. Do not use markdown "
        f"formatting, just plain text. Keep it under 400 characters."
    )

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.8,
                },
            )
        if r.status_code != 200:
            logger.warning("Groq summary failed: HTTP %s %s", r.status_code, r.text[:300])
            return None
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        return text or None
    except Exception:
        logger.exception("Groq summary request failed")
        return None


async def cmd_level(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    user = update.effective_user
    label = _user_label(user)
    info = await asyncio.to_thread(dbmod.get_level_info, chat.id, user.id, label)
    await _reply_autodelete(update, context, dbmod.format_level_message(info), parse_mode="HTML")


async def cmd_xpleaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.fetch_xp_leaderboard, chat.id, 10)
    if not rows:
        await _reply_autodelete(update, context, "No XP recorded in this group yet.")
        return
    lines = ["🎖️ <b>XP Leaderboard</b>", ""]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, row in enumerate(rows, start=1):
        medal = medals.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        lines.append(f"{medal} {safe} — Level {row.level} ({row.xp} XP)")
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_streak(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    user = update.effective_user
    label = _user_label(user)
    info = await asyncio.to_thread(dbmod.get_streak_info, chat.id, user.id, label)
    safe = html.escape(info.display_name or label, quote=False)
    text = (
        f"🔥 <b>{safe}</b>\n"
        f"Current streak: <b>{info.current_streak}</b> day(s)\n"
        f"Longest streak: <b>{info.longest_streak}</b> day(s)"
    )
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def cmd_mystats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    user = update.effective_user
    label = _user_label(user)
    stats = await asyncio.to_thread(dbmod.get_my_stats, chat.id, user.id, label)
    await _reply_autodelete(update, context, dbmod.format_my_stats_message(stats), parse_mode="HTML")


async def cmd_streakboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await _reply_autodelete(update, context, "Only group admins can view the streakboard.")
        return
    try:
        rows = await asyncio.to_thread(dbmod.fetch_full_streakboard, chat.id)
        if not rows:
            await _reply_autodelete(update, context, "No streak data recorded in this group yet.")
            return
        text = dbmod.format_streakboard_html(rows)
        await _reply_autodelete(update, context, text, parse_mode="HTML")
    except Exception as exc:
        logger.exception("cmd_streakboard failed chat_id=%s", chat.id)
        await _reply_autodelete(
            update,
            context,
            f"⚠️ /streakboard failed: <code>{html.escape(repr(exc)[:500], quote=False)}</code>",
            parse_mode="HTML",
        )


async def cmd_badges(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    user = update.effective_user
    rows = await asyncio.to_thread(dbmod.get_user_badges, chat.id, user.id)
    if not rows:
        await _reply_autodelete(update, context, "No badges earned yet — keep showing up to VCs!")
        return
    lines = ["🏅 <b>Your badges</b>", ""]
    for b in rows:
        lines.append(b.badge_label)
    await _reply_autodelete(update, context, "\n".join(lines), parse_mode="HTML")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    digest = await asyncio.to_thread(dbmod.fetch_weekly_digest, chat.id)
    text = dbmod.format_weekly_digest_message(chat.title or "This group", digest)
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def hourly_monthly_gate(context: ContextTypes.DEFAULT_TYPE) -> None:
    """On the 1st from MONTHLY_REPORT_HOUR_UTC onward, post last month's leaderboard (retries if send fails)."""
    hour = int(os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))
    now = datetime.now(timezone.utc)
    if now.day != 1 or now.hour < hour:
        return

    report_y, report_m = dbmod.previous_calendar_month(now.year, now.month)
    chat_ids = await asyncio.to_thread(dbmod.list_chats_with_monthly_reports)
    bot = context.bot

    for chat_id in chat_ids:
        if await asyncio.to_thread(dbmod.monthly_report_already_sent, chat_id, report_y, report_m):
            continue
        rows, start, end = await asyncio.to_thread(dbmod.fetch_month_vc_stats, chat_id, report_y, report_m)
        if not rows:
            continue
        if start and end:
            subtitle = f"{_month_name(report_m)} {report_y}: {_format_date_utc(start)} → {_format_date_utc(end)} (UTC)"
        else:
            subtitle = f"{_month_name(report_m)} {report_y} (UTC)"
        text = _format_vc_stats_html(
            f"Monthly VC report — {_month_name(report_m)} {report_y}",
            subtitle,
            rows,
        )
        try:
            await bot.send_message(chat_id, text, parse_mode="HTML")
            await asyncio.to_thread(dbmod.mark_monthly_report_sent, chat_id, report_y, report_m)
        except Exception:
            logger.exception("Failed monthly report chat_id=%s", chat_id)


async def daily_streak_reset_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Runs once daily, just after UTC midnight: zeroes current_streak for anyone who didn't
    cross the present threshold (20+ min in a call) on any day now more than 1 day in the past.
    This is what makes a broken streak show 0 the next day, instead of staying stale until
    the person's next VC (which would recompute it) or the weekly digest (which only ran
    weekly and could leave a dead streak visible for up to 6 extra days)."""
    try:
        reset_count = await asyncio.to_thread(dbmod.reset_expired_streaks_all)
        if reset_count:
            logger.info("Daily streak reset: cleared %s expired streak(s)", reset_count)
    except Exception:
        logger.exception("Daily streak reset job failed")


async def weekly_digest_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Every Monday from MONTHLY_REPORT_HOUR_UTC onward, post the weekly digest to all opted-in chats."""
    hour = int(os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))
    now = datetime.now(timezone.utc)
    if now.weekday() != 0 or now.hour != hour:
        # Only fire once, in the top-of-hour window matching hour setting, on Monday.
        return

    chat_ids = await asyncio.to_thread(dbmod.list_chats_with_monthly_reports)
    bot = context.bot
    for chat_id in chat_ids:
        try:
            digest = await asyncio.to_thread(dbmod.fetch_weekly_digest, chat_id)
            if digest.total_sessions == 0:
                continue
            try:
                chat = await bot.get_chat(chat_id)
                title = chat.title or "This group"
            except Exception:
                title = "This group"
            text = dbmod.format_weekly_digest_message(title, digest)
            await bot.send_message(chat_id, text, parse_mode="HTML")
            await asyncio.to_thread(dbmod.reset_expired_streaks, chat_id)
        except Exception:
            logger.exception("Failed weekly digest chat_id=%s", chat_id)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    err = context.error
    if isinstance(err, Conflict):
        msg = str(err).strip()
        logger.error("Telegram Conflict: %s", msg)
        if "webhook is active" in msg.lower() or "deletewebhook" in msg.lower():
            logger.error(
                "This usually means something is still calling getUpdates with this BOT_TOKEN "
                "(e.g. an old Render Worker, a second Web service, or python bot.py on your PC) "
                "while this app uses a webhook. Suspend/delete every other service and stop local "
                "bots; only one receiver may use this token."
            )
        else:
            logger.error(
                "If the message mentions another getUpdates request: only one long-poll client "
                "may run per token. Stop duplicate Render services and any local bot process."
            )
        return
    logger.error(
        "Unhandled exception: %s",
        err,
        exc_info=(type(err), err, err.__traceback__) if err and getattr(err, "__traceback__", None) else None,
    )


async def _webhook_post_register_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Re-log getWebhookInfo after run_webhook has called setWebhook (first log is often stale)."""
    if not _use_webhook():
        return
    logger.info("Webhook health check ~20s after boot (state should reflect this deploy's setWebhook)")
    _log_webhook_info(context.application.bot.token.strip())


async def post_init(application: Application) -> None:
    if (
        os.getenv("RENDER", "").strip().lower() == "true"
        and _use_webhook()
        and not _env_truthy("KEEP_ALIVE_DISABLE")
    ):
        _start_render_keepalive_thread()

    jq = application.job_queue
    if jq is None:
        return
    if _use_webhook():
        jq.run_once(_webhook_post_register_check, when=20, name="webhook_post_register_check")
    jq.run_repeating(
        hourly_monthly_gate,
        interval=3600,
        first=20,
        name="hourly_monthly_gate",
    )
    jq.run_repeating(
        weekly_digest_job,
        interval=3600,
        first=25,
        name="weekly_digest_job",
    )
    jq.run_daily(
        daily_streak_reset_job,
        time=dt_time(hour=0, minute=5, tzinfo=timezone.utc),
        name="daily_streak_reset_job",
    )
    logger.info("Scheduled hourly check for monthly VC reports (UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))
    logger.info("Scheduled hourly check for weekly digest (Mondays, UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))
    logger.info("Scheduled daily streak reset job (00:05 UTC)")


def main() -> None:
    _configure_debug_loggers()
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set BOT_TOKEN in environment or .env file")

    _log_bot_identity(token)

    dbmod.init_db()

    if not app_state.admin_relay_chat_id():
        logger.warning(
            "ADMIN_RELAY_CHAT_ID is not set — DM relay is disabled (private messages to the "
            "bot will just be ignored)."
        )
    if not app_state.parse_admin_user_ids():
        logger.warning(
            "ADMIN_USER_IDS is not set — /message, /broadcast, and admin-group reply relay are disabled."
        )

    app = (
        Application.builder()
        .token(token)
        .job_queue(JobQueue())
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("vcreport", cmd_vcreport))
    app.add_handler(CommandHandler("attendance", cmd_attendance))
    app.add_handler(CommandHandler("monthreport", cmd_monthreport))
    app.add_handler(CommandHandler("vcstatus", cmd_vcstatus))
    app.add_handler(CommandHandler("reports", cmd_reports))
    app.add_handler(CommandHandler("removeuser", cmd_removeuser))
    app.add_handler(CommandHandler("finduser", cmd_finduser))
    app.add_handler(CommandHandler("level", cmd_level))
    app.add_handler(CommandHandler("xpleaderboard", cmd_xpleaderboard))
    app.add_handler(CommandHandler("streak", cmd_streak))
    app.add_handler(CommandHandler("badges", cmd_badges))
    app.add_handler(CommandHandler("weekly", cmd_weekly))
    app.add_handler(CommandHandler("mystats", cmd_mystats))
    app.add_handler(CommandHandler("streakboard", cmd_streakboard))
    app.add_handler(CommandHandler("message", cmd_message))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("exportdata", cmd_exportdata))
    app.add_handler(CommandHandler("user", cmd_user))
    app.add_handler(CommandHandler("mywarns", cmd_mywarns))

    # --- Moderation (Rose-style names) ---
    app.add_handler(CommandHandler("warn", cmd_warn))
    app.add_handler(CommandHandler("warns", cmd_warns))
    app.add_handler(CommandHandler("resetwarn", cmd_resetwarn))
    app.add_handler(CommandHandler("warnlimit", cmd_warnlimit))
    app.add_handler(CommandHandler("warnmode", cmd_warnmode))
    app.add_handler(CommandHandler("ban", cmd_ban))
    app.add_handler(CommandHandler("tban", cmd_tban))
    app.add_handler(CommandHandler("unban", cmd_unban))
    app.add_handler(CommandHandler("kick", cmd_kick))
    app.add_handler(CommandHandler("mute", cmd_mute))
    app.add_handler(CommandHandler("tmute", cmd_tmute))
    app.add_handler(CommandHandler("unmute", cmd_unmute))
    app.add_handler(CommandHandler("blocklist", cmd_blocklist))
    app.add_handler(CommandHandler("addblocklist", cmd_addblocklist))
    app.add_handler(CommandHandler("unblocklist", cmd_unblocklist))
    app.add_handler(CommandHandler("blocklistmode", cmd_blocklistmode))
    app.add_handler(CommandHandler("filter", cmd_filter))
    app.add_handler(CommandHandler("filters", cmd_filters))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("lock", cmd_lock))
    app.add_handler(CommandHandler("unlock", cmd_unlock))
    app.add_handler(CommandHandler("locks", cmd_locks))
    app.add_handler(CommandHandler("allowlink", cmd_allowlink))
    app.add_handler(CommandHandler("disallowlink", cmd_disallowlink))
    app.add_handler(CommandHandler("allowlist", cmd_allowlist))
    app.add_handler(CommandHandler("addtopic", cmd_addtopic))
    app.add_handler(CommandHandler("topics", cmd_topics))
    app.add_handler(CommandHandler("deletetopic", cmd_deletetopic))
    app.add_handler(CommandHandler("deletedtopics", cmd_deletedtopics))
    app.add_handler(CommandHandler("topicdone", cmd_topicdone))
    app.add_handler(CommandHandler("alltopics", cmd_alltopics))
    app.add_handler(CommandHandler("upvote", cmd_upvote))
    app.add_handler(CommandHandler("modlog", cmd_modlog))
    app.add_handler(CommandHandler("health", cmd_health))
    app.add_handler(CommandHandler("captcha", cmd_captcha))
    app.add_handler(CommandHandler("setflood", cmd_setflood))
    app.add_handler(CommandHandler("floodmode", cmd_floodmode))
    app.add_handler(CommandHandler("timer", cmd_timer))
    app.add_handler(CommandHandler("canceltimer", cmd_canceltimer))
    # Inline-button callbacks: generic confirm/cancel (ban, removeuser, broadcast) and
    # new-member captcha verification. Matched by callback_data prefix via `pattern`.
    app.add_handler(CallbackQueryHandler(on_confirmation_callback, pattern=r"^(confirm|cancel):"))
    app.add_handler(CallbackQueryHandler(on_captcha_callback, pattern=r"^captcha:"))
    app.add_handler(CallbackQueryHandler(on_help_menu_callback, pattern=r"^menu:"))
    # Auto-enforcement on plain text messages — separate handler groups (1, 2, 4, 5, 6) so
    # all always run alongside command dispatch in the default group (0); PTB only runs
    # the first matching handler *within* a group, not across groups.
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_text_check_blocklist),
        group=1,
    )
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_text_check_filters),
        group=2,
    )
    # Passive: remembers username -> id for every group message, regardless of type,
    # so moderation commands can resolve @username later. See on_track_known_user.
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, on_track_known_user), group=3)
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, on_text_admin_tag),
        group=4,
    )
    app.add_handler(
        MessageHandler(filters.ChatType.GROUPS & (filters.TEXT | filters.CAPTION), on_text_check_links),
        group=5,
    )
    # Flood control: counts every group message (including commands — a burst of
    # commands is still a flood), so no filters.TEXT/~COMMAND restriction here.
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, on_text_check_flood), group=6)
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & VideoChatServiceFilter(),
            on_video_chat_service,
        )
    )
    # Records "joined the group" timestamps for /mystats (going forward only — see
    # on_new_chat_members's docstring for what this can't recover historically).
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.StatusUpdate.NEW_CHAT_MEMBERS,
            on_new_chat_members,
        )
    )
    # Admin relay: any non-command private DM to the bot -> copied into admin group.
    app.add_handler(
        MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_private_message)
    )
    # Admin relay: a reply inside a group chat (checked against ADMIN_RELAY_CHAT_ID
    # inside the handler itself) -> routed back to the DM sender.
    app.add_handler(
        MessageHandler(filters.REPLY & filters.ChatType.GROUPS, on_admin_relay_reply)
    )
    app.add_error_handler(error_handler)

    use_webhook = _use_webhook()
    if not use_webhook:
        _start_http_on_port_for_render()

    try:
        from assistant import start_assistant_background

        start_assistant_background()
    except Exception:
        logger.exception("Could not start VC assistant thread")

    logger.info("Bot starting (group VC tracker)")
    try:
        if use_webhook:
            raw_port = os.environ["PORT"]
            port = int(raw_port)
            base = _webhook_public_base()
            if not base:
                raise SystemExit("Webhook mode needs RENDER_EXTERNAL_URL or WEBHOOK_URL")
            path = _webhook_path_segment()
            webhook_url = f"{base}/{path}"
            secret = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip() or None
            logger.info(
                "Webhook mode (no getUpdates): public URL ends with /%s — stop any other bot "
                "process using this token to avoid stealing updates.",
                path,
            )
            logger.info(
                "Telegram webhook status below may still show 521/pending from the *previous* "
                "deploy; setWebhook runs when the app finishes starting. Check the ~20s "
                "follow-up log line webhook_post_register_check."
            )
            _log_webhook_info(token)
            webhook_kwargs: dict = {
                "listen": "0.0.0.0",
                "port": port,
                "url_path": path,
                "webhook_url": webhook_url,
                "allowed_updates": Update.ALL_TYPES,
                "drop_pending_updates": True,
                "secret_token": secret,
                "bootstrap_retries": 5,
            }
            # Render sends SIGTERM; avoid signal-handler edge cases on some runtimes.
            if os.getenv("RENDER", "").strip().lower() == "true":
                webhook_kwargs["stop_signals"] = None
                logger.info(
                    "Render: free tier often breaks inbound webhooks (521). If /start does nothing, "
                    "set FORCE_POLLING=1 (one instance only) or use uptime ping + paid tier."
                )
            app.run_webhook(**webhook_kwargs)
        else:
            logger.info("Polling mode: bot will use getUpdates (needs exactly one poller for this token).")
            app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except InvalidToken:
        logger.error(
            "Telegram rejected BOT_TOKEN. In @BotFather use /token or /mybots → API Token, "
            "copy the full value, paste into Render → Environment → BOT_TOKEN (no quotes or spaces), "
            "save, then redeploy. If the token was ever leaked, use /revoke first."
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
