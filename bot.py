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
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict

import httpx
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ChatMemberStatus
from telegram.error import Conflict, InvalidToken
from telegram.ext import Application, CommandHandler, ContextTypes, JobQueue, MessageHandler, filters
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
            self.wfile.write(b"ok")

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
        " <i>Counts from 1 July 2026</i>",
        f"<i>More than {threshold_min} minutes in one call = +1 present day (once per call).</i>",
        "",
    ]
    for i, row in enumerate(rows, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        day_word = "day" if row.present_days == 1 else "days"
        lines.append(f"{medal} {safe} — <b>{row.present_days}</b> {day_word}")
    return "\n".join(lines)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "🎙️ <b>BooksDiscuss VC Tracker Bot</b>\n"

        "📊 <b>What I track</b>\n"
        "VCs joined, total hours in calls, and present attendance "
        "(20+ min in a call = +1 present day).\n"
        "After every VC ends, I post the call summary, present attendance, "
        "and an AI recap, automatically.\n\n"

        "📅 <b>Tracking started: 3 August 2026</b>\n"
        "<i>All stats below are counted from this date onward.</i>\n\n"

        "<b>📈 Stats &amp; Reports</b>\n"
        "• /vcreport — all-time stats: VCs joined and total hours\n"
        "• /attendance — present-day leaderboard for this group\n"
        "• /monthreport — previous month's participant stats\n"
        "• /weekly — this week's digest (top hours + streaks)\n\n"

        "<b>🎮 Your Progress</b>\n"
        "• /level — your XP and level\n"
        "• /xpleaderboard — top XP earners in this group\n"
        "• /streak — your current and longest VC streak\n"
        "• /badges — your earned badges\n\n"

        "<b>🔧 Utility</b>\n"
        "• /vcstatus — this group's chat id + whether tracking is active\n\n"

        "<b>🛠️ Admin only</b>\n"
        "• /reports on|off — toggle automatic monthly report (posted on the 1st, UTC)\n"
        "• /removeuser USER_ID — remove a user from VC stats and attendance\n"
        "• /finduser NAME — find a user's id by name or old @username\n"
        "• /message USER_ID text — DM any known user directly\n"
        "• /broadcast text — message everyone who has joined a VC\n\n"

        "<i>This bot belongd to→ @BooksDiscuss </i>",
        parse_mode="HTML",
    )


async def _is_group_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.effective_user or not update.effective_chat:
        return False
    try:
        m = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    except Exception:
        return False
    return m.status in (ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR)


async def cmd_vcreport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return

    rows, start, end = await asyncio.to_thread(dbmod.fetch_alltime_vc_stats, chat.id)
    if not rows:
        await update.message.reply_text("No recorded VC data in this group yet.")
        return
    subtitle = f"{_format_date_utc(start)} → {_format_date_utc(end)} (UTC)"
    text = _format_vc_stats_html("All-time VC report", subtitle, rows)
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_attendance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return

    rows = await asyncio.to_thread(dbmod.fetch_all_attendance, chat.id)
    if not rows:
        threshold_min = dbmod.present_threshold_sec() // 60
        await update.message.reply_text(
            f"No present attendance recorded in this group yet.\n\n"
            f"Stay more than {threshold_min} minutes in a voice/video call to earn +1 present day."
        )
        return

    await update.message.reply_text(_format_attendance_html(rows), parse_mode="HTML")


async def cmd_monthreport(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return

    now = datetime.now(timezone.utc)
    y, m = dbmod.previous_calendar_month(now.year, now.month)
    rows, start, end = await asyncio.to_thread(dbmod.fetch_month_vc_stats, chat.id, y, m)
    if not rows:
        await update.message.reply_text(
            f"No recorded VC data for {_month_name(m)} {y} in this group."
        )
        return
    if start and end:
        subtitle = f"{_month_name(m)} {y}: {_format_date_utc(start)} → {_format_date_utc(end)} (UTC)"
    else:
        subtitle = f"{_month_name(m)} {y} (UTC)"
    text = _format_vc_stats_html(f"Monthly VC report — {_month_name(m)} {y}", subtitle, rows)
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_vcstatus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
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
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_finduser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await update.message.reply_text("Only group admins can search the database.")
        return

    query = " ".join(context.args).strip().lstrip("@")
    if not query:
        await update.message.reply_text(
            "Usage: /finduser NAME_OR_USERNAME\n\n"
            "Examples:\n"
            "/finduser udvega\n"
            "/finduser Palxp1\n"
            "/finduser Kumar",
        )
        return

    rows = await asyncio.to_thread(dbmod.find_users_in_chat, chat.id, query)
    if not rows:
        await update.message.reply_text(
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

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_removeuser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return
    if not await _is_group_admin(update, context):
        await update.message.reply_text("Only group admins can remove users from the database.")
        return

    if not context.args:
        await update.message.reply_text(
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
        await update.message.reply_text("User id must be a number, e.g. /removeuser 1087968824")
        return
    if user_id <= 0:
        await update.message.reply_text("User id must be a positive Telegram user id.")
        return

    result = await asyncio.to_thread(dbmod.remove_user_from_chat, chat.id, user_id)
    if result.vc_rows_deleted == 0 and result.attendance_rows_deleted == 0:
        await update.message.reply_text(
            f"No database records found for user id <code>{user_id}</code> in this group.",
            parse_mode="HTML",
        )
        return

    label = html.escape(result.display_name or str(user_id), quote=False)
    lines = [
        f"Removed <b>{label}</b> (<code>{user_id}</code>) from this group's stats:",
        f"• VC call records deleted: <b>{result.vc_rows_deleted}</b>",
        f"• Attendance records deleted: <b>{result.attendance_rows_deleted}</b>",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this in a group.")
        return
    if not await _is_group_admin(update, context):
        await update.message.reply_text("Only group admins can change this setting.")
        return

    arg = (context.args[0].lower() if context.args else "").strip()
    if arg not in ("on", "off"):
        await update.message.reply_text("Usage: /reports on  or  /reports off")
        return
    enabled = arg == "on"
    await asyncio.to_thread(dbmod.set_monthly_reports, chat.id, enabled)
    await update.message.reply_text(
        "Monthly auto-reports are now " + ("enabled" if enabled else "disabled") + " for this group."
    )


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
    """Admin-only: /message USER_ID text — DM anyone by Telegram user id directly."""
    if not update.message or not update.effective_user:
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Usage: /message USER_ID your text here")
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("USER_ID must be a number.")
        return

    text = " ".join(context.args[1:])
    try:
        await context.bot.send_message(target_id, text)
        await update.message.reply_text("✅ Sent.")
    except Exception:
        logger.exception("cmd_message failed target=%s", target_id)
        await update.message.reply_text(
            "❌ Failed to send — user may have blocked the bot or never messaged it before."
        )


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
    ever joined a tracked VC in the target group."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if not _is_admin_user(update.effective_user.id):
        await update.message.reply_text("Admins only.")
        return

    target_chat_id = _broadcast_target_chat_id()
    if target_chat_id is None:
        await update.message.reply_text(
            "No target group configured. Set BROADCAST_CHAT_ID, or make sure ASSISTANT_GROUP_IDS is set."
        )
        return

    users = await asyncio.to_thread(dbmod.fetch_all_known_user_ids, target_chat_id)
    if not users:
        await update.message.reply_text("No VC participants recorded yet to broadcast to.")
        return

    source_msg = update.message.reply_to_message
    text_arg = " ".join(context.args) if context.args else None
    if not source_msg and not text_arg:
        await update.message.reply_text(
            "Usage: /broadcast your text  — or reply to a message with /broadcast"
        )
        return

    sent, failed = 0, 0
    for uid, _label in users:
        try:
            if source_msg:
                await context.bot.copy_message(
                    chat_id=uid,
                    from_chat_id=update.effective_chat.id,
                    message_id=source_msg.message_id,
                )
            else:
                await context.bot.send_message(uid, text_arg)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    await update.message.reply_text(
        f"📣 Broadcast done: {sent} sent, {failed} failed (out of {len(users)})."
    )


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
) -> str | None:
    """Ask Groq (free tier, Llama 3.3 70B) for a short natural-language VC recap.
    Returns None if GROQ_API_KEY is missing or the call fails — caller should skip silently."""
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

    prompt = (
        f"Write a short, upbeat 2-3 sentence recap of a voice chat that just ended in the "
        f"Telegram group \"{chat_title}\". The call lasted {total_min} minutes total. "
        f"Participants and their approximate time in the call:\n{roster_text}\n\n"
        f"Mention who stayed longest and roughly how many people joined. Keep it casual and "
        f"friendly, like a group chat bot, not formal. Do not use markdown formatting, just plain text. "
        f"Keep it under 400 characters."
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
                    "model": "openai/gpt-oss-120b",
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
        await update.message.reply_text("Use this command in a group.")
        return
    user = update.effective_user
    label = _user_label(user)
    info = await asyncio.to_thread(dbmod.get_level_info, chat.id, user.id, label)
    await update.message.reply_text(dbmod.format_level_message(info), parse_mode="HTML")


async def cmd_xpleaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return
    rows = await asyncio.to_thread(dbmod.fetch_xp_leaderboard, chat.id, 10)
    if not rows:
        await update.message.reply_text("No XP recorded in this group yet.")
        return
    lines = ["🎖️ <b>XP Leaderboard</b>", ""]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, row in enumerate(rows, start=1):
        medal = medals.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        lines.append(f"{medal} {safe} — Level {row.level} ({row.xp} XP)")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_streak(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
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
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_badges(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return
    user = update.effective_user
    rows = await asyncio.to_thread(dbmod.get_user_badges, chat.id, user.id)
    if not rows:
        await update.message.reply_text("No badges earned yet — keep showing up to VCs!")
        return
    lines = ["🏅 <b>Your badges</b>", ""]
    for b in rows:
        lines.append(b.badge_label)
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Use this command in a group.")
        return
    digest = await asyncio.to_thread(dbmod.fetch_weekly_digest, chat.id)
    text = dbmod.format_weekly_digest_message(chat.title or "This group", digest)
    await update.message.reply_text(text, parse_mode="HTML")


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
    logger.info("Scheduled hourly check for monthly VC reports (UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))
    logger.info("Scheduled hourly check for weekly digest (Mondays, UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))


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
    app.add_handler(CommandHandler("message", cmd_message))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & VideoChatServiceFilter(),
            on_video_chat_service,
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