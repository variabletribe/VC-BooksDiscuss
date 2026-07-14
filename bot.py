"""
Group voice/video chat tracker for Telegram.

Telegram’s Bot API does not expose “everyone who joined the VC” or real per-user
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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "I track group voice/video chats and store stats in a database.\n\n"
        "Optional: with a Telethon user session (ASSISTANT_GROUP_IDS + "
        "TELEGRAM_SESSION_STRING on the host), an assistant account can poll who is "
        "in the VC and post join times (see repo session_login.py).\n\n"
        "Without that, Telegram only gives bots invite-style hints — not every joiner.\n\n"
        "• After each VC ends, I post the call summary + present attendance (20+ min = +1 day).\n"
        "• /vcreport — all-time stats: VCs joined and total hours (first recorded call → now).\n"
        "• /monthreport — previous calendar month’s participant stats.\n"
        "• /vcstatus — show this group’s chat id and whether VC tracking is active.\n"
        "• /reports on|off — admins only; automatic monthly report on the 1st (UTC).\n\n"
        "If I miss events: @BotFather → /setprivacy → Disable."
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
    signal_mono: float,
    duration_sec: int,
) -> None:
    """If assistant never saw the call (short VC between polls), still post Telegram duration."""
    wait = float(os.getenv("ASSISTANT_FALLBACK_WAIT_SECONDS", "8"))
    await asyncio.sleep(wait)
    if app_state.assistant_vc_report_mono.get(chat_id, 0) > signal_mono:
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
            "Telegram’s channel state, and the Bot API did not report who joined. "
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
            sig = time.monotonic()
            if not app_state.assistant_running:
                logger.warning(
                    "VC ended chat_id=%s — assistant configured but not running; using hint fallback",
                    chat_id,
                )
            asyncio.create_task(
                _assistant_vc_fallback_report(chat_id, sig, duration_sec),
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
        session = _sessions.get(chat_id)
        if session is None:
            session = VCSession(started_at=now, participants={})
            _sessions[chat_id] = session
        for user in msg.video_chat_participants_invited.users:
            if app_state.is_vc_participant(user.id, user.username) and user.id not in session.participants:
                session.participants[user.id] = (_user_label(user), now)
        logger.info(
            "VC participants invited chat_id=%s count=%s",
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

        logger.info(
            "VC ended chat_id=%s duration=%s participants=%s",
            chat_id,
            duration_sec,
            len(session.participants) if session else 0,
        )


async def hourly_monthly_gate(context: ContextTypes.DEFAULT_TYPE) -> None:
    """On the 1st from MONTHLY_REPORT_HOUR_UTC onward, post last month’s leaderboard (retries if send fails)."""
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
    logger.info("Webhook health check ~20s after boot (state should reflect this deploy’s setWebhook)")
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
    logger.info("Scheduled hourly check for monthly VC reports (UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))


def main() -> None:
    _configure_debug_loggers()
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set BOT_TOKEN in environment or .env file")

    _log_bot_identity(token)

    dbmod.init_db()

    app = (
        Application.builder()
        .token(token)
        .job_queue(JobQueue())
        .post_init(post_init)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("vcreport", cmd_vcreport))
    app.add_handler(CommandHandler("monthreport", cmd_monthreport))
    app.add_handler(CommandHandler("vcstatus", cmd_vcstatus))
    app.add_handler(CommandHandler("reports", cmd_reports))
    app.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & VideoChatServiceFilter(),
            on_video_chat_service,
        )
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
