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
        logger.info(
            "getWebhookInfo: url=%r pending_updates=%s last_error_date=%s last_error=%r",
            res.get("url"),
            res.get("pending_update_count"),
            res.get("last_error_date"),
            res.get("last_error_message"),
        )
    except Exception:
        logger.warning("getWebhookInfo failed (non-fatal)", exc_info=True)


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


def _format_leaderboard_html(year: int, month: int, rows: list[dbmod.LeaderRow], title: str) -> str:
    lines = [
        f"📊 <b>{html.escape(title)}</b>",
        f"<b>{_month_name(month)} {year}</b> (estimated VC time, invite-based)",
        "",
    ]
    for i, row in enumerate(rows, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        lines.append(f"{medal} {safe} — <b>{_format_duration(row.total_seconds)}</b>")
    lines.append("")
    lines.append(
        "<i>Not everyone who joins a VC appears here — only people Telegram reports "
        "via invite-style events. Times are estimates.</i>"
    )
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
        "• After each VC ends, I reply with that call’s summary.\n"
        "• /vcreport — this month’s leaderboard (most time first).\n"
        "• /vcreport last — previous calendar month.\n"
        "• /reports on|off — admins only; automatic monthly report (1st, UTC).\n\n"
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

    args = context.args or []
    now = datetime.now(timezone.utc)
    if args and args[0].lower() == "last":
        y, m = dbmod.previous_calendar_month(now.year, now.month)
        title = "Monthly VC leaderboard"
    else:
        y, m = now.year, now.month
        title = "VC leaderboard (month to date)"

    rows = await asyncio.to_thread(dbmod.fetch_month_leaderboard, chat.id, y, m)
    if not rows:
        await update.message.reply_text(
            f"No recorded VC time for {_month_name(m)} {y} in this group."
        )
        return
    text = _format_leaderboard_html(y, m, rows, title)
    await update.message.reply_text(text, parse_mode="HTML")


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
    wait = float(os.getenv("ASSISTANT_FALLBACK_WAIT_SECONDS", "15"))
    await asyncio.sleep(wait)
    if app_state.assistant_vc_report_mono.get(chat_id, 0) > signal_mono:
        return
    ended = datetime.now(timezone.utc)
    await asyncio.to_thread(
        dbmod.record_vc_session,
        chat_id,
        ended,
        duration_sec,
        None,
        [],
    )
    await asyncio.to_thread(dbmod.ensure_chat, chat_id, None)
    text = (
        "📞 <b>Voice/video chat ended</b>\n\n"
        f"<b>Call length (Telegram):</b> {duration_sec // 60} min {duration_sec % 60} s "
        f"({duration_sec} s total)\n\n"
        "<i>No per-person breakdown: the assistant never saw an active group call in "
        "Telegram’s channel state, or participant lists stayed empty for this call. "
        "Redeploy the latest code (uses getGroupCall). Add ASSISTANT_DEBUG=1 on the host "
        "and check logs. The assistant user must be in this supergroup.</i>"
    )
    if not await _http_bot_send_message(chat_id, text):
        logger.error("Assistant fallback could not send chat_id=%s", chat_id)


class VideoChatServiceFilter(MessageFilter):
    def filter(self, message) -> bool:
        return bool(
            message.video_chat_started
            or message.video_chat_ended
            or message.video_chat_participants_invited
        )


async def on_video_chat_service(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = update.message
    if not msg or not msg.chat:
        return

    chat = msg.chat
    if chat.type not in ("group", "supergroup"):
        return

    chat_id = chat.id
    now = msg.date

    if app_state.assistant_running and chat_id in app_state.assistant_chat_ids:
        if msg.video_chat_started or msg.video_chat_participants_invited:
            return
        if msg.video_chat_ended:
            _sessions.pop(chat_id, None)
            duration_sec = msg.video_chat_ended.duration
            sig = time.monotonic()
            asyncio.create_task(
                _assistant_vc_fallback_report(chat_id, sig, duration_sec),
                name=f"vc-fallback-{chat_id}",
            )
            return

    await asyncio.to_thread(dbmod.ensure_chat, chat_id, chat.title or None)

    if msg.video_chat_started:
        _sessions[chat_id] = VCSession(started_at=now, participants={})
        logger.info("VC started chat_id=%s", chat_id)
        return

    if msg.video_chat_participants_invited and msg.video_chat_participants_invited.users:
        session = _sessions.get(chat_id)
        if session is None:
            session = VCSession(started_at=now, participants={})
            _sessions[chat_id] = session
        for user in msg.video_chat_participants_invited.users:
            if user.id not in session.participants:
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

        if not session or not session.participants:
            lines.append("")
            lines.append(
                "<i>No names recorded. Bots cannot see a full “who joined” list — only "
                "some people appear when Telegram sends invite-style updates. "
                "Try inviting members to the call, and ensure privacy is disabled "
                "(@BotFather → /setprivacy → Disable).</i>"
            )
        else:
            rows = sorted(parts, key=lambda x: -x[2])
            lines.append("")
            lines.append(
                f"<b>People listed (Telegram invite updates only):</b> {len(rows)} "
                f"<i>— not everyone who joined</i>"
            )
            lines.append("")
            for _uid, label, est_sec in rows:
                m_part = est_sec // 60
                s_part = est_sec % 60
                safe = html.escape(label, quote=False)
                lines.append(f"• {safe}: ~{m_part} min {s_part} s")

        lines.append("")
        lines.append(
            "<i>Telegram does not expose real per-person VC time for bots. "
            "These minutes are rough estimates from invite events, capped by call length.</i>"
        )

        text = "\n".join(lines)
        await msg.reply_text(text, parse_mode="HTML")
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
        rows = await asyncio.to_thread(dbmod.fetch_month_leaderboard, chat_id, report_y, report_m)
        if not rows:
            continue
        text = _format_leaderboard_html(
            report_y,
            report_m,
            rows,
            "Monthly VC leaderboard",
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


async def post_init(application: Application) -> None:
    jq = application.job_queue
    if jq is None:
        return
    jq.run_repeating(
        hourly_monthly_gate,
        interval=3600,
        first=20,
        name="hourly_monthly_gate",
    )
    logger.info("Scheduled hourly check for monthly VC reports (UTC hour=%s)", os.getenv("MONTHLY_REPORT_HOUR_UTC", "9"))


def main() -> None:
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("Set BOT_TOKEN in environment or .env file")

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
            app.run_webhook(**webhook_kwargs)
        else:
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
