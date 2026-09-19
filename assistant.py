"""
User-account (Telethon) assistant: polls supergroup video/voice chats and tracks
who appears in the live participant list, accumulating approximate time in call.

Requires:
  TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION_STRING (from session_login.py)
  ASSISTANT_GROUP_IDS=-100111,-100222  (supergroup chat ids)
  BOT_TOKEN (to post the summary as the bot)

The assistant must be a normal USER account (phone login via session_login.py).
Do NOT use a bot token / BotFather session — Telegram returns BotMethodInvalidError for
GetGroupCall and GetGroupParticipants on bot accounts.

Optional: ASSISTANT_JOIN_VC=1 makes the assistant actually join the group call itself
(as a silent, muted-audio listener) whenever a tracked VC starts, and leave when it ends.
This is entirely separate from the text-chat "welcome" message sent to new VC joiners
(see _post_vc_join_welcome) — that always works with no extra setup; actually joining the
call needs two more things:
  1. The `py-tgcalls` package with the `telethon` extra (see requirements.txt).
  2. The `ffmpeg` binary present on PATH (used to transcode the silence file for the call).
     On Render's native Python runtime this is NOT installed by default — either add
     `apt-get install -y ffmpeg` to the build command (works on some Render environments,
     not guaranteed), or switch this service to `runtime: docker` with the included
     Dockerfile, which installs ffmpeg explicitly.
If either piece is missing or fails, ASSISTANT_JOIN_VC is silently disabled (logged once)
and everything else — VC tracking, the text welcome message, all bot commands — keeps
working exactly as before. This feature never crashes or blocks the tracker.
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import tempfile
import time
import wave
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Set

import httpx
from telethon import TelegramClient, functions, utils
from telethon.errors import AuthKeyDuplicatedError
from telethon.sessions import StringSession
from telethon.tl.types import GroupCallDiscarded, PeerUser, User

import db as dbmod
import state as app_state

logger = logging.getLogger(__name__)


class AssistantConfigError(Exception):
    """Raised for problems that a retry will never fix: missing env vars, an
    unauthorized session, or a session that belongs to a bot account. The
    background retry loop in start_assistant_background() logs these once and
    stops — unlike transient errors (network blips, AuthKeyDuplicatedError),
    which it retries with backoff."""


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


# --- Optional: actually joining the VC's audio (ASSISTANT_JOIN_VC=1) --------
#
# CRITICAL: py-tgcalls's compatibility layer (pytgcalls/sync.py) captures "the current
# asyncio event loop" the FIRST time the `pytgcalls` package is imported anywhere in the
# process, and silently reroutes every later PyTgCalls call onto that captured loop. If
# that import happened at module load time (i.e. as soon as bot.py does
# `from assistant import start_assistant_background` on the MAIN thread, before this
# module's own background thread/event loop even exists), every pytgcalls call gets
# routed onto the wrong loop and Telethon raises "asyncio event loop must not change
# after connection" the moment PyTgCalls touches the client. So the import is deferred
# until _load_pytgcalls() is called from inside run_assistant() itself, which only
# happens once the assistant's OWN event loop is the one actually running — see
# start_assistant_background(), which also reuses one persistent loop across retries so
# this stays correct even after a reconnect, not just on the very first attempt.
PyTgCalls = None  # type: ignore[assignment]
pytgcalls_filters = None  # type: ignore[assignment]
GroupCallConfig = MediaStream = StreamEnded = AudioQuality = None  # type: ignore
_PYTGCALLS_IMPORT_ERROR: Exception | None = None
_pytgcalls_load_attempted = False


def _load_pytgcalls() -> None:
    """Imports py-tgcalls the first time it's actually needed. Must only be called from
    code already running inside the assistant's own event loop (i.e. from within
    run_assistant()) — see the module-level comment above for why. Safe to call more
    than once; only does the import once."""
    global PyTgCalls, pytgcalls_filters, GroupCallConfig, MediaStream, StreamEnded
    global AudioQuality, _PYTGCALLS_IMPORT_ERROR, _pytgcalls_load_attempted
    if _pytgcalls_load_attempted:
        return
    _pytgcalls_load_attempted = True
    try:
        from pytgcalls import PyTgCalls as _PyTgCalls
        from pytgcalls import filters as _pytgcalls_filters
        from pytgcalls.types import GroupCallConfig as _GroupCallConfig
        from pytgcalls.types import MediaStream as _MediaStream
        from pytgcalls.types import StreamEnded as _StreamEnded
        from pytgcalls.types.stream import AudioQuality as _AudioQuality

        PyTgCalls = _PyTgCalls
        pytgcalls_filters = _pytgcalls_filters
        GroupCallConfig = _GroupCallConfig
        MediaStream = _MediaStream
        StreamEnded = _StreamEnded
        AudioQuality = _AudioQuality
        _PYTGCALLS_IMPORT_ERROR = None
    except Exception as exc:  # pragma: no cover - depends on optional install
        _PYTGCALLS_IMPORT_ERROR = exc


# Set once run_assistant() has started PyTgCalls; None means the feature is off/unavailable.
_pytgcalls_app: "PyTgCalls | None" = None
# The assistant's own Telegram user id, set once run_assistant() calls get_me().
_assistant_self_id: int | None = None

_SILENCE_SECONDS = 5
_SILENCE_SAMPLE_RATE = 48000


def _silence_file_path() -> str:
    """Path to a short local silence WAV, generated once with the stdlib `wave` module
    (no ffmpeg needed to create it — ffmpeg is only used later, by pytgcalls, to
    transcode it for the call). Looped indefinitely via the on-stream-end handler
    registered in run_assistant(), so its length doesn't matter beyond "not tiny"."""
    path = os.path.join(tempfile.gettempdir(), "vcbot_silence.wav")
    if not os.path.exists(path) or os.path.getsize(path) < 1000:
        n_bytes_per_sec = _SILENCE_SAMPLE_RATE * 2  # 16-bit mono
        with wave.open(path, "wb") as w:
            w.setnchannels(1)
            w.setsampwidth(2)
            w.setframerate(_SILENCE_SAMPLE_RATE)
            silence_chunk = b"\x00\x00" * _SILENCE_SAMPLE_RATE
            for _ in range(_SILENCE_SECONDS):
                w.writeframes(silence_chunk)
        logger.info("Assistant: generated silence file at %s", path)
    return path


def _silence_stream() -> "MediaStream":
    return MediaStream(
        _silence_file_path(),
        audio_parameters=AudioQuality.HIGH,
        video_flags=MediaStream.Flags.IGNORE,
    )


async def _on_vc_audio_stream_end(client: "PyTgCalls", update: "StreamEnded") -> None:
    """Our silence file is short; when it finishes playing, immediately replay it so
    the assistant stays audible-silent for as long as it remains joined to the call."""
    try:
        await client.play(update.chat_id, _silence_stream())
    except Exception:
        logger.debug(
            "Assistant: VC silence loop restart failed (likely already left) chat_id=%s",
            update.chat_id,
        )


async def _join_vc_audio(chat_id: int, st: "_CallState") -> None:
    """Best-effort: have the assistant actually join chat_id's live call as a silent
    audio participant. Never raises — any failure just leaves st.joined_call_audio
    False so a later poll iteration can retry."""
    if _pytgcalls_app is None or st.joined_call_audio:
        return
    st.joined_call_audio = True
    start = time.monotonic()
    try:
        await _pytgcalls_app.play(
            chat_id,
            _silence_stream(),
            config=GroupCallConfig(auto_start=False),
        )
        logger.info(
            "Assistant: joined VC audio chat_id=%s (took %.1fs)",
            chat_id,
            time.monotonic() - start,
        )
    except Exception:
        logger.exception(
            "Assistant: failed to join VC audio chat_id=%s (after %.1fs)",
            chat_id,
            time.monotonic() - start,
        )
        st.joined_call_audio = False


async def _leave_vc_audio(chat_id: int, st: "_CallState") -> None:
    if _pytgcalls_app is None or not st.joined_call_audio:
        return
    st.joined_call_audio = False
    try:
        await _pytgcalls_app.leave_call(chat_id)
        logger.info("Assistant: left VC audio chat_id=%s", chat_id)
    except Exception:
        logger.debug("Assistant: leave_call failed/already left chat_id=%s", chat_id)


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


def _user_label(u: User | None, uid: int) -> str:
    if not u:
        return str(uid)
    parts = [x for x in (u.first_name, u.last_name) if x]
    name = " ".join(parts).strip()
    if u.username:
        name = f"{name} (@{u.username})" if name else f"@{u.username}"
    return name or str(uid)


def _parse_group_ids(raw: str) -> set[int]:
    return app_state.parse_assistant_group_ids(raw)


@dataclass
class _CallState:
    call_id: int
    started_at: datetime
    last_ids: Set[int] = field(default_factory=set)
    seen_ids: Set[int] = field(default_factory=set)
    join_at: Dict[int, datetime] = field(default_factory=dict)
    accumulated: Dict[int, float] = field(default_factory=dict)
    user_cache: Dict[int, User] = field(default_factory=dict)
    hint_labels: Dict[int, str] = field(default_factory=dict)
    vc_title: str | None = None
    joined_call_audio: bool = False  # True once the assistant itself joined this call's audio


async def _send_bot_message(chat_id: int, text: str) -> bool:
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(timeout=60.0) as http:
        r = await http.post(
            url,
            data={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"},
        )
        if r.status_code != 200:
            logger.error("Bot sendMessage HTTP failed: %s %s", r.status_code, r.text[:500])
            return False
    return True


async def _post_vc_summary(telethon: TelegramClient, chat_id: int, text: str) -> bool:
    """Try Bot API over HTTP, then post as the user so summaries still arrive if bot HTTP fails."""
    if await _send_bot_message(chat_id, text):
        return True
    try:
        await telethon.send_message(chat_id, text, parse_mode="html")
        logger.warning(
            "VC summary sent as the assistant user (bot HTTP failed; may not show as the bot)"
        )
        return True
    except Exception:
        logger.exception("VC summary: user send_message also failed")
        return False


def _call_input(call) -> tuple[int, int] | None:
    cid = getattr(call, "id", None)
    ah = getattr(call, "access_hash", None)
    if cid is None or ah is None:
        return None
    return int(cid), int(ah)


def _is_live_group_call(call) -> bool:
    if call is None:
        return False
    if isinstance(call, GroupCallDiscarded):
        return False
    return _call_input(call) is not None


def _is_trackable_user(uid: int, user: User | None = None) -> bool:
    # Exclude the assistant's own account: once ASSISTANT_JOIN_VC=1 makes it actually
    # join the call, it starts showing up in Telegram's own participant list like any
    # other user. Without this it would count itself toward VC stats/attendance/XP and
    # the call would never look "empty" even after every real person has left.
    if _assistant_self_id is not None and uid == _assistant_self_id:
        return False
    username = user.username if user else None
    return app_state.is_vc_participant(uid, username)


async def _fetch_participants(
    client: TelegramClient, call, chat_id: int
) -> tuple[Set[int], Dict[int, User], str | None, Dict[int, str]]:
    """Merge GetGroupCall + GetGroupParticipants for the fullest participant list.

    Also returns the group call's title (the VC "topic"/name, if the call was
    started or renamed with one) — only available via phone.GetGroupCallRequest,
    not from the lightweight InputGroupCall handed to us by GetFullChannelRequest.

    A participant doesn't always show up as a PeerUser: someone using "join as
    [the group]" (common for anonymous admins — the same identity Telegram uses for
    their anonymous text messages) appears as a PeerChat/PeerChannel peer instead.
    Previously these were silently dropped — the admin who started the call would
    join, never show up in ids at all, and never get counted anywhere. We can't
    attribute that to their specific personal account (Telegram genuinely doesn't
    tell us who's behind the anonymous identity), but we can at least track SOME
    presence for it instead of losing it entirely, using a synthetic negative id
    (never collides with a real positive Telegram user id) with an explicit label.
    Returned as a 4th dict (extra_labels) so the caller can seed st.hint_labels.
    """
    from telethon.tl.types import InputGroupCall, PeerChannel, PeerChat

    pair = _call_input(call)
    if not pair:
        return set(), {}, None, {}
    cid, ah = pair
    inp = InputGroupCall(id=cid, access_hash=ah)
    ids: Set[int] = set()
    users: Dict[int, User] = {}
    extra_labels: Dict[int, str] = {}
    title: str | None = None

    def _handle_peer(peer) -> None:
        if isinstance(peer, PeerUser):
            if _is_trackable_user(peer.user_id):
                ids.add(peer.user_id)
            return
        if isinstance(peer, (PeerChat, PeerChannel)):
            raw_id = getattr(peer, "channel_id", None) or getattr(peer, "chat_id", None)
            pseudo_id = -abs(raw_id) if raw_id else -abs(chat_id)
            ids.add(pseudo_id)
            extra_labels.setdefault(pseudo_id, "Anonymous Admin (joined as the group)")
            return
        if os.getenv("ASSISTANT_DEBUG"):
            logger.info("Assistant: unhandled VC participant peer type %r", type(peer).__name__)

    try:
        res = await client(functions.phone.GetGroupCallRequest(call=inp, limit=500))
        raw_title = getattr(res.call, "title", None)
        if raw_title and raw_title.strip():
            title = raw_title.strip()
        for p in res.participants:
            _handle_peer(p.peer)
        for u in res.users:
            if isinstance(u, User) and _is_trackable_user(u.id, u):
                users[u.id] = u
        if os.getenv("ASSISTANT_DEBUG"):
            logger.info(
                "Assistant GetGroupCall: %s participant user id(s), title=%r",
                len(ids),
                title,
            )
    except Exception:
        logger.exception("Assistant GetGroupCall failed; falling back to GetGroupParticipants")

    offset = ""
    while True:
        try:
            res = await client(
                functions.phone.GetGroupParticipantsRequest(
                    call=inp,
                    ids=[],
                    sources=[],
                    offset=offset,
                    limit=256,
                )
            )
        except Exception:
            logger.exception("Assistant GetGroupParticipants failed")
            break
        for p in res.participants:
            _handle_peer(p.peer)
        for u in res.users:
            if isinstance(u, User) and _is_trackable_user(u.id, u):
                users[u.id] = u
        offset = res.next_offset or ""
        if not offset:
            break
    if os.getenv("ASSISTANT_DEBUG"):
        logger.info("Assistant merged participant ids: %s", len(ids))
    return ids, users, title, extra_labels


async def _resolve_users(client: TelegramClient, st: _CallState, uids: Set[int]) -> None:
    missing = [
        uid for uid in uids if uid not in st.user_cache and _is_trackable_user(uid)
    ]
    if not missing:
        return
    try:
        results = await asyncio.gather(
            *[client.get_entity(uid) for uid in missing],
            return_exceptions=True,
        )
        failed = 0
        for ent in results:
            if isinstance(ent, BaseException):
                failed += 1
                continue
            if isinstance(ent, User) and _is_trackable_user(ent.id, ent):
                st.user_cache[ent.id] = ent
        if failed:
            logger.warning("Assistant could not resolve %s user name(s)", failed)
    except Exception:
        logger.exception("Assistant could not resolve %s user name(s)", len(missing))


def _participant_seconds(st: _CallState, uid: int, ended_at: datetime) -> int:
    sec = float(st.accumulated.get(uid, 0))
    ja = st.join_at.get(uid)
    if ja is not None:
        sec += (ended_at - ja).total_seconds()
    return max(0, int(round(sec)))


def _apply_bot_hints(st: _CallState, chat_id: int, now: datetime) -> None:
    hint = app_state.peek_bot_vc_hint(chat_id)
    if hint is None:
        return
    if hint.started_at and hint.started_at < st.started_at:
        st.started_at = hint.started_at
    for uid, (label, first_seen) in hint.participants.items():
        if not _is_trackable_user(uid):
            continue
        st.hint_labels[uid] = label
        # Confirmed joiner (e.g. call starter) — not the same as invite-only users.
        if uid not in st.join_at and uid not in st.accumulated:
            if first_seen.tzinfo is None:
                first_seen = first_seen.replace(tzinfo=timezone.utc)
            st.join_at[uid] = first_seen
    for uid, label in hint.invite_labels.items():
        if _is_trackable_user(uid):
            st.hint_labels.setdefault(uid, label)


async def _post_vc_join_welcome(client: TelegramClient, chat_id: int, st: "_CallState", uid: int) -> None:
    """Posts a one-line welcome into the group's TEXT chat (not the call's audio) the
    moment someone is seen joining the live voice/video chat. Fire-and-forget: any
    failure here must never break the poll loop, so callers wrap this in create_task
    and this function itself never raises."""
    try:
        label = _label_from_state(st, uid)
        safe = html.escape(label, quote=False)
        text = f"👋 Welcome {safe}, grab a seat, listen to the talk, and unmute whenever you want to share."
        await _post_vc_summary(client, chat_id, text)
    except Exception:
        logger.exception("VC join welcome failed chat_id=%s uid=%s", chat_id, uid)


def _label_from_state(st: _CallState, uid: int) -> str:
    if uid in st.hint_labels:
        return st.hint_labels[uid]
    return _user_label(st.user_cache.get(uid), uid)


def _merge_confirmed_participants(st: _CallState, chat_id: int, ended_at: datetime) -> None:
    """Backfill confirmed joiners the live poll may have missed (short calls, slow first fetch)."""
    hint = app_state.peek_bot_vc_hint(chat_id)
    if hint:
        for uid, (label, first_seen) in hint.participants.items():
            if not _is_trackable_user(uid) or uid in st.accumulated:
                continue
            st.hint_labels[uid] = label
            if first_seen.tzinfo is None:
                first_seen = first_seen.replace(tzinfo=timezone.utc)
            st.accumulated[uid] = max(0.0, (ended_at - first_seen).total_seconds())

    for uid in st.last_ids | st.seen_ids:
        if not _is_trackable_user(uid) or uid in st.accumulated:
            continue
        st.accumulated[uid] = max(0.0, (ended_at - st.started_at).total_seconds())


async def _get_chat_title(client: TelegramClient, chat_id: int) -> str:
    try:
        entity = await client.get_entity(chat_id)
        title = getattr(entity, "title", None)
        if title:
            return title
    except Exception:
        logger.exception("Assistant could not resolve chat title for chat_id=%s", chat_id)
    return "this group"


async def _finalize_call(
    client: TelegramClient,
    chat_id: int,
    st: _CallState,
    ended_at: datetime,
) -> None:
    # Leaving the call's audio is local to this process (not a DB write), so it runs
    # regardless of which path below wins the finalize claim.
    await _leave_vc_audio(chat_id, st)

    # Claim ownership of this VC-end event BEFORE any slow work (DB writes, resolving
    # users, HTTP calls). bot.py's fallback report (_assistant_vc_fallback_report) also
    # tries to claim before it writes; whichever path gets here first wins, and the
    # other silently skips. This prevents every real call from being recorded twice.
    if not app_state.try_claim_vc_finalize(chat_id):
        logger.info(
            "Assistant: VC end for chat_id=%s already claimed by another path; skipping duplicate record",
            chat_id,
        )
        return

    for uid, ja in list(st.join_at.items()):
        st.accumulated[uid] = st.accumulated.get(uid, 0) + (ended_at - ja).total_seconds()
    st.join_at.clear()

    _merge_confirmed_participants(st, chat_id, ended_at)

    all_uids = {uid for uid in st.accumulated if _is_trackable_user(uid)}
    await _resolve_users(client, st, all_uids)

    duration_sec = max(0, int((ended_at - st.started_at).total_seconds()))
    rows: list[tuple[int, str, int]] = []
    for uid in all_uids:
        sec_i = min(_participant_seconds(st, uid, ended_at), duration_sec)
        label = _label_from_state(st, uid)
        rows.append((uid, label, sec_i))

    rows.sort(key=lambda x: (-x[2], x[1].lower()))
    app_state.take_bot_vc_hint(chat_id)

    await asyncio.to_thread(
        dbmod.record_vc_session,
        chat_id,
        ended_at,
        duration_sec,
        st.started_at,
        rows,
    )

    await asyncio.to_thread(dbmod.ensure_chat, chat_id, None)

    lines = [
        "📞 <b>Voice/video chat ended</b>",
        "",
    ]
    if st.vc_title:
        lines.append(f"<b>Topic:</b> {html.escape(st.vc_title, quote=False)}")
        lines.append("")
    lines.append(f"<b>Call length (tracked):</b> {duration_sec // 60} min {duration_sec % 60} s")
    lines.append("")
    lines.append(f"<b>People in VC (assistant):</b> {len(rows)}")
    lines.append("")
    for rank, (_uid, label, sec) in enumerate(rows, start=1):
        mp, sp = sec // 60, sec % 60
        safe = html.escape(label, quote=False)
        lines.append(f"{rank}. {safe}: <b>{mp} min {sp} s</b>")
    lines.append("")
    lines.append(
        "<i>Tracked by the live participant list every few seconds. The time they spent in the call is listed here.</i>"
    )
    text = "\n".join(lines)
    await _post_vc_summary(client, chat_id, text)

    earned = await asyncio.to_thread(dbmod.record_present_attendance, chat_id, rows)
    attendance_text = dbmod.format_attendance_message(earned)
    await _post_vc_summary(client, chat_id, attendance_text)

    # --- Badges + AI recap ---------------------------------------------------
    # These previously only ran in bot.py's fallback path (_assistant_vc_fallback_report).
    # The healthy/normal path is THIS function (the Telethon assistant), so badges and
    # the AI-generated recap were never actually sent while the assistant was working.
    # Imported lazily (not at module top) to avoid a circular import: bot.py imports
    # assistant.py from inside main(), so by the time this function runs, bot.py's
    # module-level code has already finished executing.
    try:
        badges = await asyncio.to_thread(dbmod.check_and_award_session_badges, chat_id, rows)
        if badges:
            from bot import _format_badges_earned_html

            badge_text = _format_badges_earned_html(badges)
            await _post_vc_summary(client, chat_id, badge_text)
    except Exception:
        logger.exception("Assistant: badge check/post failed chat_id=%s", chat_id)

    try:
        from bot import generate_ai_vc_summary

        chat_title = await _get_chat_title(client, chat_id)
        ai_summary = await generate_ai_vc_summary(
            chat_title, duration_sec, rows, vc_topic=st.vc_title
        )
        if ai_summary:
            safe_summary = html.escape(ai_summary, quote=False)
            await _post_vc_summary(client, chat_id, f"🤖 <i>{safe_summary}</i>")
    except Exception:
        logger.exception("Assistant: AI recap failed chat_id=%s", chat_id)

    logger.info("Assistant finalized VC chat_id=%s participants=%s", chat_id, len(rows))


async def _poll_loop(client: TelegramClient, chat_ids: set[int]) -> None:
    interval = float(os.getenv("ASSISTANT_POLL_SECONDS", "2"))
    states: Dict[int, _CallState] = {}

    while True:
        now = datetime.now(timezone.utc)
        for chat_id in chat_ids:
            try:
                inp = await client.get_input_entity(chat_id)
                try:
                    channel_inp = utils.get_input_channel(inp)
                except TypeError:
                    logger.warning(
                        "Assistant: %s is not a channel/megagroup (upgrade group to supergroup or fix id); skipping",
                        chat_id,
                    )
                    continue
                full = await client(
                    functions.channels.GetFullChannelRequest(channel=channel_inp)
                )
                call = full.full_chat.call
                st = states.get(chat_id)

                if os.getenv("ASSISTANT_DEBUG"):
                    logger.info(
                        "Assistant poll chat=%s call=%s active=%s has_state=%s",
                        chat_id,
                        type(call).__name__ if call is not None else None,
                        _is_live_group_call(call),
                        st is not None,
                    )

                active = _is_live_group_call(call)
                if not active:
                    if st is not None:
                        await _finalize_call(client, chat_id, st, now)
                        del states[chat_id]
                    continue

                call_id = getattr(call, "id", None)
                if call_id is None:
                    continue
                if st is not None and st.call_id != call_id:
                    await _finalize_call(client, chat_id, st, now)
                    st = None
                is_new_call = st is None
                if st is None:
                    states[chat_id] = _CallState(call_id=int(call_id), started_at=now)
                    st = states[chat_id]

                # Join the moment the voice chat starts — don't wait on a participant
                # fetch first (that's a whole extra network round-trip, and was making
                # the join noticeably slower than the call itself in quick tests).
                if is_new_call and _pytgcalls_app is not None and not st.joined_call_audio:
                    asyncio.create_task(
                        _join_vc_audio(chat_id, st), name=f"vc-join-audio-{chat_id}"
                    )

                _apply_bot_hints(st, chat_id, now)

                current_ids, user_map, call_title, extra_labels = await _fetch_participants(
                    client, call, chat_id
                )
                st.user_cache.update(user_map)
                st.seen_ids.update(current_ids)
                st.hint_labels.update(extra_labels)
                if call_title:
                    st.vc_title = call_title

                joined = current_ids - st.last_ids
                left = st.last_ids - current_ids
                for uid in joined:
                    st.seen_ids.add(uid)
                    st.join_at[uid] = now
                    if uid > 0 and _is_trackable_user(uid):
                        asyncio.create_task(
                            _post_vc_join_welcome(client, chat_id, st, uid),
                            name=f"vc-join-welcome-{chat_id}-{uid}",
                        )
                for uid in left:
                    ja = st.join_at.pop(uid, None)
                    if ja is not None:
                        st.accumulated[uid] = st.accumulated.get(uid, 0) + (now - ja).total_seconds()
                st.last_ids = current_ids

            except Exception:
                logger.exception("Assistant poll error chat_id=%s", chat_id)

        sleep_for = interval
        if any(app_state.vc_wake_mono.get(cid, 0) > time.monotonic() - 5 for cid in chat_ids):
            sleep_for = min(0.5, interval)
        for cid in chat_ids:
            app_state.vc_wake_mono.pop(cid, None)
        await asyncio.sleep(sleep_for)


async def run_assistant() -> None:
    raw_ids = (os.environ.get("ASSISTANT_GROUP_IDS") or "").strip()
    session_s = (os.environ.get("TELEGRAM_SESSION_STRING") or "").strip()
    api_id = int((os.environ.get("TELEGRAM_API_ID") or "0").strip() or "0")
    api_hash = (os.environ.get("TELEGRAM_API_HASH") or "").strip()

    if not raw_ids or not session_s or not api_id or not api_hash:
        logger.info(
            "Assistant disabled (set TELEGRAM_SESSION_STRING, TELEGRAM_API_ID, "
            "TELEGRAM_API_HASH, ASSISTANT_GROUP_IDS to enable)."
        )
        raise AssistantConfigError("missing required env var(s)")

    chat_ids = _parse_group_ids(raw_ids)
    if not chat_ids:
        logger.warning("ASSISTANT_GROUP_IDS has no valid ids: %r", raw_ids)
        raise AssistantConfigError("ASSISTANT_GROUP_IDS has no valid ids")

    client = TelegramClient(StringSession(session_s), api_id, api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error(
                "Assistant: session not authorized. Run session_login.py locally and set TELEGRAM_SESSION_STRING."
            )
            raise AssistantConfigError("session not authorized")

        me = await client.get_me()
        if getattr(me, "bot", False):
            logger.error(
                "Assistant: TELEGRAM_SESSION_STRING is for a BOT. Telegram forbids "
                "GetGroupCall / GetGroupParticipants for bot accounts (BotMethodInvalidError). "
                "Run session_login.py on your PC, sign in with a normal USER phone number "
                "(the personal account you add to the group — not @BotFather, not BOT_TOKEN). "
                "Put the printed StringSession in TELEGRAM_SESSION_STRING. "
                "Until then, remove ASSISTANT_GROUP_IDS or fix the session so the bot can use invite-based VC again."
            )
            raise AssistantConfigError("session belongs to a bot account")

        app_state.assistant_chat_ids = set(chat_ids)
        app_state.assistant_running = True
        label = f"@{me.username}" if me.username else str(me.id)
        logger.info(
            "Assistant connected as user %s; tracking %s group(s): %s",
            label,
            len(chat_ids),
            sorted(chat_ids),
        )

        global _assistant_self_id
        _assistant_self_id = me.id

        global _pytgcalls_app
        _pytgcalls_app = None
        if _env_truthy("ASSISTANT_JOIN_VC"):
            _load_pytgcalls()
            if _PYTGCALLS_IMPORT_ERROR is not None:
                logger.warning(
                    "ASSISTANT_JOIN_VC=1 but py-tgcalls isn't installed/importable (%s); "
                    "the assistant will keep tracking VCs normally, just without joining "
                    "the call itself. Add py-tgcalls[telethon] to requirements.txt to enable it.",
                    _PYTGCALLS_IMPORT_ERROR,
                )
            else:
                try:
                    # Generate the silence file now, off the critical path, instead of
                    # lazily on the first join — shaves a little off how long the very
                    # first VC join of this process takes.
                    await asyncio.to_thread(_silence_file_path)
                    pytgcalls_app = PyTgCalls(client)
                    await pytgcalls_app.start()
                    pytgcalls_app.on_update(
                        pytgcalls_filters.stream_end(StreamEnded.Type.AUDIO)
                    )(_on_vc_audio_stream_end)
                    _pytgcalls_app = pytgcalls_app
                    logger.info(
                        "Assistant: ASSISTANT_JOIN_VC=1 — will join tracked calls as a "
                        "silent audio participant."
                    )
                except Exception:
                    logger.exception(
                        "Assistant: PyTgCalls failed to start (often a missing ffmpeg "
                        "binary on the host) — continuing without VC audio-join."
                    )
                    _pytgcalls_app = None

        await _poll_loop(client, chat_ids)
    finally:
        app_state.assistant_running = False
        app_state.assistant_chat_ids.clear()
        _pytgcalls_app = None
        _assistant_self_id = None
        if client.is_connected():
            await client.disconnect()


def start_assistant_background() -> None:
    """Runs the assistant in a background thread and keeps it alive.

    Transient failures — network hiccups, and especially AuthKeyDuplicatedError,
    which Telegram raises when the same session is briefly used from two IPs at
    once (typical for a few seconds right after a Render redeploy, while the old
    instance is still shutting down as the new one boots) — are retried with
    exponential backoff instead of permanently killing VC tracking until the next
    manual deploy.

    Permanent configuration problems (missing env vars, an unauthorized session,
    a bot-token session) raise AssistantConfigError and are logged once, then
    left alone — retrying those forever would just spam the logs for no benefit.
    """

    def _runner() -> None:
        base_delay = 5.0
        max_delay = 300.0
        delay = base_delay

        # One event loop for this thread's entire lifetime, reused across every retry
        # attempt below (rather than asyncio.run() making a fresh loop each time). This
        # matters specifically for _load_pytgcalls(): py-tgcalls pins its internal
        # sync-compat wrapper to whichever loop is running the first time it's imported,
        # and a new loop per retry would silently re-break VC audio-join on every
        # reconnect even though the very first run worked.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while True:
            started = time.monotonic()
            try:
                loop.run_until_complete(run_assistant())
                logger.warning(
                    "Assistant: run_assistant() returned without error (unexpected); not retrying."
                )
                return
            except AssistantConfigError as exc:
                logger.error("Assistant permanently disabled (config issue): %s", exc)
                return
            except AuthKeyDuplicatedError:
                logger.error(
                    "Assistant: AuthKeyDuplicatedError — TELEGRAM_SESSION_STRING was used from "
                    "two IPs at the same time. This is common for a few seconds right after a "
                    "Render redeploy (the old instance is still shutting down) and usually "
                    "clears up on its own once that instance fully stops. If it keeps recurring, "
                    "something else is also using this exact session string at the same time "
                    "(e.g. still running session_login.py locally, or a second Render service/"
                    "instance) — stop that, or generate a fresh session with session_login.py "
                    "and update TELEGRAM_SESSION_STRING. Retrying in %.0fs.",
                    delay,
                )
            except Exception:
                logger.exception("Assistant thread crashed; retrying in %.0fs", delay)

            # Ran for a while before failing -> treat the next failure as fresh
            # rather than climbing the backoff toward max_delay forever.
            if time.monotonic() - started > 60:
                delay = base_delay
            else:
                delay = min(delay * 2, max_delay)
            time.sleep(delay)

    import threading

    t = threading.Thread(target=_runner, name="telethon-assistant", daemon=True)
    t.start()
