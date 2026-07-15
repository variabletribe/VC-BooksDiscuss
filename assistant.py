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
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Set

import httpx
from telethon import TelegramClient, functions, utils
from telethon.sessions import StringSession
from telethon.tl.types import GroupCallDiscarded, PeerUser, User

import db as dbmod
import state as app_state

logger = logging.getLogger(__name__)


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
    username = user.username if user else None
    return app_state.is_vc_participant(uid, username)


async def _fetch_participants(client: TelegramClient, call) -> tuple[Set[int], Dict[int, User]]:
    """Merge GetGroupCall + GetGroupParticipants for the fullest participant list."""
    from telethon.tl.types import InputGroupCall

    pair = _call_input(call)
    if not pair:
        return set(), {}
    cid, ah = pair
    inp = InputGroupCall(id=cid, access_hash=ah)
    ids: Set[int] = set()
    users: Dict[int, User] = {}

    try:
        res = await client(functions.phone.GetGroupCallRequest(call=inp, limit=500))
        for p in res.participants:
            peer = p.peer
            if isinstance(peer, PeerUser) and _is_trackable_user(peer.user_id):
                ids.add(peer.user_id)
        for u in res.users:
            if isinstance(u, User) and _is_trackable_user(u.id, u):
                users[u.id] = u
        if os.getenv("ASSISTANT_DEBUG"):
            logger.info("Assistant GetGroupCall: %s participant user id(s)", len(ids))
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
            peer = p.peer
            if isinstance(peer, PeerUser) and _is_trackable_user(peer.user_id):
                ids.add(peer.user_id)
        for u in res.users:
            if isinstance(u, User) and _is_trackable_user(u.id, u):
                users[u.id] = u
        offset = res.next_offset or ""
        if not offset:
            break
    if os.getenv("ASSISTANT_DEBUG"):
        logger.info("Assistant merged participant ids: %s", len(ids))
    return ids, users


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


async def _finalize_call(
    client: TelegramClient,
    chat_id: int,
    st: _CallState,
    ended_at: datetime,
) -> None:
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
        f"<b>Call length (tracked):</b> {duration_sec // 60} min {duration_sec % 60} s",
        "",
        f"<b>People in VC (assistant):</b> {len(rows)}",
        "",
    ]
    for rank, (_uid, label, sec) in enumerate(rows, start=1):
        mp, sp = sec // 60, sec % 60
        safe = html.escape(label, quote=False)
        lines.append(f"{rank}. {safe}: <b>{mp} min {sp} s</b>")
    lines.append("")
    lines.append(
        "<i>Tracked by the live participant list every few seconds. The time they spent in the call is listed here.</i>"
    )
    text = "\n".join(lines)
    if await _post_vc_summary(client, chat_id, text):
        app_state.assistant_vc_report_mono[chat_id] = time.monotonic()

    earned = await asyncio.to_thread(dbmod.record_present_attendance, chat_id, rows)
    attendance_text = dbmod.format_attendance_message(earned)
    await _post_vc_summary(client, chat_id, attendance_text)

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
                if st is None:
                    states[chat_id] = _CallState(call_id=int(call_id), started_at=now)
                    st = states[chat_id]

                _apply_bot_hints(st, chat_id, now)

                current_ids, user_map = await _fetch_participants(client, call)
                st.user_cache.update(user_map)
                st.seen_ids.update(current_ids)

                joined = current_ids - st.last_ids
                left = st.last_ids - current_ids
                for uid in joined:
                    st.seen_ids.add(uid)
                    st.join_at[uid] = now
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
        return

    chat_ids = _parse_group_ids(raw_ids)
    if not chat_ids:
        logger.warning("ASSISTANT_GROUP_IDS has no valid ids: %r", raw_ids)
        return

    client = TelegramClient(StringSession(session_s), api_id, api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error(
                "Assistant: session not authorized. Run session_login.py locally and set TELEGRAM_SESSION_STRING."
            )
            return

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
            return

        app_state.assistant_chat_ids = set(chat_ids)
        app_state.assistant_running = True
        label = f"@{me.username}" if me.username else str(me.id)
        logger.info(
            "Assistant connected as user %s; tracking %s group(s): %s",
            label,
            len(chat_ids),
            sorted(chat_ids),
        )
        await _poll_loop(client, chat_ids)
    finally:
        app_state.assistant_running = False
        app_state.assistant_chat_ids.clear()
        if client.is_connected():
            await client.disconnect()


def start_assistant_background() -> None:
    def _runner() -> None:
        try:
            asyncio.run(run_assistant())
        except Exception:
            logger.exception("Assistant thread crashed")

    import threading

    t = threading.Thread(target=_runner, name="telethon-assistant", daemon=True)
    t.start()
