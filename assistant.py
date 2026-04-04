"""
User-account (Telethon) assistant: polls supergroup video/voice chats and tracks
who appears in the live participant list, accumulating approximate time in call.

Requires:
  TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION_STRING (from session_login.py)
  ASSISTANT_GROUP_IDS=-100111,-100222  (supergroup chat ids)
  BOT_TOKEN (to post the summary as the bot)

The assistant user must be a member of each tracked group (add that account like a normal user).
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Set

import httpx
from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.tl.types import GroupCall, InputChannel, PeerUser, User

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
    out: set[int] = set()
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            logger.warning("Skip invalid ASSISTANT_GROUP_IDS entry: %r", part)
    return out


@dataclass
class _CallState:
    call_id: int
    started_at: datetime
    last_ids: Set[int] = field(default_factory=set)
    join_at: Dict[int, datetime] = field(default_factory=dict)
    accumulated: Dict[int, float] = field(default_factory=dict)
    user_cache: Dict[int, User] = field(default_factory=dict)


async def _send_bot_message(chat_id: int, text: str) -> None:
    token = (os.environ.get("BOT_TOKEN") or "").strip()
    if not token:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            url,
            data={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"},
        )
        if r.status_code != 200:
            logger.error("sendMessage failed: %s %s", r.status_code, r.text[:500])


async def _fetch_participants(client: TelegramClient, call: GroupCall) -> tuple[Set[int], Dict[int, User]]:
    from telethon.tl.types import InputGroupCall

    inp = InputGroupCall(id=call.id, access_hash=call.access_hash)
    offset = ""
    ids: Set[int] = set()
    users: Dict[int, User] = {}
    while True:
        res = await client(
            functions.phone.GetGroupParticipantsRequest(
                call=inp,
                ids=[],
                sources=[],
                offset=offset,
                limit=256,
            )
        )
        for p in res.participants:
            peer = p.peer
            if isinstance(peer, PeerUser):
                ids.add(peer.user_id)
        for u in res.users:
            if isinstance(u, User):
                users[u.id] = u
        offset = res.next_offset or ""
        if not offset:
            break
    return ids, users


async def _finalize_call(
    client: TelegramClient,
    chat_id: int,
    st: _CallState,
    ended_at: datetime,
) -> None:
    for uid, ja in list(st.join_at.items()):
        st.accumulated[uid] = st.accumulated.get(uid, 0) + (ended_at - ja).total_seconds()
    st.join_at.clear()

    duration_sec = max(0, int((ended_at - st.started_at).total_seconds()))
    rows: list[tuple[int, str, int]] = []
    for uid, sec in st.accumulated.items():
        sec_i = int(round(sec))
        if sec_i < 1:
            continue
        label = _user_label(st.user_cache.get(uid), uid)
        rows.append((uid, label, min(sec_i, duration_sec)))

    rows.sort(key=lambda x: -x[2])

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
    for _uid, label, sec in rows:
        mp, sp = sec // 60, sec % 60
        safe = html.escape(label, quote=False)
        lines.append(f"• {safe}: <b>{mp} min {sp} s</b>")
    lines.append("")
    lines.append(
        "<i>Tracked by polling the live participant list every few seconds. "
        "Very short joins between polls may show as 0. Invite-only Bot API data is not used here.</i>"
    )
    text = "\n".join(lines)
    await _send_bot_message(chat_id, text)
    logger.info("Assistant finalized VC chat_id=%s participants=%s", chat_id, len(rows))


async def _poll_loop(client: TelegramClient, chat_ids: set[int]) -> None:
    interval = float(os.getenv("ASSISTANT_POLL_SECONDS", "5"))
    states: Dict[int, _CallState] = {}

    while True:
        now = datetime.now(timezone.utc)
        for chat_id in chat_ids:
            try:
                inp = await client.get_input_entity(chat_id)
                if not isinstance(inp, InputChannel):
                    logger.warning("Assistant: %s is not a supergroup (channel); skipping", chat_id)
                    continue
                full = await client(functions.channels.GetFullChannelRequest(channel=inp))
                call = full.full_chat.call
                st = states.get(chat_id)

                active = isinstance(call, GroupCall)
                if not active:
                    if st is not None:
                        await _finalize_call(client, chat_id, st, now)
                        del states[chat_id]
                    continue

                if st is None or st.call_id != call.id:
                    states[chat_id] = _CallState(call_id=call.id, started_at=now)
                    st = states[chat_id]

                current_ids, user_map = await _fetch_participants(client, call)
                st.user_cache.update(user_map)

                joined = current_ids - st.last_ids
                left = st.last_ids - current_ids
                for uid in joined:
                    st.join_at[uid] = now
                for uid in left:
                    ja = st.join_at.pop(uid, None)
                    if ja is not None:
                        st.accumulated[uid] = st.accumulated.get(uid, 0) + (now - ja).total_seconds()
                st.last_ids = current_ids

            except Exception:
                logger.exception("Assistant poll error chat_id=%s", chat_id)

        await asyncio.sleep(interval)


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
        logger.warning("ASSISTANT_GROUP_IDS has no valid ids")
        return

    app_state.assistant_chat_ids = chat_ids
    client = TelegramClient(StringSession(session_s), api_id, api_hash)

    await client.connect()
    if not await client.is_user_authorized():
        logger.error("Assistant: session not authorized. Run session_login.py locally and set TELEGRAM_SESSION_STRING.")
        return

    app_state.assistant_running = True
    logger.info("Assistant connected; tracking %s group(s): %s", len(chat_ids), sorted(chat_ids))

    try:
        await _poll_loop(client, chat_ids)
    finally:
        app_state.assistant_running = False
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
