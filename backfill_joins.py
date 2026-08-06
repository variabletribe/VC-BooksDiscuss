"""
One-off script: backfill the exact "joined group" date for every CURRENT member,
using Telegram's own participant records.

Every member entry Telegram stores (a ChannelParticipant) carries a `date` field —
the real timestamp they joined. The Bot API never exposes this, which is why bot.py's
live on_new_chat_members handler can only see joins happening from now on. But your
Telethon USER account (the same one already used for VC tracking) can call the raw
channels.GetParticipantsRequest method and read that field directly for everyone
currently in the group — no waiting required.

Requires the same env vars as assistant.py:
  TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION_STRING, ASSISTANT_GROUP_IDS
  MONGODB_URI (and optionally MONGODB_DB_NAME)

Run once (locally, or via Render's Shell tab for your service):

    python backfill_joins.py

Safe to re-run — it always overwrites with Telegram's current authoritative date, so
running it again just reconfirms the same values (unless someone left and rejoined
since the last run, in which case it correctly updates to their new join date).

Known limits (not bugs — these are gaps in what Telegram's API itself exposes):
- The group CREATOR has no join `date` field at all (they've always been there since
  creation) — their entry is skipped and /mystats will show "Unknown" for them.
- For members who were promoted to admin, the `date` on their admin record can reflect
  the promotion date rather than the original join date, if Telegram didn't carry the
  original one forward. Usually still correct, but flagged here for transparency.
- If the group's member list is restricted to admins only, this will raise
  ChatAdminRequiredError — the assistant account needs to be an admin, or that group
  setting needs to be relaxed, for the backfill to see everyone.
- Deleted accounts, other bots, and Telegram's anonymous-group-admin placeholder are
  skipped automatically.
"""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.tl.types import (
    ChannelParticipantCreator,
    ChannelParticipantsRecent,
    User,
)

import db as dbmod
import state as app_state


def _user_label(user: User) -> str:
    parts = [x for x in (user.first_name, user.last_name) if x]
    name = " ".join(parts).strip()
    if user.username:
        name = f"{name} (@{user.username})" if name else f"@{user.username}"
    return name or str(user.id)


async def backfill_chat(client: TelegramClient, chat_id: int) -> None:
    print(f"--- Backfilling joins for chat_id={chat_id} ---")
    try:
        entity = await client.get_entity(chat_id)
    except Exception as exc:
        print(f"  could not resolve entity for {chat_id}: {exc}")
        return

    offset = 0
    limit = 200
    total_written = 0
    total_skipped_no_date = 0

    while True:
        try:
            result = await client(
                functions.channels.GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsRecent(),
                    offset=offset,
                    limit=limit,
                    hash=0,
                )
            )
        except Exception as exc:
            print(f"  GetParticipants failed at offset={offset}: {exc}")
            print("  (If this says ChatAdminRequiredError, the assistant account needs to be")
            print("   an admin in this group, or the member-list-visibility setting relaxed.)")
            break

        if not result.participants:
            break

        users_by_id = {u.id: u for u in result.users if isinstance(u, User)}

        for p in result.participants:
            uid = getattr(p, "user_id", None)
            if uid is None:
                continue
            user = users_by_id.get(uid)
            if user is None or getattr(user, "bot", False):
                continue
            if not app_state.is_vc_participant(uid, getattr(user, "username", None)):
                continue

            if isinstance(p, ChannelParticipantCreator):
                total_skipped_no_date += 1
                continue

            join_date = getattr(p, "date", None)
            if join_date is None:
                total_skipped_no_date += 1
                continue

            label = _user_label(user)
            dbmod.set_group_join_backfill(chat_id, uid, label, join_date)
            total_written += 1

        offset += len(result.participants)
        if len(result.participants) < limit:
            break

    print(f"  wrote {total_written} join date(s); skipped {total_skipped_no_date} (no date available)")


async def main() -> None:
    load_dotenv()
    session_s = (os.environ.get("TELEGRAM_SESSION_STRING") or "").strip()
    api_id = int((os.environ.get("TELEGRAM_API_ID") or "0").strip() or "0")
    api_hash = (os.environ.get("TELEGRAM_API_HASH") or "").strip()
    raw_ids = (os.environ.get("ASSISTANT_GROUP_IDS") or "").strip()

    if not session_s or not api_id or not api_hash or not raw_ids:
        raise SystemExit(
            "Set TELEGRAM_SESSION_STRING, TELEGRAM_API_ID, TELEGRAM_API_HASH, and "
            "ASSISTANT_GROUP_IDS before running this (same values assistant.py uses)."
        )

    chat_ids = app_state.parse_assistant_group_ids(raw_ids)
    if not chat_ids:
        raise SystemExit(f"No valid chat ids found in ASSISTANT_GROUP_IDS={raw_ids!r}")

    dbmod.init_db()

    client = TelegramClient(StringSession(session_s), api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("Session not authorized — re-run session_login.py and update TELEGRAM_SESSION_STRING.")

    me = await client.get_me()
    if getattr(me, "bot", False):
        raise SystemExit(
            "TELEGRAM_SESSION_STRING is for a BOT account — this needs the same normal USER "
            "account session as assistant.py, not a bot token."
        )

    for chat_id in sorted(chat_ids):
        await backfill_chat(client, chat_id)

    await client.disconnect()
    print("Done. Run /mystats in the group to confirm join dates now show up.")


if __name__ == "__main__":
    asyncio.run(main())