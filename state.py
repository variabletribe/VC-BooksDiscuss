"""Shared flags between bot.py and assistant.py."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from datetime import datetime

# Telegram placeholder for anonymous group-call users — not a real participant.
GROUP_ANONYMOUS_BOT_ID = 1087968824

assistant_running: bool = False
# Populated when assistant thread connects; may be empty if session failed.
assistant_chat_ids: set[int] = set()
# Set when assistant successfully posts a VC end summary (time.monotonic()); used for fallback dedupe.
assistant_vc_report_mono: dict[int, float] = {}


def parse_assistant_group_ids(raw: str | None = None) -> set[int]:
    """Parse ASSISTANT_GROUP_IDS env (comma-separated supergroup ids)."""
    text = (raw if raw is not None else os.environ.get("ASSISTANT_GROUP_IDS") or "").strip()
    out: set[int] = set()
    for part in text.replace(" ", "").split(","):
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            pass
    return out


def configured_assistant_groups() -> set[int]:
    """Groups that should use assistant + hint tracking (from env, even if thread is down)."""
    return parse_assistant_group_ids()


@dataclass
class BotVCHint:
    """Hints from Bot API service messages while the assistant polls the live list."""

    started_at: datetime | None = None
    # Confirmed joiners only (e.g. call starter) — not invitees who never joined.
    participants: dict[int, tuple[str, datetime]] = field(default_factory=dict)
    # Display-name hints from invite events; not proof of participation.
    invite_labels: dict[int, str] = field(default_factory=dict)


# chat_id -> hints from Bot API while assistant polls the live list
bot_vc_hints: dict[int, BotVCHint] = {}
# chat_id -> monotonic time; assistant shortens next sleep when bot signals VC activity
vc_wake_mono: dict[int, float] = {}


def is_vc_participant(user_id: int, username: str | None = None) -> bool:
    """False for Telegram system accounts that appear in VC lists but are not real people."""
    if user_id == GROUP_ANONYMOUS_BOT_ID:
        return False
    if username and username.lower() == "groupanonymousbot":
        return False
    return True


def signal_vc_wake(chat_id: int) -> None:
    vc_wake_mono[chat_id] = time.monotonic()


def note_bot_vc_started(chat_id: int, when: datetime, user_id: int | None, label: str | None) -> None:
    hint = bot_vc_hints.setdefault(chat_id, BotVCHint())
    if hint.started_at is None:
        hint.started_at = when
    if (
        user_id is not None
        and label
        and is_vc_participant(user_id)
        and user_id not in hint.participants
    ):
        hint.participants[user_id] = (label, when)
    signal_vc_wake(chat_id)


def note_bot_vc_invited(chat_id: int, when: datetime, users: list[tuple[int, str]]) -> None:
    hint = bot_vc_hints.setdefault(chat_id, BotVCHint())
    if hint.started_at is None:
        hint.started_at = when
    for uid, label in users:
        if is_vc_participant(uid):
            hint.invite_labels[uid] = label
    signal_vc_wake(chat_id)


def take_bot_vc_hint(chat_id: int) -> BotVCHint | None:
    return bot_vc_hints.pop(chat_id, None)


def peek_bot_vc_hint(chat_id: int) -> BotVCHint | None:
    return bot_vc_hints.get(chat_id)
