"""Shared flags between bot.py and assistant.py."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime

assistant_running: bool = False
# Supergroup IDs (negative) where the assistant tracks VC; bot skips Bot-API VC summaries for these.
assistant_chat_ids: set[int] = set()
# Set when assistant successfully posts a VC end summary (time.monotonic()); used for fallback dedupe.
assistant_vc_report_mono: dict[int, float] = {}


@dataclass
class BotVCHint:
    """Participant names/times the Bot API saw (invite events, call starter)."""

    started_at: datetime | None = None
    participants: dict[int, tuple[str, datetime]] = field(default_factory=dict)


# chat_id -> hints from Bot API while assistant polls the live list
bot_vc_hints: dict[int, BotVCHint] = {}
# chat_id -> monotonic time; assistant shortens next sleep when bot signals VC activity
vc_wake_mono: dict[int, float] = {}


def signal_vc_wake(chat_id: int) -> None:
    vc_wake_mono[chat_id] = time.monotonic()


def note_bot_vc_started(chat_id: int, when: datetime, user_id: int | None, label: str | None) -> None:
    hint = bot_vc_hints.setdefault(chat_id, BotVCHint())
    if hint.started_at is None:
        hint.started_at = when
    if user_id is not None and label and user_id not in hint.participants:
        hint.participants[user_id] = (label, when)
    signal_vc_wake(chat_id)


def note_bot_vc_invited(chat_id: int, when: datetime, users: list[tuple[int, str]]) -> None:
    hint = bot_vc_hints.setdefault(chat_id, BotVCHint())
    if hint.started_at is None:
        hint.started_at = when
    for uid, label in users:
        if uid not in hint.participants:
            hint.participants[uid] = (label, when)
    signal_vc_wake(chat_id)


def take_bot_vc_hint(chat_id: int) -> BotVCHint | None:
    return bot_vc_hints.pop(chat_id, None)


def peek_bot_vc_hint(chat_id: int) -> BotVCHint | None:
    return bot_vc_hints.get(chat_id)
