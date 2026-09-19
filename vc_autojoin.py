"""
Optional: makes the Telethon assistant account actually JOIN a group's
voice/video chat (not just observe the participant list, which is all
assistant.py's tracking ever needed) as soon as one starts, and leave once it
ends.

This is completely separate from VC time-tracking / summaries in assistant.py.
Every function here is best-effort: exceptions are caught and logged, never
raised into assistant.py's poll loop. If py-tgcalls isn't installed, ffmpeg
isn't found, or Telegram rejects the join, this module just logs a warning and
does nothing — the existing "who was in the call how long" tracking and
end-of-call summary keep working exactly as before either way.

Requires:
  - the optional `py-tgcalls` dependency (see requirements.txt)
  - an `ffmpeg` binary on PATH. render.yaml's buildCommand downloads a static
    one to ./bin/ffmpeg at deploy time (Render's native Python runtime has no
    apt-get/root access, so a prebuilt static binary is the practical option;
    see the comments in render.yaml). If ffmpeg is missing, joins will likely
    fail — this is logged, not silently swallowed.

Disable entirely, without touching any code, by setting env var:
  VC_AUTOJOIN_DISABLE=1

NOTE: this was written and reviewed but NOT tested against a live Telegram
group call (no such environment is available here). The pytgcalls API has
shifted across versions, so if joins fail after deploying, check the logs for
"VC auto-join:" lines first — the exact exception name/message will say
whether it's an import problem, missing ffmpeg, or an API mismatch to fix.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
import wave
from typing import Dict, Set

logger = logging.getLogger(__name__)

# Length of the generated silent clip, and how long before it runs out we
# re-issue it. Keeping actual audio flowing isn't required to stay joined to
# a Telegram group call (you don't get dropped just for going quiet), but
# refreshing periodically is cheap insurance against any client/version that
# behaves otherwise.
_SILENCE_SECONDS = 300
_REFRESH_MARGIN_SECONDS = 240
_SILENCE_RATE = 48000
_SILENCE_PATH = os.path.join(tempfile.gettempdir(), "vc_autojoin_silence.wav")

_caller = None  # PyTgCalls instance once init() succeeds; None = feature off
_joined_chat_ids: Set[int] = set()
_stream_started_mono: Dict[int, float] = {}

_IMPORT_ERROR: Exception | None = None
try:
    from pytgcalls import PyTgCalls
    from pytgcalls.types import MediaStream
    from pytgcalls.exceptions import (
        AlreadyJoinedError,
        NoActiveGroupCall,
        NotInCallError,
    )
except Exception as exc:  # pragma: no cover - depends on optional dependency
    PyTgCalls = None  # type: ignore
    MediaStream = None  # type: ignore

    class AlreadyJoinedError(Exception):  # type: ignore
        pass

    class NoActiveGroupCall(Exception):  # type: ignore
        pass

    class NotInCallError(Exception):  # type: ignore
        pass

    _IMPORT_ERROR = exc


def enabled() -> bool:
    if (os.environ.get("VC_AUTOJOIN_DISABLE") or "").strip() == "1":
        return False
    return PyTgCalls is not None


def _repo_bin_on_path() -> None:
    """Put ./bin (where render.yaml's buildCommand drops a static ffmpeg) on PATH."""
    bin_dir = os.path.join(os.getcwd(), "bin")
    path = os.environ.get("PATH", "")
    if os.path.isdir(bin_dir) and bin_dir not in path.split(os.pathsep):
        os.environ["PATH"] = bin_dir + os.pathsep + path


def _ensure_silence_file() -> str:
    """A local silent WAV file, generated once with the stdlib `wave` module
    (no ffmpeg needed just to create it) and reused for every join/refresh."""
    if os.path.exists(_SILENCE_PATH) and os.path.getsize(_SILENCE_PATH) > 1000:
        return _SILENCE_PATH
    n_frames = _SILENCE_SECONDS * _SILENCE_RATE
    with wave.open(_SILENCE_PATH, "wb") as f:
        f.setnchannels(1)
        f.setsampwidth(2)
        f.setframerate(_SILENCE_RATE)
        f.writeframes(b"\x00\x00" * n_frames)
    return _SILENCE_PATH


async def init(telethon_client) -> None:
    """Call once, after the Telethon client is connected + authorized (same
    client assistant.py already uses for VC tracking)."""
    global _caller
    if not enabled():
        if _IMPORT_ERROR is not None:
            logger.info(
                "VC auto-join disabled: py-tgcalls not installed/importable (%s). "
                "It's listed in requirements.txt; if this still fires, check the "
                "deploy's pip install logs.",
                _IMPORT_ERROR,
            )
        else:
            logger.info("VC auto-join disabled via VC_AUTOJOIN_DISABLE=1")
        return
    try:
        _repo_bin_on_path()
        if shutil.which("ffmpeg") is None:
            logger.warning(
                "VC auto-join: no ffmpeg found on PATH; joins will likely fail. "
                "render.yaml's buildCommand should have placed one at ./bin/ffmpeg "
                "during deploy — check the build logs."
            )
        _ensure_silence_file()
        _caller = PyTgCalls(telethon_client)
        await _caller.start()
        logger.info("VC auto-join: initialized")
    except Exception:
        logger.exception("VC auto-join: failed to initialize; feature disabled for this run")
        _caller = None


async def on_call_started(chat_id: int) -> None:
    """Join the group call as the assistant account. Safe to call repeatedly
    (e.g. if the poll loop calls it again before state settles)."""
    if _caller is None:
        return
    try:
        await _caller.play(
            chat_id,
            MediaStream(_ensure_silence_file(), video_flags=MediaStream.IGNORE),
        )
        _joined_chat_ids.add(chat_id)
        _stream_started_mono[chat_id] = time.monotonic()
        logger.info("VC auto-join: joined chat_id=%s", chat_id)
    except AlreadyJoinedError:
        _joined_chat_ids.add(chat_id)
        _stream_started_mono.setdefault(chat_id, time.monotonic())
    except NoActiveGroupCall:
        logger.debug("VC auto-join: no active call for chat_id=%s (already ended)", chat_id)
    except Exception:
        logger.exception("VC auto-join: failed to join chat_id=%s", chat_id)


async def refresh_if_needed(chat_id: int) -> None:
    """Called on each poll tick while a call is active; re-issues the silent
    stream shortly before the clip would run out. No-op unless we're actually
    joined to this chat_id."""
    if _caller is None or chat_id not in _joined_chat_ids:
        return
    started = _stream_started_mono.get(chat_id, 0.0)
    if time.monotonic() - started < _REFRESH_MARGIN_SECONDS:
        return
    try:
        await _caller.play(
            chat_id,
            MediaStream(_ensure_silence_file(), video_flags=MediaStream.IGNORE),
        )
        _stream_started_mono[chat_id] = time.monotonic()
    except Exception:
        logger.exception("VC auto-join: failed to refresh stream chat_id=%s", chat_id)


async def on_call_ended(chat_id: int) -> None:
    """Leave the group call. Safe to call even if we were never joined."""
    if _caller is None or chat_id not in _joined_chat_ids:
        _joined_chat_ids.discard(chat_id)
        _stream_started_mono.pop(chat_id, None)
        return
    try:
        leave = getattr(_caller, "leave_group_call", None) or getattr(_caller, "leave_call", None)
        if leave is not None:
            await leave(chat_id)
        logger.info("VC auto-join: left chat_id=%s", chat_id)
    except NotInCallError:
        pass
    except Exception:
        logger.exception("VC auto-join: failed to leave chat_id=%s", chat_id)
    finally:
        _joined_chat_ids.discard(chat_id)
        _stream_started_mono.pop(chat_id, None)
