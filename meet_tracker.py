"""Google Meet attendance: /gmeetrec starts a watch, meet_watch_job records it when the meeting ends."""
from __future__ import annotations

import asyncio
import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone

from google.apps import meet_v2
from google.oauth2.credentials import Credentials

import db as dbmod

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/meetings.space.readonly"]
WATCH_MAX_AGE = timedelta(hours=12)   # a watch with no ended meeting is dropped after this
MEET_CODE_RE = re.compile(r"^[a-z]{3}-[a-z]{4}-[a-z]{3}$")


def normalize_meet_code(raw: str) -> str | None:
    t = (raw or "").strip().lower().split("?")[0].rstrip("/")
    t = t.replace("https://meet.google.com/", "").replace("http://meet.google.com/", "")
    if re.fullmatch(r"[a-z]{10}", t):
        t = f"{t[:3]}-{t[3:7]}-{t[7:]}"
    return t if MEET_CODE_RE.match(t) else None


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip().strip('"').strip("'")


def _client():
    cid = _env("GOOGLE_CLIENT_ID")
    secret = _env("GOOGLE_CLIENT_SECRET")
    logger.info(
        "Meet creds check: id_ok=%s secret_prefix_ok=%s secret_len=%s",
        cid.endswith(".apps.googleusercontent.com"),
        secret.startswith("GOCSPX-"),
        len(secret),
    )
    creds = Credentials(
        None,
        refresh_token=_env("GOOGLE_REFRESH_TOKEN"),
        client_id=cid,
        client_secret=secret,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=SCOPES,
    )
    return meet_v2.ConferenceRecordsServiceClient(credentials=creds)


def _has(msg, field: str) -> bool:
    return msg._pb.HasField(field)


def _latest_record(code: str):
    c = _client()
    req = meet_v2.ListConferenceRecordsRequest(filter=f'space.meeting_code = "{code}"')
    recs = list(c.list_conference_records(request=req))
    recs.sort(key=lambda r: r.start_time, reverse=True)
    return c, (recs[0] if recs else None)


def find_live_conference(code: str):
    """'error' | None (nothing live) | conference record name (live)."""
    try:
        _c, rec = _latest_record(code)
    except Exception:
        logger.exception("Meet API lookup failed for %s", code)
        return "error"
    if rec is None or _has(rec, "end_time"):
        return None
    return rec.name


def _collect(c, rec, ended: bool):
    """[(google_key, display_name, seconds)] for everyone seen so far."""
    totals: dict[str, list] = {}
    for p in c.list_participants(parent=rec.name):
        if p.signed_in_user.user:
            key, name = p.signed_in_user.user, p.signed_in_user.display_name
        elif p.anonymous_user.display_name:
            key, name = f"anon:{p.anonymous_user.display_name}", p.anonymous_user.display_name
        elif p.phone_user.display_name:
            key, name = f"phone:{p.phone_user.display_name}", p.phone_user.display_name
        else:
            continue
        entry = totals.setdefault(key, [name or key, 0])   # created even if still in the call
        for s in c.list_participant_sessions(parent=p.name):
            if not _has(s, "start_time"):
                continue
            if _has(s, "end_time"):
                end = s.end_time
            elif ended:
                end = rec.end_time
            else:
                continue
            entry[1] += max(0, int((end - s.start_time).total_seconds()))
    return [(k, v[0], v[1]) for k, v in totals.items()]


def _check_watch(w: dict):
    c, rec = _latest_record(w["code"])
    if rec is None:
        return None
    ended = _has(rec, "end_time")
    return rec, _collect(c, rec, ended), ended


def _badges_text(badges) -> str:
    lines = ["🏅 <b>New badge(s) unlocked!</b>", ""]
    by_user: dict = {}
    for b in badges:
        by_user.setdefault(b.user_id, []).append(b)
    for blist in by_user.values():
        lines.append(f"{html.escape(blist[0].display_name, quote=False)}: "
                     + ", ".join(b.badge_label for b in blist))
    return "\n".join(lines)


async def meet_watch_job(context) -> None:
    now = datetime.now(timezone.utc)
    for w in await asyncio.to_thread(dbmod.list_meet_watches):
        chat_id = w["chat_id"]
        try:
            result = await asyncio.to_thread(_check_watch, w)
        except Exception:
            logger.exception("Meet watch check failed %s", w.get("_id"))
            continue

        if result is None:
            created = w.get("created_at")
            if created and now - created.replace(tzinfo=timezone.utc) > WATCH_MAX_AGE:
                await asyncio.to_thread(dbmod.remove_meet_watch, w["_id"])
            continue

        rec, raw, ended = result

        # Permanent codes are assigned the first time someone is seen, even while live.
        resolved = []   # (record_id, shown_name, seconds, meet_code)
        try:
            for gkey, label, secs in raw:
                doc = await asyncio.to_thread(dbmod.get_or_assign_meet_code, chat_id, gkey, label)
                tg = doc.get("linked_tg_user_id")
                uid = int(tg) if tg else -int(doc["code"])
                shown = label if tg else f"{label} [#{doc['code']}]"
                resolved.append((uid, shown, secs, int(doc["code"])))
        except Exception:
            logger.exception("Meet code assignment failed chat_id=%s", chat_id)
            continue

        if not ended:
            continue

        # Claim first (delete the watch), so a crash can't make us record the meeting twice.
        if not await asyncio.to_thread(dbmod.remove_meet_watch, w["_id"]):
            continue

        if not resolved:
            await context.bot.send_message(chat_id, f"🎥 Meet {w['code']} ended, but no participants were found.")
            continue

        parts = [(uid, shown, secs) for uid, shown, secs, _c in resolved]
        dur = int((rec.end_time - rec.start_time).total_seconds())
        await asyncio.to_thread(dbmod.record_vc_session, chat_id, rec.end_time, dur, rec.start_time, parts)
        earned = await asyncio.to_thread(dbmod.record_present_attendance, chat_id, parts)
        badges = await asyncio.to_thread(dbmod.check_and_award_session_badges, chat_id, parts)

        lines = [f"🎥 <b>Google Meet ended</b> ({html.escape(w['code'], quote=False)})",
                 f"<b>Length:</b> {dur // 60} min", ""]
        for uid, shown, secs, code in sorted(resolved, key=lambda x: -x[2]):
            tag = "" if uid > 0 else " (unlinked)"
            lines.append(f"• {html.escape(shown, quote=False)} — {secs // 60} min — Meet code <b>{code}</b>{tag}")
        await context.bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        await context.bot.send_message(chat_id, dbmod.format_attendance_message(earned), parse_mode="HTML")
        if badges:
            await context.bot.send_message(chat_id, _badges_text(badges), parse_mode="HTML")
