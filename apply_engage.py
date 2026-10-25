#!/usr/bin/env python3
"""
apply_engage.py - celebrations, streak reminder + freezes, monthly awards and recaps,
and Read & Record of the week (from reactions).

Run AFTER apply_rnr.py, from the folder that contains bot.py and db.py:

    python apply_engage.py --dry-run
    python apply_engage.py

Same safety as the other scripts: nothing changes if an edit point is missing, the result
is syntax-checked, .bak copies are kept, and running it twice is harmless.
"""

from __future__ import annotations

import ast
import re
import shutil
import sys
from pathlib import Path

PREREQ_BOT = "# Read & Record: /rnr, /myrnr"
MARKER_BOT = "# Engagement: celebrations, streak reminder + freezes"
MARKER_DB = "# Engagement: celebrations, streak freezes, monthly awards and recaps"

DB_ENGAGE = r'''

# =============================================================================
# Engagement: celebrations, streak freezes, monthly awards and recaps, R&R reactions
# =============================================================================

LEVEL_TITLES = {
    1: "Newcomer", 2: "Reader", 3: "Page-Turner", 4: "Bookworm", 5: "Scholar",
    6: "Sage", 7: "Oracle", 8: "Legend", 9: "Luminary", 10: "Grandmaster",
}
STREAK_MILESTONES = (14, 21, 50, 100, 200, 365)    # 7 and 30 days already have badges
HOURS_MILESTONES = (10, 25, 50, 100, 250, 500, 1000)
VC_COUNT_MILESTONES = (10, 25, 50, 200, 500, 1000)  # 100 is the Veteran badge
FREEZE_EVERY_DAYS = 14


def freeze_cap() -> int:
    """Most streak freezes a member can hold (FREEZES_MAX on Render, default 2)."""
    try:
        return max(1, int(os.getenv("FREEZES_MAX", "2")))
    except ValueError:
        return 2


_celeb_ready = False


def _celebrations():
    global _celeb_ready
    coll = _coll("celebrations")
    if not _celeb_ready:
        coll.create_index([("posted", ASCENDING), ("created_at", ASCENDING)])
        coll.create_index("created_at", expireAfterSeconds=60 * 24 * 3600)
        _celeb_ready = True
    return coll


def queue_celebration(chat_id: int, key: str, text: str) -> bool:
    """Queue an announcement. The key makes each one happen once (False = already queued)."""
    try:
        _celebrations().insert_one(
            {
                "_id": f"{chat_id}:{key}",
                "chat_id": chat_id,
                "text": text,
                "posted": False,
                "created_at": datetime.now(timezone.utc),
            }
        )
        return True
    except DuplicateKeyError:
        return False


def celebrations_due(older_than_seconds: int = 20, limit: int = 60) -> list[dict]:
    """Waiting announcements, oldest first. The small delay lets the call summary post first."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)
    return list(
        _celebrations().find({"posted": False, "created_at": {"$lte": cutoff}}).sort("created_at", ASCENDING).limit(limit)
    )


def celebrations_mark(ids: list, status: str) -> None:
    _celebrations().update_many({"_id": {"$in": ids}}, {"$set": {"posted": True, "status": status}})


def _celebrate_after_attendance(chat_id, uid, name, sec, xp_gain, doc, crosses_threshold) -> None:
    """Called from record_present_attendance for every participant. Queues announcements for
    level-ups and milestones, and grants a streak freeze every FREEZE_EVERY_DAYS streak days.
    Runs after the call's session is stored, so totals already include it."""
    if uid <= 0:
        return
    safe = html.escape(name, quote=False)
    xp_after = int(doc.get("xp", 0))
    lvl_after = _level_for_xp(xp_after)[0]
    lvl_before = _level_for_xp(max(0, xp_after - xp_gain))[0]
    if lvl_after > lvl_before:
        title = LEVEL_TITLES.get(lvl_after)
        suffix = f" ({title})" if title else ""
        queue_celebration(chat_id, f"level:{uid}:{lvl_after}", f"🎉 {safe} reached <b>Level {lvl_after}</b>{suffix}!")

    if sec > 0:
        stats = get_user_vc_stats(chat_id, uid)
        for h in HOURS_MILESTONES:
            if stats.total_seconds - sec < h * 3600 <= stats.total_seconds:
                queue_celebration(chat_id, f"hours:{uid}:{h}", f"⏱️ {safe} passed <b>{h} hours</b> in voice chats!")
        if stats.vc_count in VC_COUNT_MILESTONES:
            queue_celebration(
                chat_id, f"vcs:{uid}:{stats.vc_count}", f"📞 {safe} joined their <b>{stats.vc_count}th voice chat</b>!"
            )

    if crosses_threshold:
        streak = int(doc.get("current_streak", 0))
        day = str(doc.get("last_present_date") or "")
        if streak in STREAK_MILESTONES:
            queue_celebration(chat_id, f"streak:{uid}:{streak}:{day}", f"🔥 {safe} is on a <b>{streak}-day streak</b>!")
        if streak > 0 and streak % FREEZE_EVERY_DAYS == 0 and int(doc.get("freezes", 0)) < freeze_cap():
            text = f"🧊 {safe} earned a <b>streak freeze</b>: one missed day won't break the streak."
            if queue_celebration(chat_id, f"freeze:{uid}:{streak}:{day}", text):
                _coll("user_attendance").update_one({"_id": f"{chat_id}:{uid}"}, {"$inc": {"freezes": 1}})


def get_freezes(chat_id: int, user_id: int) -> int:
    doc = _coll("user_attendance").find_one({"_id": f"{chat_id}:{user_id}"}) or {}
    return int(doc.get("freezes", 0))


def grant_freeze(chat_id: int, user_id: int, display_name: str, n: int = 1, cap: int = 5) -> int:
    """Admin grant. Returns the member's new freeze count (never above `cap`)."""
    coll = _coll("user_attendance")
    doc_id = f"{chat_id}:{user_id}"
    coll.update_one(
        {"_id": doc_id},
        {
            "$setOnInsert": {
                "chat_id": chat_id, "user_id": user_id, "display_name": display_name[:512],
                "xp": 0, "present_days": 0, "current_streak": 0, "longest_streak": 0, "badges": {},
            }
        },
        upsert=True,
    )
    have = int((coll.find_one({"_id": doc_id}) or {}).get("freezes", 0))
    new = min(cap, have + n)
    coll.update_one({"_id": doc_id}, {"$set": {"freezes": new}})
    return new


def reset_expired_streaks_all() -> int:
    """Zero out current_streak for ANY user in ANY chat whose last_present_date is more than
    1 day in the past (they missed a day). Runs once daily just after UTC midnight.

    A member who holds a streak freeze and missed exactly ONE day keeps the streak: the
    freeze is used up and the missed day is bridged. Missing two or more days still resets it.
    Returns the number of streaks that were reset."""
    coll = _coll("user_attendance")
    today = datetime.now(timezone.utc).date()
    cutoff = today.toordinal() - 1
    yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    reset_count = 0
    for doc in coll.find({"current_streak": {"$gt": 0}}):
        last_day_str = doc.get("last_present_date")
        if not last_day_str:
            continue
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d").date()
        if last_day.toordinal() >= cutoff:
            continue
        if last_day.toordinal() == cutoff - 1 and int(doc.get("freezes", 0)) > 0:
            used = coll.update_one(
                {"_id": doc["_id"], "freezes": {"$gt": 0}},
                {"$inc": {"freezes": -1}, "$set": {"last_present_date": yesterday}},
            )
            if used.modified_count:
                name = html.escape(str(doc.get("display_name", "A member")), quote=False)
                queue_celebration(
                    int(doc["chat_id"]), f"frozen:{doc['user_id']}:{yesterday}",
                    f"🧊 A freeze saved {name}'s <b>{int(doc.get('current_streak', 0))}-day streak</b>!",
                )
                continue
        coll.update_one({"_id": doc["_id"]}, {"$set": {"current_streak": 0}})
        reset_count += 1
    return reset_count


def streaks_at_risk(chat_id: int, min_streak: int = 3, limit: int = 15) -> list[dict]:
    """Members with a streak worth saving who haven't attended yet today (UTC day)."""
    yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    cursor = (
        _coll("user_attendance")
        .find({"chat_id": chat_id, "current_streak": {"$gte": min_streak}, "last_present_date": yesterday})
        .sort("current_streak", DESCENDING)
        .limit(limit)
    )
    return [
        {"user_id": int(d["user_id"]), "name": str(d.get("display_name", "")),
         "streak": int(d.get("current_streak", 0)), "freezes": int(d.get("freezes", 0))}
        for d in cursor
    ]


def engage_posted(key: str) -> str | None:
    doc = _coll("meta").find_one({"_id": key})
    return doc.get("day") if doc else None


def engage_set_posted(key: str, value: str) -> None:
    _coll("meta").update_one({"_id": key}, {"$set": {"day": value}}, upsert=True)


def _month_filters(chat_id: int, year: int, month: int):
    start, end = month_bounds_utc(year, month)
    prefix = f"{year:04d}-{month:02d}-"
    rnr = {"chat_id": chat_id, "deleted": {"$ne": True}, "day": {"$gte": prefix + "01", "$lte": prefix + "31"}}
    topics = {
        "chat_id": chat_id, "added_at": {"$gte": start, "$lt": end},
        "added_by_id": {"$gt": 0}, "source": {"$nin": ["vc", "vc_unrecorded"]},
    }
    return start, end, rnr, topics


def month_awards(chat_id: int, year: int, month: int) -> list[dict]:
    """Winners for the month: [{"key", "user_id", "name", "value"}]. Categories without data are skipped."""
    start, end, rnr_filter, topic_filter = _month_filters(chat_id, year, month)
    out: list[dict] = []
    rows = [r for r in fetch_vc_stats(chat_id, start, end) if r.user_id > 0]
    if rows:
        top = rows[0]
        out.append({"key": "voice", "user_id": top.user_id, "name": top.display_name, "value": top.total_seconds})
        reg = max(rows, key=lambda r: (r.vc_count, r.total_seconds))
        out.append({"key": "regular", "user_id": reg.user_id, "name": reg.display_name, "value": reg.vc_count})

    def top_counter(cursor, id_field, name_field):
        counts: dict[int, list] = {}
        for d in cursor:
            c = counts.setdefault(int(d[id_field]), [0, ""])
            c[0] += 1
            c[1] = d.get(name_field) or c[1]
        if not counts:
            return None
        uid, (n, name) = max(counts.items(), key=lambda kv: kv[1][0])
        return uid, name, n

    best = top_counter(_coll("rnr_records").find(rnr_filter, {"user_id": 1, "display_name": 1}), "user_id", "display_name")
    if best:
        out.append({"key": "rnr", "user_id": best[0], "name": best[1], "value": best[2]})
    best = top_counter(_coll("topics").find(topic_filter, {"added_by_id": 1, "added_by_name": 1}), "added_by_id", "added_by_name")
    if best:
        out.append({"key": "ideas", "user_id": best[0], "name": best[1], "value": best[2]})
    if not out:
        return []  # a month with no activity gets no awards (the streak award alone would be odd)
    keeper = list(
        _coll("user_attendance").find({"chat_id": chat_id, "current_streak": {"$gte": 3}}).sort("current_streak", DESCENDING).limit(1)
    )
    if keeper:
        k = keeper[0]
        out.append({"key": "streak", "user_id": int(k["user_id"]), "name": str(k.get("display_name", "")), "value": int(k["current_streak"])})
    return out


def member_month_recap(chat_id: int, user_id: int, year: int, month: int) -> dict:
    start, end, rnr_filter, topic_filter = _month_filters(chat_id, year, month)
    rows = [r for r in fetch_vc_stats(chat_id, start, end) if r.user_id > 0]
    idx = next((i for i, r in enumerate(rows) if r.user_id == user_id), None)
    att = _coll("user_attendance").find_one({"_id": f"{chat_id}:{user_id}"}) or {}
    xp = int(att.get("xp", 0))
    return {
        "vcs": rows[idx].vc_count if idx is not None else 0,
        "seconds": rows[idx].total_seconds if idx is not None else 0,
        "rank": idx + 1 if idx is not None else None,
        "participants": len(rows),
        "recordings": _coll("rnr_records").count_documents({**rnr_filter, "user_id": user_id}),
        "topics": _coll("topics").count_documents({**topic_filter, "added_by_id": user_id}),
        "streak": int(att.get("current_streak", 0)),
        "best_streak": int(att.get("longest_streak", 0)),
        "freezes": int(att.get("freezes", 0)),
        "xp": xp,
        "level": _level_for_xp(xp)[0],
    }


def month_active_user_ids(chat_id: int, year: int, month: int) -> list[int]:
    start, end, rnr_filter, topic_filter = _month_filters(chat_id, year, month)
    ids = {r.user_id for r in fetch_vc_stats(chat_id, start, end) if r.user_id > 0}
    ids |= {int(d["user_id"]) for d in _coll("rnr_records").find(rnr_filter, {"user_id": 1})}
    ids |= {int(d["added_by_id"]) for d in _coll("topics").find(topic_filter, {"added_by_id": 1})}
    return sorted(i for i in ids if i > 0)


def recap_opted_out(user_id: int) -> bool:
    doc = _coll("recap_prefs").find_one({"_id": user_id})
    return bool(doc and doc.get("optout"))


def set_recap_optout(user_id: int, optout: bool) -> None:
    _coll("recap_prefs").update_one({"_id": user_id}, {"$set": {"optout": optout}}, upsert=True)


def rnr_set_reaction(chat_id: int, msg_id: int, reactor_id: int, has_reaction: bool) -> None:
    """Remember who reacted to a recorded audio (own reactions don't count)."""
    coll = _coll("rnr_records")
    doc_id = f"{chat_id}:{msg_id}"
    if has_reaction:
        coll.update_one(
            {"_id": doc_id, "deleted": {"$ne": True}, "user_id": {"$ne": reactor_id}},
            {"$addToSet": {"reactor_ids": reactor_id}},
        )
    else:
        coll.update_one({"_id": doc_id}, {"$pull": {"reactor_ids": reactor_id}})


def rnr_week_winner(chat_id: int, days: list[str], min_reactions: int = 2) -> dict | None:
    """The recorded audio (sent on one of `days`) with the most members reacting."""
    best, best_n = None, 0
    cursor = _coll("rnr_records").find({"chat_id": chat_id, "deleted": {"$ne": True}, "day": {"$in": days}})
    for d in sorted(cursor, key=lambda x: x.get("audio_at") or datetime.min):
        n = len(d.get("reactor_ids") or [])
        if n > best_n:
            best, best_n = d, n
    if best is None or best_n < min_reactions:
        return None
    return {"doc": best, "count": best_n}
'''

BOT_ENGAGE = r'''# =============================================================================
# Engagement: celebrations, streak reminder + freezes, monthly awards and recaps,
# Read & Record of the week (from member reactions)
#
#   FREEZES_MAX              most streak freezes a member holds (default 2)
#   STREAK_REMINDER_HOUR     group-time hour of the "streaks at risk" post (default 20)
#   RNR_WINNER_MIN_REACTIONS reactions needed to win Read & Record of the week (default 2)
# Reactions only reach the bot if it is an ADMIN of the group.
# =============================================================================

from telegram import ReplyParameters
from telegram.ext import MessageReactionHandler


def _engage_offset() -> timedelta:
    try:
        return timedelta(minutes=int(os.getenv("RNR_TZ_OFFSET_MINUTES", "330")))
    except ValueError:
        return timedelta(minutes=330)


def _plural(n: int, one: str, many: str | None = None) -> str:
    return f"{n} {one if n == 1 else (many or one + 's')}"


async def celebration_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Every 30s: post queued level-up / milestone / freeze announcements, grouped per chat."""
    try:
        docs = await asyncio.to_thread(dbmod.celebrations_due)
    except Exception:
        logger.exception("Celebration worker could not read the queue")
        return
    by_chat: dict[int, list[dict]] = {}
    for d in docs:
        by_chat.setdefault(int(d["chat_id"]), []).append(d)
    for chat_id, items in by_chat.items():
        for i in range(0, len(items), 8):
            chunk = items[i : i + 8]
            text = chunk[0]["text"] if len(chunk) == 1 else "🎉 <b>Celebrations</b>\n\n" + "\n".join(d["text"] for d in chunk)
            status = "sent"
            try:
                await context.bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception:
                logger.exception("Celebration post failed chat_id=%s", chat_id)
                status = "failed"
            await asyncio.to_thread(dbmod.celebrations_mark, [d["_id"] for d in chunk], status)


def _streak_reminder_hour() -> int:
    try:
        return min(21, max(0, int(os.getenv("STREAK_REMINDER_HOUR", "20"))))
    except ValueError:
        return 20


async def streak_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Evening post naming members whose streak ends if they skip today. Checked every 30
    minutes; stored per chat per day so a restart never double-posts."""
    try:
        local = datetime.now(timezone.utc) + _engage_offset()
        hour = _streak_reminder_hour()
        if not (hour <= local.hour < hour + 3):
            return
        today = local.strftime("%Y-%m-%d")
        chat_ids = await asyncio.to_thread(dbmod.list_chats_with_monthly_reports)
    except Exception:
        logger.exception("Streak reminder setup failed")
        return
    deadline = (datetime(2000, 1, 1) + _engage_offset()).strftime("%H:%M")  # when the attendance day (UTC) ends
    for chat_id in chat_ids:
        try:
            key = f"streak_reminder:{chat_id}"
            if await asyncio.to_thread(dbmod.engage_posted, key) == today:
                continue
            await asyncio.to_thread(dbmod.engage_set_posted, key, today)
            rows = await asyncio.to_thread(dbmod.streaks_at_risk, chat_id)
            if not rows:
                continue
            lines = ["🔥 <b>Streaks at risk today</b>", f"<i>Join a call before {deadline} to keep your streak alive.</i>", ""]
            for r in rows:
                freeze = " 🧊" if r["freezes"] else ""
                lines.append(f"• {html.escape(r['name'], quote=False)}: {_plural(r['streak'], 'day')}{freeze}")
            if any(r["freezes"] for r in rows):
                lines += ["", "<i>🧊 = has a freeze that covers one missed day.</i>"]
            await context.bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        except Exception:
            logger.exception("Streak reminder failed chat_id=%s", chat_id)


_AWARD_FORMATS = {
    "voice": ("🎧", "Voice Champion", lambda v: _format_duration_hours(int(v))),
    "regular": ("🤝", "Most Regular", lambda v: _plural(int(v), "voice chat")),
    "rnr": ("🎙️", "Storyteller", lambda v: _plural(int(v), "Read &amp; Record")),
    "ideas": ("💡", "Idea Machine", lambda v: _plural(int(v), "topic")),
    "streak": ("🔥", "Streak Keeper", lambda v: f"{int(v)}-day streak"),
}


def _format_awards(year: int, month: int, awards: list[dict]) -> str:
    lines = [f"🏆 <b>Awards: {_month_name(month)} {year}</b>", ""]
    for a in awards:
        icon, title, fmt = _AWARD_FORMATS[a["key"]]
        lines.append(f"{icon} <b>{title}</b>: {html.escape(a['name'] or str(a['user_id']), quote=False)} ({fmt(a['value'])})")
    lines += ["", "<i>Thank you all. See you next month!</i>"]
    return "\n".join(lines)


def _format_recap(year: int, month: int, r: dict, footer: bool = True) -> str:
    lines = [f"📖 <b>Your {_month_name(month)} in books</b>", ""]
    if r["vcs"]:
        rank = f" · #{r['rank']} of {r['participants']}" if r["rank"] else ""
        lines.append(f"🎧 {_plural(r['vcs'], 'voice chat')} · {_format_duration_hours(r['seconds'])}{rank}")
    else:
        lines.append("🎧 No voice chats this month")
    if r["recordings"]:
        lines.append(f"🎙️ {_plural(r['recordings'], 'Read &amp; Record')}")
    if r["topics"]:
        lines.append(f"💡 {_plural(r['topics'], 'topic')} suggested")
    lines += ["", f"🔥 Streak now: <b>{r['streak']}</b> day(s) · best {r['best_streak']}", f"🎖️ Level {r['level']} · {r['xp']} XP"]
    if r["freezes"]:
        lines.append(f"🧊 Streak freezes: {r['freezes']}")
    if footer:
        lines += ["", "<i>Don't want these? Send /recap off to me.</i>"]
    return "\n".join(lines)


async def monthly_awards_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """On the 1st (an hour after the monthly report): post last month's awards in the group,
    then DM each active member their own recap (members who haven't started the bot, or who
    sent /recap off, are skipped). Marked done before sending, so it can never post twice."""
    now = datetime.now(timezone.utc)
    try:
        hour = min(23, int(os.getenv("MONTHLY_REPORT_HOUR_UTC", "9")) + 1)
    except ValueError:
        hour = 10
    if now.day != 1 or now.hour < hour:
        return
    year, month = dbmod.previous_calendar_month(now.year, now.month)
    try:
        chat_ids = await asyncio.to_thread(dbmod.list_chats_with_monthly_reports)
    except Exception:
        logger.exception("Awards job could not list chats")
        return
    for chat_id in chat_ids:
        try:
            key = f"awards:{chat_id}:{year}-{month:02d}"
            if await asyncio.to_thread(dbmod.engage_posted, key):
                continue
            awards = await asyncio.to_thread(dbmod.month_awards, chat_id, year, month)
            await asyncio.to_thread(dbmod.engage_set_posted, key, "started")
            if awards:
                await context.bot.send_message(chat_id, _format_awards(year, month, awards), parse_mode="HTML")
            for uid in await asyncio.to_thread(dbmod.month_active_user_ids, chat_id, year, month):
                try:
                    if await asyncio.to_thread(dbmod.recap_opted_out, uid):
                        continue
                    recap = await asyncio.to_thread(dbmod.member_month_recap, chat_id, uid, year, month)
                    await context.bot.send_message(uid, _format_recap(year, month, recap), parse_mode="HTML")
                except Exception:
                    logger.debug("Recap DM to %s skipped", uid)  # usually: they never started the bot
                await asyncio.sleep(0.2)
        except Exception:
            logger.exception("Monthly awards failed chat_id=%s", chat_id)


async def cmd_myrecap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Private chat: /myrecap shows last month in books, /myrecap this = the month so far."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        await _reply_autodelete(update, context, "Send /myrecap to me in a private chat.")
        return
    chat_id = _broadcast_target_chat_id()
    if chat_id is None:
        await update.message.reply_text("No main group is configured yet.")
        return
    now = datetime.now(timezone.utc)
    if context.args and context.args[0].lower() in ("this", "current"):
        year, month = now.year, now.month
    else:
        year, month = dbmod.previous_calendar_month(now.year, now.month)
    recap = await asyncio.to_thread(dbmod.member_month_recap, chat_id, update.effective_user.id, year, month)
    await update.message.reply_text(_format_recap(year, month, recap, footer=False), parse_mode="HTML")


async def cmd_recap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Private chat: /recap on|off - the monthly recap message I send you."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type != "private":
        await _reply_autodelete(update, context, "Send /recap to me in a private chat.")
        return
    uid = update.effective_user.id
    if not context.args or context.args[0].lower() not in ("on", "off"):
        off = await asyncio.to_thread(dbmod.recap_opted_out, uid)
        await update.message.reply_text(
            f"Your monthly recap message is {'off' if off else 'on'}.\nUse /recap on or /recap off. "
            "/myrecap shows it any time."
        )
        return
    off = context.args[0].lower() == "off"
    await asyncio.to_thread(dbmod.set_recap_optout, uid, off)
    await update.message.reply_text("Okay, I won't send you a monthly recap." if off else "Okay, you'll get a recap each month.")


async def cmd_givefreeze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Bot admin, in the group: /givefreeze (reply to a member, or give their id/@username)."""
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in the group.")
        return
    if not _is_admin_user(update.effective_user.id):
        await _reply_autodelete(update, context, "Bot admins only.")
        return
    args = context.args or []
    reply = update.message.reply_to_message
    if reply is not None and reply.from_user:
        target_id, label = reply.from_user.id, _user_label(reply.from_user)
    elif args:
        resolved = await _resolve_user_ref(context, chat.id, args[0])
        if resolved is None:
            await _reply_autodelete(update, context, f"Couldn't resolve {html.escape(args[0], quote=False)}.", parse_mode="HTML")
            return
        target_id, label = resolved
    else:
        await _reply_autodelete(update, context, "Reply to a member, or give their user id / @username.")
        return
    total = await asyncio.to_thread(dbmod.grant_freeze, chat.id, target_id, label, 1, 5)
    await _reply_autodelete(
        update, context, f"🧊 {html.escape(label, quote=False)} now has {_plural(total, 'streak freeze')}.", parse_mode="HTML"
    )


async def cmd_streak(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await _reply_autodelete(update, context, "Use this command in a group.")
        return
    user = update.effective_user
    label = _user_label(user)
    info = await asyncio.to_thread(dbmod.get_streak_info, chat.id, user.id, label)
    freezes = await asyncio.to_thread(dbmod.get_freezes, chat.id, user.id)
    safe = html.escape(info.display_name or label, quote=False)
    text = (
        f"🔥 <b>{safe}</b>\n"
        f"Current streak: <b>{info.current_streak}</b> day(s)\n"
        f"Longest streak: <b>{info.longest_streak}</b> day(s)\n"
        f"🧊 Streak freezes: <b>{freezes}</b> <i>(you earn one every {dbmod.FREEZE_EVERY_DAYS} streak days, "
        f"max {dbmod.freeze_cap()}; it covers one missed day)</i>"
    )
    await _reply_autodelete(update, context, text, parse_mode="HTML")


async def on_message_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """A member reacted to (or un-reacted from) a message. Counted only for recorded audios."""
    r = update.message_reaction
    if r is None or r.user is None or r.user.is_bot or r.chat.type not in ("group", "supergroup"):
        return
    try:
        await asyncio.to_thread(dbmod.rnr_set_reaction, r.chat.id, r.message_id, r.user.id, bool(r.new_reaction))
    except Exception:
        logger.exception("Reaction tracking failed chat_id=%s", r.chat.id)


async def rnr_week_winner_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Monday, an hour after the morning Read & Record posts: announce last week's most
    appreciated recording (most members reacting), as a reply to the original audio."""
    local = datetime.now(timezone.utc) + _engage_offset()
    start_hour = min(23, _rnr_morning_hour() + 1)
    if local.weekday() != 0 or not (start_hour <= local.hour < start_hour + 6):
        return
    today = local.strftime("%Y-%m-%d")
    days = [(local - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, 0, -1)]
    try:
        minimum = max(1, int(os.getenv("RNR_WINNER_MIN_REACTIONS", "2")))
        chat_ids = await asyncio.to_thread(dbmod.rnr_chat_ids)
    except Exception:
        logger.exception("R&R winner setup failed")
        return
    for chat_id in chat_ids:
        try:
            key = f"rnr_winner:{chat_id}"
            if await asyncio.to_thread(dbmod.engage_posted, key) == today:
                continue
            winner = await asyncio.to_thread(dbmod.rnr_week_winner, chat_id, days, minimum)
            await asyncio.to_thread(dbmod.engage_set_posted, key, today)
            if not winner:
                continue
            d = winner["doc"]
            text = (
                f"🏆 <b>Read &amp; Record of the week</b>\n"
                f"RNR #{d['serial']} by <b>{html.escape(str(d.get('display_name', '')), quote=False)}</b>: "
                f"{_plural(winner['count'], 'member')} reacted ❤️\n<i>Listen again 👆</i>"
            )
            await context.bot.send_message(
                chat_id, text, parse_mode="HTML",
                reply_parameters=ReplyParameters(message_id=int(d["msg_id"]), allow_sending_without_reply=True),
            )
        except Exception:
            logger.exception("R&R winner post failed chat_id=%s", chat_id)


'''

DB_OPS = [
    ("sub", "db: celebrate level-ups and milestones when a call is recorded",
     r'''        if crosses_threshold:
            earned.append(AttendanceRow(uid, name, int(doc.get("present_days", 1))))''',
     r'''        try:  # announcements must never break attendance
            _celebrate_after_attendance(chat_id, uid, name, sec, xp_gain, doc, crosses_threshold)
        except Exception:
            pass

        if crosses_threshold:
            earned.append(AttendanceRow(uid, name, int(doc.get("present_days", 1))))'''),
    ("remove_func", "db: replaced by the freeze-aware version below", "reset_expired_streaks_all"),
    ("append", "db: engagement functions", DB_ENGAGE),
]

BOT_OPS = [
    ("remove_func", "bot: replaced by the freeze-aware /streak below", "cmd_streak"),
    ("sub", "bot: engagement code",
     r'''async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:''',
     BOT_ENGAGE + "async def on_track_known_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:"),
    ("sub", "bot: engagement help entries",
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),''',
     r'''    "cmds": ("everyone", "/cmds", "List this group's custom commands.", "Anyone"),
    "myrecap": ("everyone", "/myrecap [this]  (in a private chat with the bot)", "Your month in books: voice chats, hours, Read & Records, topics, streak and level. Last month by default, or 'this' for the month so far.", "Anyone"),
    "recap": ("everyone", "/recap on|off  (in a private chat with the bot)", "Switch the monthly recap message I send you on or off.", "Anyone"),
    "givefreeze": ("botadmin", "/givefreeze [reply / id / @user]", "Give a member a streak freeze (covers one missed day), e.g. for an emergency.", "Bot admin"),'''),
    ("sub", "bot: register engagement commands and reactions",
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))''',
     r'''    app.add_handler(CommandHandler("cmds", cmd_cmds))
    app.add_handler(CommandHandler("myrecap", cmd_myrecap))
    app.add_handler(CommandHandler("recap", cmd_recap))
    app.add_handler(CommandHandler("givefreeze", cmd_givefreeze))
    app.add_handler(MessageReactionHandler(on_message_reaction))'''),
    ("sub", "bot: schedule the engagement jobs",
     r'''        name="daily_streak_reset_job",
    )''',
     r'''        name="daily_streak_reset_job",
    )
    jq.run_repeating(celebration_worker, interval=30, first=55, name="celebration_worker")
    jq.run_repeating(streak_reminder_job, interval=1800, first=100, name="streak_reminder_job")
    jq.run_repeating(monthly_awards_job, interval=3600, first=120, name="monthly_awards_job")
    jq.run_repeating(rnr_week_winner_job, interval=1800, first=140, name="rnr_week_winner_job")'''),
]

# ----------------------------------------------------------------------------
# Patch engine
# ----------------------------------------------------------------------------


def anchor_regex(anchor: str) -> re.Pattern:
    """Exact text, except trailing spaces on each line and whitespace-only lines may differ."""
    lines = anchor.split("\n")
    pieces = [re.escape(l.rstrip()) if l.strip() else "" for l in lines]
    return re.compile("[ \\t]*\n".join(pieces))


def _func_span(text: str, name: str) -> tuple[int, int] | None:
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            return start, node.end_lineno
    return None


def apply_ops(text: str, ops: list, label: str) -> tuple[str | None, list[str]]:
    """Returns (new_text, problems). new_text is None if any problem was found."""
    problems: list[str] = []
    original = text

    # Pass 1: every edit point must exist exactly once in the ORIGINAL file.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            n = len(list(anchor_regex(op[2]).finditer(original)))
            if n != 1:
                problems.append(f"{label}: '{name}' - edit point found {n} times (expected 1)")
        elif kind == "delete_line":
            n = sum(1 for l in original.split("\n") if op[2] in l)
            if n != 1:
                problems.append(f"{label}: '{name}' - line found {n} times (expected 1)")
        elif kind in ("remove_func", "replace_func"):
            if _func_span(original, op[2]) is None:
                problems.append(f"{label}: '{name}' - function {op[2]} not found")
    if problems:
        return None, problems

    # Pass 2: apply in order.
    for op in ops:
        kind, name = op[0], op[1]
        if kind == "sub":
            pat, new = anchor_regex(op[2]), op[3]
            matches = list(pat.finditer(text))
            if len(matches) != 1:
                return None, [f"{label}: '{name}' - edit point changed during patching ({len(matches)} matches)"]
            m = matches[0]
            text = text[: m.start()] + new + text[m.end():]
        elif kind == "append":
            text = text.rstrip("\n") + "\n" + op[2]
        elif kind == "delete_line":
            lines = text.split("\n")
            lines = [l for l in lines if op[2] not in l]
            text = "\n".join(lines)
        elif kind == "remove_func":
            span = _func_span(text, op[2])
            if span is None:
                return None, [f"{label}: '{name}' - function vanished during patching"]
            start, end = span
            lines = text.split("\n")
            while end < len(lines) and not lines[end].strip():
                end += 1  # also swallow the blank lines after it
            text = "\n".join(lines[: start - 1] + lines[end:])
        elif kind == "replace_func":
            span = _func_span(text, op[2])
            if span is None:
                return None, [f"{label}: '{name}' - function vanished during patching"]
            start, end = span
            lines = text.split("\n")
            text = "\n".join(lines[: start - 1] + op[3].rstrip("\n").split("\n") + lines[end:])
    try:
        compile(text, label, "exec")
    except SyntaxError as exc:
        return None, [f"{label}: patched code has a syntax error at line {exc.lineno}: {exc.msg}"]
    return text, []


def patch_file(path: Path, ops: list, marker: str, dry: bool) -> bool:
    raw = path.read_bytes().decode("utf-8")
    crlf = "\r\n" in raw
    text = raw.replace("\r\n", "\n")
    if marker in text:
        print(f"- {path.name}: already patched, skipping")
        return True
    new_text, problems = apply_ops(text, ops, path.name)
    if new_text is None:
        print(f"X {path.name}: NOT patched")
        for p in problems:
            print("    " + p)
        return False
    if dry:
        print(f"OK {path.name}: all {len(ops)} edit points found (dry run, nothing written)")
        return True
    shutil.copy2(path, path.with_name(path.name + ".bak"))
    if crlf:
        new_text = new_text.replace("\n", "\r\n")
    path.write_bytes(new_text.encode("utf-8"))
    print(f"OK {path.name}: patched ({len(ops)} edits), original saved as {path.name}.bak")
    return True


def main() -> int:
    dry = "--dry-run" in sys.argv
    here = Path(".")
    missing = [n for n in ("bot.py", "db.py") if not (here / n).exists()]
    if missing:
        print(f"Run this from the folder that contains bot.py and db.py (missing: {', '.join(missing)})")
        return 1
    texts = {n: (here / n).read_bytes().decode("utf-8").replace("\r\n", "\n") for n in ("bot.py", "db.py")}
    if PREREQ_BOT not in texts["bot.py"]:
        print("The Read & Record feature isn't in bot.py yet. Run apply_rnr.py first, then this script.")
        return 1
    jobs = (("db.py", DB_OPS, MARKER_DB), ("bot.py", BOT_OPS, MARKER_BOT))
    ok = True
    for name, ops, marker in jobs:
        if marker in texts[name]:
            continue
        new_text, problems = apply_ops(texts[name], ops, name)
        if new_text is None:
            ok = False
            print(f"X {name}: cannot be patched")
            for p in problems:
                print("    " + p)
    if not ok:
        print("\nNothing was changed. Send me the lines above and I'll adjust the script.")
        return 1
    for name, ops, marker in jobs:
        if not patch_file(here / name, ops, marker, dry):
            return 1
    if not dry:
        print("\nDone. Commit and push bot.py and db.py, then let Render redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())