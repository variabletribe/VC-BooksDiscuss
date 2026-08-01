"""Persistence for VC stats: MongoDB Atlas (MONGODB_URI)."""

from __future__ import annotations

import html
import os
from datetime import datetime, timezone
from typing import Iterable, NamedTuple

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError


class LeaderRow(NamedTuple):
    user_id: int
    display_name: str
    total_seconds: int


class VCStatsRow(NamedTuple):
    user_id: int
    display_name: str
    vc_count: int
    total_seconds: int


class AttendanceRow(NamedTuple):
    user_id: int
    display_name: str
    present_days: int


class RemoveUserResult(NamedTuple):
    user_id: int
    display_name: str | None
    vc_rows_deleted: int
    attendance_rows_deleted: int


class FindUserRow(NamedTuple):
    user_id: int
    display_name: str
    vc_count: int
    total_seconds: int
    present_days: int


class LevelInfo(NamedTuple):
    user_id: int
    display_name: str
    xp: int
    level: int
    xp_into_level: int
    xp_for_next_level: int  # None-equivalent handled as -1 when at max level


class StreakInfo(NamedTuple):
    user_id: int
    display_name: str
    current_streak: int
    longest_streak: int


class BadgeEarned(NamedTuple):
    user_id: int
    display_name: str
    badge_id: str
    badge_label: str


class WeeklyDigest(NamedTuple):
    period_start: datetime
    period_end: datetime
    top_by_hours: list[VCStatsRow]
    top_streaks: list[StreakInfo]
    total_sessions: int
    total_participant_seconds: int


# XP: 1 XP per minute in VC. Level thresholds are cumulative XP required.
LEVEL_THRESHOLDS: list[tuple[int, int]] = [
    (1, 0),
    (2, 500),
    (3, 1500),
    (4, 3500),
    (5, 7000),
    (6, 12000),
    (7, 20000),
    (8, 32000),
    (9, 50000),
    (10, 75000),
]

BADGES: dict[str, dict] = {
    "night_owl": {"label": "🦉 Night Owl", "desc": "In a VC after midnight, 10 times"},
    "marathoner": {"label": "🏃 Marathoner", "desc": "Single session 3+ hours"},
    "iron_streak_7": {"label": "🔥 Week Warrior", "desc": "7-day streak"},
    "iron_streak_30": {"label": "⚡ Iron Streak", "desc": "30-day streak"},
    "veteran_100": {"label": "🎖️ Veteran", "desc": "100 total sessions"},
}


def _level_for_xp(xp: int) -> tuple[int, int, int]:
    """Return (level, xp_into_level, xp_for_next_level). xp_for_next_level is -1 at max level."""
    level = 1
    threshold = 0
    next_threshold = LEVEL_THRESHOLDS[1][1] if len(LEVEL_THRESHOLDS) > 1 else -1
    for lvl, req in LEVEL_THRESHOLDS:
        if xp >= req:
            level = lvl
            threshold = req
        else:
            next_threshold = req
            break
    else:
        next_threshold = -1
    if level == LEVEL_THRESHOLDS[-1][0]:
        next_threshold = -1
    return level, xp - threshold, next_threshold


_client: MongoClient | None = None
_db = None


def init_db() -> None:
    """Connect to MongoDB Atlas and ensure indexes exist."""
    global _client, _db
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise RuntimeError("MONGODB_URI environment variable is not set")
    _client = MongoClient(uri)
    db_name = os.environ.get("MONGODB_DB_NAME", "vc_bot")
    _db = _client[db_name]

    # chat_settings: _id = chat_id
    # vc_sessions: participants embedded, indexed by chat_id + ended_at for range queries
    _db.vc_sessions.create_index([("chat_id", ASCENDING), ("ended_at", ASCENDING)])
    _db.vc_sessions.create_index([("participants.user_id", ASCENDING)])
    # monthly_report_sent: unique per chat/year/month
    _db.monthly_report_sent.create_index(
        [("chat_id", ASCENDING), ("year", ASCENDING), ("month", ASCENDING)],
        unique=True,
    )
    # user_attendance: _id = "chat_id:user_id" — also holds xp, level, streaks, badges now
    _db.user_attendance.create_index([("chat_id", ASCENDING)])
    _db.user_attendance.create_index([("chat_id", ASCENDING), ("xp", DESCENDING)])
    _db.user_attendance.create_index([("chat_id", ASCENDING), ("current_streak", DESCENDING)])


def _coll(name: str):
    assert _db is not None, "init_db() must be called first"
    return _db[name]


def ensure_chat(chat_id: int, title: str | None = None) -> None:
    coll = _coll("chat_settings")
    update: dict = {"$setOnInsert": {"monthly_reports": True}}
    if title:
        update["$set"] = {"title": title}
    coll.update_one({"_id": chat_id}, update, upsert=True)


def set_monthly_reports(chat_id: int, enabled: bool) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"monthly_reports": enabled}, "$setOnInsert": {}},
        upsert=True,
    )


def get_monthly_reports_enabled(chat_id: int) -> bool:
    coll = _coll("chat_settings")
    doc = coll.find_one({"_id": chat_id})
    if doc is None:
        return True
    return bool(doc.get("monthly_reports", True))


def list_chats_with_monthly_reports() -> list[int]:
    coll = _coll("chat_settings")
    cursor = coll.find({"monthly_reports": True}, {"_id": 1})
    return [int(d["_id"]) for d in cursor]


def record_vc_session(
    chat_id: int,
    ended_at: datetime,
    duration_sec: int,
    started_at: datetime | None,
    participants: Iterable[tuple[int, str, int]],
) -> None:
    """participants: (user_id, display_name, estimated_seconds)."""
    coll = _coll("vc_sessions")
    if ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=timezone.utc)
    if started_at and started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    participant_docs = [
        {"user_id": uid, "display_name": name[:512], "estimated_seconds": est}
        for uid, name, est in participants
    ]
    coll.insert_one(
        {
            "chat_id": chat_id,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_sec": duration_sec,
            "participants": participant_docs,
        }
    )


def month_bounds_utc(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def _match_stage(chat_id: int, period_start: datetime | None, period_end_exclusive: datetime | None) -> dict:
    match: dict = {"chat_id": chat_id}
    ended_at_filter: dict = {}
    if period_start is not None:
        if period_start.tzinfo is None:
            period_start = period_start.replace(tzinfo=timezone.utc)
        ended_at_filter["$gte"] = period_start
    if period_end_exclusive is not None:
        if period_end_exclusive.tzinfo is None:
            period_end_exclusive = period_end_exclusive.replace(tzinfo=timezone.utc)
        ended_at_filter["$lt"] = period_end_exclusive
    if ended_at_filter:
        match["ended_at"] = ended_at_filter
    return match


def fetch_leaderboard(
    chat_id: int,
    period_start: datetime,
    period_end_exclusive: datetime,
) -> list[LeaderRow]:
    coll = _coll("vc_sessions")
    pipeline = [
        {"$match": _match_stage(chat_id, period_start, period_end_exclusive)},
        {"$unwind": "$participants"},
        {
            "$group": {
                "_id": "$participants.user_id",
                "dname": {"$last": "$participants.display_name"},
                "total": {"$sum": "$participants.estimated_seconds"},
            }
        },
        {"$sort": {"total": -1}},
    ]
    rows = list(coll.aggregate(pipeline))
    return [LeaderRow(int(r["_id"]), str(r["dname"]), int(r["total"])) for r in rows]


def previous_calendar_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def monthly_report_already_sent(chat_id: int, year: int, month: int) -> bool:
    coll = _coll("monthly_report_sent")
    return coll.find_one({"chat_id": chat_id, "year": year, "month": month}) is not None


def mark_monthly_report_sent(chat_id: int, year: int, month: int) -> None:
    coll = _coll("monthly_report_sent")
    try:
        coll.insert_one({"chat_id": chat_id, "year": year, "month": month})
    except DuplicateKeyError:
        pass


def fetch_month_leaderboard(chat_id: int, year: int, month: int) -> list[LeaderRow]:
    start, end = month_bounds_utc(year, month)
    return fetch_leaderboard(chat_id, start, end)


def fetch_vc_date_range(
    chat_id: int,
    period_start: datetime | None = None,
    period_end_exclusive: datetime | None = None,
) -> tuple[datetime | None, datetime | None]:
    coll = _coll("vc_sessions")
    pipeline = [
        {"$match": _match_stage(chat_id, period_start, period_end_exclusive)},
        {
            "$group": {
                "_id": None,
                "min_ended": {"$min": "$ended_at"},
                "max_ended": {"$max": "$ended_at"},
            }
        },
    ]
    rows = list(coll.aggregate(pipeline))
    if not rows:
        return None, None
    return rows[0]["min_ended"], rows[0]["max_ended"]


def fetch_vc_stats(
    chat_id: int,
    period_start: datetime | None = None,
    period_end_exclusive: datetime | None = None,
) -> list[VCStatsRow]:
    coll = _coll("vc_sessions")
    pipeline = [
        {"$match": _match_stage(chat_id, period_start, period_end_exclusive)},
        {"$unwind": "$participants"},
        {
            "$group": {
                "_id": "$participants.user_id",
                "dname": {"$last": "$participants.display_name"},
                "vcs": {"$sum": 1},
                "total": {"$sum": "$participants.estimated_seconds"},
            }
        },
        {"$sort": {"total": -1, "vcs": -1}},
    ]
    rows = list(coll.aggregate(pipeline))
    return [
        VCStatsRow(int(r["_id"]), str(r["dname"]), int(r["vcs"] or 0), int(r["total"] or 0))
        for r in rows
    ]


def fetch_alltime_vc_stats(chat_id: int) -> tuple[list[VCStatsRow], datetime | None, datetime | None]:
    rows = fetch_vc_stats(chat_id)
    start, end = fetch_vc_date_range(chat_id)
    return rows, start, end


def fetch_month_vc_stats(chat_id: int, year: int, month: int) -> tuple[list[VCStatsRow], datetime | None, datetime | None]:
    start, end = month_bounds_utc(year, month)
    rows = fetch_vc_stats(chat_id, start, end)
    range_start, range_end = fetch_vc_date_range(chat_id, start, end)
    return rows, range_start, range_end


def present_threshold_sec() -> int:
    try:
        return max(1, int(os.getenv("PRESENT_MIN_SECONDS", "1200")))
    except ValueError:
        return 1200


def record_present_attendance(
    chat_id: int,
    participants: Iterable[tuple[int, str, int]],
) -> list[AttendanceRow]:
    """+1 present day per user who stayed longer than the threshold in this call.
    Also awards XP (1 per minute of estimated_seconds, all participants regardless of
    threshold) and updates day-streaks for those who cross the present threshold.
    """
    coll = _coll("user_attendance")
    threshold = present_threshold_sec()
    today = datetime.now(timezone.utc).date()
    earned: list[AttendanceRow] = []

    for uid, name, sec in participants:
        doc_id = f"{chat_id}:{uid}"
        xp_gain = max(0, sec // 60)

        existing = coll.find_one({"_id": doc_id})
        current_streak = int(existing.get("current_streak", 0)) if existing else 0
        longest_streak = int(existing.get("longest_streak", 0)) if existing else 0
        last_day_str = existing.get("last_present_date") if existing else None

        update: dict = {
            "$inc": {"xp": xp_gain},
            "$set": {
                "chat_id": chat_id,
                "user_id": uid,
                "display_name": name[:512],
            },
            "$setOnInsert": {
                "present_days": 0,
                "current_streak": 0,
                "longest_streak": 0,
                "badges": [],
            },
        }

        if sec > threshold:
            update["$inc"]["present_days"] = 1
            if last_day_str:
                last_day = datetime.strptime(last_day_str, "%Y-%m-%d").date()
                gap = (today - last_day).days
                if gap == 1:
                    new_streak = current_streak + 1
                elif gap == 0:
                    new_streak = max(current_streak, 1)
                else:
                    new_streak = 1
            else:
                new_streak = 1
            new_longest = max(longest_streak, new_streak)
            update["$set"]["current_streak"] = new_streak
            update["$set"]["longest_streak"] = new_longest
            update["$set"]["last_present_date"] = today.strftime("%Y-%m-%d")

        doc = coll.find_one_and_update(
            {"_id": doc_id}, update, upsert=True, return_document=True
        )

        if sec > threshold:
            earned.append(AttendanceRow(uid, name, int(doc["present_days"])))

    return earned


def get_level_info(chat_id: int, user_id: int, display_name: str = "") -> LevelInfo:
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    xp = int(doc["xp"]) if doc and "xp" in doc else 0
    name = doc["display_name"] if doc else display_name
    level, into_level, for_next = _level_for_xp(xp)
    return LevelInfo(user_id, str(name), xp, level, into_level, for_next)


def fetch_xp_leaderboard(chat_id: int, limit: int = 10) -> list[LevelInfo]:
    coll = _coll("user_attendance")
    cursor = coll.find({"chat_id": chat_id}).sort("xp", DESCENDING).limit(limit)
    out = []
    for d in cursor:
        xp = int(d.get("xp", 0))
        level, into_level, for_next = _level_for_xp(xp)
        out.append(LevelInfo(int(d["user_id"]), str(d["display_name"]), xp, level, into_level, for_next))
    return out


def get_streak_info(chat_id: int, user_id: int, display_name: str = "") -> StreakInfo:
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    if not doc:
        return StreakInfo(user_id, display_name, 0, 0)
    return StreakInfo(
        user_id,
        str(doc["display_name"]),
        int(doc.get("current_streak", 0)),
        int(doc.get("longest_streak", 0)),
    )


def fetch_streak_leaderboard(chat_id: int, limit: int = 10) -> list[StreakInfo]:
    coll = _coll("user_attendance")
    cursor = (
        coll.find({"chat_id": chat_id})
        .sort([("current_streak", DESCENDING), ("longest_streak", DESCENDING)])
        .limit(limit)
    )
    return [
        StreakInfo(
            int(d["user_id"]),
            str(d["display_name"]),
            int(d.get("current_streak", 0)),
            int(d.get("longest_streak", 0)),
        )
        for d in cursor
    ]


def reset_expired_streaks(chat_id: int) -> int:
    """Call once daily (e.g. alongside monthly report job): zero out streaks for users
    whose last_present_date is more than 1 day in the past (they missed a day)."""
    coll = _coll("user_attendance")
    today = datetime.now(timezone.utc).date()
    cutoff = (today.toordinal() - 1)
    reset_count = 0
    for doc in coll.find({"chat_id": chat_id, "current_streak": {"$gt": 0}}):
        last_day_str = doc.get("last_present_date")
        if not last_day_str:
            continue
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d").date()
        if last_day.toordinal() < cutoff:
            coll.update_one({"_id": doc["_id"]}, {"$set": {"current_streak": 0}})
            reset_count += 1
    return reset_count


def award_badge(chat_id: int, user_id: int, display_name: str, badge_id: str) -> bool:
    """Award a badge if not already held. Returns True if newly awarded."""
    if badge_id not in BADGES:
        return False
    coll = _coll("user_attendance")
    doc_id = f"{chat_id}:{user_id}"
    result = coll.update_one(
        {"_id": doc_id, "badges": {"$ne": badge_id}},
        {
            "$addToSet": {"badges": badge_id},
            "$set": {"chat_id": chat_id, "user_id": user_id, "display_name": display_name[:512]},
            "$setOnInsert": {"xp": 0, "present_days": 0, "current_streak": 0, "longest_streak": 0},
        },
        upsert=True,
    )
    return result.modified_count > 0 or result.upserted_id is not None


def get_user_badges(chat_id: int, user_id: int) -> list[BadgeEarned]:
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    if not doc:
        return []
    name = str(doc.get("display_name", ""))
    out = []
    for bid in doc.get("badges", []):
        meta = BADGES.get(bid)
        if meta:
            out.append(BadgeEarned(user_id, name, bid, meta["label"]))
    return out


def check_and_award_session_badges(
    chat_id: int,
    participants: Iterable[tuple[int, str, int]],
) -> list[BadgeEarned]:
    """Call after record_vc_session + record_present_attendance with the same participants.
    Checks marathoner (single session 3+ hrs) and night_owl/veteran thresholds."""
    newly_earned: list[BadgeEarned] = []
    coll = _coll("user_attendance")
    now_hour = datetime.now(timezone.utc).hour

    for uid, name, sec in participants:
        if sec >= 3 * 3600:
            if award_badge(chat_id, uid, name, "marathoner"):
                newly_earned.append(BadgeEarned(uid, name, "marathoner", BADGES["marathoner"]["label"]))

        doc = coll.find_one({"_id": f"{chat_id}:{uid}"})
        streak = int(doc.get("current_streak", 0)) if doc else 0
        if streak >= 7 and award_badge(chat_id, uid, name, "iron_streak_7"):
            newly_earned.append(BadgeEarned(uid, name, "iron_streak_7", BADGES["iron_streak_7"]["label"]))
        if streak >= 30 and award_badge(chat_id, uid, name, "iron_streak_30"):
            newly_earned.append(BadgeEarned(uid, name, "iron_streak_30", BADGES["iron_streak_30"]["label"]))

    return newly_earned


def fetch_weekly_digest(chat_id: int, now: datetime | None = None) -> WeeklyDigest:
    """Stats for the last 7 days: top performers by hours + streak leaders."""
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    period_end = now
    period_start = now.fromtimestamp(now.timestamp() - 7 * 86400, tz=timezone.utc)

    stats = fetch_vc_stats(chat_id, period_start, period_end)
    streaks = fetch_streak_leaderboard(chat_id, limit=5)

    sessions_coll = _coll("vc_sessions")
    match = _match_stage(chat_id, period_start, period_end)
    total_sessions = sessions_coll.count_documents(match)
    total_seconds = sum(r.total_seconds for r in stats)

    return WeeklyDigest(
        period_start=period_start,
        period_end=period_end,
        top_by_hours=stats[:5],
        top_streaks=[s for s in streaks if s.current_streak > 0][:5],
        total_sessions=total_sessions,
        total_participant_seconds=total_seconds,
    )


def format_weekly_digest_message(chat_title: str, digest: WeeklyDigest) -> str:
    lines = [
        f"📊 <b>Weekly Digest — {html.escape(chat_title, quote=False)}</b>",
        f"<i>{digest.period_start.strftime('%b %d')} – {digest.period_end.strftime('%b %d')}</i>",
        "",
        f"🗓️ {digest.total_sessions} VC session(s) · {digest.total_participant_seconds // 3600}h total",
        "",
    ]
    if digest.top_by_hours:
        lines.append("<b>🏆 Top by hours</b>")
        medals = ["🥇", "🥈", "🥉", "4.", "5."]
        for i, row in enumerate(digest.top_by_hours):
            safe = html.escape(row.display_name, quote=False)
            hrs = row.total_seconds / 3600
            lines.append(f"{medals[i]} {safe} — {hrs:.1f}h ({row.vc_count} VCs)")
        lines.append("")
    if digest.top_streaks:
        lines.append("<b>🔥 Streak leaders</b>")
        for row in digest.top_streaks:
            safe = html.escape(row.display_name, quote=False)
            lines.append(f"• {safe} — {row.current_streak} day streak")
    if not digest.top_by_hours and not digest.top_streaks:
        lines.append("<i>No activity this week.</i>")
    return "\n".join(lines)


def format_level_message(info: LevelInfo) -> str:
    safe = html.escape(info.display_name, quote=False)
    if info.xp_for_next_level == -1:
        progress = f"MAX LEVEL — {info.xp} XP total"
    else:
        needed = info.xp_for_next_level - (info.xp - info.xp_into_level)
        progress = f"{info.xp_into_level}/{needed + info.xp_into_level} XP to Level {info.level + 1}"
    return f"🎖️ <b>{safe}</b> — Level {info.level}\n<i>{progress}</i>"


def remove_user_from_chat(chat_id: int, user_id: int) -> RemoveUserResult:
    """Delete all VC participant entries and attendance for one user in this group."""
    sessions_coll = _coll("vc_sessions")
    attendance_coll = _coll("user_attendance")

    name = None
    latest = sessions_coll.find(
        {"chat_id": chat_id, "participants.user_id": user_id},
        {"participants.$": 1, "ended_at": 1},
    ).sort("ended_at", DESCENDING).limit(1)
    latest = list(latest)
    if latest:
        parts = latest[0].get("participants", [])
        if parts:
            name = parts[0].get("display_name")

    if name is None:
        att = attendance_coll.find_one({"_id": f"{chat_id}:{user_id}"})
        if att is not None:
            name = att.get("display_name")

    # Count sessions containing this user before removal (approximation of rows deleted)
    vc_deleted = sessions_coll.count_documents(
        {"chat_id": chat_id, "participants.user_id": user_id}
    )
    sessions_coll.update_many(
        {"chat_id": chat_id, "participants.user_id": user_id},
        {"$pull": {"participants": {"user_id": user_id}}},
    )

    att_result = attendance_coll.delete_one({"_id": f"{chat_id}:{user_id}"})

    return RemoveUserResult(
        user_id=user_id,
        display_name=str(name) if name else None,
        vc_rows_deleted=int(vc_deleted or 0),
        attendance_rows_deleted=int(att_result.deleted_count or 0),
    )


def find_users_in_chat(chat_id: int, query: str, limit: int = 15) -> list[FindUserRow]:
    """Search stored display names (includes old @usernames) for this group."""
    term = query.strip().lstrip("@")
    if not term:
        return []

    sessions_coll = _coll("vc_sessions")
    attendance_coll = _coll("user_attendance")

    by_id: dict[int, FindUserRow] = {}

    pipeline = [
        {"$match": {"chat_id": chat_id}},
        {"$unwind": "$participants"},
        {"$match": {"participants.display_name": {"$regex": term, "$options": "i"}}},
        {
            "$group": {
                "_id": "$participants.user_id",
                "dname": {"$last": "$participants.display_name"},
                "vcs": {"$sum": 1},
                "total": {"$sum": "$participants.estimated_seconds"},
            }
        },
        {"$sort": {"total": -1}},
        {"$limit": limit},
    ]
    for r in sessions_coll.aggregate(pipeline):
        by_id[int(r["_id"])] = FindUserRow(
            int(r["_id"]), str(r["dname"]), int(r["vcs"] or 0), int(r["total"] or 0), 0
        )

    att_rows = attendance_coll.find(
        {"chat_id": chat_id, "display_name": {"$regex": term, "$options": "i"}}
    )
    for att in att_rows:
        uid = int(att["user_id"])
        if uid in by_id:
            row = by_id[uid]
            by_id[uid] = FindUserRow(
                row.user_id, row.display_name, row.vc_count, row.total_seconds, int(att["present_days"])
            )
        else:
            by_id[uid] = FindUserRow(
                uid, att["display_name"], 0, 0, int(att["present_days"])
            )

    for user_id, row in list(by_id.items()):
        if row.present_days:
            continue
        att = attendance_coll.find_one({"_id": f"{chat_id}:{user_id}"})
        if att:
            by_id[user_id] = FindUserRow(
                row.user_id, row.display_name, row.vc_count, row.total_seconds, int(att["present_days"])
            )

    rows = sorted(by_id.values(), key=lambda x: (-x.total_seconds, x.display_name.lower()))
    return rows[:limit]


def fetch_all_attendance(chat_id: int) -> list[AttendanceRow]:
    coll = _coll("user_attendance")
    cursor = coll.find({"chat_id": chat_id}).sort(
        [("present_days", DESCENDING), ("display_name", ASCENDING)]
    )
    return [
        AttendanceRow(int(d["user_id"]), str(d["display_name"]), int(d["present_days"]))
        for d in cursor
    ]


def format_attendance_message(earned: list[AttendanceRow]) -> str:
    threshold_min = present_threshold_sec() // 60
    lines = [
        "📋 <b>Present attendance</b>",
        " <i>Counts from 1 July 2026</i>",
        "",
        f"<i>More than {threshold_min} minutes in one call = +1 present day (once per call).</i>",
        "",
    ]
    if earned:
        for row in sorted(earned, key=lambda r: (-r.present_days, r.display_name)):
            safe = html.escape(row.display_name, quote=False)
            day_word = "day" if row.present_days == 1 else "days"
            lines.append(f"✅ {safe} — Present <b>{row.present_days}</b> {day_word}")
    else:
        lines.append("<i>No one reached the present threshold in this call.</i>")

    return "\n".join(lines)
