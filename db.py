"""Persistence for VC stats: MongoDB Atlas (MONGODB_URI)."""

from __future__ import annotations

import html
import os
import re
from datetime import datetime, timezone
from typing import Iterable, NamedTuple

from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
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
    badge_desc: str
    count: int


class UserVCStats(NamedTuple):
    vc_count: int
    total_seconds: int
    first_vc_at: datetime | None


class MyStats(NamedTuple):
    user_id: int
    display_name: str
    present_days: int
    vc_count: int
    total_seconds: int
    group_joined_at: datetime | None
    first_vc_at: datetime | None
    current_streak: int
    longest_streak: int
    xp: int
    level: int
    xp_into_level: int
    xp_for_next_level: int


class WeeklyDigest(NamedTuple):
    period_start: datetime
    period_end: datetime
    top_by_hours: list[VCStatsRow]
    top_streaks: list[StreakInfo]
    total_sessions: int
    total_participant_seconds: int


class ExportRow(NamedTuple):
    """One row of /exportdata — everything about a user who has joined at least one VC."""

    user_id: int
    display_name: str
    vc_count: int
    total_seconds: int
    present_days: int
    current_streak: int
    longest_streak: int
    xp: int
    level: int
    group_joined_at: datetime | None
    first_vc_at: datetime | None


class TopicRow(NamedTuple):
    """One VC topic. state is 'active' | 'done' | 'deleted'."""

    serial: int
    text: str
    state: str
    votes: int = 0


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

# Simple, repeatable badge system: every badge can be earned more than once and
# is tracked as a per-badge count (e.g. "Marathoner ×3"), rather than a flat
# earned-once list. Kept intentionally short — five badges, no tiers/rarity —
# so it stays easy to remember while still feeling premium via formatting.
BADGES: dict[str, dict] = {
    "marathoner": {"label": "🏃 Marathoner", "desc": "Spent 3+ hours in a single VC"},
    "night_owl": {"label": "🦉 Night Owl", "desc": "Joined a VC after midnight"},
    "week_warrior": {"label": "🔥 Week Warrior", "desc": "Kept a 7-day attendance streak"},
    "iron_streak": {"label": "⚡ Iron Streak", "desc": "Kept a 30-day attendance streak"},
    "veteran": {"label": "🎖️ Veteran", "desc": "Joined 100 VC sessions"},
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
    # relay_map: _id = the forwarded/info message id in the admin relay chat.
    # Auto-expires after 30 days so this collection doesn't grow forever.
    _db.relay_map.create_index("created_at", expireAfterSeconds=60 * 60 * 24 * 30)
    # known_users: username -> user_id memory per chat, so /warn @username (and other
    # moderation commands) can resolve a bare @username without Telegram's getChat, which
    # only works for usernames the bot has already been introduced to some other way.
    _db.known_users.create_index([("chat_id", ASCENDING), ("username", ASCENDING)])
    # topics: VC topic suggestions, permanent serial numbers (see _next_topic_serial).
    _db.topics.create_index([("chat_id", ASCENDING), ("state", ASCENDING), ("serial", ASCENDING)])


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

    IMPORTANT: MongoDB forbids a field appearing in both `$inc`/`$set` and
    `$setOnInsert` in the same update — it raises a WriteError ("would create a
    conflict"). We build the update dict per-user so a field is only ever placed
    in one operator, never both. This applies to present_days AND to
    current_streak/longest_streak: whichever branch (crossing vs. not crossing
    the threshold) sets them via $set must NOT also default them via
    $setOnInsert in that same call, or every present-day write throws and the
    whole update (including the xp $inc) silently fails to apply.
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

        inc: dict = {"xp": xp_gain}
        set_fields: dict = {
            "chat_id": chat_id,
            "user_id": uid,
            "display_name": name[:512],
        }
        set_on_insert: dict = {
            "badges": {},
        }

        crosses_threshold = sec > threshold
        if crosses_threshold:
            # present_days AND current_streak/longest_streak are all being
            # $set/$inc'd this call, so none of them may also appear in
            # $setOnInsert (that's the bug that made every present-day write
            # for a fresh OR existing doc throw a WriteError and silently
            # drop the whole update, including present_days and xp).
            inc["present_days"] = 1
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
            set_fields["current_streak"] = new_streak
            set_fields["longest_streak"] = new_longest
            set_fields["last_present_date"] = today.strftime("%Y-%m-%d")
        else:
            # Not incrementing/setting these this call, so it's safe to default
            # them here on first-ever insert for this user.
            set_on_insert["present_days"] = 0
            set_on_insert["current_streak"] = 0
            set_on_insert["longest_streak"] = 0

        update: dict = {"$inc": inc, "$set": set_fields, "$setOnInsert": set_on_insert}

        doc = coll.find_one_and_update(
            {"_id": doc_id}, update, upsert=True, return_document=True
        )

        if crosses_threshold:
            earned.append(AttendanceRow(uid, name, int(doc.get("present_days", 1))))

    return earned


def get_level_info(chat_id: int, user_id: int, display_name: str = "") -> LevelInfo:
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    xp = int(doc["xp"]) if doc and "xp" in doc else 0
    name = doc["display_name"] if doc else display_name
    level, into_level, for_next = _level_for_xp(xp)
    return LevelInfo(user_id, str(name), xp, level, into_level, for_next)


def get_user_vc_stats(chat_id: int, user_id: int) -> UserVCStats:
    """VC count, total seconds, and first-ever-joined-a-VC date for one user,
    derived from vc_sessions (no separate tracking needed — this data already exists)."""
    coll = _coll("vc_sessions")
    pipeline = [
        {"$match": {"chat_id": chat_id, "participants.user_id": user_id}},
        {"$unwind": "$participants"},
        {"$match": {"participants.user_id": user_id}},
        {
            "$group": {
                "_id": None,
                "vcs": {"$sum": 1},
                "total": {"$sum": "$participants.estimated_seconds"},
                "first_at": {"$min": {"$ifNull": ["$started_at", "$ended_at"]}},
            }
        },
    ]
    rows = list(coll.aggregate(pipeline))
    if not rows:
        return UserVCStats(0, 0, None)
    r = rows[0]
    return UserVCStats(int(r.get("vcs", 0) or 0), int(r.get("total", 0) or 0), r.get("first_at"))


def set_group_join_backfill(chat_id: int, user_id: int, display_name: str, when: datetime) -> None:
    """Overwrite group_joined_at with an authoritative value sourced straight from Telegram's
    own participant records (the `date` field on a ChannelParticipant, fetched via the
    Telethon assistant's channels.GetParticipantsRequest — only a user account can see this,
    not a bot). Unlike record_group_join (which only fills the field if it's still empty,
    for live "X joined" events), this always overwrites — it's meant for the one-time
    backfill_joins.py script, which is the actually-correct source of truth."""
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    coll = _coll("user_attendance")
    doc_id = f"{chat_id}:{user_id}"
    coll.update_one(
        {"_id": doc_id},
        {
            "$set": {
                "chat_id": chat_id,
                "user_id": user_id,
                "display_name": display_name[:512],
                "group_joined_at": when,
            },
            "$setOnInsert": {
                "xp": 0,
                "present_days": 0,
                "current_streak": 0,
                "longest_streak": 0,
                "badges": {},
            },
        },
        upsert=True,
    )


def record_group_join(chat_id: int, user_id: int, display_name: str, when: datetime) -> None:
    """Record the first known "joined the group" timestamp for a user.

    Only ever set once — a later leave+rejoin does not overwrite the original first-seen
    date. There is NO historical join date for members who were already in the group before
    this feature shipped; for them group_joined_at stays unset and /mystats notes that
    tracking only starts from the day this was added, going forward.
    """
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    coll = _coll("user_attendance")
    doc_id = f"{chat_id}:{user_id}"
    existing = coll.find_one({"_id": doc_id})
    if existing and existing.get("group_joined_at"):
        return
    coll.update_one(
        {"_id": doc_id},
        {
            "$set": {
                "chat_id": chat_id,
                "user_id": user_id,
                "display_name": display_name[:512],
                "group_joined_at": when,
            },
            "$setOnInsert": {
                "xp": 0,
                "present_days": 0,
                "current_streak": 0,
                "longest_streak": 0,
                "badges": {},
            },
        },
        upsert=True,
    )


def get_my_stats(chat_id: int, user_id: int, fallback_display_name: str = "") -> MyStats:
    """Everything shown by /mystats, gathered in one place."""
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})

    display_name = (str(doc["display_name"]) if doc and doc.get("display_name") else "") or fallback_display_name
    present_days = int(doc.get("present_days", 0)) if doc else 0
    current_streak = int(doc.get("current_streak", 0)) if doc else 0
    longest_streak = int(doc.get("longest_streak", 0)) if doc else 0
    xp = int(doc.get("xp", 0)) if doc else 0
    group_joined_at = doc.get("group_joined_at") if doc else None

    level, into_level, for_next = _level_for_xp(xp)
    vc_stats = get_user_vc_stats(chat_id, user_id)

    return MyStats(
        user_id=user_id,
        display_name=display_name,
        present_days=present_days,
        vc_count=vc_stats.vc_count,
        total_seconds=vc_stats.total_seconds,
        group_joined_at=group_joined_at,
        first_vc_at=vc_stats.first_vc_at,
        current_streak=current_streak,
        longest_streak=longest_streak,
        xp=xp,
        level=level,
        xp_into_level=into_level,
        xp_for_next_level=for_next,
    )


def _fmt_date_or_none(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d %b %Y")


def format_my_stats_message(stats: MyStats) -> str:
    safe = html.escape(stats.display_name, quote=False)
    joined_group = _fmt_date_or_none(stats.group_joined_at) or "Unknown (joined before join-tracking started)"
    first_vc = _fmt_date_or_none(stats.first_vc_at) or "Hasn't joined a VC yet"

    hrs = stats.total_seconds / 3600
    vc_word = "VC" if stats.vc_count == 1 else "VCs"
    day_word = "day" if stats.present_days == 1 else "days"

    if stats.xp_for_next_level == -1:
        level_line = f"Level {stats.level} (MAX) — {stats.xp} XP total"
    else:
        span = stats.xp_for_next_level - (stats.xp - stats.xp_into_level)
        level_line = f"Level {stats.level} — {stats.xp_into_level}/{span} XP to Level {stats.level + 1}"

    lines = [
        f"📇 <b>{safe}'s stats</b>",
        "",
        f"📅 Joined group: <b>{joined_group}</b>",
        f"🎙️ First VC: <b>{first_vc}</b>",
        "",
        f"📋 Present days: <b>{stats.present_days}</b> {day_word}",
        f"📞 VCs joined: <b>{stats.vc_count}</b> {vc_word}",
        f"⏱️ Total time in calls: <b>{hrs:.1f}h</b>",
        "",
        f"🔥 Current streak: <b>{stats.current_streak}</b> day(s)",
        f"🏆 Longest streak: <b>{stats.longest_streak}</b> day(s)",
        "",
        f"🎖️ {level_line}",
    ]
    return "\n".join(lines)


def fetch_full_streakboard(chat_id: int) -> list[StreakInfo]:
    """Users with real streak data in this group, no limit — for the admin /streakboard command.

    Filters out users who only have a user_attendance doc for unrelated reasons (e.g.
    backfill_joins.py creates one per member just to store their join date, and
    check_and_award_session_badges/session_count bumps can also create docs with no
    streak at all). Without this filter the query returns every member ever seen,
    and in a large group the formatted message blows past Telegram's 4096-char
    limit, so reply_text() throws BadRequest("Text is too long") and the command
    fails silently (see fetch_all_attendance, which has the same guard for
    present_days)."""
    coll = _coll("user_attendance")
    cursor = coll.find(
        {
            "chat_id": chat_id,
            "$or": [
                {"current_streak": {"$gt": 0}},
                {"longest_streak": {"$gt": 0}},
            ],
        }
    ).sort(
        [
            ("current_streak", DESCENDING),
            ("longest_streak", DESCENDING),
            ("display_name", ASCENDING),
        ]
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


def format_streakboard_html(rows: list[StreakInfo]) -> str:
    """Renders up to 60 rows and truncates with a note if there are more — a second
    safety net alongside the fetch_full_streakboard filter above, so even a chat with
    an unusually large number of users with real streaks can't regenerate the same
    'Text is too long' failure."""
    lines = ["🔥 <b>Streak board</b>", "<i>Current streak (best streak)</i>", ""]
    MAX_ROWS = 60
    shown = rows[:MAX_ROWS]
    for i, row in enumerate(shown, start=1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        safe = html.escape(row.display_name, quote=False)
        lines.append(f"{medal} {safe} — <b>{row.current_streak}</b>🔥 (best: {row.longest_streak})")
    if len(rows) > MAX_ROWS:
        lines.append("")
        lines.append(f"<i>+ {len(rows) - MAX_ROWS} more not shown.</i>")
    return "\n".join(lines)


def reset_expired_streaks_all() -> int:
    """Zero out current_streak for ANY user in ANY chat whose last_present_date is more than
    1 day in the past — i.e. at least one full calendar day passed with no present-attendance
    (20+ min) call. Meant to run once daily, shortly after UTC midnight, so a broken streak
    shows 0 the next day rather than staying stale until the person's next VC or the weekly
    digest catches it."""
    coll = _coll("user_attendance")
    today = datetime.now(timezone.utc).date()
    cutoff = today.toordinal() - 1
    reset_count = 0
    for doc in coll.find({"current_streak": {"$gt": 0}}):
        last_day_str = doc.get("last_present_date")
        if not last_day_str:
            continue
        last_day = datetime.strptime(last_day_str, "%Y-%m-%d").date()
        if last_day.toordinal() < cutoff:
            coll.update_one({"_id": doc["_id"]}, {"$set": {"current_streak": 0}})
            reset_count += 1
    return reset_count


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


def _migrate_badges_field(doc: dict) -> dict:
    """Older data stored `badges` as a flat list of earned-once ids
    (e.g. ["night_owl", "marathoner"]). The current schema stores a
    {badge_id: count} map so repeatable badges can show "×3" etc. If we find
    the old list format on a user's doc, convert it in place (count=1 each)
    the first time we touch it, so old earned badges aren't lost."""
    badges = doc.get("badges")
    if isinstance(badges, list):
        counts = {bid: 1 for bid in badges if bid in BADGES}
        coll = _coll("user_attendance")
        coll.update_one({"_id": doc["_id"]}, {"$set": {"badges": counts}})
        doc["badges"] = counts
    elif not isinstance(badges, dict):
        doc["badges"] = {}
    return doc


def award_badge(chat_id: int, user_id: int, display_name: str, badge_id: str) -> int:
    """Increment this user's count for badge_id and return the new count.
    Badges are repeatable by design (e.g. Marathoner ×3) — call this only when
    the calling code has already confirmed the condition was met this call."""
    if badge_id not in BADGES:
        return 0
    coll = _coll("user_attendance")
    doc_id = f"{chat_id}:{user_id}"

    existing = coll.find_one({"_id": doc_id})
    if existing is not None:
        _migrate_badges_field(existing)

    doc = coll.find_one_and_update(
        {"_id": doc_id},
        {
            "$inc": {f"badges.{badge_id}": 1},
            "$set": {"chat_id": chat_id, "user_id": user_id, "display_name": display_name[:512]},
            "$setOnInsert": {"xp": 0, "present_days": 0, "current_streak": 0, "longest_streak": 0},
        },
        upsert=True,
        return_document=True,
    )
    return int((doc.get("badges") or {}).get(badge_id, 1))


def get_user_badges(chat_id: int, user_id: int) -> list[BadgeEarned]:
    coll = _coll("user_attendance")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    if not doc:
        return []
    doc = _migrate_badges_field(doc)
    name = str(doc.get("display_name", ""))
    badges_map: dict = doc.get("badges") or {}

    order = {bid: i for i, bid in enumerate(BADGES.keys())}
    out = []
    for bid, count in badges_map.items():
        meta = BADGES.get(bid)
        if meta and int(count) > 0:
            out.append(BadgeEarned(user_id, name, bid, meta["label"], meta["desc"], int(count)))
    out.sort(key=lambda b: order.get(b.badge_id, 999))
    return out


def check_and_award_session_badges(
    chat_id: int,
    participants: Iterable[tuple[int, str, int]],
) -> list[BadgeEarned]:
    """Call after record_vc_session + record_present_attendance with the same participants.

    Repeatable badge triggers:
    - marathoner: every session with 3+ hours in a single VC.
    - night_owl: every 10th post-midnight-UTC session (10, 20, 30, ...).
    - week_warrior: every time the attendance streak reaches exactly 7 days
      (fires again if the streak resets and climbs back to 7).
    - iron_streak: every time the attendance streak reaches exactly 30 days.
    - veteran: every 100th total VC session (100, 200, 300, ...).

    Same MongoDB rule applies here: session_count / night_owl_count must not
    appear in both $inc and $setOnInsert within the same update call.
    """
    newly_earned: list[BadgeEarned] = []
    coll = _coll("user_attendance")
    now_hour = datetime.now(timezone.utc).hour
    is_midnight_window = now_hour == 0  # UTC hour 0; widen this window if desired

    for uid, name, sec in participants:
        if sec >= 3 * 3600:
            count = award_badge(chat_id, uid, name, "marathoner")
            meta = BADGES["marathoner"]
            newly_earned.append(BadgeEarned(uid, name, "marathoner", meta["label"], meta["desc"], count))

        doc_id = f"{chat_id}:{uid}"
        inc: dict = {"session_count": 1}
        set_on_insert: dict = {
            "xp": 0,
            "present_days": 0,
            "current_streak": 0,
            "longest_streak": 0,
            "badges": {},
        }
        if is_midnight_window:
            inc["night_owl_count"] = 1
        else:
            set_on_insert["night_owl_count"] = 0

        update = {
            "$inc": inc,
            "$set": {"chat_id": chat_id, "user_id": uid, "display_name": name[:512]},
            "$setOnInsert": set_on_insert,
        }
        doc = coll.find_one_and_update({"_id": doc_id}, update, upsert=True, return_document=True)

        streak = int(doc.get("current_streak", 0))
        if streak == 7:
            count = award_badge(chat_id, uid, name, "week_warrior")
            meta = BADGES["week_warrior"]
            newly_earned.append(BadgeEarned(uid, name, "week_warrior", meta["label"], meta["desc"], count))
        if streak == 30:
            count = award_badge(chat_id, uid, name, "iron_streak")
            meta = BADGES["iron_streak"]
            newly_earned.append(BadgeEarned(uid, name, "iron_streak", meta["label"], meta["desc"], count))

        night_owl_count = int(doc.get("night_owl_count", 0))
        if is_midnight_window and night_owl_count > 0 and night_owl_count % 10 == 0:
            count = award_badge(chat_id, uid, name, "night_owl")
            meta = BADGES["night_owl"]
            newly_earned.append(BadgeEarned(uid, name, "night_owl", meta["label"], meta["desc"], count))

        session_count = int(doc.get("session_count", 0))
        if session_count > 0 and session_count % 100 == 0:
            count = award_badge(chat_id, uid, name, "veteran")
            meta = BADGES["veteran"]
            newly_earned.append(BadgeEarned(uid, name, "veteran", meta["label"], meta["desc"], count))

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
        # xp_for_next_level is the *cumulative* XP threshold for the next level, and
        # (info.xp - info.xp_into_level) is the current level's own threshold, so the
        # difference is already the full XP span needed for this level — do not add
        # xp_into_level again or the denominator ends up too large.
        span = info.xp_for_next_level - (info.xp - info.xp_into_level)
        progress = f"{info.xp_into_level}/{span} XP to Level {info.level + 1}"
    return f"🎖️ <b>{safe}</b> — Level {info.level}\n<i>{progress}</i>"


def preview_remove_user(chat_id: int, user_id: int) -> RemoveUserResult:
    """Read-only counterpart to remove_user_from_chat — same name lookup and same counts,
    but deletes nothing. Used to show a confirmation preview before the real deletion."""
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

    vc_count = sessions_coll.count_documents({"chat_id": chat_id, "participants.user_id": user_id})
    att_count = attendance_coll.count_documents({"_id": f"{chat_id}:{user_id}"})

    return RemoveUserResult(
        user_id=user_id,
        display_name=str(name) if name else None,
        vc_rows_deleted=int(vc_count),
        attendance_rows_deleted=int(att_count),
    )


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
    """Only people with real present-day attendance — NOT everyone who merely has a
    user_attendance doc (e.g. from backfill_joins.py, which creates a doc per member
    just to store their join date, regardless of whether they've ever attended a VC).
    Without this filter, a large group can produce a message far beyond Telegram's
    4096-char limit and fail outright ("Text is too long")."""
    coll = _coll("user_attendance")
    cursor = coll.find({"chat_id": chat_id, "present_days": {"$gt": 0}}).sort(
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
        " <i>Counts from 4 August 2026</i>",
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


# --- DM relay (bot <-> admin private group) ---------------------------------

def save_relay_mapping(message_id: int, user_chat_id: int, display_name: str) -> None:
    """Remember which private-chat user a forwarded/info message in the admin relay
    chat came from, so a reply to that message can be routed back to them."""
    coll = _coll("relay_map")
    coll.update_one(
        {"_id": message_id},
        {
            "$set": {
                "user_chat_id": user_chat_id,
                "display_name": display_name[:512],
                "created_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


def get_relay_mapping(message_id: int) -> dict | None:
    coll = _coll("relay_map")
    doc = coll.find_one({"_id": message_id})
    if not doc:
        return None
    return {
        "user_chat_id": int(doc["user_chat_id"]),
        "display_name": str(doc.get("display_name", "")),
    }


def fetch_all_known_user_ids(chat_id: int) -> list[tuple[int, str]]:
    """Distinct users who have appeared in at least one VC session for this chat —
    used as the /broadcast audience ("those who have used the VC once")."""
    coll = _coll("vc_sessions")
    pipeline = [
        {"$match": {"chat_id": chat_id}},
        {"$unwind": "$participants"},
        {
            "$group": {
                "_id": "$participants.user_id",
                "display_name": {"$last": "$participants.display_name"},
            }
        },
    ]
    rows = list(coll.aggregate(pipeline))
    return [(int(r["_id"]), str(r["display_name"])) for r in rows]


# --- Admin-only export / lookup (bot.py: /exportdata, /user) ----------------


def fetch_export_data(chat_id: int) -> list[ExportRow]:
    """Full stats for every user who has joined at least one VC in this chat —
    the base set is fetch_vc_stats() (derived from vc_sessions), NOT every
    user_attendance doc, so users who only exist there for unrelated reasons
    (group-join backfill, badge bumps with 0 VC time) are correctly excluded.
    Attendance/XP/streak fields are merged in from user_attendance where present,
    defaulting to 0/None for a user who somehow has vc_sessions rows but no
    attendance doc yet (shouldn't normally happen, but kept defensive)."""
    vc_rows = fetch_vc_stats(chat_id)
    if not vc_rows:
        return []

    user_ids = [r.user_id for r in vc_rows]
    att_coll = _coll("user_attendance")
    att_docs = {
        int(d["user_id"]): d
        for d in att_coll.find({"chat_id": chat_id, "user_id": {"$in": user_ids}})
    }

    sessions_coll = _coll("vc_sessions")
    pipeline = [
        {"$match": {"chat_id": chat_id}},
        {"$unwind": "$participants"},
        {
            "$group": {
                "_id": "$participants.user_id",
                "first_at": {"$min": {"$ifNull": ["$started_at", "$ended_at"]}},
            }
        },
    ]
    first_vc_map = {int(r["_id"]): r.get("first_at") for r in sessions_coll.aggregate(pipeline)}

    out: list[ExportRow] = []
    for r in vc_rows:
        doc = att_docs.get(r.user_id)
        xp = int(doc.get("xp", 0)) if doc else 0
        level, _, _ = _level_for_xp(xp)
        out.append(
            ExportRow(
                user_id=r.user_id,
                display_name=r.display_name,
                vc_count=r.vc_count,
                total_seconds=r.total_seconds,
                present_days=int(doc.get("present_days", 0)) if doc else 0,
                current_streak=int(doc.get("current_streak", 0)) if doc else 0,
                longest_streak=int(doc.get("longest_streak", 0)) if doc else 0,
                xp=xp,
                level=level,
                group_joined_at=doc.get("group_joined_at") if doc else None,
                first_vc_at=first_vc_map.get(r.user_id),
            )
        )

    out.sort(key=lambda x: -x.total_seconds)
    return out


def export_rows_to_csv(rows: list[ExportRow]) -> bytes:
    """Render ExportRow list as CSV bytes (UTF-8), ready to send as a Telegram document."""
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "user_id",
            "display_name",
            "vc_count",
            "total_seconds",
            "total_hours",
            "present_days",
            "current_streak",
            "longest_streak",
            "xp",
            "level",
            "group_joined_at_utc",
            "first_vc_at_utc",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.user_id,
                row.display_name,
                row.vc_count,
                row.total_seconds,
                round(row.total_seconds / 3600, 2),
                row.present_days,
                row.current_streak,
                row.longest_streak,
                row.xp,
                row.level,
                _fmt_date_or_none(row.group_joined_at) or "",
                _fmt_date_or_none(row.first_vc_at) or "",
            ]
        )
    return buf.getvalue().encode("utf-8")


def has_any_data(chat_id: int, user_id: int) -> bool:
    """True if this user has an attendance doc OR appears in any vc_sessions
    participant list for this chat — used by /user to distinguish "genuinely
    no data" from get_my_stats()'s zero-filled defaults (which it returns even
    for a completely unknown user_id)."""
    if _coll("user_attendance").find_one({"_id": f"{chat_id}:{user_id}"}):
        return True
    if _coll("vc_sessions").find_one({"chat_id": chat_id, "participants.user_id": user_id}):
        return True
    return False


# =============================================================================
# Moderation: warnings, per-chat warn settings, blocklist, filters
# (backing store for bot.py's Rose-style /warn, /ban, /mute, /blocklist, /filter)
# =============================================================================


def get_chat_mod_settings(chat_id: int) -> dict:
    """warn_limit / warn_mode for this chat, defaulting to 3 warns -> kick."""
    coll = _coll("chat_settings")
    doc = coll.find_one({"_id": chat_id}) or {}
    return {
        "warn_limit": int(doc.get("warn_limit", 3)),
        "warn_mode": str(doc.get("warn_mode", "kick")),
    }


def set_warn_limit(chat_id: int, limit: int) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"warn_limit": limit}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def set_warn_mode(chat_id: int, mode: str) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"warn_mode": mode}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def get_locked_types(chat_id: int) -> list[str]:
    """Content types currently locked (e.g. 'links') for /lock, /unlock, /locks."""
    coll = _coll("chat_settings")
    doc = coll.find_one({"_id": chat_id}) or {}
    return sorted(doc.get("locked_types", []))


def lock_type(chat_id: int, lock_type_name: str) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$addToSet": {"locked_types": lock_type_name}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def unlock_type(chat_id: int, lock_type_name: str) -> bool:
    coll = _coll("chat_settings")
    result = coll.update_one({"_id": chat_id}, {"$pull": {"locked_types": lock_type_name}})
    return result.modified_count > 0


def add_warning(
    chat_id: int,
    user_id: int,
    display_name: str,
    reason: str,
    by_id: int,
    by_name: str,
) -> tuple[int, list[dict]]:
    """Appends a warning and returns (new_count, all_entries)."""
    coll = _coll("warnings")
    doc_id = f"{chat_id}:{user_id}"
    entry = {
        "reason": (reason or "No reason given")[:512],
        "at": datetime.now(timezone.utc),
        "by_id": by_id,
        "by_name": by_name[:512],
    }
    doc = coll.find_one_and_update(
        {"_id": doc_id},
        {
            "$push": {"warns": entry},
            "$set": {"chat_id": chat_id, "user_id": user_id, "display_name": display_name[:512]},
        },
        upsert=True,
        return_document=True,
    )
    warns = doc.get("warns", [])
    return len(warns), warns


# -------------------- Warning history with watermark -------------------------

def get_warnings(chat_id: int, user_id: int) -> tuple[int, list[dict], str]:
    """(active_count, active_entries, stored_display_name). "Active" means issued after
    the last /resetwarn watermark (see reset_warnings) — this is what /warnlimit and
    /warns compare against, unchanged from before."""
    coll = _coll("warnings")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    if not doc:
        return 0, [], ""
    cleared_before = doc.get("cleared_before")
    warns = doc.get("warns", [])
    active = [w for w in warns if not cleared_before or w["at"] > cleared_before]
    return len(active), active, str(doc.get("display_name", ""))


def reset_warnings(chat_id: int, user_id: int) -> bool:
    """Clears a user's ACTIVE warning count back to zero WITHOUT deleting their warning
    history. Every warning ever issued (reason, date, who by) stays queryable forever via
    get_warning_history, for /mywarns and the bot-owner /user lookup.

    Implemented by advancing a 'cleared_before' watermark on the document rather than
    deleting the warns array: a warning counts as active only if it was issued after the
    watermark. Nothing is ever removed from `warns`, so /resetwarn is now purely a
    "start a fresh count" action, not a destructive one.

    Returns True if there was at least one active warning to clear (matches the old
    delete-based return semantics: False means "nothing to do")."""
    coll = _coll("warnings")
    doc_id = f"{chat_id}:{user_id}"
    doc = coll.find_one({"_id": doc_id})
    if not doc:
        return False
    cleared_before = doc.get("cleared_before")
    warns = doc.get("warns", [])
    active = [w for w in warns if not cleared_before or w["at"] > cleared_before]
    if not active:
        return False
    coll.update_one({"_id": doc_id}, {"$set": {"cleared_before": datetime.now(timezone.utc)}})
    return True


def get_warning_history(chat_id: int, user_id: int) -> tuple[list[dict], str]:
    """Every warning ever issued to this user in this chat — including ones cleared by a
    past /resetwarn — each annotated with an 'active' bool. Oldest first. Backs /mywarns
    and the bot-owner /user lookup; unlike get_warnings, this never hides cleared warnings."""
    coll = _coll("warnings")
    doc = coll.find_one({"_id": f"{chat_id}:{user_id}"})
    if not doc:
        return [], ""
    cleared_before = doc.get("cleared_before")
    warns = doc.get("warns", [])
    out = [{**w, "active": (not cleared_before or w["at"] > cleared_before)} for w in warns]
    return out, str(doc.get("display_name", ""))


def format_warning_history_html(label: str, history: list[dict]) -> str:
    """Shared renderer for /mywarns and /user: full warning history with date, reason,
    and whether each entry is still active or was cleared by a later /resetwarn."""
    safe_label = html.escape(label, quote=False)
    if not history:
        return f"✅ {safe_label} has no warning history."
    active_count = sum(1 for w in history if w.get("active"))
    lines = [
        f"⚠️ <b>Warning history — {safe_label}</b>",
        f"<i>{active_count} active / {len(history)} total</i>",
        "",
    ]
    for i, w in enumerate(history, start=1):
        at = w.get("at")
        when = at.strftime("%d %b %Y %H:%M UTC") if isinstance(at, datetime) else "?"
        reason = html.escape(w.get("reason") or "No reason given", quote=False)
        by = html.escape(w.get("by_name") or "", quote=False)
        status = "🟡 Active" if w.get("active") else "⚪ Cleared"
        lines.append(f"{i}. {status} — {when}")
        lines.append(f"   Reason: {reason}" + (f" (by {by})" if by else ""))
    return "\n".join(lines)


# ---------------------------------------------------------------------------

def get_blocklist(chat_id: int) -> tuple[list[str], str]:
    """(sorted words, mode). mode defaults to 'delete' (message removed, sender untouched)."""
    coll = _coll("blocklist")
    doc = coll.find_one({"_id": chat_id})
    if not doc:
        return [], "delete"
    return sorted(doc.get("words", [])), str(doc.get("mode", "delete"))


def add_blocklist_words(chat_id: int, words: list[str]) -> list[str]:
    """Adds lowercase, deduped words; returns the ones actually newly added."""
    coll = _coll("blocklist")
    existing, _mode = get_blocklist(chat_id)
    existing_set = set(existing)
    normalized = {w.strip().lower() for w in words if w.strip()}
    new_words = sorted(normalized - existing_set)
    if new_words:
        coll.update_one(
            {"_id": chat_id},
            {
                "$addToSet": {"words": {"$each": new_words}},
                "$setOnInsert": {"chat_id": chat_id, "mode": "delete"},
            },
            upsert=True,
        )
    return new_words


def remove_blocklist_words(chat_id: int, words: list[str]) -> list[str]:
    coll = _coll("blocklist")
    existing, _mode = get_blocklist(chat_id)
    existing_set = set(existing)
    normalized = {w.strip().lower() for w in words if w.strip()}
    to_remove = sorted(normalized & existing_set)
    if to_remove:
        coll.update_one({"_id": chat_id}, {"$pull": {"words": {"$in": to_remove}}})
    return to_remove


def set_blocklist_mode(chat_id: int, mode: str) -> None:
    coll = _coll("blocklist")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"mode": mode}, "$setOnInsert": {"chat_id": chat_id, "words": []}},
        upsert=True,
    )


def find_blocked_word(chat_id: int, text: str) -> str | None:
    """Case-insensitive EXACT WORD match — a blocklisted word only matches when it
    appears as a whole word (bounded by non-word characters or start/end of text), not
    as a substring of a longer word. E.g. blocklisting "bc" must NOT match "bcz" or
    "abcd". Uses explicit (?<!\\w)...(?!\\w) lookaround rather than \\b so multi-word
    blocklist phrases (e.g. "bad word") still match correctly across their internal
    space."""
    words, _mode = get_blocklist(chat_id)
    if not words:
        return None
    lowered = text.lower()
    for w in words:
        if re.search(rf"(?<!\w){re.escape(w)}(?!\w)", lowered):
            return w
    return None


# --- Filters (Rose's /filter: keyword -> canned auto-reply) -----------------
#
# Each filter is stored as a dict, not a plain string, so bold/italic/spacing and
# non-text content (photo/video/sticker/etc.) survive round-tripping intact:
#   {"type": "text"|"photo"|"video"|"sticker"|"document"|"animation"|"voice"|"audio"|"video_note",
#    "text": str | None,       # body for text, caption for media, None for sticker/video_note
#    "entities": [ {type, offset, length, url?, language?, custom_emoji_id?}, ... ],
#    "file_id": str | None}    # None for type "text"


def get_filters(chat_id: int) -> dict[str, dict]:
    """{keyword: filter_data}, keywords lowercase."""
    coll = _coll("filters")
    doc = coll.find_one({"_id": chat_id})
    if not doc:
        return {}
    return {str(k): v for k, v in (doc.get("filters") or {}).items()}


def add_filter(chat_id: int, keyword: str, filter_data: dict) -> None:
    coll = _coll("filters")
    key = keyword.strip().lower()
    coll.update_one(
        {"_id": chat_id},
        {"$set": {f"filters.{key}": filter_data, "chat_id": chat_id}},
        upsert=True,
    )


def remove_filter(chat_id: int, keyword: str) -> bool:
    coll = _coll("filters")
    key = keyword.strip().lower()
    result = coll.update_one({"_id": chat_id}, {"$unset": {f"filters.{key}": ""}})
    return result.modified_count > 0


def find_filter_match(chat_id: int, text: str) -> tuple[str, dict] | None:
    """Case-insensitive substring match against saved keywords; returns (keyword,
    filter_data) for the first match, or None."""
    filters_map = get_filters(chat_id)
    if not filters_map:
        return None
    lowered = text.lower()
    for keyword, filter_data in filters_map.items():
        if keyword in lowered:
            return keyword, filter_data
    return None


# --- Known-user memory (enables /warn @username etc. without a reply) -------


def record_known_user(chat_id: int, user_id: int, username: str | None, display_name: str) -> None:
    """Remember this user's username -> id mapping for this chat. Called passively on every
    message the bot sees in the group, so moderation commands can later resolve a bare
    @username without Telegram's getChat — which only works for usernames the bot has
    already been introduced to some other way, and normally fails for an arbitrary group
    member who has never DMed the bot."""
    coll = _coll("known_users")
    coll.update_one(
        {"_id": f"{chat_id}:{user_id}"},
        {
            "$set": {
                "chat_id": chat_id,
                "user_id": user_id,
                "username": (username or "").strip().lstrip("@").lower() or None,
                "display_name": display_name[:512],
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )


def resolve_username(chat_id: int, username: str) -> tuple[int, str] | None:
    """Look up a bare @username (case-insensitive) against usernames seen for this chat."""
    coll = _coll("known_users")
    key = username.strip().lstrip("@").lower()
    if not key:
        return None
    doc = coll.find_one({"chat_id": chat_id, "username": key})
    if not doc:
        return None
    return int(doc["user_id"]), str(doc.get("display_name") or f"@{key}")


# =============================================================================
# VC Topic Management (/addtopic, /topics, /deletetopic, /deletedtopics,
# /topicdone, /alltopics)
# =============================================================================


def _next_topic_serial(chat_id: int) -> int:
    """Atomically returns the next permanent serial number for this chat's topics.
    Backed by a dedicated counter document (not "highest existing id + 1", which would
    race under concurrent /addtopic calls and would also collide once every topic for a
    chat has been deleted) — findOneAndUpdate with $inc is atomic at the database level,
    so two people running /addtopic at the same instant can never get the same serial,
    and a serial is never reused even after its topic is deleted."""
    coll = _coll("topic_counters")
    doc = coll.find_one_and_update(
        {"_id": chat_id},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(doc["seq"])


def add_topic(chat_id: int, text: str, added_by_id: int, added_by_name: str) -> int:
    """Adds a new active topic with the next permanent serial number; returns that serial."""
    serial = _next_topic_serial(chat_id)
    coll = _coll("topics")
    coll.insert_one(
        {
            "_id": f"{chat_id}:{serial}",
            "chat_id": chat_id,
            "serial": serial,
            "text": text[:1000],
            "state": "active",
            "added_by_id": added_by_id,
            "added_by_name": added_by_name[:512],
            "added_at": datetime.now(timezone.utc),
            "votes": 0,
            "voter_ids": [],
        }
    )
    return serial


def get_topic(chat_id: int, serial: int) -> dict | None:
    return _coll("topics").find_one({"_id": f"{chat_id}:{serial}"})


def get_active_topics(chat_id: int) -> list[TopicRow]:
    """Sorted by votes (highest first), then serial ascending as a tiebreak — so the
    topic the group most wants to discuss next surfaces at the top of /topics."""
    coll = _coll("topics")
    cursor = coll.find({"chat_id": chat_id, "state": "active"}).sort(
        [("votes", DESCENDING), ("serial", ASCENDING)]
    )
    return [
        TopicRow(int(d["serial"]), str(d["text"]), str(d["state"]), int(d.get("votes", 0))) for d in cursor
    ]


def get_deleted_topics(chat_id: int) -> list[TopicRow]:
    coll = _coll("topics")
    cursor = coll.find({"chat_id": chat_id, "state": "deleted"}).sort("serial", ASCENDING)
    return [
        TopicRow(int(d["serial"]), str(d["text"]), str(d["state"]), int(d.get("votes", 0))) for d in cursor
    ]


def get_all_topics(chat_id: int) -> list[TopicRow]:
    """Sorted by serial (chronological record), unlike get_active_topics which sorts by votes."""
    coll = _coll("topics")
    cursor = coll.find({"chat_id": chat_id}).sort("serial", ASCENDING)
    return [
        TopicRow(int(d["serial"]), str(d["text"]), str(d["state"]), int(d.get("votes", 0))) for d in cursor
    ]


def mark_topic_done(chat_id: int, serial: int, by_id: int, by_name: str) -> bool:
    """Only succeeds if the topic exists and is currently active (per spec: '/topicdone
    marks an Active topic as Done') — returns False for an unknown serial or one that's
    already done/deleted, so the caller can give an accurate error instead of a silent no-op."""
    coll = _coll("topics")
    result = coll.update_one(
        {"_id": f"{chat_id}:{serial}", "state": "active"},
        {
            "$set": {
                "state": "done",
                "done_by_id": by_id,
                "done_by_name": by_name[:512],
                "done_at": datetime.now(timezone.utc),
            }
        },
    )
    return result.modified_count > 0


def delete_topic(chat_id: int, serial: int, by_id: int, by_name: str) -> bool:
    """Moves a topic (active or done) to deleted. Idempotent-safe: returns False if it's
    already deleted, rather than silently no-opping. The serial itself is never reused —
    that's guaranteed by _next_topic_serial, not by anything here."""
    coll = _coll("topics")
    result = coll.update_one(
        {"_id": f"{chat_id}:{serial}", "state": {"$ne": "deleted"}},
        {
            "$set": {
                "state": "deleted",
                "deleted_by_id": by_id,
                "deleted_by_name": by_name[:512],
                "deleted_at": datetime.now(timezone.utc),
            }
        },
    )
    return result.modified_count > 0


# =============================================================================
# Health check
# =============================================================================


def ping() -> bool:
    """True if the MongoDB connection is alive and responding."""
    if _client is None:
        return False
    try:
        _client.admin.command("ping")
        return True
    except Exception:
        return False


# =============================================================================
# Moderation audit log (/modlog)
# =============================================================================


class ModLogEntry(NamedTuple):
    action: str
    target_id: int
    target_name: str
    by_id: int
    by_name: str
    reason: str
    at: datetime


def log_mod_action(
    chat_id: int,
    action: str,
    target_id: int,
    target_name: str,
    by_id: int,
    by_name: str,
    reason: str = "",
) -> None:
    """Records one moderation action (ban/tban/unban/kick/mute/tmute/unmute/warn/resetwarn/
    auto-punishment) so /modlog can answer "who did what, to whom, when, why" — Telegram's
    own admin log isn't easily searchable and isn't visible to the bot at all."""
    coll = _coll("mod_log")
    coll.insert_one(
        {
            "chat_id": chat_id,
            "action": action,
            "target_id": target_id,
            "target_name": (target_name or "")[:512],
            "by_id": by_id,
            "by_name": (by_name or "")[:512],
            "reason": (reason or "")[:512],
            "at": datetime.now(timezone.utc),
        }
    )


def fetch_mod_log(chat_id: int, limit: int = 20) -> list[ModLogEntry]:
    coll = _coll("mod_log")
    cursor = coll.find({"chat_id": chat_id}).sort("at", DESCENDING).limit(limit)
    return [
        ModLogEntry(
            action=str(d["action"]),
            target_id=int(d["target_id"]),
            target_name=str(d.get("target_name", "")),
            by_id=int(d["by_id"]),
            by_name=str(d.get("by_name", "")),
            reason=str(d.get("reason", "")),
            at=d["at"],
        )
        for d in cursor
    ]


# =============================================================================
# Anti-spam settings: captcha (new-member verification) + flood control
# =============================================================================


def get_antispam_settings(chat_id: int) -> dict:
    """captcha_enabled / flood_limit / flood_window_seconds / flood_mode for this chat.
    flood_limit=0 means flood control is off (the default — opt-in, since it can be
    disruptive if set too aggressively for a chatty group)."""
    coll = _coll("chat_settings")
    doc = coll.find_one({"_id": chat_id}) or {}
    return {
        "captcha_enabled": bool(doc.get("captcha_enabled", False)),
        "flood_limit": int(doc.get("flood_limit", 0)),
        "flood_window_seconds": int(doc.get("flood_window_seconds", 10)),
        "flood_mode": str(doc.get("flood_mode", "mute")),
    }


def set_captcha_enabled(chat_id: int, enabled: bool) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"captcha_enabled": enabled}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


def set_flood_limit(chat_id: int, limit: int, window_seconds: int) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {
            "$set": {"flood_limit": limit, "flood_window_seconds": window_seconds},
            "$setOnInsert": {"monthly_reports": True},
        },
        upsert=True,
    )


def set_flood_mode(chat_id: int, mode: str) -> None:
    coll = _coll("chat_settings")
    coll.update_one(
        {"_id": chat_id},
        {"$set": {"flood_mode": mode}, "$setOnInsert": {"monthly_reports": True}},
        upsert=True,
    )


# =============================================================================
# Topic duplicate detection + upvoting
# =============================================================================


def find_active_topic_by_text(chat_id: int, text: str) -> TopicRow | None:
    """Case-insensitive, whitespace-normalized exact match against ACTIVE topics only —
    used to block duplicate /addtopic submissions. Deliberately simple exact matching
    rather than fuzzy/similarity matching, which would be fragile and surprising
    (e.g. rejecting genuinely different topics that merely share a few words)."""
    normalized = " ".join(text.strip().lower().split())
    if not normalized:
        return None
    coll = _coll("topics")
    for d in coll.find({"chat_id": chat_id, "state": "active"}):
        existing_norm = " ".join(str(d["text"]).strip().lower().split())
        if existing_norm == normalized:
            return TopicRow(int(d["serial"]), str(d["text"]), str(d["state"]), int(d.get("votes", 0)))
    return None


def upvote_topic(chat_id: int, serial: int, voter_id: int) -> tuple[bool, int]:
    """Returns (was_new_vote, new_vote_count). Idempotent per voter — each user can only
    upvote a given topic once. This is a single atomic find_one_and_update (filter
    requires voter_id NOT already in voter_ids) rather than a separate check-then-update
    — a plain "read voter_ids, then update" would leave a race window where two
    near-simultaneous calls from the same user (e.g. a rapid double-tap) could both pass
    the check before either write lands: $addToSet would still only add the id once, but
    $inc would fire twice, inflating the vote count for a single real vote."""
    coll = _coll("topics")
    doc_id = f"{chat_id}:{serial}"
    updated = coll.find_one_and_update(
        {"_id": doc_id, "state": "active", "voter_ids": {"$ne": voter_id}},
        {"$push": {"voter_ids": voter_id}, "$inc": {"votes": 1}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is not None:
        return True, int(updated.get("votes", 0))

    # No document matched: either the topic doesn't exist/isn't active, or this voter
    # already voted. Read-only lookup just to report an accurate current count.
    doc = coll.find_one({"_id": doc_id})
    if doc is None or doc.get("state") != "active":
        return False, 0
    return False, int(doc.get("votes", 0))


def award_engagement_xp(chat_id: int, user_id: int, display_name: str, amount: int) -> None:
    """Small XP grant for non-VC engagement (e.g. upvoting a topic). Feeds into the same
    user_attendance.xp field VC time does, so it shows up in /level, /mystats, and
    /xpleaderboard without a separate XP system."""
    coll = _coll("user_attendance")
    coll.update_one(
        {"_id": f"{chat_id}:{user_id}"},
        {
            "$inc": {"xp": amount},
            "$set": {"chat_id": chat_id, "user_id": user_id, "display_name": display_name[:512]},
            "$setOnInsert": {"present_days": 0, "current_streak": 0, "longest_streak": 0, "badges": {}},
        },
        upsert=True,
    )
