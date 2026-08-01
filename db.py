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
    # user_attendance: _id = "chat_id:user_id"
    _db.user_attendance.create_index([("chat_id", ASCENDING)])


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
    """+1 present day per user who stayed longer than the threshold in this call."""
    coll = _coll("user_attendance")
    threshold = present_threshold_sec()
    earned: list[AttendanceRow] = []
    for uid, name, sec in participants:
        if sec <= threshold:
            continue
        doc_id = f"{chat_id}:{uid}"
        doc = coll.find_one_and_update(
            {"_id": doc_id},
            {
                "$inc": {"present_days": 1},
                "$set": {
                    "chat_id": chat_id,
                    "user_id": uid,
                    "display_name": name[:512],
                },
            },
            upsert=True,
            return_document=True,
        )
        earned.append(AttendanceRow(uid, name, int(doc["present_days"])))
    return earned


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
