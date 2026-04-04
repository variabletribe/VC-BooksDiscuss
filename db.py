"""Persistence for VC stats: SQLite locally or PostgreSQL on Render (DATABASE_URL)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, NamedTuple

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    pass


class ChatSettings(Base):
    __tablename__ = "chat_settings"

    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    monthly_reports: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)


class VCSessionRow(Base):
    __tablename__ = "vc_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    duration_sec: Mapped[int] = mapped_column(Integer, nullable=False)

    participants: Mapped[list["VCParticipantRow"]] = relationship(back_populates="session")


class MonthlyReportSent(Base):
    """Tracks auto monthly posts so restarts on the 1st do not duplicate."""

    __tablename__ = "monthly_report_sent"
    __table_args__ = (UniqueConstraint("chat_id", "year", "month", name="uq_monthly_chat"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, index=True, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)


class VCParticipantRow(Base):
    __tablename__ = "vc_participants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[int] = mapped_column(ForeignKey("vc_sessions.id"), nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    display_name: Mapped[str] = mapped_column(String(512), nullable=False)
    estimated_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    session: Mapped["VCSessionRow"] = relationship(back_populates="participants")


class LeaderRow(NamedTuple):
    user_id: int
    display_name: str
    total_seconds: int


_engine = None
SessionLocal = None


def _normalize_database_url(url: str) -> str:
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def init_db() -> None:
    global _engine, SessionLocal
    url = os.environ.get("DATABASE_URL", "sqlite:///./vc_stats.db")
    url = _normalize_database_url(url)
    kwargs: dict = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    _engine = create_engine(url, **kwargs)
    SessionLocal = sessionmaker(_engine, expire_on_commit=False)
    Base.metadata.create_all(_engine)


def ensure_chat(chat_id: int, title: str | None = None) -> None:
    assert SessionLocal is not None
    with SessionLocal() as s:
        row = s.get(ChatSettings, chat_id)
        if row is None:
            s.add(ChatSettings(chat_id=chat_id, monthly_reports=True, title=title))
        elif title:
            row.title = title
        s.commit()


def set_monthly_reports(chat_id: int, enabled: bool) -> None:
    assert SessionLocal is not None
    with SessionLocal() as s:
        row = s.get(ChatSettings, chat_id)
        if row is None:
            s.add(ChatSettings(chat_id=chat_id, monthly_reports=enabled, title=None))
        else:
            row.monthly_reports = enabled
        s.commit()


def get_monthly_reports_enabled(chat_id: int) -> bool:
    assert SessionLocal is not None
    with SessionLocal() as s:
        row = s.get(ChatSettings, chat_id)
        if row is None:
            return True
        return row.monthly_reports


def list_chats_with_monthly_reports() -> list[int]:
    assert SessionLocal is not None
    with SessionLocal() as s:
        q = select(ChatSettings.chat_id).where(ChatSettings.monthly_reports.is_(True))
        return list(s.scalars(q).all())


def record_vc_session(
    chat_id: int,
    ended_at: datetime,
    duration_sec: int,
    started_at: datetime | None,
    participants: Iterable[tuple[int, str, int]],
) -> None:
    """participants: (user_id, display_name, estimated_seconds)."""
    assert SessionLocal is not None
    if ended_at.tzinfo is None:
        ended_at = ended_at.replace(tzinfo=timezone.utc)
    if started_at and started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    with SessionLocal() as s:
        row = VCSessionRow(
            chat_id=chat_id,
            started_at=started_at,
            ended_at=ended_at,
            duration_sec=duration_sec,
        )
        s.add(row)
        s.flush()
        for uid, name, est in participants:
            s.add(
                VCParticipantRow(
                    session_id=row.id,
                    user_id=uid,
                    display_name=name[:512],
                    estimated_seconds=est,
                )
            )
        s.commit()


def month_bounds_utc(year: int, month: int) -> tuple[datetime, datetime]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def fetch_leaderboard(
    chat_id: int,
    period_start: datetime,
    period_end_exclusive: datetime,
) -> list[LeaderRow]:
    assert SessionLocal is not None
    if period_start.tzinfo is None:
        period_start = period_start.replace(tzinfo=timezone.utc)
    if period_end_exclusive.tzinfo is None:
        period_end_exclusive = period_end_exclusive.replace(tzinfo=timezone.utc)

    uid = VCParticipantRow.user_id
    name = func.max(VCParticipantRow.display_name).label("dname")
    total = func.sum(VCParticipantRow.estimated_seconds).label("total")

    with SessionLocal() as s:
        q = (
            select(uid, name, total)
            .join(VCSessionRow, VCParticipantRow.session_id == VCSessionRow.id)
            .where(
                VCSessionRow.chat_id == chat_id,
                VCSessionRow.ended_at >= period_start,
                VCSessionRow.ended_at < period_end_exclusive,
            )
            .group_by(uid)
            .order_by(total.desc())
        )
        rows = s.execute(q).all()
        return [LeaderRow(int(r[0]), str(r[1]), int(r[2])) for r in rows]


def previous_calendar_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def monthly_report_already_sent(chat_id: int, year: int, month: int) -> bool:
    assert SessionLocal is not None
    with SessionLocal() as s:
        q = select(MonthlyReportSent.id).where(
            MonthlyReportSent.chat_id == chat_id,
            MonthlyReportSent.year == year,
            MonthlyReportSent.month == month,
        )
        return s.scalar(q) is not None


def mark_monthly_report_sent(chat_id: int, year: int, month: int) -> None:
    assert SessionLocal is not None
    with SessionLocal() as s:
        s.add(MonthlyReportSent(chat_id=chat_id, year=year, month=month))
        try:
            s.commit()
        except IntegrityError:
            s.rollback()


def fetch_month_leaderboard(chat_id: int, year: int, month: int) -> list[LeaderRow]:
    start, end = month_bounds_utc(year, month)
    return fetch_leaderboard(chat_id, start, end)
