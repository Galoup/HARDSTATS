from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from zoneinfo import ZoneInfo

PARIS_TZ = ZoneInfo("Europe/Paris")


def now_paris() -> dt.datetime:
    return dt.datetime.now(tz=PARIS_TZ)


def iso_z(dt_: dt.datetime) -> str:
    """ISO string with timezone offset."""
    if dt_.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt_.isoformat(timespec="seconds")


def parse_hhmm(s: str) -> dt.time:
    s = (s or "").strip()
    parts = s.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid time (expected HH:MM): {s!r}")
    hh = int(parts[0])
    mm = int(parts[1])
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(f"Invalid time (expected HH:MM): {s!r}")
    return dt.time(hour=hh, minute=mm)


def parse_yyyy_mm_dd(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


def combine_paris(d: dt.date, t: dt.time) -> dt.datetime:
    return dt.datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=PARIS_TZ)


@dataclass(frozen=True)
class NextRuns:
    next_collect: dt.datetime
    next_recap: dt.datetime


def next_aligned_collect(now: dt.datetime, every_minutes: int, second: int = 10) -> dt.datetime:
    if every_minutes <= 0 or every_minutes > 24 * 60:
        raise ValueError("collect_minutes must be in 1..1440")
    if now.tzinfo is None:
        raise ValueError("now must be tz-aware")

    base = now.replace(second=0, microsecond=0)
    minute = base.minute
    bucket = (minute // every_minutes) * every_minutes
    current_bucket_start = base.replace(minute=bucket)
    if base == current_bucket_start:
        # if exactly on boundary, schedule next one
        current_bucket_start = base

    next_time = current_bucket_start + dt.timedelta(minutes=every_minutes)
    return next_time.replace(second=second, microsecond=0)


def next_recap_time(now: dt.datetime, recap_hhmm: dt.time, second: int = 15) -> dt.datetime:
    if now.tzinfo is None:
        raise ValueError("now must be tz-aware")
    today = now.date()
    target = combine_paris(today, recap_hhmm).replace(second=second, microsecond=0)
    if now < target:
        return target
    return (target + dt.timedelta(days=1)).replace(second=second, microsecond=0)


def floor_to_minute(ts: dt.datetime) -> dt.datetime:
    if ts.tzinfo is None:
        raise ValueError("ts must be tz-aware")
    return ts.replace(second=0, microsecond=0)
