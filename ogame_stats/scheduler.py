from __future__ import annotations

import datetime as dt
import logging
import time

from .utils_time import now_paris, parse_hhmm, next_aligned_collect, next_recap_time

log = logging.getLogger(__name__)


def sleep_until(target: dt.datetime) -> None:
    while True:
        now = now_paris()
        if now >= target:
            return
        sec = (target - now).total_seconds()
        time.sleep(min(30.0, max(1.0, sec)))


def run_loop(*, collect_fn, recap_fn, alerts_fn, collect_minutes: int, recap_time: str, grace_s: int = 120) -> None:
    recap_t = parse_hhmm(recap_time)

    while True:
        now = now_paris()
        next_collect = next_aligned_collect(now, collect_minutes)
        next_recap = next_recap_time(now, recap_t)

        prev_collect = next_collect - dt.timedelta(minutes=collect_minutes)
        prev_recap = next_recap - dt.timedelta(days=1)

        # Run if we are within a small grace window after the intended boundary.
        due_collect = dt.timedelta(0) <= (now - prev_collect) <= dt.timedelta(seconds=grace_s)
        due_recap = dt.timedelta(0) <= (now - prev_recap) <= dt.timedelta(seconds=grace_s)

        # If both due, do collect first (fresh snapshot), then recap.
        if due_collect:
            collect_fn()
            alerts_fn()
        if due_recap:
            recap_fn()

        now = now_paris()
        next_collect = next_aligned_collect(now, collect_minutes)
        next_recap = next_recap_time(now, recap_t)
        wake = next_collect if next_collect < next_recap else next_recap

        log.info("Next wake: %s (collect=%s recap=%s)", wake.isoformat(timespec="seconds"), next_collect.isoformat(timespec="seconds"), next_recap.isoformat(timespec="seconds"))
        sleep_until(wake)
