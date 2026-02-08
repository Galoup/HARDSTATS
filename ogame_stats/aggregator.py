from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

from . import store

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Delta:
    points: int
    rank: int


def last_update_delta(con, *, server_key: str, player_id: int, metric_type: str) -> tuple[store.SnapshotRow | None, Delta | None]:
    rows = store.fetch_two_latest(con, server_key=server_key, player_id=player_id, metric_type=metric_type)
    if len(rows) < 2:
        return (rows[0] if rows else None), None
    last, prev = rows[0], rows[1]
    return last, Delta(points=last.points - prev.points, rank=prev.rank - last.rank)


def rolling_24h_delta(con, *, server_key: str, player_id: int, metric_type: str) -> tuple[store.SnapshotRow | None, Delta | None]:
    last = store.get_latest_snapshot(con, server_key=server_key, player_id=player_id, metric_type=metric_type)
    if not last:
        return None, None

    target = last.api_timestamp - 24 * 3600
    base = store.fetch_snapshot_near_or_before(con, server_key=server_key, player_id=player_id, metric_type=metric_type, target_ts=target)
    if not base or base.api_timestamp == last.api_timestamp:
        return last, None

    return last, Delta(points=last.points - base.points, rank=base.rank - last.rank)


def daily_recap_delta(
    con,
    *,
    server_key: str,
    player_id: int,
    metric_type: str,
    start_ts: int,
    end_ts: int,
) -> tuple[store.SnapshotRow | None, store.SnapshotRow | None, Delta | None]:
    end_row = store.fetch_snapshot_at_or_before(
        con,
        server_key=server_key,
        player_id=player_id,
        metric_type=metric_type,
        api_timestamp_max=end_ts,
    )
    start_row = store.fetch_snapshot_at_or_before(
        con,
        server_key=server_key,
        player_id=player_id,
        metric_type=metric_type,
        api_timestamp_max=start_ts,
    )

    if not end_row or not start_row or start_row.api_timestamp == end_row.api_timestamp:
        return start_row, end_row, None

    return start_row, end_row, Delta(points=end_row.points - start_row.points, rank=start_row.rank - end_row.rank)


def weekly_series(con, *, server_key: str, player_id: int, metric_type: str, end_ts: int) -> list[store.SnapshotRow]:
    min_ts = end_ts - 7 * 24 * 3600
    return store.fetch_series_last_days(con, server_key=server_key, player_id=player_id, metric_type=metric_type, min_ts=min_ts)


def mean_abs_delta(points_series: list[int]) -> float:
    if len(points_series) < 2:
        return 0.0
    deltas = [abs(points_series[i] - points_series[i - 1]) for i in range(1, len(points_series))]
    return sum(deltas) / max(1, len(deltas))
