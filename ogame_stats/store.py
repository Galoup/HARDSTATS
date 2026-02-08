from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotRow:
    id: int
    server_key: str
    player_id: int
    fetched_at: str
    api_timestamp: int
    metric_type: str
    points: int
    rank: int


def connect(sqlite_path: Path) -> sqlite3.Connection:
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(sqlite_path))
    con.row_factory = sqlite3.Row
    return con


def migrate(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS servers(
          server_key TEXT PRIMARY KEY,
          community TEXT,
          server_id TEXT,
          name TEXT,
          base_url TEXT,
          meta_json TEXT,
          created_at TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS players_cache(
          server_key TEXT,
          fetched_at TEXT,
          api_timestamp INTEGER,
          player_id INTEGER,
          player_name TEXT,
          status TEXT,
          alliance_id INTEGER,
          name_norm TEXT,
          PRIMARY KEY(server_key, player_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_players_cache_name ON players_cache(server_key, name_norm)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          server_key TEXT,
          player_id INTEGER,
          fetched_at TEXT,
          api_timestamp INTEGER,
          metric_type TEXT,
          points INTEGER,
          rank INTEGER
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshots_key ON snapshots(server_key, player_id, metric_type, api_timestamp)"
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          server_key TEXT,
          player_id INTEGER,
          category TEXT,
          created_at TEXT,
          api_timestamp INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_log_cat ON alerts_log(server_key, player_id, category, created_at)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs_state(
          job_key TEXT PRIMARY KEY,
          value_json TEXT,
          updated_at TEXT
        )
        """
    )

    con.commit()


def upsert_server(
    con: sqlite3.Connection,
    *,
    server_key: str,
    community: str,
    server_id: str,
    name: str,
    base_url: str,
    meta: dict[str, Any] | None,
    created_at: str,
) -> None:
    meta_json = json.dumps(meta or {}, ensure_ascii=False)
    con.execute(
        """
        INSERT INTO servers(server_key, community, server_id, name, base_url, meta_json, created_at)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(server_key) DO UPDATE SET
          community=excluded.community,
          server_id=excluded.server_id,
          name=excluded.name,
          base_url=excluded.base_url,
          meta_json=excluded.meta_json
        """,
        (server_key, community, server_id, name, base_url, meta_json, created_at),
    )
    con.commit()


def name_norm(name: str) -> str:
    return (name or "").strip().casefold()


def replace_players_cache(
    con: sqlite3.Connection,
    *,
    server_key: str,
    fetched_at: str,
    api_timestamp: int,
    players: list[tuple[int, str, str | None, int | None]],
) -> None:
    # Replace-all strategy keeps it simple and avoids stale players.
    con.execute("DELETE FROM players_cache WHERE server_key=?", (server_key,))
    con.executemany(
        """
        INSERT INTO players_cache(server_key, fetched_at, api_timestamp, player_id, player_name, status, alliance_id, name_norm)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        [
            (server_key, fetched_at, api_timestamp, pid, pname, status, aid, name_norm(pname))
            for (pid, pname, status, aid) in players
        ],
    )
    con.commit()


def get_player_id_by_name(con: sqlite3.Connection, *, server_key: str, player_name: str) -> int | None:
    nn = name_norm(player_name)
    row = con.execute(
        "SELECT player_id FROM players_cache WHERE server_key=? AND name_norm=? LIMIT 1",
        (server_key, nn),
    ).fetchone()
    return int(row[0]) if row else None


def list_player_names(con: sqlite3.Connection, *, server_key: str, limit: int = 10) -> list[str]:
    rows = con.execute(
        "SELECT player_name FROM players_cache WHERE server_key=? ORDER BY player_name LIMIT ?",
        (server_key, limit),
    ).fetchall()
    return [str(r[0]) for r in rows]


def get_players_cache_fetched_at(con: sqlite3.Connection, *, server_key: str) -> tuple[str, int] | None:
    row = con.execute(
        "SELECT fetched_at, api_timestamp FROM players_cache WHERE server_key=? LIMIT 1",
        (server_key,),
    ).fetchone()
    if not row:
        return None
    return (str(row[0]), int(row[1]))


def get_latest_snapshot(con: sqlite3.Connection, *, server_key: str, player_id: int, metric_type: str) -> SnapshotRow | None:
    row = con.execute(
        """
        SELECT id, server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank
        FROM snapshots
        WHERE server_key=? AND player_id=? AND metric_type=?
        ORDER BY api_timestamp DESC
        LIMIT 1
        """,
        (server_key, player_id, metric_type),
    ).fetchone()
    if not row:
        return None
    return SnapshotRow(**dict(row))


def insert_snapshot_if_new(
    con: sqlite3.Connection,
    *,
    server_key: str,
    player_id: int,
    fetched_at: str,
    api_timestamp: int,
    metric_type: str,
    points: int,
    rank: int,
) -> bool:
    last = get_latest_snapshot(con, server_key=server_key, player_id=player_id, metric_type=metric_type)
    if last and last.api_timestamp == api_timestamp:
        return False

    con.execute(
        """
        INSERT INTO snapshots(server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank)
        VALUES(?,?,?,?,?,?,?)
        """,
        (server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank),
    )
    con.commit()
    return True


def fetch_two_latest(con: sqlite3.Connection, *, server_key: str, player_id: int, metric_type: str) -> list[SnapshotRow]:
    rows = con.execute(
        """
        SELECT id, server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank
        FROM snapshots
        WHERE server_key=? AND player_id=? AND metric_type=?
        ORDER BY api_timestamp DESC
        LIMIT 2
        """,
        (server_key, player_id, metric_type),
    ).fetchall()
    return [SnapshotRow(**dict(r)) for r in rows]


def fetch_snapshot_at_or_before(
    con: sqlite3.Connection,
    *,
    server_key: str,
    player_id: int,
    metric_type: str,
    api_timestamp_max: int,
) -> SnapshotRow | None:
    row = con.execute(
        """
        SELECT id, server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank
        FROM snapshots
        WHERE server_key=? AND player_id=? AND metric_type=? AND api_timestamp<=?
        ORDER BY api_timestamp DESC
        LIMIT 1
        """,
        (server_key, player_id, metric_type, api_timestamp_max),
    ).fetchone()
    return SnapshotRow(**dict(row)) if row else None


def fetch_snapshot_near_or_before(con: sqlite3.Connection, *, server_key: str, player_id: int, metric_type: str, target_ts: int) -> SnapshotRow | None:
    # We prefer <= target (no future leakage). If missing, fallback to nearest after.
    row = fetch_snapshot_at_or_before(
        con,
        server_key=server_key,
        player_id=player_id,
        metric_type=metric_type,
        api_timestamp_max=target_ts,
    )
    if row:
        return row

    r2 = con.execute(
        """
        SELECT id, server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank
        FROM snapshots
        WHERE server_key=? AND player_id=? AND metric_type=? AND api_timestamp>?
        ORDER BY api_timestamp ASC
        LIMIT 1
        """,
        (server_key, player_id, metric_type, target_ts),
    ).fetchone()
    return SnapshotRow(**dict(r2)) if r2 else None


def fetch_series_last_days(con: sqlite3.Connection, *, server_key: str, player_id: int, metric_type: str, min_ts: int) -> list[SnapshotRow]:
    rows = con.execute(
        """
        SELECT id, server_key, player_id, fetched_at, api_timestamp, metric_type, points, rank
        FROM snapshots
        WHERE server_key=? AND player_id=? AND metric_type=? AND api_timestamp>=?
        ORDER BY api_timestamp ASC
        """,
        (server_key, player_id, metric_type, min_ts),
    ).fetchall()
    return [SnapshotRow(**dict(r)) for r in rows]


def get_jobs_state(con: sqlite3.Connection, job_key: str) -> dict[str, Any] | None:
    row = con.execute("SELECT value_json FROM jobs_state WHERE job_key=?", (job_key,)).fetchone()
    if not row:
        return None
    try:
        return json.loads(str(row[0]))
    except Exception:
        return None


def set_jobs_state(con: sqlite3.Connection, job_key: str, value: dict[str, Any], updated_at: str) -> None:
    value_json = json.dumps(value, ensure_ascii=False)
    con.execute(
        """
        INSERT INTO jobs_state(job_key, value_json, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(job_key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at
        """,
        (job_key, value_json, updated_at),
    )
    con.commit()


def log_alert(con: sqlite3.Connection, *, server_key: str, player_id: int, category: str, created_at: str, api_timestamp: int) -> None:
    con.execute(
        "INSERT INTO alerts_log(server_key, player_id, category, created_at, api_timestamp) VALUES(?,?,?,?,?)",
        (server_key, player_id, category, created_at, api_timestamp),
    )
    con.commit()


def last_alert_time(con: sqlite3.Connection, *, server_key: str, player_id: int, category: str) -> tuple[str, int] | None:
    row = con.execute(
        """
        SELECT created_at, api_timestamp
        FROM alerts_log
        WHERE server_key=? AND player_id=? AND category=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (server_key, player_id, category),
    ).fetchone()
    if not row:
        return None
    return (str(row[0]), int(row[1]))


def list_alerts(con: sqlite3.Connection, *, server_key: str, player_id: int, since_iso: str, limit: int = 50) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT category, created_at, api_timestamp
        FROM alerts_log
        WHERE server_key=? AND player_id=? AND created_at>=?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (server_key, player_id, since_iso, limit),
    ).fetchall()
    return [dict(r) for r in rows]
