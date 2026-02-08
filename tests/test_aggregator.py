import sqlite3

from ogame_stats import store
from ogame_stats.aggregator import daily_recap_delta, last_update_delta


def test_last_update_delta(tmp_path):
    db = tmp_path / "t.sqlite"
    con = store.connect(db)
    store.migrate(con)

    server_key = "fr:s1-fr"
    player_id = 42
    metric = "global"

    store.insert_snapshot_if_new(
        con,
        server_key=server_key,
        player_id=player_id,
        fetched_at="2025-01-01T00:00:00+01:00",
        api_timestamp=100,
        metric_type=metric,
        points=1000,
        rank=200,
    )
    store.insert_snapshot_if_new(
        con,
        server_key=server_key,
        player_id=player_id,
        fetched_at="2025-01-01T01:00:00+01:00",
        api_timestamp=200,
        metric_type=metric,
        points=1100,
        rank=180,
    )

    last, d = last_update_delta(con, server_key=server_key, player_id=player_id, metric_type=metric)
    assert last.api_timestamp == 200
    assert d.points == 100
    assert d.rank == 20  # 200->180 is +20 places


def test_daily_recap_delta(tmp_path):
    db = tmp_path / "t.sqlite"
    con = store.connect(db)
    store.migrate(con)

    server_key = "fr:s1-fr"
    player_id = 42
    metric = "economy"

    # start snapshot at 1000, end at 2000
    store.insert_snapshot_if_new(
        con,
        server_key=server_key,
        player_id=player_id,
        fetched_at="x",
        api_timestamp=1000,
        metric_type=metric,
        points=5000,
        rank=100,
    )
    store.insert_snapshot_if_new(
        con,
        server_key=server_key,
        player_id=player_id,
        fetched_at="y",
        api_timestamp=2000,
        metric_type=metric,
        points=6500,
        rank=90,
    )

    s, e, d = daily_recap_delta(
        con,
        server_key=server_key,
        player_id=player_id,
        metric_type=metric,
        start_ts=1000,
        end_ts=2000,
    )
    assert d.points == 1500
    assert d.rank == 10
