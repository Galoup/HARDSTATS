from __future__ import annotations

import argparse
import datetime as dt
import difflib
import logging
import sys
from pathlib import Path
from typing import Any

from . import __version__, aggregator, store
from .config import load_config, write_example_config
from .discord_webhook import DiscordWebhook
from .ogame_api import ApiError, LobbyClient, METRIC_TO_TYPE_ID, OGameApiClient, PlayerNotFound, UniverseNotFound
from .publisher import find_latest_report, publish_report
from .renderer import render_report
from .utils_time import PARIS_TZ, combine_paris, iso_z, now_paris, parse_hhmm, parse_yyyy_mm_dd
from .utils_url import join_public_url

log = logging.getLogger(__name__)


def setup_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _server_id_from_base_url(base_url: str) -> str | None:
    # https://s123-fr.ogame.gameforge.com -> s123-fr
    try:
        host = base_url.split("//", 1)[-1].split("/", 1)[0]
        if host.endswith(".ogame.gameforge.com"):
            return host.split(".", 1)[0]
    except Exception:
        return None
    return None


def resolve_server_base_url(*, community: str, server_id: str, base_url_override: str) -> tuple[str, str]:
    if base_url_override:
        sid = _server_id_from_base_url(base_url_override) or server_id
        if not sid:
            raise UniverseNotFound("base_url provided but server_id could not be derived")
        return sid.lower(), base_url_override.rstrip("/")

    if not server_id:
        raise UniverseNotFound("universe.server_id is required (use list-universes to find it)")

    sid = server_id.strip().lower()
    return sid, f"https://{sid}.ogame.gameforge.com"


def cmd_init(args: argparse.Namespace) -> int:
    p = write_example_config(args.config, force=args.force)
    print(str(p))
    return 0


def cmd_list_universes(args: argparse.Namespace) -> int:
    client = LobbyClient()
    servers = client.list_servers_for_community(args.community)

    if not servers:
        print(f"No servers found for community={args.community!r}")
        return 2

    # simple table
    print(f"Universes for community={args.community} (count={len(servers)})")
    print("serverId\tname\tcommunity\tlanguage\tbase_url\tmeta_keys(sample)")
    for s in servers:
        keys = ""
        try:
            keys = ",".join(sorted(list(s.raw.keys()))[:8])
        except Exception:
            keys = ""
        print(f"{s.server_id}\t{s.name}\t{s.community}\t{s.language}\t{s.base_url}\t{keys}")
    return 0


def _fetch_universe_name(lobby: LobbyClient, *, community: str, server_id: str) -> tuple[str, dict[str, Any] | None]:
    servers = lobby.list_servers_for_community(community)
    for s in servers:
        if s.server_id.lower() == server_id.lower():
            return s.name, s.raw
    return server_id, None


def _ensure_players_cache(con, *, api: OGameApiClient, server_key: str, force: bool = False) -> None:
    # Players list updates daily; cache it and refresh if empty.
    cached = store.get_players_cache_fetched_at(con, server_key=server_key)
    if cached and not force:
        fetched_at_iso, _ts = cached
        try:
            fetched_at = dt.datetime.fromisoformat(fetched_at_iso)
            # refresh roughly daily (players.xml updates daily)
            if (now_paris() - fetched_at).total_seconds() < 20 * 3600:
                return
        except Exception:
            # if parsing fails, keep cache (avoid constant refresh loops)
            return

    ts, players = api.fetch_players()
    fetched_at = iso_z(now_paris())
    store.replace_players_cache(
        con,
        server_key=server_key,
        fetched_at=fetched_at,
        api_timestamp=ts,
        players=[(p.player_id, p.name, p.status, p.alliance_id) for p in players],
    )
    log.info("Players cache refreshed: %s players (api_timestamp=%s)", len(players), ts)


def _resolve_player_id(con, *, api: OGameApiClient, server_key: str, player_name: str) -> int:
    pid = store.get_player_id_by_name(con, server_key=server_key, player_name=player_name)
    if pid:
        return pid

    _ensure_players_cache(con, api=api, server_key=server_key, force=True)
    pid = store.get_player_id_by_name(con, server_key=server_key, player_name=player_name)
    if pid:
        return pid

    # fuzzy suggestions
    rows = con.execute(
        "SELECT player_name FROM players_cache WHERE server_key=?",
        (server_key,),
    ).fetchall()
    names = [str(r[0]) for r in rows]
    suggestions = difflib.get_close_matches(player_name, names, n=5, cutoff=0.6)
    msg = f"Player not found: {player_name!r}"
    if suggestions:
        msg += f". Did you mean: {', '.join(suggestions)}?"
    raise PlayerNotFound(msg)


def _find_player_in_highscore_block(entries, player_id: int):
    for e in entries:
        if e.player_id == player_id:
            return e
    return None


def _fetch_player_highscore(api: OGameApiClient, *, type_id: int, player_id: int, hint_rank: int | None) -> tuple[int, int, int]:
    # Returns: (api_timestamp, points, rank)
    # Strategy: start near hint_rank, expand window; fallback to sequential scan.
    window = 500

    def try_window(start_offset: int) -> tuple[int, int, int] | None:
        # OGame API uses 0-based offsets for start/end.
        start = max(0, start_offset)
        end = start + window - 1
        snap = api.fetch_highscore_block(type_id=type_id, start=start, end=end)
        hit = _find_player_in_highscore_block(snap.entries, player_id)
        if hit:
            return snap.api_timestamp, hit.points, hit.rank
        return None

    if hint_rank and hint_rank > 0:
        for span in (250, 1000, 3000):
            start = max(0, (hint_rank - span) - 1)
            for offset in (0, span, span * 2):
                res = try_window(start + offset)
                if res:
                    return res

    # Sequential scan from 1 in blocks until found.
    start = 0
    while True:
        snap = api.fetch_highscore_block(type_id=type_id, start=start, end=start + window - 1)
        hit = _find_player_in_highscore_block(snap.entries, player_id)
        if hit:
            return snap.api_timestamp, hit.points, hit.rank
        if snap.total and start >= snap.total:
            break
        if not snap.entries:
            break
        start += window
        if start > 200000:
            break

    raise PlayerNotFound(f"Player id {player_id} not found in highscore type={type_id}")


def cmd_collect(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    cfg.data_dir.mkdir(parents=True, exist_ok=True)

    server_id, base_url = resolve_server_base_url(
        community=cfg.community,
        server_id=cfg.server_id,
        base_url_override=cfg.base_url_override,
    )
    server_key = f"{cfg.community}:{server_id}"

    con = store.connect(cfg.sqlite_path)
    store.migrate(con)

    lobby = LobbyClient()
    universe_name, meta = _fetch_universe_name(lobby, community=cfg.community, server_id=server_id)
    store.upsert_server(
        con,
        server_key=server_key,
        community=cfg.community,
        server_id=server_id,
        name=universe_name,
        base_url=base_url,
        meta=meta,
        created_at=iso_z(now_paris()),
    )

    api = OGameApiClient(base_url)
    _ensure_players_cache(con, api=api, server_key=server_key)

    player_id = _resolve_player_id(con, api=api, server_key=server_key, player_name=cfg.player_name)
    fetched_at = iso_z(now_paris())

    inserted = 0
    for metric_type, type_id in METRIC_TO_TYPE_ID.items():
        last = store.get_latest_snapshot(con, server_key=server_key, player_id=player_id, metric_type=metric_type)
        hint_rank = last.rank if last else None

        api_ts, points, rank = _fetch_player_highscore(api, type_id=type_id, player_id=player_id, hint_rank=hint_rank)
        ok = store.insert_snapshot_if_new(
            con,
            server_key=server_key,
            player_id=player_id,
            fetched_at=fetched_at,
            api_timestamp=api_ts,
            metric_type=metric_type,
            points=points,
            rank=rank,
        )
        if ok:
            inserted += 1

    log.info("Collect done: inserted=%s metrics (server=%s player=%s id=%s)", inserted, server_id, cfg.player_name, player_id)
    return 0


def _recap_window_ts(*, report_date: dt.date, recap_time: str) -> tuple[int, int]:
    t = parse_hhmm(recap_time)
    end_dt = combine_paris(report_date, t)
    start_dt = end_dt - dt.timedelta(days=1)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def cmd_render(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    server_id, base_url = resolve_server_base_url(
        community=cfg.community,
        server_id=cfg.server_id,
        base_url_override=cfg.base_url_override,
    )
    server_key = f"{cfg.community}:{server_id}"

    con = store.connect(cfg.sqlite_path)
    store.migrate(con)

    api = OGameApiClient(base_url)
    _ensure_players_cache(con, api=api, server_key=server_key)
    player_id = _resolve_player_id(con, api=api, server_key=server_key, player_name=cfg.player_name)

    lobby = LobbyClient()
    universe_name, _meta = _fetch_universe_name(lobby, community=cfg.community, server_id=server_id)

    report_date = parse_yyyy_mm_dd(args.date) if args.date else now_paris().date()
    start_ts, end_ts = _recap_window_ts(report_date=report_date, recap_time=cfg.recap_time)

    since_iso = iso_z(now_paris() - dt.timedelta(days=7))
    alerts = store.list_alerts(con, server_key=server_key, player_id=player_id, since_iso=since_iso)

    safe_player = "".join(c for c in cfg.player_name if c.isalnum() or c in ("-", "_")) or "player"
    out_name = f"report_{report_date.isoformat()}_{server_id}_{safe_player}.html"
    out_path = (cfg.out_dir / out_name).resolve()

    template_path = (Path(__file__).resolve().parent.parent / "templates" / "report_template.html").resolve()
    render_report(
        con=con,
        template_path=template_path,
        out_path=out_path,
        server_id=server_id,
        universe_name=universe_name,
        player_name=cfg.player_name,
        server_key=server_key,
        player_id=player_id,
        report_date=report_date,
        recap_start_ts=start_ts,
        recap_end_ts=end_ts,
        public_base_url=cfg.public_base_url,
        alerts=alerts,
    )

    store.set_jobs_state(
        con,
        "render",
        {"last_report_path": str(out_path), "last_report_name": out_name, "date": report_date.isoformat()},
        updated_at=iso_z(now_paris()),
    )

    print(str(out_path))
    return 0


def cmd_publish(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    cfg.publish_dir.mkdir(parents=True, exist_ok=True)

    report_path: Path | None = None
    if args.report:
        rp = Path(args.report)
        report_path = rp if rp.is_absolute() else (cfg.base_dir / rp).resolve()
    else:
        # Prefer the last rendered report if we have it.
        con = store.connect(cfg.sqlite_path)
        store.migrate(con)
        st = store.get_jobs_state(con, "render") or {}
        last_path = st.get("last_report_path")
        if last_path:
            p = Path(str(last_path))
            if p.exists() and p.is_file():
                report_path = p
        if report_path is None:
            report_path = find_latest_report(cfg.out_dir)

    if report_path is None:
        log.error("No report found in out_dir=%s", cfg.out_dir)
        return 2

    res = publish_report(
        report_path=report_path,
        publish_dir=cfg.publish_dir,
        latest_filename=cfg.latest_filename,
        keep_history=cfg.keep_history,
        generate_index=not bool(args.no_index),
    )

    print(str(res.published_latest))
    return 0


def _ogame_vibe(delta_points: int) -> str:
    if delta_points > 0:
        return "GG ✅ ça mine sec ⛏️😎"
    if delta_points < 0:
        return "aie 😬 ça pique, cerveau en feu 🧠🔥"
    return "RAS, on tient la ligne."


def _make_recap_embed(*, universe: str, player: str, date: dt.date, snapshot_hhmm: str, period: str, summary: str, fields: list[dict[str, Any]], report_links: list[str]) -> dict[str, Any]:
    desc = f"Snapshot: {snapshot_hhmm} • Période: {period}"
    if report_links:
        desc += "\n" + "\n".join(report_links)

    return {
        "embeds": [
            {
                "title": f"🧾 OGame FR • {universe} • Récap du {date.strftime('%d/%m/%Y')} — {player}",
                "description": desc,
                "color": 0x2E8B57,
                "fields": fields,
                "footer": {"text": "Public API only • no login • no botting"},
            }
        ]
    }


def cmd_post_recap(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)

    server_id, base_url = resolve_server_base_url(
        community=cfg.community,
        server_id=cfg.server_id,
        base_url_override=cfg.base_url_override,
    )
    server_key = f"{cfg.community}:{server_id}"

    con = store.connect(cfg.sqlite_path)
    store.migrate(con)

    api = OGameApiClient(base_url)
    _ensure_players_cache(con, api=api, server_key=server_key)
    player_id = _resolve_player_id(con, api=api, server_key=server_key, player_name=cfg.player_name)

    lobby = LobbyClient()
    universe_name, _meta = _fetch_universe_name(lobby, community=cfg.community, server_id=server_id)

    report_date = now_paris().date()
    start_ts, end_ts = _recap_window_ts(report_date=report_date, recap_time=cfg.recap_time)

    metrics_main = ["global", "economy", "research", "military"]
    deltas: dict[str, int] = {}
    deltas_rank: dict[str, int] = {}
    snapshot_hhmm = "--:--"

    for m in metrics_main:
        _s, e, d = aggregator.daily_recap_delta(con, server_key=server_key, player_id=player_id, metric_type=m, start_ts=start_ts, end_ts=end_ts)
        if e:
            snapshot_hhmm = dt.datetime.fromtimestamp(e.api_timestamp, tz=PARIS_TZ).strftime("%H:%M")
        deltas[m] = d.points if d else 0
        deltas_rank[m] = d.rank if d else 0

    summary = f"📌 🌍 Global {_signed(deltas['global'])} | 💰 Éco {_signed(deltas['economy'])} | 🧠 Rech {_signed(deltas['research'])} | ⚔️ Mili {_signed(deltas['military'])}"

    fields: list[dict[str, Any]] = [
        {"name": "Résumé", "value": summary + "\n" + _ogame_vibe(deltas["global"]), "inline": False},
        {"name": "🌍 Global", "value": f"Points: {_signed(deltas['global'])}\nRang: {_signed(deltas_rank['global'])}", "inline": True},
        {"name": "💰 Économie", "value": f"Points: {_signed(deltas['economy'])}\nRang: {_signed(deltas_rank['economy'])}", "inline": True},
        {"name": "🧠 Recherche", "value": f"Points: {_signed(deltas['research'])}\nRang: {_signed(deltas_rank['research'])}", "inline": True},
        {"name": "⚔️ Militaire", "value": f"Points: {_signed(deltas['military'])}\nRang: {_signed(deltas_rank['military'])}", "inline": True},
    ]

    # Military detail
    detail = []
    for m, label in (
        ("military_built", "Built"),
        ("military_destroyed", "Destroyed"),
        ("military_lost", "Lost"),
    ):
        _s, _e, d = aggregator.daily_recap_delta(con, server_key=server_key, player_id=player_id, metric_type=m, start_ts=start_ts, end_ts=end_ts)
        if d:
            detail.append(f"{label}: {_signed(d.points)}")
    if detail:
        fields.append({"name": "Mili Détail", "value": " • ".join(detail), "inline": False})

    # TOP/FLOP based on last update rank moves
    moves = []
    for m, label in (
        ("global", "🌍 Global"),
        ("economy", "💰 Éco"),
        ("research", "🧠 Rech"),
        ("military", "⚔️ Mili"),
    ):
        _last, d = aggregator.last_update_delta(con, server_key=server_key, player_id=player_id, metric_type=m)
        if d:
            moves.append((d.rank, label, d.points))

    if moves:
        best = max(moves, key=lambda x: x[0])
        worst = min(moves, key=lambda x: x[0])
        fields.append(
            {
                "name": "TOP / FLOP (dernière maj)",
                "value": f"TOP: {best[1]} (+{best[0]} places)\nFLOP: {worst[1]} ({worst[0]} places)",
                "inline": False,
            }
        )
    else:
        fields.append({"name": "TOP / FLOP (dernière maj)", "value": "Pas assez de data (encore).", "inline": False})

    # Optional report link or attachment.
    report_links: list[str] = []
    attachment_path: Path | None = None

    # Always render the report file locally.
    safe_player = safe_player_name(cfg.player_name)
    out_name = f"report_{report_date.isoformat()}_{server_id}_{safe_player}.html"
    out_path = (cfg.out_dir / out_name).resolve()
    template_path = (Path(__file__).resolve().parent.parent / "templates" / "report_template.html").resolve()
    since_iso = iso_z(now_paris() - dt.timedelta(days=7))
    alerts = store.list_alerts(con, server_key=server_key, player_id=player_id, since_iso=since_iso)
    render_report(
        con=con,
        template_path=template_path,
        out_path=out_path,
        server_id=server_id,
        universe_name=universe_name,
        player_name=cfg.player_name,
        server_key=server_key,
        player_id=player_id,
        report_date=report_date,
        recap_start_ts=start_ts,
        recap_end_ts=end_ts,
        public_base_url=cfg.public_base_url,
        alerts=alerts,
    )

    if cfg.public_base_url:
        # Publish locally to docs/ (or user-defined publish_dir) so GitHub Pages can serve it.
        try:
            publish_report(
                report_path=out_path,
                publish_dir=cfg.publish_dir,
                latest_filename=cfg.latest_filename,
                keep_history=cfg.keep_history,
                generate_index=True,
            )
        except Exception as e:  # noqa: BLE001
            log.error("Publish failed (will still post links): %s", e)

        latest_clean = join_public_url(cfg.public_base_url, cfg.latest_filename) + "?theme=clean"
        latest_neon = join_public_url(cfg.public_base_url, cfg.latest_filename) + "?theme=neon"

        val = f"🧼 Clean: {latest_clean}\n✨ Neon: {latest_neon}"
        if cfg.keep_history:
            dated = join_public_url(cfg.public_base_url, out_name)
            val += f"\n🗓️ Daté: {dated}"

        fields.append({"name": "📄 Rapport", "value": val, "inline": False})
        attachment_path = None  # must not attach when public_base_url is set
    else:
        attachment_path = out_path

    period = f"{(dt.datetime.fromtimestamp(start_ts, tz=PARIS_TZ)).strftime('%d/%m/%Y %H:%M')} → {(dt.datetime.fromtimestamp(end_ts, tz=PARIS_TZ)).strftime('%d/%m/%Y %H:%M')}"

    payload = _make_recap_embed(
        universe=universe_name,
        player=cfg.player_name,
        date=report_date,
        snapshot_hhmm=snapshot_hhmm,
        period=period,
        summary=summary,
        fields=fields,
        report_links=report_links,
    )

    wh = DiscordWebhook(
        cfg.discord_webhook_url,
        username=cfg.discord_username,
        avatar_url=cfg.discord_avatar_url,
        dry_run=cfg.discord_dry_run,
    )
    wh.send(payload, attachment_path=attachment_path)

    # mark recap posted
    store.set_jobs_state(con, "recap", {"last_date": report_date.isoformat()}, updated_at=iso_z(now_paris()))
    return 0


def _signed(v: int) -> str:
    return ("+" if v > 0 else "") + f"{v:,}".replace(",", " ")


def safe_player_name(player_name: str) -> str:
    return "".join(c for c in player_name if c.isalnum() or c in ("-", "_")) or "player"


def cmd_run(args: argparse.Namespace) -> int:
    from .scheduler import run_loop

    cfg = load_config(args.config)

    server_id, base_url = resolve_server_base_url(
        community=cfg.community,
        server_id=cfg.server_id,
        base_url_override=cfg.base_url_override,
    )
    server_key = f"{cfg.community}:{server_id}"

    con = store.connect(cfg.sqlite_path)
    store.migrate(con)

    api = OGameApiClient(base_url)
    lobby = LobbyClient()
    universe_name, _meta = _fetch_universe_name(lobby, community=cfg.community, server_id=server_id)

    _ensure_players_cache(con, api=api, server_key=server_key)
    player_id = _resolve_player_id(con, api=api, server_key=server_key, player_name=cfg.player_name)

    wh = DiscordWebhook(
        cfg.discord_webhook_url,
        username=cfg.discord_username,
        avatar_url=cfg.discord_avatar_url,
        dry_run=cfg.discord_dry_run,
    )

    def do_collect() -> None:
        fetched_at = iso_z(now_paris())
        inserted = 0
        for metric_type, type_id in METRIC_TO_TYPE_ID.items():
            last = store.get_latest_snapshot(con, server_key=server_key, player_id=player_id, metric_type=metric_type)
            hint_rank = last.rank if last else None
            api_ts, points, rank = _fetch_player_highscore(api, type_id=type_id, player_id=player_id, hint_rank=hint_rank)
            ok = store.insert_snapshot_if_new(
                con,
                server_key=server_key,
                player_id=player_id,
                fetched_at=fetched_at,
                api_timestamp=api_ts,
                metric_type=metric_type,
                points=points,
                rank=rank,
            )
            if ok:
                inserted += 1
        log.info("[run] collect inserted=%s", inserted)

    def do_alerts() -> None:
        if not cfg.alerts_enabled:
            return

        thresholds = cfg.thresholds
        jump = int(thresholds.get("rank_jump_1h", 25))
        drop = int(thresholds.get("rank_drop_1h", 25))
        pct_24h = float(thresholds.get("pct_change_24h", 0.006))
        lost_spike_factor = float(thresholds.get("lost_spike_factor", 2.5))

        # only evaluate on the latest update.
        for m, label in (
            ("global", "🌍 Global"),
            ("economy", "💰 Éco"),
            ("research", "🧠 Rech"),
            ("military", "⚔️ Mili"),
        ):
            last, d = aggregator.last_update_delta(con, server_key=server_key, player_id=player_id, metric_type=m)
            if not last or not d:
                continue

            # cooldown
            cat_top = f"TOP:{m}"
            cat_flop = f"FLOP:{m}"
            cat_pct = f"PCT24H:{m}"

            if d.rank >= jump:
                _maybe_send_alert(
                    con,
                    wh=wh,
                    server_key=server_key,
                    player_id=player_id,
                    category=cat_top,
                    title=f"🚨 TOP {label} — {cfg.player_name} ({universe_name})",
                    body=f"Gain de rang: +{d.rank} places • Points: {_signed(d.points)}\nGG ✅ ça déroule 😎",
                    api_ts=last.api_timestamp,
                    cooldown_minutes=cfg.alerts_cooldown_minutes,
                )
            elif d.rank <= -drop:
                _maybe_send_alert(
                    con,
                    wh=wh,
                    server_key=server_key,
                    player_id=player_id,
                    category=cat_flop,
                    title=f"⚠️ FLOP {label} — {cfg.player_name} ({universe_name})",
                    body=f"Perte de rang: {d.rank} places • Points: {_signed(d.points)}\naie 😬",
                    api_ts=last.api_timestamp,
                    cooldown_minutes=cfg.alerts_cooldown_minutes,
                )

            # 24h percent movement (points)
            last2, d24 = aggregator.rolling_24h_delta(con, server_key=server_key, player_id=player_id, metric_type=m)
            if last2 and d24:
                base = max(1, last2.points - d24.points)
                pct = abs(d24.points) / base
                if pct >= pct_24h:
                    vibe = "ça mine sec ⛏️😎" if d24.points > 0 else "aie 😬"
                    _maybe_send_alert(
                        con,
                        wh=wh,
                        server_key=server_key,
                        player_id=player_id,
                        category=cat_pct,
                        title=f"📈 Mouvement 24h {label} — {cfg.player_name} ({universe_name})",
                        body=f"Δ24h: {_signed(d24.points)} points ({pct*100:.2f}%) • Rang: {_signed(d24.rank)}\n{vibe}",
                        api_ts=last2.api_timestamp,
                        cooldown_minutes=cfg.alerts_cooldown_minutes,
                    )

        # Lost spike (7d mean abs delta)
        last_lost, d_lost = aggregator.last_update_delta(con, server_key=server_key, player_id=player_id, metric_type="military_lost")
        if last_lost and d_lost:
            series = aggregator.weekly_series(con, server_key=server_key, player_id=player_id, metric_type="military_lost", end_ts=last_lost.api_timestamp)
            mean_abs = aggregator.mean_abs_delta([r.points for r in series])
            if mean_abs > 0 and abs(d_lost.points) >= lost_spike_factor * mean_abs:
                cat = "SPIKE:military_lost"
                vibe = "ouch 🩹 ça a chauffé" if d_lost.points > 0 else "bizarre... ça remonte ?"
                _maybe_send_alert(
                    con,
                    wh=wh,
                    server_key=server_key,
                    player_id=player_id,
                    category=cat,
                    title=f"💥 Spike Lost — {cfg.player_name} ({universe_name})",
                    body=f"Δ1h Lost: {_signed(d_lost.points)} • x{lost_spike_factor:.1f} vs moyenne 7j (~{mean_abs:.0f})\n{vibe}",
                    api_ts=last_lost.api_timestamp,
                    cooldown_minutes=cfg.alerts_cooldown_minutes,
                )

    def do_recap() -> None:
        st = store.get_jobs_state(con, "recap") or {}
        today = now_paris().date().isoformat()
        if st.get("last_date") == today:
            return
        # Post recap (also renders + attaches if needed)
        ns = argparse.Namespace(config=str(cfg.path))
        cmd_post_recap(ns)

    run_loop(
        collect_fn=do_collect,
        recap_fn=do_recap,
        alerts_fn=do_alerts,
        collect_minutes=cfg.collect_minutes,
        recap_time=cfg.recap_time,
        grace_s=180,
    )
    return 0


def _maybe_send_alert(
    con,
    *,
    wh: DiscordWebhook,
    server_key: str,
    player_id: int,
    category: str,
    title: str,
    body: str,
    api_ts: int,
    cooldown_minutes: int,
) -> None:
    last = store.last_alert_time(con, server_key=server_key, player_id=player_id, category=category)
    if last:
        created_at_iso, _ = last
        created_at = dt.datetime.fromisoformat(created_at_iso)
        if (now_paris() - created_at).total_seconds() < cooldown_minutes * 60:
            return

    payload = {
        "embeds": [
            {
                "title": title,
                "description": body,
                "color": 0xFFCC00,
            }
        ]
    }

    wh.send(payload)
    store.log_alert(con, server_key=server_key, player_id=player_id, category=category, created_at=iso_z(now_paris()), api_timestamp=api_ts)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ogame_stats", add_help=True)
    p.add_argument("--debug", action="store_true", help="Enable debug logging")
    p.add_argument("--version", action="version", version=f"ogame_stats {__version__}")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Generate example config.yaml")
    p_init.add_argument("--config", required=True)
    p_init.add_argument("--force", action="store_true")
    p_init.set_defaults(func=cmd_init)

    p_list = sub.add_parser("list-universes", help="List universes from Lobby API")
    p_list.add_argument("--community", default="fr")
    p_list.set_defaults(func=cmd_list_universes)

    p_collect = sub.add_parser("collect", help="Collect 1 snapshot batch")
    p_collect.add_argument("--config", required=True)
    p_collect.set_defaults(func=cmd_collect)

    p_render = sub.add_parser("render", help="Render HTML report")
    p_render.add_argument("--config", required=True)
    p_render.add_argument("--date", default="")
    p_render.set_defaults(func=cmd_render)

    p_pub = sub.add_parser("publish", help="Publish latest report to output.publish_dir (for GitHub Pages)")
    p_pub.add_argument("--config", required=True)
    p_pub.add_argument("--report", default="", help="Optional explicit report path; defaults to latest in out_dir")
    p_pub.add_argument("--no-index", action="store_true", help="Do not generate docs/index.html")
    p_pub.set_defaults(func=cmd_publish)

    p_recap = sub.add_parser("post-recap", help="Post daily recap to Discord")
    p_recap.add_argument("--config", required=True)
    p_recap.set_defaults(func=cmd_post_recap)

    p_run = sub.add_parser("run", help="Light daemon: collect hourly + alerts + recap 21:00 Paris")
    p_run.add_argument("--config", required=True)
    p_run.set_defaults(func=cmd_run)

    return p


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    setup_logging(bool(args.debug))

    try:
        return int(args.func(args))
    except UniverseNotFound as e:
        log.error("Universe error: %s", e)
        return 2
    except PlayerNotFound as e:
        log.error("Player error: %s", e)
        return 3
    except ApiError as e:
        log.error("API error: %s", e)
        return 4
    except Exception as e:  # noqa: BLE001
        log.exception("Unhandled error: %s", e)
        return 1
