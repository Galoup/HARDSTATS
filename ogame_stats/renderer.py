from __future__ import annotations

import datetime as dt
import logging
import math
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from . import aggregator, store
from .utils_time import PARIS_TZ, iso_z

log = logging.getLogger(__name__)


def _fmt_int(n: int | None) -> str:
    if n is None:
        return "-"
    return f"{n:,}".replace(",", " ")


def _fmt_signed(n: int | None) -> str:
    if n is None:
        return "-"
    sign = "+" if n > 0 else ""
    return f"{sign}{n:,}".replace(",", " ")


def _mk_svg_sparkline(points: list[int], *, w: int = 220, h: int = 48) -> str:
    if len(points) < 2:
        return ""

    mn = min(points)
    mx = max(points)
    if mx == mn:
        mx = mn + 1

    def x(i: int) -> float:
        return (i / (len(points) - 1)) * (w - 2) + 1

    def y(v: int) -> float:
        t = (v - mn) / (mx - mn)
        return (h - 2) - t * (h - 2) + 1

    pts = " ".join(f"{x(i):.2f},{y(v):.2f}" for i, v in enumerate(points))
    return (
        f"<svg viewBox='0 0 {w} {h}' width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg' aria-hidden='true'>"
        f"<polyline fill='none' stroke='currentColor' stroke-width='2' points='{pts}' />"
        "</svg>"
    )


def render_report(
    *,
    con,
    template_path: Path,
    out_path: Path,
    server_id: str,
    universe_name: str,
    player_name: str,
    server_key: str,
    player_id: int,
    report_date: dt.date,
    recap_start_ts: int,
    recap_end_ts: int,
    public_base_url: str,
    alerts: list[dict[str, Any]],
) -> Path:
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    tpl = env.get_template(template_path.name)

    metrics = [
        "global",
        "economy",
        "research",
        "military",
        "military_built",
        "military_destroyed",
        "military_lost",
        "honor",
    ]

    cards: dict[str, Any] = {}
    latest_api_ts: int | None = None

    for m in metrics:
        last, d_last = aggregator.last_update_delta(con, server_key=server_key, player_id=player_id, metric_type=m)
        _, d_24h = aggregator.rolling_24h_delta(con, server_key=server_key, player_id=player_id, metric_type=m)
        s_row, e_row, d_daily = aggregator.daily_recap_delta(
            con,
            server_key=server_key,
            player_id=player_id,
            metric_type=m,
            start_ts=recap_start_ts,
            end_ts=recap_end_ts,
        )

        if last and (latest_api_ts is None or last.api_timestamp > latest_api_ts):
            latest_api_ts = last.api_timestamp

        series = aggregator.weekly_series(con, server_key=server_key, player_id=player_id, metric_type=m, end_ts=(last.api_timestamp if last else recap_end_ts))
        spark = _mk_svg_sparkline([r.points for r in series[-40:]])

        cards[m] = {
            "last": last,
            "delta_last": d_last,
            "delta_24h": d_24h,
            "delta_daily": d_daily,
            "spark_svg": spark,
        }

    labels = {
        "global": "🌍 Global",
        "economy": "💰 Éco",
        "research": "🧠 Rech",
        "military": "⚔️ Mili",
        "military_built": "Built",
        "military_destroyed": "Destroyed",
        "military_lost": "Lost",
        "honor": "Honor",
    }

    top_flop = []
    for m in ("global", "economy", "research", "military"):
        d = cards.get(m, {}).get("delta_last")
        if not d:
            continue
        if d.rank == 0 and d.points == 0:
            continue
        top_flop.append(
            {
                "metric": m,
                "label": labels.get(m, m),
                "rank_delta": d.rank,
                "points_delta": d.points,
                "kind": "TOP" if d.rank > 0 else ("FLOP" if d.rank < 0 else "MOVE"),
            }
        )
    top_flop.sort(key=lambda x: abs(int(x["rank_delta"])), reverse=True)
    top_flop = top_flop[:6]

    snapshot_dt = (
        dt.datetime.fromtimestamp(latest_api_ts, tz=PARIS_TZ) if latest_api_ts else dt.datetime(report_date.year, report_date.month, report_date.day, tzinfo=PARIS_TZ)
    )

    ctx = {
        "server_id": server_id,
        "universe_name": universe_name,
        "player_name": player_name,
        "report_date": report_date.isoformat(),
        "snapshot_iso": iso_z(snapshot_dt),
        "snapshot_hhmm": snapshot_dt.strftime("%H:%M"),
        "recap_period": {
            "start": dt.datetime.fromtimestamp(recap_start_ts, tz=PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
            "end": dt.datetime.fromtimestamp(recap_end_ts, tz=PARIS_TZ).strftime("%d/%m/%Y %H:%M"),
        },
        "cards": cards,
        "top_flop": top_flop,
        "alerts": alerts,
        "public_base_url": public_base_url,
        "fmt_int": _fmt_int,
        "fmt_signed": _fmt_signed,
    }

    html = tpl.render(**ctx)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path
