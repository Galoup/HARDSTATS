from __future__ import annotations

import datetime as dt
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishResult:
    source_report: Path
    published_latest: Path
    published_dated: Path | None
    published_index: Path | None


def find_latest_report(out_dir: Path) -> Path | None:
    if not out_dir.exists():
        return None

    # Prefer our report naming convention.
    candidates = [p for p in out_dir.glob("report_*.html") if p.is_file()]
    if not candidates:
        candidates = [p for p in out_dir.glob("*.html") if p.is_file()]
    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def publish_report(
    *,
    report_path: Path,
    publish_dir: Path,
    latest_filename: str = "latest.html",
    keep_history: bool = True,
    generate_index: bool = True,
) -> PublishResult:
    if not report_path.exists() or not report_path.is_file():
        raise FileNotFoundError(f"Report not found: {report_path}")

    publish_dir.mkdir(parents=True, exist_ok=True)

    latest_name = (latest_filename or "latest.html").strip()
    if "/" in latest_name or "\\" in latest_name:
        raise ValueError("latest_filename must be a filename (no path separators)")

    latest_path = (publish_dir / latest_name).resolve()
    shutil.copy2(report_path, latest_path)
    log.info("Published latest: %s -> %s", report_path.name, latest_path)

    dated_path: Path | None = None
    if keep_history:
        if report_path.name != latest_name:
            dated_path = (publish_dir / report_path.name).resolve()
            shutil.copy2(report_path, dated_path)
            log.info("Published history: %s -> %s", report_path.name, dated_path)

    index_path: Path | None = None
    if generate_index:
        index_path = (publish_dir / "index.html").resolve()
        _write_index(publish_dir=publish_dir, index_path=index_path, latest_filename=latest_name)

    return PublishResult(
        source_report=report_path.resolve(),
        published_latest=latest_path,
        published_dated=dated_path,
        published_index=index_path,
    )


def _write_index(*, publish_dir: Path, index_path: Path, latest_filename: str) -> None:
    # Simple static index for GitHub Pages (no JS).
    reports = [p for p in publish_dir.glob("report_*.html") if p.is_file()]

    def parse_date(p: Path) -> dt.date | None:
        # report_YYYY-MM-DD_....
        try:
            parts = p.stem.split("_", 2)
            if len(parts) >= 2 and parts[0] == "report":
                return dt.date.fromisoformat(parts[1])
        except Exception:
            return None
        return None

    def sort_key(p: Path):
        d = parse_date(p)
        # Prefer date; fallback mtime
        return (d or dt.date.min, p.stat().st_mtime)

    reports.sort(key=sort_key, reverse=True)

    now = dt.datetime.now().isoformat(timespec="seconds")
    items = "\n".join(
        f"<li><a href=\"{p.name}\">{p.name}</a></li>"
        for p in reports
    )

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>OGame Stats Reports</title>
  <style>
    body {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 28px; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <h1>OGame Stats Reports</h1>
  <p>Latest: <a href="{latest_filename}"><code>{latest_filename}</code></a></p>
  <p>Generated: <code>{now}</code></p>
  <h2>History</h2>
  <ul>
    {items if items else "<li>No reports yet.</li>"}
  </ul>
</body>
</html>
"""

    index_path.write_text(html, encoding="utf-8")
    log.info("Published index: %s", index_path)

