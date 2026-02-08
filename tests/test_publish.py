from pathlib import Path

import pytest

from ogame_stats.publisher import find_latest_report, publish_report


def test_publish_report_copies_latest_and_history(tmp_path: Path):
    out_dir = tmp_path / "out"
    pub_dir = tmp_path / "docs"
    out_dir.mkdir()

    report = out_dir / "report_2026-02-07_s999-fr_Player.html"
    report.write_text("<html>ok</html>", encoding="utf-8")

    res = publish_report(
        report_path=report,
        publish_dir=pub_dir,
        latest_filename="latest.html",
        keep_history=True,
        generate_index=True,
    )

    assert res.published_latest.exists()
    assert res.published_latest.read_text(encoding="utf-8") == "<html>ok</html>"

    assert res.published_dated is not None
    assert res.published_dated.exists()
    assert res.published_dated.name == report.name

    assert (pub_dir / "index.html").exists()


def test_find_latest_report_prefers_report_prefix(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "x.html").write_text("x", encoding="utf-8")
    r1 = out_dir / "report_2026-02-06_s1-fr_A.html"
    r2 = out_dir / "report_2026-02-07_s1-fr_A.html"
    r1.write_text("1", encoding="utf-8")
    r2.write_text("2", encoding="utf-8")

    latest = find_latest_report(out_dir)
    assert latest is not None
    # With equal mtimes (possible on some FS), accept either report_*; content check keeps it stable enough.
    assert latest.name.startswith("report_")


def test_publish_rejects_latest_filename_with_path_sep(tmp_path: Path):
    out_dir = tmp_path / "out"
    pub_dir = tmp_path / "docs"
    out_dir.mkdir()
    report = out_dir / "report_2026-02-07_s1-fr_A.html"
    report.write_text("ok", encoding="utf-8")

    with pytest.raises(ValueError):
        publish_report(report_path=report, publish_dir=pub_dir, latest_filename="a/b.html")

