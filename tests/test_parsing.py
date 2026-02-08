import pytest

from ogame_stats.ogame_api import _parse_highscore_xml


def test_parse_highscore_xml_basic():
    xml = """
    <highscore timestamp="1700000000" category="1" type="0" total="3">
      <player id="1" position="1" score="100" />
      <player id="2" position="2" score="90" />
      <player id="3" position="3" score="80" />
    </highscore>
    """.strip()

    snap = _parse_highscore_xml(xml)
    assert snap.api_timestamp == 1700000000
    assert snap.total == 3
    assert snap.entries[0].player_id == 1
    assert snap.entries[0].rank == 1
    assert snap.entries[0].points == 100
