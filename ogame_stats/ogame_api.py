from __future__ import annotations

import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from .config import dump_debug_keys

log = logging.getLogger(__name__)

LOBBY_SERVERS_URL = "https://lobby.ogame.gameforge.com/api/servers"

SERVER_ID_RE = re.compile(r"^s\d+-[a-z]{2}$", re.IGNORECASE)


class ApiError(RuntimeError):
    pass


class UniverseNotFound(ApiError):
    pass


class PlayerNotFound(ApiError):
    pass


@dataclass(frozen=True)
class LobbyServer:
    server_id: str
    name: str
    community: str
    language: str
    raw: dict[str, Any]

    @property
    def base_url(self) -> str:
        return f"https://{self.server_id}.ogame.gameforge.com"


@dataclass(frozen=True)
class PlayerEntry:
    player_id: int
    name: str
    status: str | None
    alliance_id: int | None


@dataclass(frozen=True)
class HighscoreEntry:
    player_id: int
    rank: int
    points: int


@dataclass(frozen=True)
class HighscoreSnapshot:
    api_timestamp: int
    entries: list[HighscoreEntry]
    total: int | None = None


def _request_json(session: requests.Session, url: str, *, timeout_s: float) -> Any:
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _request_text(session: requests.Session, url: str, *, timeout_s: float) -> str:
    r = session.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.text


def _retry_get(session: requests.Session, url: str, *, timeout_s: float, tries: int = 3) -> requests.Response:
    last_exc: Exception | None = None
    for i in range(tries):
        try:
            r = session.get(url, timeout=timeout_s)
            r.raise_for_status()
            return r
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if i < tries - 1:
                time.sleep(0.6 * (2**i))
                continue
            raise ApiError(f"HTTP failed for {url}: {e}") from e
    raise ApiError(f"HTTP failed for {url}: {last_exc}")


class LobbyClient:
    def __init__(self, session: requests.Session | None = None, timeout_s: float = 20.0):
        self.session = session or requests.Session()
        self.timeout_s = timeout_s
        self.session.headers.setdefault(
            "User-Agent",
            "HARDSTATS-OGame/0.1 (+https://lobby.ogame.gameforge.com) requests",
        )

    def list_servers(self) -> list[LobbyServer]:
        data = _request_json(self.session, LOBBY_SERVERS_URL, timeout_s=self.timeout_s)
        if log.isEnabledFor(logging.DEBUG):
            log.debug("Lobby servers JSON shape: %s", dump_debug_keys(data))

        if not isinstance(data, list):
            raise ApiError(f"Unexpected lobby payload type: {type(data).__name__}")

        out: list[LobbyServer] = []
        for item in data:
            if not isinstance(item, dict):
                continue

            server_id = str(item.get("serverId") or item.get("server_id") or "").strip()
            if not server_id:
                # Current lobby schema commonly provides (number, language) instead of serverId.
                number = item.get("number")
                language = item.get("language") or item.get("lang")
                if number not in (None, "") and language:
                    server_id = f"s{str(number).strip()}-{str(language).strip().lower()}"

            if not server_id:
                # try to derive from some known-ish key
                for k in ("id", "server", "server_id"):
                    vv = str(item.get(k) or "").strip()
                    if SERVER_ID_RE.match(vv):
                        server_id = vv
                        break

            if not server_id or not SERVER_ID_RE.match(server_id):
                continue

            name = str(item.get("name") or item.get("serverName") or server_id).strip()
            community = str(item.get("community") or item.get("country") or item.get("locale") or "").strip().lower()
            language = str(item.get("language") or item.get("lang") or "").strip().lower()
            if not community and language:
                community = language

            out.append(LobbyServer(server_id=server_id.lower(), name=name, community=community, language=language, raw=item))

        return sorted(out, key=lambda s: s.server_id)

    def list_servers_for_community(self, community: str) -> list[LobbyServer]:
        c = (community or "").strip().lower()
        servers = self.list_servers()

        def match(s: LobbyServer) -> bool:
            if s.server_id.lower().endswith(f"-{c}"):
                return True
            if s.community == c or s.language == c:
                return True
            # tolerate schema changes: search raw values
            blob = json.dumps(s.raw, ensure_ascii=False).lower()
            return f"\"{c}\"" in blob

        return [s for s in servers if match(s)]


class OGameApiClient:
    def __init__(self, base_url: str, session: requests.Session | None = None, timeout_s: float = 25.0):
        self.base_url = base_url.rstrip("/")
        self.api_base = self.base_url + "/api"
        self.session = session or requests.Session()
        self.timeout_s = timeout_s
        self.session.headers.setdefault(
            "User-Agent",
            "HARDSTATS-OGame/0.1 (public-api-readonly) requests",
        )

    def _get_maybe_json(self, url: str, params: dict[str, str] | None = None) -> Any:
        params = dict(params or {})
        params.setdefault("toJson", "1")
        try:
            r = self.session.get(url, params=params, timeout=self.timeout_s)
            r.raise_for_status()
            ct = (r.headers.get("content-type") or "").lower()
            if "json" in ct:
                return r.json()
            # some endpoints return JSON without proper content-type
            txt = r.text.strip()
            if txt.startswith("{") or txt.startswith("["):
                return json.loads(txt)
            return txt
        except Exception:
            # fallback to plain XML fetch
            r = _retry_get(self.session, url, timeout_s=self.timeout_s)
            return r.text

    def fetch_players(self) -> tuple[int, list[PlayerEntry]]:
        url = self.api_base + "/players.xml"
        payload = self._get_maybe_json(url)

        if isinstance(payload, (dict, list)):
            # JSON shape varies; fallback to XML-only if unknown
            log.debug("players.xml returned JSON-like payload; falling back to XML parser")
            payload = _request_text(self.session, url, timeout_s=self.timeout_s)

        if not isinstance(payload, str):
            raise ApiError("Unexpected players payload type")

        try:
            root = ET.fromstring(payload)
        except ET.ParseError as e:
            raise ApiError(f"Failed to parse players.xml: {e}") from e

        ts = int(root.attrib.get("timestamp", "0") or "0")
        out: list[PlayerEntry] = []
        for el in root.findall("player"):
            try:
                pid = int(el.attrib.get("id", "0") or "0")
                name = el.attrib.get("name", "")
                status = el.attrib.get("status")
                aid_raw = el.attrib.get("alliance")
                aid = int(aid_raw) if aid_raw not in (None, "") else None
                if pid and name:
                    out.append(PlayerEntry(player_id=pid, name=name, status=status, alliance_id=aid))
            except Exception:
                continue

        return ts, out

    def fetch_server_data(self) -> tuple[int, dict[str, Any]]:
        url = self.api_base + "/serverData.xml"
        payload = self._get_maybe_json(url)

        if isinstance(payload, str):
            try:
                root = ET.fromstring(payload)
            except ET.ParseError as e:
                raise ApiError(f"Failed to parse serverData.xml: {e}") from e
            ts = int(root.attrib.get("timestamp", "0") or "0")
            data: dict[str, Any] = {"_raw_xml": payload}
            # best-effort extraction
            for child in root:
                if child.tag and child.text:
                    data[child.tag] = child.text.strip()
            return ts, data

        if isinstance(payload, dict):
            # best-effort: look for timestamp at root or nested
            ts = int(payload.get("timestamp") or payload.get("_timestamp") or 0)
            return ts, payload

        raise ApiError("Unexpected serverData payload")

    def fetch_highscore_block(self, *, type_id: int, start: int, end: int) -> HighscoreSnapshot:
        url = self.api_base + "/highscore.xml"
        params = {
            "category": "1",
            "type": str(type_id),
            "start": str(start),
            "end": str(end),
        }
        payload = self._get_maybe_json(url, params=params)

        if isinstance(payload, str):
            return _parse_highscore_xml(payload)
        if isinstance(payload, dict):
            # try parse common JSON shapes
            return _parse_highscore_json(payload)
        raise ApiError("Unexpected highscore payload")


def _parse_highscore_xml(xml_text: str) -> HighscoreSnapshot:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise ApiError(f"Failed to parse highscore.xml: {e}") from e

    ts = int(root.attrib.get("timestamp", "0") or "0")
    total = root.attrib.get("total")
    total_i = int(total) if total not in (None, "") else None

    entries: list[HighscoreEntry] = []
    for el in root.findall("player"):
        try:
            pid = int(el.attrib.get("id", "0") or "0")
            rank = int(el.attrib.get("position", "0") or "0")
            points = int(el.attrib.get("score", "0") or "0")
            if pid and rank:
                entries.append(HighscoreEntry(player_id=pid, rank=rank, points=points))
        except Exception:
            continue

    return HighscoreSnapshot(api_timestamp=ts, entries=entries, total=total_i)


def _parse_highscore_json(obj: dict[str, Any]) -> HighscoreSnapshot:
    # Tolerant parsing: try a few known-ish shapes.
    if "highscore" in obj and isinstance(obj.get("highscore"), dict):
        obj = obj["highscore"]

    attrs = obj.get("@attributes") if isinstance(obj.get("@attributes"), dict) else {}
    ts = int(obj.get("timestamp") or obj.get("apiTimestamp") or attrs.get("timestamp") or 0)

    candidates: Iterable[Any] = []
    for k in ("players", "player", "data", "entries"):
        if k in obj:
            candidates = obj[k]
            break

    entries: list[HighscoreEntry] = []
    if isinstance(candidates, list):
        for it in candidates:
            if not isinstance(it, dict):
                continue
            it_attrs = it.get("@attributes") if isinstance(it.get("@attributes"), dict) else it
            pid = int(it_attrs.get("id") or it_attrs.get("playerId") or 0)
            rank = int(it_attrs.get("position") or it_attrs.get("rank") or 0)
            points = int(it_attrs.get("score") or it_attrs.get("points") or 0)
            if pid and rank:
                entries.append(HighscoreEntry(player_id=pid, rank=rank, points=points))

    total = obj.get("total") or attrs.get("total")
    total_i = int(total) if total not in (None, "") else None

    return HighscoreSnapshot(api_timestamp=ts, entries=entries, total=total_i)


METRIC_TO_TYPE_ID: dict[str, int] = {
    "global": 0,
    "economy": 1,
    "research": 2,
    "military": 3,
    "military_lost": 4,
    "military_built": 5,
    "military_destroyed": 6,
    "honor": 7,
}
