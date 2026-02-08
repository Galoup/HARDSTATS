from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Config:
    path: Path
    raw: dict[str, Any]
    base_dir: Path

    community: str
    server_id: str
    base_url_override: str
    player_name: str

    discord_webhook_url: str
    discord_username: str
    discord_avatar_url: str
    discord_dry_run: bool

    out_dir: Path
    public_base_url: str
    publish_dir: Path
    latest_filename: str
    keep_history: bool

    data_dir: Path
    sqlite_path: Path

    collect_minutes: int
    recap_time: str

    alerts_enabled: bool
    alerts_cooldown_minutes: int
    thresholds: dict[str, Any]


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(v)


def _resolve_path(base_dir: Path, p: str | Path) -> Path:
    pp = Path(p)
    if pp.is_absolute():
        return pp
    return (base_dir / pp).resolve()


def load_config(config_path: str | Path) -> Config:
    path = Path(config_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping")

    base_dir = path.parent

    community = str(raw.get("community", "fr")).strip() or "fr"

    uni = raw.get("universe") or {}
    if not isinstance(uni, dict):
        raise ValueError("universe must be a mapping")

    server_id = str(uni.get("server_id", "")).strip()
    base_url_override = str(uni.get("base_url", "")).strip()

    player_name = str(raw.get("player_name", "")).strip()
    if not player_name:
        raise ValueError("player_name is required")

    discord = raw.get("discord") or {}
    if not isinstance(discord, dict):
        raise ValueError("discord must be a mapping")

    discord_webhook_url = str(discord.get("webhook_url", "")).strip()
    discord_username = str(discord.get("username", "OGame Stats")).strip() or "OGame Stats"
    discord_avatar_url = str(discord.get("avatar_url", "")).strip()
    discord_dry_run = _as_bool(discord.get("dry_run"), default=True)

    out = raw.get("output") or {}
    if not isinstance(out, dict):
        raise ValueError("output must be a mapping")

    out_dir = _resolve_path(base_dir, out.get("out_dir", "./out"))
    public_base_url = str(out.get("public_base_url", "")).strip()
    publish_dir = _resolve_path(base_dir, out.get("publish_dir", "./docs"))
    latest_filename = str(out.get("latest_filename", "latest.html")).strip() or "latest.html"
    keep_history = _as_bool(out.get("keep_history"), default=True)

    storage = raw.get("storage") or {}
    if not isinstance(storage, dict):
        raise ValueError("storage must be a mapping")

    data_dir = _resolve_path(base_dir, storage.get("data_dir", "./data"))
    sqlite_path = _resolve_path(base_dir, storage.get("sqlite_path", "./data/ogame_stats.sqlite"))

    schedule = raw.get("schedule") or {}
    if not isinstance(schedule, dict):
        raise ValueError("schedule must be a mapping")

    collect_minutes = int(schedule.get("collect_minutes", 60))
    recap_time = str(schedule.get("recap_time", "21:00")).strip() or "21:00"

    alerts = raw.get("alerts") or {}
    if not isinstance(alerts, dict):
        raise ValueError("alerts must be a mapping")

    alerts_enabled = _as_bool(alerts.get("enabled"), default=True)
    alerts_cooldown_minutes = int(alerts.get("cooldown_minutes", 180))

    thresholds = alerts.get("thresholds") or {}
    if not isinstance(thresholds, dict):
        raise ValueError("alerts.thresholds must be a mapping")

    return Config(
        path=path,
        raw=raw,
        base_dir=base_dir,
        community=community,
        server_id=server_id,
        base_url_override=base_url_override,
        player_name=player_name,
        discord_webhook_url=discord_webhook_url,
        discord_username=discord_username,
        discord_avatar_url=discord_avatar_url,
        discord_dry_run=discord_dry_run,
        out_dir=out_dir,
        public_base_url=public_base_url,
        publish_dir=publish_dir,
        latest_filename=latest_filename,
        keep_history=keep_history,
        data_dir=data_dir,
        sqlite_path=sqlite_path,
        collect_minutes=collect_minutes,
        recap_time=recap_time,
        alerts_enabled=alerts_enabled,
        alerts_cooldown_minutes=alerts_cooldown_minutes,
        thresholds=thresholds,
    )


def write_example_config(path: str | Path, force: bool = False) -> Path:
    p = Path(path).expanduser().resolve()
    if p.exists() and not force:
        # Idempotent: keep existing config (Quickstart runs init even if config.yaml is present).
        return p

    example = {
        "community": "fr",
        "universe": {"server_id": "", "base_url": ""},
        "player_name": "Galoup",
        "discord": {
            "webhook_url": "",
            "username": "OGame Stats",
            "avatar_url": "",
            "dry_run": True,
        },
        "output": {
            "out_dir": "./out",
            "public_base_url": "",
            "publish_dir": "./docs",
            "latest_filename": "latest.html",
            "keep_history": True,
        },
        "storage": {"data_dir": "./data", "sqlite_path": "./data/ogame_stats.sqlite"},
        "schedule": {"collect_minutes": 60, "recap_time": "21:00"},
        "alerts": {
            "enabled": True,
            "cooldown_minutes": 180,
            "thresholds": {
                "rank_jump_1h": 25,
                "rank_drop_1h": 25,
                "pct_change_24h": 0.006,
                "lost_spike_factor": 2.5,
            },
        },
    }

    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(example, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return p


def dump_debug_keys(obj: Any, max_items: int = 1) -> str:
    """Human-friendly summary of JSON shapes to help tolerate schema changes."""
    try:
        if isinstance(obj, list) and obj:
            keys = sorted(list(obj[0].keys())) if isinstance(obj[0], dict) else [type(obj[0]).__name__]
            return json.dumps({"type": "list", "len": len(obj), "first_item_keys": keys}, ensure_ascii=False)
        if isinstance(obj, dict):
            keys = sorted(list(obj.keys()))
            return json.dumps({"type": "dict", "keys": keys}, ensure_ascii=False)
    except Exception:
        pass
    return json.dumps({"type": type(obj).__name__}, ensure_ascii=False)
