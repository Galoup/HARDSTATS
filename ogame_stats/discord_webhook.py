from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger(__name__)


class DiscordWebhook:
    def __init__(
        self,
        webhook_url: str,
        *,
        username: str = "OGame Stats",
        avatar_url: str = "",
        dry_run: bool = True,
        timeout_s: float = 25.0,
        session: requests.Session | None = None,
    ):
        self.webhook_url = (webhook_url or "").strip()
        self.username = username
        self.avatar_url = avatar_url
        self.dry_run = dry_run or not self.webhook_url
        self.timeout_s = timeout_s
        self.session = session or requests.Session()

    def send(self, payload: dict[str, Any], *, attachment_path: Path | None = None) -> None:
        payload = dict(payload)
        payload.setdefault("username", self.username)
        if self.avatar_url:
            payload.setdefault("avatar_url", self.avatar_url)

        if self.dry_run:
            log.info("[dry-run] Discord payload: %s", json.dumps(payload, ensure_ascii=False))
            if attachment_path:
                log.info("[dry-run] Attachment: %s", str(attachment_path))
            return

        if attachment_path:
            files = {
                "payload_json": (None, json.dumps(payload, ensure_ascii=False), "application/json"),
                # Discord expects files[0] for multipart attachments.
                "files[0]": (attachment_path.name, attachment_path.read_bytes(), "text/html"),
            }
            r = self.session.post(self.webhook_url, files=files, timeout=self.timeout_s)
            r.raise_for_status()
            return

        r = self.session.post(self.webhook_url, json=payload, timeout=self.timeout_s)
        r.raise_for_status()
