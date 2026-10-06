"""Synchronous HTTP command client for human-token hub calls.

Used from worker threads (``asyncio.to_thread``). It is not called directly
from ``async def`` bodies.
"""

from __future__ import annotations

import http.client
import json
from urllib.parse import urlparse

from collab.limits import MAX_FRAME_BYTES


class HubHTTPError(Exception):
    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"hub HTTP {status}")
        self.status = status
        self.body = body


def post_command(http_base: str, token: str, envelope: dict | str, timeout: float = 10.0) -> dict:
    parsed = urlparse(http_base)
    if parsed.scheme != "http" or not parsed.hostname or parsed.port is None:
        raise HubHTTPError(0, "hub HTTP base must be http://host:port")
    body = envelope.encode("utf-8") if isinstance(envelope, str) else json.dumps(envelope).encode("utf-8")
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    try:
        conn.request(
            "POST",
            "/v1/command",
            body=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        resp = conn.getresponse()
        raw = resp.read(MAX_FRAME_BYTES + 1)
        status = resp.status
    finally:
        conn.close()
    text = raw.decode("utf-8", errors="replace")
    if status != 200:
        raise HubHTTPError(status, text[:500])
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HubHTTPError(status, "response was not JSON") from exc
    if not isinstance(payload, dict):
        raise HubHTTPError(status, "response was not an object")
    return payload
