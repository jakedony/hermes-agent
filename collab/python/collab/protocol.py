"""Hub envelope helpers. Field names match ``collab/hub/protocol.go``.

The hub hashes the raw payload bytes it received. It does not sort or
re-encode JSON. A retry must send those same bytes. ``command_frame`` splices
a stored payload string in unchanged. A dict is encoded once, compact, with
sorted keys, and that text is what the journal keeps.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any

PROTOCOL_VERSION = 1
REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,80}$")
PROFILE_RE = re.compile(r"^[a-z0-9-]{1,32}$")


class ProtocolError(Exception):
    pass


def new_request_id(prefix: str) -> str:
    ident = f"{prefix}_{uuid.uuid4().hex[:16]}"
    if not REQUEST_ID_RE.match(ident):
        raise ProtocolError(f"request id {ident!r} is not acceptable to the hub")
    return ident


def wire_payload(payload: dict[str, Any] | None) -> str:
    body = {} if payload is None else payload
    return json.dumps(body, separators=(",", ":"), sort_keys=True)


def command_frame(
    command_type: str,
    request_id: str,
    room_id: str,
    payload: dict[str, Any] | str,
    task_id: str = "",
) -> str:
    if not REQUEST_ID_RE.match(request_id):
        raise ProtocolError("request_id is missing or malformed")
    raw = payload if isinstance(payload, str) else wire_payload(payload)
    if raw == "":
        raw = "{}"
    parts = [
        f'"v":{PROTOCOL_VERSION}',
        f'"type":{json.dumps(command_type)}',
        f'"request_id":{json.dumps(request_id)}',
        f'"room_id":{json.dumps(room_id)}',
    ]
    if task_id:
        parts.append(f'"task_id":{json.dumps(task_id)}')
    parts.append(f'"payload":{raw}')
    return "{" + ",".join(parts) + "}"


def canonical_bytes(payload: dict[str, Any]) -> bytes:
    # Local create-dedup key. Not the hub hash: that also covers type, room_id, and task_id.
    return wire_payload(payload).encode("utf-8")


def payload_key(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()


def ack_body(env: dict[str, Any]) -> dict[str, Any]:
    payload = env.get("payload")
    if isinstance(payload, dict):
        return payload
    return {}


def error_code(env: dict[str, Any]) -> str:
    payload = env.get("payload")
    if isinstance(payload, dict):
        code = payload.get("code")
        if isinstance(code, str):
            return code
    return ""


def retry_after_sec(env: dict[str, Any]) -> float:
    payload = env.get("payload")
    detail = payload.get("detail") if isinstance(payload, dict) else None
    raw = detail.get("retry_after_ms") if isinstance(detail, dict) else None
    if not isinstance(raw, (int, float)) or isinstance(raw, bool):
        return 1.0
    return max(0.0, min(float(raw) / 1000.0, 30.0))
