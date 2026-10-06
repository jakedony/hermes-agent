"""JSON bridge configuration. Secrets stay in mode-0600 files, never in this document."""

from __future__ import annotations

import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from collab.limits import (
    CHILD_BUDGET,
    CHILD_DEADLINE_SEC,
    LEASE_MARGIN_SEC,
    LEASE_SEC,
    MAX_ITERATIONS,
    RENEW_EVERY_SEC,
    ROOT_DEADLINE_SEC,
    TOOL_TIMEOUT_SEC,
)


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class AllowTarget:
    host: str
    port: int


@dataclass(frozen=True)
class BridgeConfig:
    agent_id: str
    machine_id: str
    room_id: str
    hub_ws: str
    token_file: str
    journal_path: str
    state_dir: str
    worker_mode: str
    peer_agent_id: str
    allowlist: tuple[AllowTarget, ...]
    profile: str
    peer_profile: str
    hermes_home: str
    pythonpath: str
    model: str
    model_base_url: str
    api_key_file: str
    human_socket: str
    human_socket_token_file: str
    human_hub_token_file: str
    root_deadline_sec: int
    child_deadline_sec: int
    tool_timeout_sec: int
    max_iterations: int
    child_budget: int
    lease_sec: int
    renew_every_sec: int
    lease_margin_sec: int

    @property
    def http_base(self) -> str:
        parsed = urlparse(self.hub_ws)
        scheme = "https" if parsed.scheme == "wss" else "http"
        if not parsed.netloc:
            raise ConfigError("hub_ws is missing a host")
        return f"{scheme}://{parsed.netloc}"

    def allow_pairs(self) -> frozenset[tuple[str, int]]:
        return frozenset((item.host, item.port) for item in self.allowlist)


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a non-empty string")
    return value


def _optional_str(data: dict[str, Any], key: str, default: str = "") -> str:
    value = data.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"{key} must be a string")
    return value


def _int_field(data: dict[str, Any], key: str, default: int) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{key} must be a positive integer")
    return value


def _parse_allowlist(raw: Any) -> tuple[AllowTarget, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ConfigError("allowlist must be a list of {host, port}")
    targets: list[AllowTarget] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ConfigError("allowlist entries must be objects")
        host = item.get("host")
        port = item.get("port")
        if not isinstance(host, str):
            raise ConfigError("allowlist host must be a string")
        if isinstance(port, bool) or not isinstance(port, int):
            raise ConfigError("allowlist port must be an integer")
        targets.append(normalize_target(host, port))
    return tuple(targets)


def normalize_target(host: str, port: int) -> AllowTarget:
    if host.startswith("-") or any(ch.isspace() for ch in host) or "/" in host:
        raise ConfigError("allowlist host is not an IP address")
    try:
        parsed = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ConfigError("allowlist host must be an IP address, not a hostname") from exc
    if port < 1 or port > 65535:
        raise ConfigError("allowlist port is out of range")
    return AllowTarget(host=str(parsed), port=port)


def load_config(path: str | Path) -> BridgeConfig:
    raw_text = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config is not JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("config must be a JSON object")
    mode = _optional_str(data, "worker_mode", "hermes")
    if mode not in {"hermes", "fake"}:
        raise ConfigError("worker_mode must be hermes or fake")
    hub_ws = _require_str(data, "hub_ws")
    parsed = urlparse(hub_ws)
    if parsed.scheme not in {"ws", "wss"} or not parsed.path.endswith("/ws"):
        raise ConfigError("hub_ws must be a ws or wss URL whose path ends with /ws")
    limits = data.get("limits") or {}
    if not isinstance(limits, dict):
        raise ConfigError("limits must be an object")
    return BridgeConfig(
        agent_id=_require_str(data, "agent_id"),
        machine_id=_require_str(data, "machine_id"),
        room_id=_optional_str(data, "room_id", "lab"),
        hub_ws=hub_ws,
        token_file=_require_str(data, "token_file"),
        journal_path=_require_str(data, "journal_path"),
        state_dir=_require_str(data, "state_dir"),
        worker_mode=mode,
        peer_agent_id=_require_str(data, "peer_agent_id"),
        allowlist=_parse_allowlist(data.get("allowlist")),
        profile=_optional_str(data, "profile", "default"),
        peer_profile=_optional_str(data, "peer_profile", "vm-local"),
        hermes_home=_optional_str(data, "hermes_home"),
        pythonpath=_optional_str(data, "pythonpath"),
        model=_optional_str(data, "model", "fake-model"),
        model_base_url=_optional_str(data, "model_base_url"),
        api_key_file=_optional_str(data, "api_key_file"),
        human_socket=_optional_str(data, "human_socket"),
        human_socket_token_file=_optional_str(data, "human_socket_token_file"),
        human_hub_token_file=_optional_str(data, "human_hub_token_file"),
        root_deadline_sec=_int_field(limits, "root_deadline_sec", ROOT_DEADLINE_SEC),
        child_deadline_sec=_int_field(limits, "child_deadline_sec", CHILD_DEADLINE_SEC),
        tool_timeout_sec=_int_field(limits, "tool_timeout_sec", TOOL_TIMEOUT_SEC),
        max_iterations=_int_field(limits, "max_iterations", MAX_ITERATIONS),
        child_budget=_int_field(limits, "child_budget", CHILD_BUDGET),
        lease_sec=_int_field(limits, "lease_sec", LEASE_SEC),
        renew_every_sec=_int_field(limits, "renew_every_sec", RENEW_EVERY_SEC),
        lease_margin_sec=_int_field(limits, "lease_margin_sec", LEASE_MARGIN_SEC),
    )


def read_secret_file(path: str) -> str:
    if not path:
        raise ConfigError("secret file path is empty")
    secret = Path(path).read_text(encoding="utf-8").strip()
    if not secret:
        raise ConfigError(f"secret file {path} is empty")
    return secret
