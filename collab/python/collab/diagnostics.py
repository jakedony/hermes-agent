"""Restricted local diagnostics.

Every operation uses a fixed executable and a fixed argument vector. A
model-supplied port, unit, or name is either filtered in Python after the
process exits or checked against an allowlist and a token regex before it is
placed in one argv slot that cannot be an option. Nothing is passed through a
shell. A missing binary or a permission denial is an observation. Privileges
are not escalated.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Callable

from collab.config import SERVICE_RE, AllowTarget, ConfigError, DNS_RE, normalize_target
from collab.limits import (
    DIAG_OUTPUT_CAP,
    DNS_ANSWER_CAP,
    DNS_OUTPUT_CAP,
    LOG_MAX_LINES,
    LOG_OUTPUT_CAP,
    ROUTE_OUTPUT_CAP,
    SS_OUTPUT_CAP,
    TOOL_TIMEOUT_SEC,
)

SS_CANDIDATES = ("/usr/bin/ss", "/bin/ss")
SS_ARGS = ("-H", "-l", "-n", "-t", "-u")
SYSTEMCTL_PATH = "/usr/bin/systemctl"
JOURNALCTL_PATH = "/usr/bin/journalctl"
GETENT_PATH = "/usr/bin/getent"
IP_CANDIDATES = ("/usr/sbin/ip", "/usr/bin/ip")
STATUS_PREFIX = ("show", "-p", "Id", "-p", "ActiveState", "-p", "SubState", "-p", "UnitFileState", "--")
JOURNAL_PREFIX = ("-n", str(LOG_MAX_LINES), "--no-pager", "-o", "cat", "-u")
_PORT_IN_LINE = re.compile(r":(\d+)(?!\d)")
_PERM_MARKERS = ("permission denied", "access denied", "insufficient permission", "not permitted")
_LIST_KEYS = ("listeners", "lines", "answers")


class DiagnosticError(Exception):
    pass


@dataclass(frozen=True)
class DiagPolicy:
    probes: frozenset[tuple[str, int]]
    services: frozenset[str]
    dns_names: frozenset[str]
    hub_target: tuple[str, int] | None
    timeout_sec: float = TOOL_TIMEOUT_SEC

    def forbidden(self) -> frozenset[tuple[str, int]]:
        if self.hub_target is None:
            return frozenset()
        return frozenset({self.hub_target})


def first_executable(candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def ss_path() -> str | None:
    return first_executable(SS_CANDIDATES)


def real_ss_argv(path: str) -> list[str]:
    if path not in SS_CANDIDATES:
        raise DiagnosticError("ss path is not one of the fixed candidates")
    return [path, *SS_ARGS]


def ss_argv(path: str) -> list[str]:
    """Interpreter, limit helper, and the fixed ss vector. No user strings."""
    import sys

    return [sys.executable, "-m", "collab.rlimit_exec", *real_ss_argv(path)]


def service_token(unit: str) -> bool:
    return bool(unit) and not unit.startswith("-") and SERVICE_RE.fullmatch(unit) is not None


def dns_token(name: str) -> bool:
    return bool(name) and not name.startswith("-") and DNS_RE.fullmatch(name) is not None


def argv_is_fixed(argv: object) -> bool:
    if not isinstance(argv, list) or not argv or not all(isinstance(part, str) for part in argv):
        return False
    return any(check(argv) for check in _ARGV_CHECKS)


def _ss_ok(argv: list[str]) -> bool:
    return len(argv) == 1 + len(SS_ARGS) and argv[0] in SS_CANDIDATES and tuple(argv[1:]) == SS_ARGS


def _route_ok(argv: list[str]) -> bool:
    return len(argv) == 3 and argv[0] in IP_CANDIDATES and tuple(argv[1:]) == ("route", "show")


def _status_ok(argv: list[str]) -> bool:
    prefix = [SYSTEMCTL_PATH, *STATUS_PREFIX]
    return len(argv) == len(prefix) + 1 and argv[: len(prefix)] == prefix and service_token(argv[-1])


def _logs_ok(argv: list[str]) -> bool:
    prefix = [JOURNALCTL_PATH, *JOURNAL_PREFIX]
    return len(argv) == len(prefix) + 1 and argv[: len(prefix)] == prefix and service_token(argv[-1])


def _dns_ok(argv: list[str]) -> bool:
    return len(argv) == 3 and argv[0] == GETENT_PATH and argv[1] == "ahosts" and dns_token(argv[2])


_ARGV_CHECKS: tuple[Callable[[list[str]], bool], ...] = (_ss_ok, _route_ok, _status_ok, _logs_ok, _dns_ok)


def filter_listeners(text: str, port: int | None) -> list[str]:
    kept: list[str] = []
    for line in text.splitlines():
        stripped = line.rstrip()
        if not stripped:
            continue
        if port is None or line_has_port(stripped, port):
            kept.append(stripped)
    return kept


def line_has_port(line: str, port: int) -> bool:
    return any(int(match) == port for match in _PORT_IN_LINE.findall(line))


def listening_sockets(port: int | None = None, timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    if port is not None and (isinstance(port, bool) or not isinstance(port, int) or port < 1 or port > 65535):
        return _diag(False, "listening_sockets", limitation="port is not an integer from 1 to 65535", port=port)
    path = ss_path()
    if path is None:
        return _diag(
            False,
            "listening_sockets",
            limitation="ss is not installed at /usr/bin/ss or /bin/ss; privileges were not escalated",
            port=port,
        )
    try:
        completed = _run_capped(ss_argv(path), timeout_sec, SS_OUTPUT_CAP)
    except subprocess.TimeoutExpired:
        return _diag(False, "listening_sockets", limitation="ss timed out; the process group was killed", port=port, executable=path)
    except OSError as exc:
        return _diag(False, "listening_sockets", limitation=f"ss could not be started: {exc.__class__.__name__}", port=port, executable=path)
    text = completed.stdout.decode("utf-8", errors="replace")
    limitation = _capture_limitation("ss", completed, SS_OUTPUT_CAP)
    listeners = filter_listeners(text, port)
    return _diag(
        completed.returncode == 0,
        "listening_sockets",
        limitation=limitation,
        port=port,
        executable=path,
        listeners=listeners[:80],
        truncated=len(listeners) > 80 or len(completed.stdout) >= SS_OUTPUT_CAP,
    )


def classify_probe(
    host: Any,
    port: Any,
    allowlist: frozenset[tuple[str, int]],
    forbidden: frozenset[tuple[str, int]] = frozenset(),
) -> str | None:
    """Return a rejection reason, or None when the pair may be dialed."""
    if not isinstance(host, str) or not host or host.startswith("-") or any(ch.isspace() for ch in host):
        return "host is empty, has whitespace, or looks like an option"
    if isinstance(port, bool) or not isinstance(port, int):
        return "port must be an integer"
    try:
        target = normalize_target(host, port)
    except ConfigError:
        return "host must be an IP address and port must be 1..65535"
    pair = (target.host, target.port)
    if pair in forbidden:
        return "probe target is the hub forward, not a service address"
    if pair not in allowlist:
        return "host and port are not on the probe allowlist together"
    return None


def tcp_probe(
    host: Any,
    port: Any,
    allowlist: frozenset[tuple[str, int]],
    timeout_sec: float = TOOL_TIMEOUT_SEC,
    forbidden: frozenset[tuple[str, int]] = frozenset(),
) -> dict[str, Any]:
    reason = classify_probe(host, port, allowlist, forbidden)
    if reason is not None:
        return _diag(False, "tcp_probe", limitation=reason, host=host if isinstance(host, str) else "", port=port, error=reason)
    target = normalize_target(str(host), int(port))
    started = time.monotonic()
    import socket

    try:
        with socket.create_connection((target.host, target.port), timeout=timeout_sec):
            pass
    except TimeoutError:
        elapsed = int((time.monotonic() - started) * 1000)
        return _diag(False, "tcp_probe", limitation="tcp connect timed out", host=target.host, port=target.port, error="timed out", elapsed_ms=elapsed)
    except OSError as exc:
        elapsed = int((time.monotonic() - started) * 1000)
        message = f"{exc.__class__.__name__}: {exc.errno}" if exc.errno else exc.__class__.__name__
        return _diag(False, "tcp_probe", limitation=message, host=target.host, port=target.port, error=message, elapsed_ms=elapsed)
    elapsed = int((time.monotonic() - started) * 1000)
    return _diag(True, "tcp_probe", host=target.host, port=target.port, elapsed_ms=elapsed)


def classify_service(unit: Any, services: frozenset[str]) -> str | None:
    if not isinstance(unit, str) or not unit or unit.startswith("-") or any(ch.isspace() for ch in unit):
        return "unit is empty, has whitespace, or looks like an option"
    if not service_token(unit):
        return "unit is not a safe service token"
    if unit not in services:
        return "unit is not on the service allowlist"
    return None


def service_status(unit: Any, services: frozenset[str], timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    reason = classify_service(unit, services)
    if reason is not None:
        return _diag(False, "service_status", limitation=reason, unit=_text(unit))
    if not os.path.isfile(SYSTEMCTL_PATH) or not os.access(SYSTEMCTL_PATH, os.X_OK):
        return _diag(
            False,
            "service_status",
            limitation="systemctl is not installed at /usr/bin/systemctl; privileges were not escalated",
            unit=unit,
        )
    argv = [SYSTEMCTL_PATH, *STATUS_PREFIX, str(unit)]
    return _exec_observation("service_status", argv, timeout_sec, SS_OUTPUT_CAP, unit=unit)


def service_logs(unit: Any, services: frozenset[str], timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    reason = classify_service(unit, services)
    if reason is not None:
        return _diag(False, "service_logs", limitation=reason, unit=_text(unit))
    if not os.path.isfile(JOURNALCTL_PATH) or not os.access(JOURNALCTL_PATH, os.X_OK):
        return _diag(
            False,
            "service_logs",
            limitation="journalctl is not installed at /usr/bin/journalctl; privileges were not escalated",
            unit=unit,
        )
    argv = [JOURNALCTL_PATH, *JOURNAL_PREFIX, str(unit)]
    return _exec_observation("service_logs", argv, timeout_sec, LOG_OUTPUT_CAP, unit=unit, logs=True)


def route_show(timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    path = first_executable(IP_CANDIDATES)
    if path is None:
        return _diag(False, "route_show", limitation="ip is not installed at /usr/sbin/ip or /usr/bin/ip; privileges were not escalated")
    return _exec_observation("route_show", [path, "route", "show"], timeout_sec, ROUTE_OUTPUT_CAP)


def classify_dns(name: Any, allowlist: frozenset[str]) -> tuple[str | None, str | None]:
    """Return ``(canonical, None)`` or ``(None, reason)``. The canonical spelling is the allowlist entry."""
    if not isinstance(name, str) or not name or name.startswith("-") or any(ch.isspace() for ch in name):
        return None, "name is empty, has whitespace, or looks like an option"
    if not dns_token(name):
        return None, "name is not a single DNS name"
    folded = name.casefold()
    for allowed in allowlist:
        if allowed.casefold() == folded:
            return allowed, None
    return None, "name is not on the DNS allowlist"


def dns_lookup(name: Any, allowlist: frozenset[str], timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    canonical, reason = classify_dns(name, allowlist)
    if reason is not None or canonical is None:
        return _diag(False, "dns_lookup", limitation=reason or "name is not on the DNS allowlist", name=_text(name))
    if not os.path.isfile(GETENT_PATH) or not os.access(GETENT_PATH, os.X_OK):
        return _diag(False, "dns_lookup", limitation="getent is not installed at /usr/bin/getent; privileges were not escalated", name=canonical)
    body = _exec_observation("dns_lookup", [GETENT_PATH, "ahosts", canonical], timeout_sec, DNS_OUTPUT_CAP, name=canonical)
    answers = _answers(body.pop("lines", []))
    body["answers"] = answers
    body["truncated"] = bool(body.get("truncated")) or len(answers) >= DNS_ANSWER_CAP
    return body


def allowlist_from_pairs(pairs: list[dict[str, Any]] | tuple[AllowTarget, ...]) -> frozenset[tuple[str, int]]:
    found: set[tuple[str, int]] = set()
    for item in pairs:
        if isinstance(item, AllowTarget):
            found.add((item.host, item.port))
            continue
        if not isinstance(item, dict):
            continue
        try:
            target = normalize_target(str(item.get("host", "")), int(item.get("port", 0)))
        except (TypeError, ValueError, ConfigError):
            continue
        found.add((target.host, target.port))
    return frozenset(found)


def policy_from_start(start: dict[str, Any], timeout_sec: float) -> DiagPolicy:
    services = start.get("services") if isinstance(start.get("services"), list) else []
    names = start.get("dns_names") if isinstance(start.get("dns_names"), list) else []
    return DiagPolicy(
        probes=allowlist_from_pairs(start.get("allowlist") or []),
        services=frozenset(item for item in services if isinstance(item, str)),
        dns_names=frozenset(item for item in names if isinstance(item, str)),
        hub_target=_hub_from_start(start.get("hub_target")),
        timeout_sec=timeout_sec,
    )


def _hub_from_start(raw: Any) -> tuple[str, int] | None:
    if not isinstance(raw, dict):
        return None
    try:
        target = normalize_target(str(raw.get("host", "")), int(raw.get("port", 0)))
    except (TypeError, ValueError, ConfigError):
        return None
    return target.host, target.port


def _diag(ok: bool, operation: str, **fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"ok": ok, "operation": operation, "limitation": fields.pop("limitation", None)}
    body.update(fields)
    return body


def dumps_result(body: dict[str, Any]) -> str:
    text = json.dumps(body, sort_keys=True, default=str)
    if len(text) <= DIAG_OUTPUT_CAP:
        return text
    clipped = dict(body)
    clipped["truncated"] = True
    for key in _LIST_KEYS:
        if isinstance(clipped.get(key), list):
            clipped[key] = clipped[key][:10]
    return json.dumps(clipped, sort_keys=True, default=str)[:DIAG_OUTPUT_CAP]


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _answers(lines: Any) -> list[str]:
    if not isinstance(lines, list):
        return []
    found: list[str] = []
    for line in lines:
        if not isinstance(line, str) or not line.strip():
            continue
        found.append(line.strip()[:300])
        if len(found) >= DNS_ANSWER_CAP:
            break
    return found


def _is_permission(code: int, err: str) -> bool:
    if code == 0:
        return False
    lowered = err.lower()
    return any(marker in lowered for marker in _PERM_MARKERS)


def _capture_limitation(label: str, completed: subprocess.CompletedProcess[bytes], cap: int) -> str | None:
    if len(completed.stdout) >= cap:
        return f"{label} output hit the capture cap and was truncated"
    if completed.returncode != 0:
        err = completed.stderr.decode("utf-8", errors="replace")[:500]
        return f"{label} exited {completed.returncode} without extra privileges: {err}".strip()
    return None


def _exec_observation(operation: str, argv: list[str], timeout_sec: float, cap: int, logs: bool = False, **fields: Any) -> dict[str, Any]:
    fields.setdefault("executable", argv[0] if argv else "")
    if not argv_is_fixed(argv):
        return _diag(False, operation, limitation="refusing an argv that is not a fixed diagnostic", **fields)
    try:
        completed = spawn_fixed(argv, timeout_sec, cap)
    except subprocess.TimeoutExpired:
        return _diag(False, operation, limitation=f"{operation} timed out; the process group was killed", **fields)
    except DiagnosticError as exc:
        return _diag(False, operation, limitation=str(exc), **fields)
    except OSError as exc:
        return _diag(False, operation, limitation=f"{operation} could not be started: {exc.__class__.__name__}", **fields)
    return _from_completed(operation, completed, cap, logs, **fields)


def _from_completed(
    operation: str,
    completed: subprocess.CompletedProcess[bytes],
    cap: int,
    logs: bool,
    **fields: Any,
) -> dict[str, Any]:
    text = completed.stdout.decode("utf-8", errors="replace")
    err = completed.stderr.decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()][:80]
    truncated = len(completed.stdout) >= cap or len(lines) >= 80
    if logs and _is_permission(completed.returncode, err):
        return _diag(
            True,
            operation,
            limitation="unprivileged account cannot read the journal; privileges were not escalated",
            lines=lines,
            truncated=truncated,
            **fields,
        )
    return _diag(
        completed.returncode == 0,
        operation,
        limitation=_capture_limitation(operation, completed, cap),
        lines=lines,
        truncated=truncated,
        **fields,
    )


def spawn_fixed(argv: list[str], timeout_sec: float, cap: int) -> subprocess.CompletedProcess[bytes]:
    if not argv_is_fixed(argv):
        raise DiagnosticError("refusing an argv that is not a fixed diagnostic")
    import sys

    wrapped = [sys.executable, "-m", "collab.rlimit_exec", *argv]
    return _run_capped(wrapped, timeout_sec, cap)


def _run_capped(argv: list[str], timeout_sec: float, cap: int) -> subprocess.CompletedProcess[bytes]:
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        _kill_group(proc.pid)
        proc.communicate(timeout=2)
        raise
    if len(stdout) > cap:
        stdout = stdout[:cap]
    if len(stderr) > cap:
        stderr = stderr[:cap]
    return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)


def _kill_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def is_ip_literal(host: str) -> bool:
    import ipaddress

    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True
