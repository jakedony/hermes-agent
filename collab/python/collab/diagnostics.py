"""Restricted local diagnostics.

``listening_sockets`` runs a fixed ``ss`` argument vector. The port filter is
applied in Python after the process exits, so a model-supplied port never
becomes a shell word or an ``ss`` expression.

``tcp_probe`` opens one TCP connection to an IP and port that appear together
on the bridge allowlist. Hostnames, leading dashes, and any other option-shaped
input are rejected before a socket is created.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import signal
import subprocess
import time
from typing import Any

from collab.config import AllowTarget, ConfigError, normalize_target
from collab.limits import DIAG_OUTPUT_CAP, SS_OUTPUT_CAP, TOOL_TIMEOUT_SEC

SS_CANDIDATES = ("/usr/bin/ss", "/bin/ss")
SS_ARGS = ("-H", "-l", "-n", "-t", "-u")
_PORT_IN_LINE = re.compile(r":(\d+)(?!\d)")


class DiagnosticError(Exception):
    pass


def ss_path() -> str | None:
    for candidate in SS_CANDIDATES:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def ss_argv(path: str) -> list[str]:
    """Interpreter, limit helper, and the fixed ss vector. No user strings."""
    if path not in SS_CANDIDATES:
        raise DiagnosticError("ss path is not one of the fixed candidates")
    import sys

    return [sys.executable, "-m", "collab.rlimit_exec", path, *SS_ARGS]


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
    argv = ss_argv(path)
    try:
        completed = _run_capped(argv, timeout_sec, SS_OUTPUT_CAP)
    except subprocess.TimeoutExpired:
        return _diag(False, "listening_sockets", limitation="ss timed out; the process group was killed", port=port, executable=path)
    except OSError as exc:
        return _diag(False, "listening_sockets", limitation=f"ss could not be started: {exc.__class__.__name__}", port=port, executable=path)
    text = completed.stdout.decode("utf-8", errors="replace")
    if len(completed.stdout) >= SS_OUTPUT_CAP:
        limitation = "ss output hit the capture cap and was truncated"
    elif completed.returncode != 0:
        err = completed.stderr.decode("utf-8", errors="replace")[:500]
        limitation = f"ss exited {completed.returncode} without extra privileges: {err}".strip()
    else:
        limitation = None
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


def classify_probe(host: Any, port: Any, allowlist: frozenset[tuple[str, int]]) -> str | None:
    """Return a rejection reason, or None when the pair may be dialed."""
    if not isinstance(host, str) or not host or host.startswith("-") or any(ch.isspace() for ch in host):
        return "host is empty, has whitespace, or looks like an option"
    if isinstance(port, bool) or not isinstance(port, int):
        return "port must be an integer"
    try:
        target = normalize_target(host, port)
    except ConfigError:
        return "host must be an IP address and port must be 1..65535"
    if (target.host, target.port) not in allowlist:
        return "host and port are not on the probe allowlist together"
    return None


def tcp_probe(host: Any, port: Any, allowlist: frozenset[tuple[str, int]], timeout_sec: float = TOOL_TIMEOUT_SEC) -> dict[str, Any]:
    reason = classify_probe(host, port, allowlist)
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


def _diag(ok: bool, operation: str, **fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"ok": ok, "operation": operation, "limitation": fields.pop("limitation", None)}
    body.update(fields)
    return body


def dumps_result(body: dict[str, Any]) -> str:
    text = json.dumps(body, sort_keys=True, default=str)
    if len(text) > DIAG_OUTPUT_CAP:
        body = dict(body)
        body["truncated"] = True
        for key in ("listeners",):
            if isinstance(body.get(key), list):
                body[key] = body[key][:10]
        text = json.dumps(body, sort_keys=True, default=str)[:DIAG_OUTPUT_CAP]
    return text


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
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True
