"""Deterministic worker that does not import Hermes.

It still calls the real diagnostic functions, so a listening-socket or probe
result can be a real observation. Tests that use this path are fake-agent
tests, not hosted-model collaboration.

``context.stall`` sleeps in this process so a bridge test can kill it mid-run.
``context.stall_child`` is copied onto the delegated task only.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from collab.diagnostics import (
    dns_lookup,
    listening_sockets,
    policy_from_start,
    route_show,
    service_logs,
    service_status,
    tcp_probe,
)
from collab.limits import TOOL_TIMEOUT_SEC
from collab.protocol import PROFILE_RE, new_request_id
from collab.protocol_client import ProtocolClient
from collab.result_shape import evidence_from_tool


def run_fake(start: dict[str, Any], client: ProtocolClient) -> dict[str, Any]:
    grant = start["grant"]
    role = str(grant.get("role") or "worker")
    context = grant.get("context") if isinstance(grant.get("context"), dict) else {}
    timeout = float(start.get("tool_timeout_sec") or TOOL_TIMEOUT_SEC)
    policy = policy_from_start(start, timeout)
    machine = str(start.get("machine_id") or "")
    _note_start(str(start.get("state_dir") or ""), str(grant.get("attempt_id") or ""))
    if context.get("stall") is True:
        _stall()
    evidence: list[dict[str, str]] = []
    limitations: list[str] = []
    local = _observe(context, policy, machine, evidence, limitations)
    child = ""
    if role == "coordinator":
        child = _delegate(client, grant, context)
        evidence.append(evidence_from_tool("delegate_investigation", machine, child))
    summary = _summary(local, child)
    return {"type": "result", "summary": summary, "limitations": limitations, "evidence": evidence}


def _note_start(state_dir: str, attempt_id: str) -> None:
    if not state_dir or not attempt_id or "/" in attempt_id or attempt_id.startswith("."):
        return
    root = Path(state_dir) / "runs"
    root.mkdir(parents=True, exist_ok=True)
    with open(root / attempt_id, "a", encoding="utf-8") as handle:
        handle.write("start\n")


def _stall() -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        time.sleep(0.2)


def _record(operation: str, body: dict[str, Any], machine: str, evidence: list, limitations: list, parts: list) -> None:
    text = json.dumps(body, sort_keys=True)
    evidence.append(evidence_from_tool(operation, machine, text))
    limitation = body.get("limitation")
    if limitation and len(limitations) < 8:
        limitations.append(str(limitation))
    parts.append(text)


def _observe(context: dict[str, Any], policy, machine: str, evidence: list, limitations: list) -> str:
    parts: list[str] = []
    host = context.get("host")
    port = context.get("port")
    if host is not None and port is not None:
        probe = tcp_probe(host, port, policy.probes, timeout_sec=policy.timeout_sec, forbidden=policy.forbidden())
        _record("tcp_probe", probe, machine, evidence, limitations, parts)
    listen_port = port if isinstance(port, int) and not isinstance(port, bool) else None
    _record("listening_sockets", listening_sockets(listen_port, timeout_sec=policy.timeout_sec), machine, evidence, limitations, parts)
    _record("route_show", route_show(timeout_sec=policy.timeout_sec), machine, evidence, limitations, parts)
    _named_tools(context, policy, machine, evidence, limitations, parts)
    return "\n".join(parts)


def _named_tools(context: dict[str, Any], policy, machine: str, evidence: list, limitations: list, parts: list) -> None:
    unit = context.get("service")
    if isinstance(unit, str):
        _record("service_status", service_status(unit, policy.services, timeout_sec=policy.timeout_sec), machine, evidence, limitations, parts)
        _record("service_logs", service_logs(unit, policy.services, timeout_sec=policy.timeout_sec), machine, evidence, limitations, parts)
    name = context.get("dns_name")
    if isinstance(name, str):
        _record("dns_lookup", dns_lookup(name, policy.dns_names, timeout_sec=policy.timeout_sec), machine, evidence, limitations, parts)


def _delegate(client: ProtocolClient, grant: dict[str, Any], context: dict[str, Any]) -> str:
    child_context: dict[str, Any] = {}
    if isinstance(context.get("port"), int) and not isinstance(context.get("port"), bool):
        child_context["port"] = context["port"]
    if isinstance(context.get("service"), str):
        child_context["service"] = context["service"]
    if isinstance(context.get("dns_name"), str):
        child_context["dns_name"] = context["dns_name"]
    if context.get("stall_child") is True:
        child_context["stall"] = True
    child_context["hint"] = "peer of the coordinator; local diagnostics only"
    profile = "vm-local"
    frame = client.delegate(
        {
            "request_id": new_request_id("dlg"),
            "objective": f"List listeners relevant to: {grant.get('objective')}",
            "context": child_context,
            "profile": profile if PROFILE_RE.match(profile) else "default",
            "timeout_sec": 120,
        },
        timeout=150,
    )
    body = frame.get("body")
    if isinstance(body, dict):
        return str(body.get("summary") or json.dumps(body)[:2000])
    return "peer returned nothing"


def _summary(local: str, child: str) -> str:
    lines = ["fake-agent observation (not a Hermes model)", local]
    if child:
        lines.append("peer: " + child)
    return "\n".join(lines)[:8000]
