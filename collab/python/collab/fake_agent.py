"""Deterministic worker that does not import Hermes.

It still calls the real diagnostic functions, so a listening-socket or probe
result can be a real observation. Tests that use this path are fake-agent
tests, not hosted-model collaboration.
"""

from __future__ import annotations

import json
from typing import Any

from collab.diagnostics import allowlist_from_pairs, listening_sockets, tcp_probe
from collab.limits import TOOL_TIMEOUT_SEC
from collab.protocol import PROFILE_RE, new_request_id
from collab.protocol_client import ProtocolClient
from collab.result_shape import evidence_from_tool


def run_fake(start: dict[str, Any], client: ProtocolClient) -> dict[str, Any]:
    grant = start["grant"]
    role = str(grant.get("role") or "worker")
    context = grant.get("context") if isinstance(grant.get("context"), dict) else {}
    allowlist = allowlist_from_pairs(start.get("allowlist") or [])
    timeout = float(start.get("tool_timeout_sec") or TOOL_TIMEOUT_SEC)
    machine = str(start.get("machine_id") or "")
    evidence: list[dict[str, str]] = []
    limitations: list[str] = []
    local = _observe(context, allowlist, timeout, machine, evidence, limitations)
    child = ""
    if role == "coordinator":
        child = _delegate(client, grant, context)
        evidence.append(evidence_from_tool("delegate_investigation", machine, child))
    summary = _summary(local, child)
    return {"type": "result", "summary": summary, "limitations": limitations, "evidence": evidence}


def _observe(context: dict[str, Any], allowlist, timeout: float, machine: str, evidence: list, limitations: list) -> str:
    parts: list[str] = []
    host = context.get("host")
    port = context.get("port")
    if host is not None and port is not None:
        probe = tcp_probe(host, port, allowlist, timeout_sec=timeout)
        text = json.dumps(probe, sort_keys=True)
        evidence.append(evidence_from_tool("tcp_probe", machine, text))
        if probe.get("limitation"):
            limitations.append(str(probe["limitation"]))
        parts.append(text)
    listen_port = port if isinstance(port, int) and not isinstance(port, bool) else None
    sockets = listening_sockets(listen_port, timeout_sec=timeout)
    text = json.dumps(sockets, sort_keys=True)
    evidence.append(evidence_from_tool("listening_sockets", machine, text))
    if sockets.get("limitation"):
        limitations.append(str(sockets["limitation"]))
    parts.append(text)
    return "\n".join(parts)


def _delegate(client: ProtocolClient, grant: dict[str, Any], context: dict[str, Any]) -> str:
    child_context: dict[str, Any] = {}
    if isinstance(context.get("port"), int) and not isinstance(context.get("port"), bool):
        child_context["port"] = context["port"]
    child_context["hint"] = "peer of the coordinator; inspect local listeners only"
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
