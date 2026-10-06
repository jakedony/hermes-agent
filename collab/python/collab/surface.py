"""Register the restricted tool surface before ``AIAgent`` is constructed.

Plugin-like toolsets are deferrable. With tool search left on, Hermes would
replace them with ``tool_search`` / ``tool_describe`` / ``tool_call``. The
worker config turns tool search off, and ``assert_surface`` refuses to run if
the live name set is anything else.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from collab.diagnostics import (
    DiagPolicy,
    dumps_result,
    dns_lookup,
    listening_sockets,
    route_show,
    service_logs,
    service_status,
    tcp_probe,
)
from collab.limits import COORDINATOR_TOOLS, TOOLSET_NAME, WORKER_TOOLS
from collab.protocol import new_request_id
from collab.protocol_client import ProtocolClient

_OBJECT = {"type": "object", "additionalProperties": False}


class SurfaceError(Exception):
    pass


def approved_names(role: str) -> frozenset[str]:
    if role == "coordinator":
        return COORDINATOR_TOOLS
    if role == "worker":
        return WORKER_TOOLS
    raise SurfaceError(f"unknown role {role}")


def assert_surface(valid_names: Any, role: str) -> None:
    approved = approved_names(role)
    found = set(valid_names or ())
    if found != approved:
        raise SurfaceError(
            f"refusing to run; tool surface {sorted(found)} != approved {sorted(approved)}"
        )


def install_tools(
    role: str,
    policy: DiagPolicy,
    client: ProtocolClient,
    delegate_timeout: float,
) -> frozenset[str]:
    from tools.registry import registry

    for name, description, properties, required, handler in _local_tools(policy):
        registry.register(
            name=name,
            toolset=TOOLSET_NAME,
            schema=_schema(name, description, properties, required),
            handler=handler,
        )
    if role == "coordinator":
        registry.register(
            name="delegate_investigation",
            toolset=TOOLSET_NAME,
            schema=_schema(
                "delegate_investigation",
                "Ask the peer machine to investigate and wait for its hub result. "
                "The peer only has local diagnostics. This does not start a second local shell.",
                {
                    "objective": {"type": "string"},
                    "context": {"type": "object"},
                    "profile": {"type": "string"},
                    "timeout_sec": {"type": "integer", "minimum": 1, "maximum": 120},
                },
                required=["objective"],
            ),
            handler=_delegate_handler(client, delegate_timeout),
        )
    return approved_names(role)


def remove_tools() -> None:
    from tools.registry import registry

    for name in COORDINATOR_TOOLS:
        registry.deregister(name)


def _schema(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    parameters: dict[str, Any] = {**_OBJECT, "properties": properties}
    if required:
        parameters["required"] = required
    return {"name": name, "description": description, "parameters": parameters}


def _local_tools(policy: DiagPolicy) -> list[tuple[str, str, dict[str, Any], list[str], Callable]]:
    timeout = policy.timeout_sec
    port = {"type": "integer", "minimum": 1, "maximum": 65535}
    unit = {"type": "string"}
    return [
        (
            "listening_sockets",
            "List listening TCP and UDP sockets with a fixed ss command. "
            "An optional port is filtered in Python after capture. "
            "Unprivileged: missing permissions are reported and not escalated.",
            {"port": port},
            [],
            _listening_handler(timeout),
        ),
        (
            "tcp_probe",
            "Open one TCP connection to an allowlisted IP and port. "
            "Hostnames, flags, the hub forward, and any address not on the allowlist are rejected.",
            {"host": {"type": "string"}, "port": port},
            ["host", "port"],
            _probe_handler(policy),
        ),
        (
            "service_status",
            "Show a fixed set of systemctl properties for one allowlisted unit. "
            "The unit is a single argv slot after -- and must match the service allowlist. "
            "Do not expect privilege escalation.",
            {"unit": unit},
            ["unit"],
            _status_handler(policy),
        ),
        (
            "service_logs",
            "Read a bounded journalctl excerpt for one allowlisted unit. "
            "Line count and byte cap are fixed. A permission denial is an observation; privileges are not escalated.",
            {"unit": unit},
            ["unit"],
            _logs_handler(policy),
        ),
        (
            "route_show",
            "Show the kernel routing table with a fixed ip route show vector. No caller arguments are passed to ip.",
            {},
            [],
            _route_handler(timeout),
        ),
        (
            "dns_lookup",
            "Resolve one allowlisted DNS name with a fixed getent ahosts vector. "
            "This is not an open resolver and does not scan.",
            {"name": {"type": "string"}},
            ["name"],
            _dns_handler(policy),
        ),
    ]


def _listening_handler(tool_timeout: float) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        port = args.get("port") if isinstance(args, dict) else None
        if port == "" or port is None:
            port = None
        return dumps_result(listening_sockets(port, timeout_sec=tool_timeout))

    return handler


def _probe_handler(policy: DiagPolicy) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        body = tcp_probe(
            args.get("host"),
            args.get("port"),
            policy.probes,
            timeout_sec=policy.timeout_sec,
            forbidden=policy.forbidden(),
        )
        return dumps_result(body)

    return handler


def _status_handler(policy: DiagPolicy) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        return dumps_result(service_status(args.get("unit"), policy.services, timeout_sec=policy.timeout_sec))

    return handler


def _logs_handler(policy: DiagPolicy) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        return dumps_result(service_logs(args.get("unit"), policy.services, timeout_sec=policy.timeout_sec))

    return handler


def _route_handler(tool_timeout: float) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        del args
        return dumps_result(route_show(timeout_sec=tool_timeout))

    return handler


def _dns_handler(policy: DiagPolicy) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        return dumps_result(dns_lookup(args.get("name"), policy.dns_names, timeout_sec=policy.timeout_sec))

    return handler


def _delegate_handler(client: ProtocolClient, delegate_timeout: float) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        objective = str(args.get("objective") or "").strip()
        if not objective:
            return json.dumps({"ok": False, "error": "objective is required"})
        context = args.get("context") if isinstance(args.get("context"), dict) else {}
        timeout = args.get("timeout_sec")
        if isinstance(timeout, bool) or not isinstance(timeout, int):
            timeout = 120
        request_id = new_request_id("dlg")
        try:
            frame = client.delegate(
                {
                    "request_id": request_id,
                    "objective": objective,
                    "context": context,
                    "profile": str(args.get("profile") or ""),
                    "timeout_sec": timeout,
                },
                timeout=delegate_timeout,
            )
        except TimeoutError:
            return json.dumps({"ok": False, "error": "delegate_investigation timed out"})
        body = frame.get("body")
        if not isinstance(body, dict):
            body = {"ok": False, "summary": "peer returned no object"}
        return json.dumps(body)

    return handler
