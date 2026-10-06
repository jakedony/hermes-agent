"""Register the restricted tool surface before ``AIAgent`` is constructed.

Plugin-like toolsets are deferrable. With tool search left on, Hermes would
replace them with ``tool_search`` / ``tool_describe`` / ``tool_call``. The
worker config turns tool search off, and ``assert_surface`` refuses to run if
the live name set is anything else.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from collab.diagnostics import dumps_result, listening_sockets, tcp_probe
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
    allowlist: frozenset[tuple[str, int]],
    client: ProtocolClient,
    tool_timeout: float,
    delegate_timeout: float,
) -> frozenset[str]:
    from tools.registry import registry

    registry.register(
        name="listening_sockets",
        toolset=TOOLSET_NAME,
        schema=_schema(
            "listening_sockets",
            "List listening TCP and UDP sockets with a fixed ss command. "
            "An optional port is filtered in Python after capture. "
            "Unprivileged: missing permissions are reported and not escalated.",
            {"port": {"type": "integer", "minimum": 1, "maximum": 65535}},
            required=[],
        ),
        handler=_listening_handler(tool_timeout),
    )
    registry.register(
        name="tcp_probe",
        toolset=TOOLSET_NAME,
        schema=_schema(
            "tcp_probe",
            "Open one TCP connection to an allowlisted IP and port. "
            "Hostnames, flags, and any address not on the allowlist are rejected.",
            {
                "host": {"type": "string"},
                "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            },
            required=["host", "port"],
        ),
        handler=_probe_handler(allowlist, tool_timeout),
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

    for name in ("listening_sockets", "tcp_probe", "delegate_investigation"):
        registry.deregister(name)


def _schema(name: str, description: str, properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    parameters: dict[str, Any] = {**_OBJECT, "properties": properties}
    if required:
        parameters["required"] = required
    return {"name": name, "description": description, "parameters": parameters}


def _listening_handler(tool_timeout: float) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        port = args.get("port") if isinstance(args, dict) else None
        if port == "" or port is None:
            port = None
        return dumps_result(listening_sockets(port, timeout_sec=tool_timeout))

    return handler


def _probe_handler(allowlist: frozenset[tuple[str, int]], tool_timeout: float) -> Callable:
    def handler(args: dict[str, Any]) -> str:
        args = args if isinstance(args, dict) else {}
        return dumps_result(tcp_probe(args.get("host"), args.get("port"), allowlist, timeout_sec=tool_timeout))

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
