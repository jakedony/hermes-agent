"""One Hermes ``run_conversation`` for a granted attempt.

Hermes is imported here, not at package import, so fake-agent workers and the
bridge process do not load it. There is no hosted-model credential: ``base_url``
is whatever the bridge put in the start frame (a local stub in the demo).
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from collab.diagnostics import policy_from_start
from collab.limits import MAX_ITERATIONS
from collab.protocol_client import ProtocolClient
from collab.result_shape import evidence_from_tool, normalize_limitations
from collab.surface import SurfaceError, assert_surface, install_tools, remove_tools

CONTEXT_LENGTH = 128000
log = logging.getLogger("collab.hermes")


def prepare_hermes_home(home: Path, model: str, base_url: str, api_key: str) -> None:
    if "\n" in model or "\n" in base_url or "\n" in api_key:
        raise ValueError("model, base_url, and api_key must be single-line")
    home.mkdir(parents=True, exist_ok=True)
    config_path = home / "config.yaml"
    config_path.write_text(
        "model:\n"
        "  provider: custom\n"
        f"  base_url: {base_url}\n"
        f"  default: {model}\n"
        f"  context_length: {CONTEXT_LENGTH}\n"
        "agent:\n"
        "  api_max_retries: 1\n"
        "tools:\n"
        "  tool_search:\n"
        '    enabled: "off"\n'
        "telemetry:\n"
        "  shared_metrics:\n"
        "    enabled: false\n",
        encoding="utf-8",
    )
    env_path = home / ".env"
    env_path.write_text(f"OPENAI_API_KEY={api_key}\n", encoding="utf-8")
    os.chmod(config_path, 0o600)
    os.chmod(env_path, 0o600)


def run_hermes(start: dict[str, Any], client: ProtocolClient) -> dict[str, Any]:
    grant = start["grant"]
    role = str(grant.get("role") or "worker")
    model_info = start.get("model") if isinstance(start.get("model"), dict) else {}
    model = str(model_info.get("id") or "")
    base_url = str(model_info.get("base_url") or "")
    api_key = str(model_info.get("api_key") or "")
    home = Path(str(start.get("hermes_home") or os.environ.get("HERMES_HOME") or ""))
    if not model or not base_url or not api_key or not str(home):
        return _fail("invalid_input", "hermes worker is missing model, base_url, api_key, or HERMES_HOME")
    os.environ["HERMES_HOME"] = str(home)
    prepare_hermes_home(home, model, base_url, api_key)
    tool_timeout = float(start.get("tool_timeout_sec") or 10)
    policy = policy_from_start(start, tool_timeout)
    max_iterations = int(start.get("max_iterations") or MAX_ITERATIONS)
    machine = str(start.get("machine_id") or "")
    evidence: list[dict[str, str]] = []
    limitations: list[str] = []
    try:
        install_tools(role, policy, client, delegate_timeout=150)
    except (ImportError, OSError, RuntimeError, ValueError) as exc:
        log.warning("tool registration failed", exc_info=True)
        remove_tools()
        return _fail("invalid_input", f"tool registration failed: {exc.__class__.__name__}")
    from run_agent import AIAgent

    agent = AIAgent(
        model=model,
        provider="custom",
        base_url=base_url,
        api_key=api_key,
        api_mode="chat_completions",
        max_iterations=max_iterations,
        enabled_toolsets=["collab_diag"],
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        skip_background_review=True,
        platform="collab",
    )
    try:
        try:
            assert_surface(agent.valid_tool_names, role)
        except SurfaceError as exc:
            return _fail("invalid_input", str(exc))
        agent.tool_complete_callback = _collector(machine, evidence, limitations)
        objective = str(grant.get("objective") or "")
        result = agent.run_conversation(objective)
    finally:
        close = getattr(agent, "close", None)
        if close is not None:
            close()
        remove_tools()
    if not result.get("completed"):
        detail = str(result.get("final_response") or result.get("error") or "conversation did not complete")
        return _fail("crash", detail[:2000])
    summary = str(result.get("final_response") or "").strip() or "no summary"
    return {
        "type": "result",
        "summary": summary,
        "limitations": normalize_limitations(limitations),
        "evidence": evidence[:8],
        "api_calls": result.get("api_calls"),
    }


def _collector(machine: str, evidence: list[dict[str, str]], limitations: list[str]):
    def on_tool(tool_call_id: str, name: str, args: Any, result: Any) -> None:
        del tool_call_id, args
        text = result if isinstance(result, str) else json.dumps(result, default=str)
        evidence.append(evidence_from_tool(str(name), machine, text))
        limitation = _limitation_from(text)
        if limitation and len(limitations) < 8:
            limitations.append(limitation)

    return on_tool


def _limitation_from(text: str) -> str:
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return ""
    if isinstance(body, dict) and isinstance(body.get("limitation"), str):
        return body["limitation"]
    return ""


def _fail(kind: str, message: str) -> dict[str, Any]:
    return {"type": "fail", "class": kind, "message": message[:2000]}
