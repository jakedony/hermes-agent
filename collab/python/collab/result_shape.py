"""Clip worker results to the hub's complete/fail limits."""

from __future__ import annotations

import json
import time
from typing import Any

from collab.limits import (
    MAX_EVIDENCE_ITEMS,
    MAX_EXCERPT_BYTES,
    MAX_LIMITATION_BYTES,
    MAX_LIMITATION_ITEMS,
    MAX_SUMMARY_BYTES,
)


def observed_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    marker = "...[truncated]"
    keep = max(0, limit - len(marker))
    return text[:keep] + marker


def evidence_from_tool(operation: str, source: str, result: str) -> dict[str, str]:
    return {
        "operation": operation[:80] or "tool",
        "source": source[:80] or "unknown",
        "observed_at": observed_now(),
        "excerpt": clip(result, MAX_EXCERPT_BYTES),
    }


def normalize_evidence(items: Any, source: str) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    out: list[dict[str, str]] = []
    for item in items[:MAX_EVIDENCE_ITEMS]:
        if not isinstance(item, dict):
            continue
        operation = str(item.get("operation") or "observation")
        excerpt = item.get("excerpt")
        if not isinstance(excerpt, str):
            excerpt = json.dumps(item, default=str)
        out.append(
            {
                "operation": operation[:80],
                "source": str(item.get("source") or source)[:80],
                "observed_at": str(item.get("observed_at") or observed_now())[:40],
                "excerpt": clip(excerpt, MAX_EXCERPT_BYTES),
            }
        )
    return out


def normalize_limitations(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    lines: list[str] = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            continue
        lines.append(clip(item.strip(), MAX_LIMITATION_BYTES))
        if len(lines) >= MAX_LIMITATION_ITEMS:
            break
    return lines


def complete_payload(attempt_id: str, machine_id: str, summary: str, limitations: Any, evidence: Any) -> dict[str, Any]:
    text = clip(summary.strip() or "no summary", MAX_SUMMARY_BYTES)
    return {
        "attempt_id": attempt_id,
        "summary": text,
        "machine_id": machine_id,
        "limitations": normalize_limitations(limitations),
        "evidence": normalize_evidence(evidence, machine_id),
    }


def child_tool_body(view: dict[str, Any]) -> dict[str, Any]:
    task = view.get("task") if isinstance(view.get("task"), dict) else {}
    state = str(task.get("state") or "unknown")
    result = task.get("result")
    summary = ""
    limitations: list[str] = []
    evidence: list[Any] = []
    if isinstance(result, dict):
        summary = str(result.get("summary") or "")
        raw_limits = result.get("limitations")
        if isinstance(raw_limits, list):
            limitations = [str(item) for item in raw_limits if isinstance(item, str)]
        if isinstance(result.get("evidence"), list):
            evidence = result["evidence"]
    elif isinstance(result, str) and result:
        summary = result
    if not summary:
        summary = str(task.get("error_message") or f"child task {state}")
    return {
        "ok": state == "completed",
        "state": state,
        "task_id": task.get("task_id") or "",
        "summary": summary,
        "limitations": limitations,
        "evidence": evidence,
    }
