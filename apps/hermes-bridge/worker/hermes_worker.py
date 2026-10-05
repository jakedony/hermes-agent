#!/usr/bin/env python3
"""Hermes bridge worker: one Hermes conversation behind an NDJSON pipe.

The Go bridge starts one worker per active session and talks to it over
stdin/stdout, one JSON object per line (see PROTOCOL.md, "Worker protocol").
The worker owns a single ``AIAgent`` and the conversation history it returns;
nothing is shared between workers, so sessions cannot see each other.

stdout is reserved for protocol records. Before Hermes is imported, file
descriptor 1 is re-pointed at stderr and the protocol stream gets a private
duplicate, so a stray ``print`` (Hermes prints some interrupt notices even with
``quiet_mode=True``) or a child process writing to fd 1 lands on stderr instead
of corrupting the pipe.

Run it with the interpreter the installed ``hermes`` command uses::

    /path/to/hermes-agent/scripts/run-in-hermes-env python3 hermes_worker.py
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
import uuid

WORKER_PROTOCOL_VERSION = 1
EXIT_OK = 0
EXIT_NOT_CONFIGURED = 3
EXIT_INIT_FAILED = 4
EXIT_PROTOCOL = 5

# What the agent may do in this milestone: answer from the model alone.
# An empty enabled_toolsets list resolves to zero tools in the installed
# Hermes (verified: the model request carries no tool schemas).
TOOL_POLICY: list[str] = []

logger = logging.getLogger("hermes_bridge.worker")


def _claim_stdout():
    """Return a private binary stream for protocol records; route fd 1 to stderr."""
    sys.stdout.flush()
    proto_fd = os.dup(1)
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    return os.fdopen(proto_fd, "wb", buffering=0)


class Channel:
    """Serialized writer for protocol records with a hard size bound."""

    def __init__(self, stream, max_record_bytes):
        self._stream = stream
        self._max = max_record_bytes
        self._lock = threading.Lock()

    def send(self, record):
        data = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(data) > self._max:
            data = json.dumps(
                {
                    "type": "result",
                    "requestId": record.get("requestId"),
                    "ok": False,
                    "code": "RESPONSE_TOO_LARGE",
                    "message": f"The answer exceeded the {self._max}-byte record limit.",
                }
            ).encode("utf-8") + b"\n"
        with self._lock:
            try:
                self._stream.write(data)
            except BrokenPipeError:
                # The bridge is gone; nothing left to report to.
                os._exit(EXIT_PROTOCOL)


class DeltaStream:
    """Coalesces ``stream_delta_callback`` pieces into ``delta`` records for the current request.

    Hermes calls the callback once per token, possibly from a provider-stream thread; one record
    per token would flood the pipe, so pieces are batched for up to FLUSH_SECONDS or FLUSH_CHARS.
    ``end()`` flushes before the turn's result is sent, so every delta precedes its result.
    """

    FLUSH_SECONDS = 0.05
    FLUSH_CHARS = 2048

    def __init__(self, channel):
        self._channel = channel
        self._lock = threading.Lock()
        self._request_id = None
        self._parts: list[str] = []
        self._chars = 0
        self._last_flush = 0.0

    def begin(self, request_id, enabled):
        with self._lock:
            self._request_id = request_id if enabled else None
            self._parts, self._chars, self._last_flush = [], 0, 0.0

    def feed(self, text):
        # None from Hermes closes a CLI display box; it is not end of stream.
        if not isinstance(text, str) or not text:
            return
        with self._lock:
            if self._request_id is None:
                return
            self._parts.append(text)
            self._chars += len(text)
            if self._chars >= self.FLUSH_CHARS or time.monotonic() - self._last_flush >= self.FLUSH_SECONDS:
                self._flush_locked()

    def end(self):
        with self._lock:
            self._flush_locked()
            self._request_id = None

    def _flush_locked(self):
        text = "".join(self._parts)
        self._parts, self._chars = [], 0
        self._last_flush = time.monotonic()
        # Bounded slices keep every delta far below the record limit, whose overflow substitute
        # is a result record and would end the request early.
        for i in range(0, len(text), self.FLUSH_CHARS):
            self._channel.send({"type": "delta", "requestId": self._request_id, "text": text[i:i + self.FLUSH_CHARS]})


def _diag(event, **fields):
    """One structured diagnostic line on stderr; never conversation content."""
    print(json.dumps({"worker": event, **fields}), file=sys.stderr, flush=True)


def _hermes_version():
    try:
        from hermes_cli import __release_date__
        from hermes_cli.version_info import get_version_info
    except ImportError:
        return "unknown"
    return f"{get_version_info().derived_version} ({__release_date__})"


def resolve_route():
    """The configured model plus its provider runtime, resolved like the gateway does.

    ``AIAgent()`` without ``model=`` sends no model id, which real endpoints
    reject ("Missing model parameter"), so the route must be resolved here.
    """
    from hermes_cli.config import load_config
    from hermes_cli.runtime_provider import resolve_runtime_with_fallback

    cfg = load_config()
    model_cfg = cfg.get("model")
    if isinstance(model_cfg, str):
        model = model_cfg
    else:
        model = (model_cfg or {}).get("default") or (model_cfg or {}).get("model") or ""
    runtime, fallback_entry = resolve_runtime_with_fallback(cfg, target_model=model or None)
    if fallback_entry is not None:
        model = fallback_entry["model"]
    route = {key: runtime.get(key) for key in
             ("api_key", "base_url", "provider", "requested_provider", "api_mode", "credential_pool",
              "request_overrides", "command")}
    route["args"] = list(runtime.get("args") or [])
    route["model"] = model
    return route


def _describe(exc):
    """Hermes's own operator-facing wording for a provider-resolution failure."""
    from hermes_cli.runtime_provider import format_runtime_provider_error

    return format_runtime_provider_error(exc)


def build_agent(args, on_delta=None):
    from run_agent import AIAgent

    return AIAgent(
        **resolve_route(),
        stream_delta_callback=on_delta,
        quiet_mode=True,
        platform="api_server",
        enabled_toolsets=TOOL_POLICY,
        max_iterations=args.max_iterations,
        run_budget_seconds=args.run_budget_seconds or None,
        skip_context_files=True,
        load_soul_identity=True,
        skip_memory=not args.memory,
        skip_background_review=True,
        session_id=f"bridge-{uuid.uuid4().hex[:16]}",
    )


class Conversation:
    """One AIAgent plus the message history it has produced so far."""

    def __init__(self, agent, channel, deltas=None):
        self.agent = agent
        self.channel = channel
        self.deltas = deltas or DeltaStream(channel)
        self.history: list[dict] = []
        self.task_id = f"bridge-task-{uuid.uuid4().hex[:12]}"
        self._lock = threading.Lock()
        self._thread = None
        self._active_request = None
        self._cancelled = set()

    def busy(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self, request_id, message, stream=False):
        with self._lock:
            if self.busy():
                self.channel.send(
                    {"type": "result", "requestId": request_id, "ok": False, "code": "WORKER_BUSY",
                     "message": "The worker is already running a request."}
                )
                return
            from agent.memory_provider import spawn_context_thread

            self._active_request = request_id
            self._thread = spawn_context_thread(self._run, name="bridge-turn", args=(request_id, message, stream))
            self._thread.start()

    def cancel(self, request_id):
        with self._lock:
            if self._active_request != request_id or not self.busy():
                return
            self._cancelled.add(request_id)
        self.agent.interrupt(hard_cancel=True, tool_reason="bridge request timeout")

    def wait(self, timeout):
        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def _run(self, request_id, message, stream):
        started = time.monotonic()
        self.deltas.begin(request_id, stream)
        try:
            result = self.agent.run_conversation(
                message, conversation_history=list(self.history), task_id=self.task_id
            )
        except Exception as exc:  # the turn boundary: any failure becomes a structured result
            logger.exception("bridge turn %s failed", request_id)
            outcome = {"type": "result", "requestId": request_id, "ok": False, "code": "HERMES_ERROR",
                       "message": f"{type(exc).__name__}: {str(exc)[:500]}"}
        else:
            outcome = self._outcome(request_id, result, time.monotonic() - started)
        finally:
            self.deltas.end()
            with self._lock:
                self._active_request = None
        self.channel.send(outcome)

    def _outcome(self, request_id, result, elapsed):
        meta = {"model": result.get("model"), "provider": result.get("provider"),
                "apiCalls": result.get("api_calls"), "elapsedMs": int(elapsed * 1000)}
        if result.get("interrupted"):
            # The interrupted turn is dropped so history keeps strict
            # user/assistant alternation; earlier turns are untouched.
            code = "CANCELLED" if request_id in self._cancelled else "HERMES_INTERRUPTED"
            self._cancelled.discard(request_id)
            return {"type": "result", "requestId": request_id, "ok": False, "code": code,
                    "message": "The turn was interrupted before it finished.", **meta}
        text = result.get("final_response")
        if result.get("failed") or not isinstance(text, str) or not text.strip():
            detail = str(text or result.get("error") or "Hermes returned no final response.")[:500]
            return {"type": "result", "requestId": request_id, "ok": False, "code": "HERMES_ERROR",
                    "message": detail, **meta}
        self.history = list(result.get("messages") or [])
        history_bytes = len(json.dumps(self.history, ensure_ascii=False, default=str).encode("utf-8"))
        return {"type": "result", "requestId": request_id, "ok": True, "text": text,
                "messageCount": len(self.history), "historyBytes": history_bytes, **meta}


def _handle(conv, record):
    kind = record.get("type")
    if kind == "ask" and isinstance(record.get("requestId"), str) and isinstance(record.get("message"), str):
        conv.start(record["requestId"], record["message"], stream=record.get("stream") is True)
    elif kind == "cancel" and isinstance(record.get("requestId"), str):
        conv.cancel(record["requestId"])
    elif kind == "shutdown":
        return False
    else:
        _diag("bad_record", recordType=str(kind)[:32])
    return True


def serve(conv, max_record_bytes):
    stdin = sys.stdin.buffer
    while True:
        line = stdin.readline(max_record_bytes + 1)
        if not line:
            return "stdin_closed"
        if len(line) > max_record_bytes or not line.endswith(b"\n"):
            _diag("oversized_or_partial_record", bytes=len(line))
            return "protocol_error"
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            _diag("invalid_json_record")
            return "protocol_error"
        if not isinstance(record, dict) or not _handle(conv, record):
            return "shutdown" if isinstance(record, dict) else "protocol_error"


def parse_args(argv):
    p = argparse.ArgumentParser(description="Hermes bridge worker (NDJSON over stdio).")
    p.add_argument("--hermes-home", default="",
                   help="Use this Hermes home instead of the inherited one (tests use a throwaway home).")
    p.add_argument("--workdir", default="", help="Working directory for the agent.")
    p.add_argument("--max-iterations", type=int, default=4)
    p.add_argument("--run-budget-seconds", type=float, default=0.0)
    p.add_argument("--memory", action="store_true",
                   help="Load built-in memory and the configured memory provider (off by default).")
    p.add_argument("--max-record-bytes", type=int, default=4 * 1024 * 1024)
    p.add_argument("--shutdown-grace-seconds", type=float, default=5.0)
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    channel = Channel(_claim_stdout(), args.max_record_bytes)
    if args.hermes_home:
        os.environ["HERMES_HOME"] = args.hermes_home
    if args.workdir:
        os.makedirs(args.workdir, exist_ok=True)
        os.chdir(args.workdir)
    deltas = DeltaStream(channel)
    try:
        agent = build_agent(args, deltas.feed)
    except Exception as exc:  # startup boundary: report why Hermes could not start, then exit
        # AuthError: the runtime resolver found no usable credentials (none set up, expired, or
        # exhausted); ProviderNotConfiguredError: AIAgent found no provider at all.
        not_configured = type(exc).__name__ in {"AuthError", "ProviderNotConfiguredError"}
        code = "HERMES_NOT_CONFIGURED" if not_configured else "HERMES_INIT_FAILED"
        _diag("init_failed", exception=type(exc).__name__)
        if not not_configured:
            logger.exception("Hermes agent construction failed")
        channel.send({"type": "fatal", "code": code, "message": f"{type(exc).__name__}: {_describe(exc)[:500]}"})
        return EXIT_NOT_CONFIGURED if not_configured else EXIT_INIT_FAILED

    conv = Conversation(agent, channel, deltas)
    channel.send({"type": "ready", "workerProtocolVersion": WORKER_PROTOCOL_VERSION, "pid": os.getpid(),
                  "hermesVersion": _hermes_version(), "tools": sorted(agent.valid_tool_names),
                  "memory": bool(args.memory)})
    _diag("ready", pid=os.getpid())
    reason = serve(conv, args.max_record_bytes)
    if conv.busy():
        agent.interrupt(hard_cancel=True, tool_reason="bridge worker shutdown")
        conv.wait(args.shutdown_grace_seconds)
    agent.close()
    _diag("exit", reason=reason)
    return EXIT_PROTOCOL if reason == "protocol_error" else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
