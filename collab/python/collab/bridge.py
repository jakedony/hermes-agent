"""Agent bridge.

The websocket is the agent session. It reconnects with bounded backoff, sends
``agent.hello`` with ``takeover``, flushes the outbox, and reconciles tasks.
Lease renewal is explicit: a connected socket does not extend a lease. The
watchdog stops the worker on a monotonic clock before the lease expires.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from collab.config import BridgeConfig, read_secret_file
from collab.frames import FrameError, encode_frame
from collab.human import load_or_create_socket_token, serve
from collab.journal import Journal
from collab.limits import (
    IDEMPOTENCY_RETENTION_SEC,
    MAX_CONTEXT_BYTES,
    MAX_FRAME_BYTES,
    MAX_OBJECTIVE_BYTES,
    TERMINAL_TASK_STATES,
)
from collab.procman import kill_pid_dir, proc_start_token, spawn_worker, stop_process_group
from collab.protocol import (
    PROFILE_RE,
    ProtocolError,
    ack_body,
    command_frame,
    error_code,
    new_request_id,
    payload_key,
    retry_after_sec,
    wire_payload,
)
from collab.result_shape import child_tool_body, complete_payload

log = logging.getLogger("collab.bridge")


class HubDown(Exception):
    pass


class Backoff:
    def __init__(self) -> None:
        self._n = 0

    def reset(self) -> None:
        self._n = 0

    def delay(self) -> float:
        raw = min(20.0, 0.5 * (2 ** self._n))
        self._n = min(self._n + 1, 8)
        return raw * random.uniform(0.5, 1.0)


class Bridge:
    def __init__(self, cfg: BridgeConfig, agent_token: str, journal: Journal | None = None) -> None:
        self.cfg = cfg
        self.agent_token = agent_token
        self.journal = journal or Journal(cfg.journal_path)
        self.stopped = asyncio.Event()
        self.helloed = asyncio.Event()
        self.ws: Any = None
        self.pending: dict[str, asyncio.Future] = {}
        self.send_lock = asyncio.Lock()
        self.reconcile_lock = asyncio.Lock()
        self.worker: Any = None
        self.attempt: dict[str, Any] | None = None
        self.starting = False
        self.stop_mono = 0.0
        self.deadline_mono = 0.0
        self.lease_reason = "crash"
        self.child_waiters: dict[str, asyncio.Event] = {}
        self._api_key = ""
        self._frame_task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()

    async def run(self) -> None:
        self._startup()
        human = None
        if self.cfg.human_socket:
            if not self.cfg.human_hub_token_file or not self.cfg.human_socket_token_file:
                raise RuntimeError("human socket requires human_hub_token_file and human_socket_token_file")
            socket_token = load_or_create_socket_token(self.cfg.human_socket_token_file)
            human_token = read_secret_file(self.cfg.human_hub_token_file)
            human = await serve(self.cfg, socket_token, human_token)
        if self.cfg.worker_mode == "hermes":
            self._api_key = read_secret_file(self.cfg.api_key_file) if self.cfg.api_key_file else ""
        tasks = [
            asyncio.create_task(self._renew_loop(), name="collab-renew"),
            asyncio.create_task(self._lease_watch(), name="collab-lease"),
            asyncio.create_task(self._poll_loop(), name="collab-poll"),
            asyncio.create_task(self._reconnect(), name="collab-session"),
        ]
        try:
            await self.stopped.wait()
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            if human is not None:
                human.close()
                await human.wait_closed()
            await self._stop_worker()

    def _spawn(self, coro: Any, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def request_stop(self) -> None:
        self.stopped.set()
        ws = self.ws
        if ws is not None:
            self._spawn(self._close_ws(ws), "collab-close")

    async def _close_ws(self, ws: Any) -> None:
        try:
            await ws.close()
        except Exception:
            log.debug("websocket close failed", exc_info=True)

    def _startup(self) -> None:
        state = self.cfg.state_dir
        from pathlib import Path

        root = Path(state)
        root.mkdir(parents=True, exist_ok=True)
        kill_pid_dir(root / "pids")
        self.journal.recover_unfinished(lambda attempt_id: f"fail_{attempt_id}"[:80], self.cfg.room_id)
        self.journal.prune(IDEMPOTENCY_RETENTION_SEC)

    async def _reconnect(self) -> None:
        backoff = Backoff()
        while not self.stopped.is_set():
            self.helloed.clear()
            try:
                await self._session()
                backoff.reset()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.helloed.clear()
                self.ws = None
                self._fail_pending(exc)
                if self.stopped.is_set():
                    break
                log.warning("hub session ended: %s", exc.__class__.__name__, exc_info=True)
                await asyncio.sleep(backoff.delay())

    async def _session(self) -> None:
        headers = {"Authorization": f"Bearer {self.agent_token}"}
        async with websockets.connect(
            self.cfg.hub_ws,
            additional_headers=headers,
            max_size=MAX_FRAME_BYTES,
            open_timeout=10,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            reader = asyncio.create_task(self._read_loop(ws), name="collab-read")
            try:
                await self._hello(ws)
                self.ws = ws
                self.helloed.set()
                await self.flush_outbox()
                await self.reconcile()
                await reader
            finally:
                self.helloed.clear()
                self.ws = None
                reader.cancel()
                self._fail_pending(HubDown("session closed"))

    async def _hello(self, ws: Any) -> None:
        self.ws = ws
        env = await self.rpc("agent.hello", new_request_id("hel"), "", {"takeover": True}, timeout=15)
        if env.get("type") != "ack":
            raise HubDown(error_code(env) or "hello rejected")
        log.info("session ready agent=%s", self.cfg.agent_id)

    async def _read_loop(self, ws: Any) -> None:
        try:
            while True:
                raw = await ws.recv()
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                self._deliver(json.loads(raw))
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, json.JSONDecodeError) as exc:
            self._fail_pending(exc)
            raise HubDown(exc.__class__.__name__) from exc

    def _deliver(self, env: Any) -> None:
        if not isinstance(env, dict):
            return
        # Events and acks share this socket; either may arrive first.
        # Match only type plus request_id. The wire has no ok or replay field.
        request_id = env.get("request_id") or ""
        future = self.pending.get(request_id) if request_id else None
        if future is not None and env.get("type") in {"ack", "error"}:
            self.pending.pop(request_id, None)
            if not future.done():
                future.set_result(env)
            return
        task_id = str(env.get("task_id") or "")
        waiter = self.child_waiters.get(task_id)
        if waiter is not None:
            waiter.set()
        if env.get("type") in {"task.created", "task.requeued"}:
            self._spawn(self._reconcile_safe(), "collab-hint")
        if env.get("type") == "task.cancelled" and self.attempt and task_id == self.attempt.get("task_id"):
            self.lease_reason = "cancelled"
            self._spawn(self._stop_worker(), "collab-cancel-worker")

    def _fail_pending(self, exc: BaseException) -> None:
        for future in list(self.pending.values()):
            if not future.done():
                future.set_exception(HubDown(exc.__class__.__name__))
        self.pending.clear()

    async def rpc(self, command_type: str, request_id: str, task_id: str, payload: dict[str, Any] | str, timeout: float = 20) -> dict[str, Any]:
        frame = command_frame(command_type, request_id, self.cfg.room_id, payload, task_id)
        loop = asyncio.get_running_loop()
        last: Exception = HubDown("no acknowledgement")
        for _ in range(3):
            ws = self.ws
            if ws is None:
                await asyncio.sleep(0.1)
                last = HubDown("not connected")
                continue
            future = loop.create_future()
            async with self.send_lock:
                if self.ws is not ws:
                    continue
                self.pending[request_id] = future
                try:
                    await ws.send(frame)
                except (ConnectionClosed, OSError) as exc:
                    self.pending.pop(request_id, None)
                    last = exc
                    continue
            try:
                return await asyncio.wait_for(future, timeout)
            except asyncio.TimeoutError as exc:
                self.pending.pop(request_id, None)
                last = exc
            except HubDown as exc:
                last = exc
        raise HubDown(str(last))

    async def flush_outbox(self) -> None:
        for item in self.journal.pending_outbox():
            try:
                parsed = json.loads(item.payload_json)
            except json.JSONDecodeError:
                self.journal.mark_terminal(item.request_id, "bad_request")
                continue
            if not isinstance(parsed, dict):
                self.journal.mark_terminal(item.request_id, "bad_request")
                continue
            # Resend the stored text. Re-encoding would change the hub's payload hash.
            env = await self.rpc(item.command_type, item.request_id, item.task_id, item.payload_json)
            self._note_flush(item.request_id, env)

    def _note_flush(self, request_id: str, env: dict[str, Any]) -> None:
        if env.get("type") == "ack":
            self.journal.mark_sent(request_id)
            return
        code = error_code(env)
        if code in {"stale", "conflict", "forbidden", "bad_request", "deadline", "capacity"}:
            self.journal.mark_terminal(request_id, code or "error")

    async def reconcile(self) -> None:
        if not self.helloed.is_set():
            return
        async with self.reconcile_lock:
            env = await self.rpc("task.list", new_request_id("lst"), "", {"assigned_to": self.cfg.agent_id, "state": ""})
            if env.get("type") != "ack":
                return
            tasks = ack_body(env).get("tasks") or []
            handlers = {"queued": self._claim_task, "running": self._attach_running}
            for task in tasks:
                if not isinstance(task, dict):
                    continue
                handler = handlers.get(str(task.get("state") or ""))
                if handler is not None:
                    await handler(task)
            await self.flush_outbox()

    async def _claim_task(self, task: dict[str, Any]) -> None:
        if self.worker is not None or self.starting:
            return
        task_id = str(task.get("task_id") or "")
        generation = int(task.get("attempt_count") or 0)
        request_id = self.journal.claim_request_id(task_id, generation, lambda: new_request_id("clm"))
        while not self.stopped.is_set():
            env = await self.rpc("task.claim", request_id, task_id, {})
            if env.get("type") == "ack":
                body = ack_body(env)
                self.journal.mark_claim_granted(task_id, generation, str(body.get("attempt_id") or ""))
                await self._accept_grant(body)
                return
            if error_code(env) == "not_ready":
                await asyncio.sleep(retry_after_sec(env))
                continue
            return

    async def _attach_running(self, task: dict[str, Any]) -> None:
        attempt_id = str(task.get("current_attempt_id") or "")
        if not attempt_id or self.starting:
            return
        if self.worker is not None and self.attempt and self.attempt.get("attempt_id") == attempt_id:
            return
        if self.journal.pending_for_attempt(attempt_id) is not None:
            return
        grant = self.journal.get_grant(attempt_id)
        if grant is not None and grant.status in {"submitted", "abandoned", "result_ready"}:
            return
        self._queue_fail(str(task.get("task_id") or ""), attempt_id, "crash", "worker is gone; the granted attempt was not rerun")

    async def _accept_grant(self, body: dict[str, Any]) -> None:
        if self.worker is not None or self.starting:
            return
        attempt_id = str(body.get("attempt_id") or "")
        if not attempt_id:
            return
        context = body.get("context") if isinstance(body.get("context"), dict) else {}
        fields = {
            "attempt_id": attempt_id,
            "task_id": str(body.get("task_id") or ""),
            "attempt_n": int(body.get("attempt_n") or 0),
            "role": str(body.get("role") or "worker"),
            "objective": str(body.get("objective") or ""),
            "context_json": json.dumps(context, sort_keys=True),
            "profile": str(body.get("profile") or ""),
            "parent_task_id": str(body.get("parent_task_id") or ""),
            "root_id": str(body.get("root_id") or ""),
            "deadline_at_ms": int(body.get("deadline_at_ms") or 0),
            "lease_sec": int(body.get("lease_sec") or self.cfg.lease_sec),
        }
        if not self.journal.insert_grant_if_absent(fields):
            return
        self.starting = True
        try:
            await self._start_worker(body, context)
        finally:
            self.starting = False

    async def _start_worker(self, body: dict[str, Any], context: dict[str, Any]) -> None:
        from pathlib import Path

        attempt_id = str(body["attempt_id"])
        start = self._start_message(body, context)
        if self.cfg.worker_mode == "hermes":
            from collab.hermes_run import prepare_hermes_home

            prepare_hermes_home(Path(self.cfg.hermes_home), self.cfg.model, self.cfg.model_base_url, self._api_key)
        handle = await asyncio.to_thread(
            spawn_worker,
            Path(self.cfg.state_dir),
            attempt_id,
            self.cfg.hermes_home or self.cfg.state_dir,
            self.cfg.pythonpath,
            start,
        )
        self.journal.set_running(attempt_id, handle.proc.pid, proc_start_token(handle.proc.pid))
        self.worker = handle
        self.attempt = dict(body)
        self.attempt["context"] = context
        self.lease_reason = "crash"
        self._arm_lease(int(body.get("lease_sec") or self.cfg.lease_sec), int(body.get("deadline_at_ms") or 0))
        self._frame_task = asyncio.create_task(self._worker_frames(handle), name="collab-worker")
        log.info("worker started attempt=%s role=%s", attempt_id, body.get("role"))

    def _start_message(self, body: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        message: dict[str, Any] = {
            "type": "start",
            "mode": self.cfg.worker_mode,
            "machine_id": self.cfg.machine_id,
            "tool_timeout_sec": self.cfg.tool_timeout_sec,
            "max_iterations": self.cfg.max_iterations,
            "allowlist": [{"host": item.host, "port": item.port} for item in self.cfg.allowlist],
            "grant": {
                "attempt_id": body.get("attempt_id"),
                "task_id": body.get("task_id"),
                "attempt_n": body.get("attempt_n"),
                "role": body.get("role"),
                "objective": body.get("objective"),
                "context": context,
                "profile": body.get("profile") or "",
                "parent_task_id": body.get("parent_task_id") or "",
                "root_id": body.get("root_id") or "",
                "deadline_at_ms": body.get("deadline_at_ms"),
                "lease_sec": body.get("lease_sec"),
            },
        }
        if self.cfg.worker_mode == "hermes":
            message["hermes_home"] = self.cfg.hermes_home
            message["model"] = {"id": self.cfg.model, "base_url": self.cfg.model_base_url, "api_key": self._api_key}
        return message

    def _arm_lease(self, lease_sec: int, deadline_at_ms: int) -> None:
        wall_left = max(0.0, deadline_at_ms / 1000.0 - time.time())
        now = time.monotonic()
        self.deadline_mono = now + wall_left
        window = min(float(lease_sec), wall_left)
        self.stop_mono = now + max(0.0, window - self.cfg.lease_margin_sec)

    async def _worker_frames(self, handle: Any) -> None:
        reader, writer = await _attach_socket(handle.sock)
        dispatch = {
            "ready": self._ignore_frame,
            "delegate": self._on_delegate,
            "result": self._on_result,
            "fail": self._on_fail,
        }
        try:
            while True:
                frame = await _read_async(reader)
                handler = dispatch.get(str(frame.get("type") or ""))
                if handler is not None:
                    await handler(frame, writer)
        except (asyncio.IncompleteReadError, FrameError, ConnectionError, OSError):
            await self._on_worker_eof()
        finally:
            writer.close()
            for fp in handle.log_files:
                fp.close()
            handle.pid_path.unlink(missing_ok=True)
            await asyncio.to_thread(_wait_proc, handle.proc)

    async def _ignore_frame(self, frame: dict[str, Any], writer: Any) -> None:
        del frame, writer

    async def _on_delegate(self, frame: dict[str, Any], writer: Any) -> None:
        body = await self._delegate(frame)
        await _write_async(writer, {"type": "delegate_result", "request_id": frame.get("request_id"), "ok": True, "body": body})

    async def _delegate(self, frame: dict[str, Any]) -> dict[str, Any]:
        if not self.attempt or self.attempt.get("role") != "coordinator":
            return {"ok": False, "summary": "only the coordinator can delegate", "state": "rejected"}
        objective = str(frame.get("objective") or "").strip()
        if not objective or len(objective) > MAX_OBJECTIVE_BYTES:
            return {"ok": False, "summary": "objective is empty or too large", "state": "rejected"}
        context = frame.get("context") if isinstance(frame.get("context"), dict) else {}
        encoded = json.dumps(context, sort_keys=True)
        if len(encoded) > MAX_CONTEXT_BYTES:
            return {"ok": False, "summary": "context is too large", "state": "rejected"}
        profile = str(frame.get("profile") or self.cfg.peer_profile)
        if not PROFILE_RE.match(profile):
            profile = self.cfg.peer_profile
        timeout = frame.get("timeout_sec")
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
            timeout = self.cfg.child_deadline_sec
        timeout = min(timeout, self.cfg.child_deadline_sec)
        payload = {
            "parent_task_id": str(self.attempt.get("task_id") or ""),
            "assigned_to": self.cfg.peer_agent_id,
            "objective": objective,
            "context": context,
            "profile": profile,
            "timeout_sec": timeout,
        }
        try:
            key = payload_key(payload)
            raw = wire_payload(payload)
            request_id, raw = self.journal.create_request_id(key, raw, lambda: new_request_id("crt"))
            env = await self._rpc_until("task.create", request_id, "", raw, time.monotonic() + timeout)
        except (HubDown, ProtocolError) as exc:
            return {"ok": False, "summary": f"delegate failed: {exc.__class__.__name__}", "state": "rejected"}
        if env.get("type") != "ack":
            return {"ok": False, "summary": error_code(env) or "task.create rejected", "state": "rejected"}
        created = ack_body(env)
        task_id = str(created.get("task_id") or "")
        self.journal.remember_created_task(key, task_id)
        deadline = min(self.deadline_mono, time.monotonic() + timeout)
        view = await self._wait_terminal(task_id, deadline)
        return child_tool_body(view)

    async def _wait_terminal(self, task_id: str, deadline_mono: float) -> dict[str, Any]:
        event = asyncio.Event()
        self.child_waiters[task_id] = event
        try:
            while time.monotonic() < deadline_mono and not self.stopped.is_set():
                try:
                    env = await self.rpc("task.get", new_request_id("get"), task_id, {})
                except HubDown:
                    await asyncio.sleep(0.2)
                    continue
                if env.get("type") == "ack":
                    view = ack_body(env)
                    state = str((view.get("task") or {}).get("state") or "")
                    if state in TERMINAL_TASK_STATES:
                        return view
                event.clear()
                remaining = deadline_mono - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(event.wait(), timeout=min(2.0, remaining))
                except asyncio.TimeoutError:
                    continue
        finally:
            self.child_waiters.pop(task_id, None)
        return {"task": {"task_id": task_id, "state": "timed_out", "error_message": "bridge wait expired", "result": None}}

    async def _on_result(self, frame: dict[str, Any], writer: Any) -> None:
        del writer
        if not self.attempt:
            return
        payload = complete_payload(
            str(self.attempt.get("attempt_id") or ""),
            self.cfg.machine_id,
            str(frame.get("summary") or ""),
            frame.get("limitations"),
            frame.get("evidence"),
        )
        self._enqueue("task.complete", f"done_{self.attempt['attempt_id']}"[:80], payload)
        await self._flush_quietly()

    async def _on_fail(self, frame: dict[str, Any], writer: Any) -> None:
        del writer
        if not self.attempt:
            return
        kind = str(frame.get("class") or "crash")
        self._queue_fail(
            str(self.attempt.get("task_id") or ""),
            str(self.attempt.get("attempt_id") or ""),
            kind,
            str(frame.get("message") or kind),
        )
        await self._flush_quietly()

    def _enqueue(self, command_type: str, request_id: str, payload: dict[str, Any]) -> None:
        if not self.attempt:
            return
        self.journal.enqueue_result(
            request_id,
            str(self.attempt.get("task_id") or ""),
            str(self.attempt.get("attempt_id") or ""),
            command_type,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            self.cfg.room_id,
        )

    def _queue_fail(self, task_id: str, attempt_id: str, kind: str, message: str) -> None:
        from collab.limits import FAIL_CLASSES

        if kind not in FAIL_CLASSES:
            kind = "crash"
        payload = {"attempt_id": attempt_id, "class": kind, "message": message[:2000]}
        self.journal.enqueue_result(
            f"fail_{attempt_id}"[:80],
            task_id,
            attempt_id,
            "task.fail",
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            self.cfg.room_id,
        )

    async def _on_worker_eof(self) -> None:
        attempt = self.attempt
        self.worker = None
        self.attempt = None
        if not attempt:
            return
        attempt_id = str(attempt.get("attempt_id") or "")
        if self.journal.pending_for_attempt(attempt_id) is not None:
            return
        grant = self.journal.get_grant(attempt_id)
        if grant is not None and grant.status in {"submitted", "abandoned", "result_ready"}:
            return
        kind = "lease_lost" if self.lease_reason == "lease_lost" else "crash"
        if self.lease_reason == "deadline":
            kind = "deadline"
        if self.lease_reason == "cancelled":
            return
        self._queue_fail(str(attempt.get("task_id") or ""), attempt_id, kind, "worker exited without a durable result")
        try:
            await self.flush_outbox()
        except HubDown:
            log.info("fail result stored for later resubmit attempt=%s", attempt_id)

    async def _stop_worker(self) -> None:
        handle = self.worker
        if handle is None:
            return
        await asyncio.to_thread(stop_process_group, handle.proc.pid, 2.0)

    async def _renew_loop(self) -> None:
        while not self.stopped.is_set():
            await asyncio.sleep(self.cfg.renew_every_sec)
            if not self.helloed.is_set() or self.worker is None or not self.attempt:
                continue
            try:
                await self._renew_once()
            except HubDown:
                log.info("lease renew deferred; socket is down")

    async def _renew_once(self) -> None:
        if not self.attempt:
            return
        env = await self.rpc(
            "attempt.renew",
            new_request_id("ren"),
            str(self.attempt.get("task_id") or ""),
            {"attempt_id": self.attempt.get("attempt_id")},
        )
        if env.get("type") == "ack":
            now = time.monotonic()
            renewed = now + self.cfg.lease_sec - self.cfg.lease_margin_sec
            self.stop_mono = min(renewed, self.deadline_mono - self.cfg.lease_margin_sec)
            return
        if error_code(env) == "lease_expired":
            self.lease_reason = "lease_lost"
            await self._stop_worker()

    async def _lease_watch(self) -> None:
        while not self.stopped.is_set():
            await asyncio.sleep(0.5)
            if self.worker is None:
                continue
            if time.monotonic() < self.stop_mono:
                continue
            if self.deadline_mono and time.monotonic() >= self.deadline_mono - self.cfg.lease_margin_sec:
                self.lease_reason = "deadline"
            else:
                self.lease_reason = "lease_lost"
            log.info("stopping worker before lease expiry reason=%s", self.lease_reason)
            await self._stop_worker()

    async def _rpc_until(self, command_type: str, request_id: str, task_id: str, payload: dict[str, Any] | str, deadline_mono: float) -> dict[str, Any]:
        last: Exception = HubDown("not connected")
        while time.monotonic() < deadline_mono and not self.stopped.is_set():
            try:
                return await self.rpc(command_type, request_id, task_id, payload)
            except HubDown as exc:
                last = exc
                await asyncio.sleep(0.2)
        raise HubDown(str(last))

    async def _flush_quietly(self) -> None:
        try:
            await self.flush_outbox()
        except HubDown:
            log.info("outbox retained for resubmit")

    async def _reconcile_safe(self) -> None:
        try:
            await self.reconcile()
        except HubDown:
            log.debug("hint reconcile skipped", exc_info=True)

    async def _poll_loop(self) -> None:
        while not self.stopped.is_set():
            await asyncio.sleep(2)
            if not self.helloed.is_set():
                continue
            try:
                await self.reconcile()
            except HubDown:
                log.debug("reconcile skipped", exc_info=True)


async def _attach_socket(sock: Any) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=MAX_FRAME_BYTES)
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await loop.connect_accepted_socket(lambda: protocol, sock)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


async def _read_async(reader: asyncio.StreamReader) -> dict[str, Any]:
    import struct

    header = await reader.readexactly(4)
    (size,) = struct.unpack(">I", header)
    if size > MAX_FRAME_BYTES:
        raise FrameError("worker frame too large")
    body = await reader.readexactly(size)
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise FrameError("worker frame is not an object")
    return payload


async def _write_async(writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
    writer.write(encode_frame(payload))
    await writer.drain()


def _wait_proc(proc: Any) -> None:
    import subprocess

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        log.info("worker pid %s did not exit after the protocol fd closed", getattr(proc, "pid", "?"))
