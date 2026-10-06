"""Bridge and hub binary: crash, disconnect, and cancel races.

Fake-agent workers do not import Hermes. This is not a second machine and not
a hosted model. Leases stay at the hub default (30s); the disconnect test
moves the hub's test clock instead of sleeping.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

from collab.bridge import Bridge
from collab.config import load_config
from collab.human import _unix_request
from test_fake_agent_hub import _free_port, _hub_binary, _sha


def _count_requests(db: Path, request_id: str) -> int:
    conn = sqlite3.connect(db, timeout=5)
    try:
        row = conn.execute("SELECT COUNT(*) FROM requests WHERE request_id=?", (request_id,)).fetchone()
    finally:
        conn.close()
    return int(row[0])


def _fail_request_ids(db: Path, attempt_id: str) -> list[str]:
    conn = sqlite3.connect(db, timeout=5)
    try:
        rows = conn.execute(
            "SELECT request_id FROM outbox WHERE attempt_id=? AND command_type='task.fail' ORDER BY created_at_ms",
            (attempt_id,),
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows]


def _advance(port: int, token: str, millis: int) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/test/advance",
        data=json.dumps({"advance_ms": millis}).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        body = json.load(resp)
    if not isinstance(body, dict):
        raise AssertionError(body)
    return body


class BridgeFaultTests(unittest.TestCase):
    def test_killed_worker_is_not_rerun_and_resubmits_the_same_request(self) -> None:
        binary = _hub_binary(Path("/tmp"))
        if binary is None:
            self.skipTest("hub binary did not build")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            port = _free_port()
            self._write(root, port, manual_clock=False, with_vm=False)
            hub = self._start_hub(binary, root, port)
            try:
                asyncio.run(self._crash(root))
            finally:
                self._stop_hub(hub)

    def test_disconnect_does_not_requeue_until_the_lease_expires(self) -> None:
        binary = _hub_binary(Path("/tmp"))
        if binary is None:
            self.skipTest("hub binary did not build")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            port = _free_port()
            self._write(root, port, manual_clock=True, with_vm=True)
            hub = self._start_hub(binary, root, port)
            try:
                asyncio.run(self._disconnect(root, port))
            finally:
                self._stop_hub(hub)

    def test_cancel_racing_complete_stays_cancelled(self) -> None:
        self._with_pair(self._cancel_wins)

    def test_complete_then_cancel_stays_completed(self) -> None:
        self._with_pair(self._complete_wins)

    def _with_pair(self, body) -> None:
        binary = _hub_binary(Path("/tmp"))
        if binary is None:
            self.skipTest("hub binary did not build")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            port = _free_port()
            self._write(root, port, manual_clock=False, with_vm=True)
            hub = self._start_hub(binary, root, port)
            try:
                asyncio.run(body(root))
            finally:
                self._stop_hub(hub)

    def _write(self, root: Path, port: int, manual_clock: bool, with_vm: bool) -> None:
        tokens = {"human": "human-secret", "pc": "pc-secret", "vm": "vm-secret"}
        for name, secret in tokens.items():
            path = root / f"{name}.token"
            path.write_text(secret + "\n", encoding="utf-8")
            path.chmod(0o600)
        (root / "socket.token").write_text("socket-secret\n", encoding="utf-8")
        hub = {
            "listen": f"127.0.0.1:{port}",
            "room_id": "lab",
            "db_path": str(root / "hub.db"),
            "manual_clock": manual_clock,
            "principals": {
                "human-jacob": {"kind": "human", "token_sha256": _sha(tokens["human"])},
                "hermes-pc": {
                    "kind": "agent",
                    "machine_id": "pc",
                    "capabilities": ["pc-network"],
                    "token_sha256": _sha(tokens["pc"]),
                },
                "hermes-vm": {
                    "kind": "agent",
                    "machine_id": "vm",
                    "capabilities": ["vm-local"],
                    "token_sha256": _sha(tokens["vm"]),
                },
            },
        }
        (root / "hub.json").write_text(json.dumps(hub), encoding="utf-8")
        common = {
            "room_id": "lab",
            "worker_mode": "fake",
            "pythonpath": "/workspace/collab/python:/workspace",
            "model": "fake-model",
            "services": ["collab-http.service"],
            "dns_names": ["vm.example.test"],
        }
        pc = {
            **common,
            "agent_id": "hermes-pc",
            "machine_id": "pc",
            "hub_ws": f"ws://127.0.0.1:{port}/ws",
            "token_file": str(root / "pc.token"),
            "journal_path": str(root / "pc.db"),
            "state_dir": str(root / "pc"),
            "peer_agent_id": "hermes-vm",
            "profile": "pc-net",
            "peer_profile": "vm-local",
            "hermes_home": str(root / "pc-hermes"),
            "allowlist": [],
            "human_socket": str(root / "pc.sock"),
            "human_socket_token_file": str(root / "socket.token"),
            "human_hub_token_file": str(root / "human.token"),
        }
        (root / "pc.json").write_text(json.dumps(pc), encoding="utf-8")
        if not with_vm:
            return
        vm = {
            **common,
            "agent_id": "hermes-vm",
            "machine_id": "vm",
            "hub_ws": f"ws://127.0.0.1:{port}/ws",
            "token_file": str(root / "vm.token"),
            "journal_path": str(root / "vm.db"),
            "state_dir": str(root / "vm"),
            "peer_agent_id": "hermes-pc",
            "profile": "vm-local",
            "peer_profile": "pc-net",
            "hermes_home": str(root / "vm-hermes"),
            "allowlist": [],
        }
        (root / "vm.json").write_text(json.dumps(vm), encoding="utf-8")

    def _start_hub(self, binary: Path, root: Path, port: int) -> subprocess.Popen:
        log = open(root / "hub.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [str(binary), "-config", str(root / "hub.json")],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        proc._hub_log = log  # closed by _stop_hub
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                log.flush()
                raise AssertionError((root / "hub.log").read_text(encoding="utf-8"))
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return proc
            except OSError:
                time.sleep(0.05)
        log.flush()
        raise AssertionError((root / "hub.log").read_text(encoding="utf-8"))

    def _stop_hub(self, hub: subprocess.Popen) -> None:
        if hub.poll() is None:
            hub.terminate()
            try:
                hub.wait(timeout=5)
            except subprocess.TimeoutExpired:
                hub.kill()
                hub.wait(timeout=5)
        log_handle = getattr(hub, "_hub_log", None)
        if log_handle is not None:
            log_handle.close()

    async def _crash(self, root: Path) -> None:
        pc = Bridge(load_config(root / "pc.json"), (root / "pc.token").read_text(encoding="utf-8").strip())
        captured: list[str] = []

        async def drop_fail(bridge: Bridge) -> None:
            fails = [item for item in bridge.journal.pending_outbox() if item.command_type == "task.fail"]
            if not fails:
                return
            captured.append(fails[0].request_id)
            bridge.before_flush = None
            ws = bridge.ws
            bridge.ws = None
            if ws is not None:
                await ws.close()

        pc.before_flush = drop_fail
        task = asyncio.create_task(pc.run())
        try:
            await self._wait_socket(root / "pc.sock")
            created = await self._human(
                root,
                {
                    "command": "investigate",
                    "objective": "stall so the worker can be killed",
                    "context": {"stall": True},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            self.assertTrue(created.get("ok"), created)
            task_id = created["hub"]["payload"]["task_id"]
            attempt_id = await self._wait_run(root / "pc")
            self._kill_worker(root / "pc", attempt_id)
            await self._wait_resubmit(root, captured)
            runs = (root / "pc" / "runs" / attempt_id).read_text(encoding="utf-8")
            self.assertEqual(runs, "start\n", runs)
            self.assertEqual(_fail_request_ids(root / "pc.db", attempt_id), [captured[0]])
            self.assertEqual(_count_requests(root / "hub.db", captured[0]), 1)
            view = await self._wait_new_attempt(root, task_id, attempt_id)
            current = view["task"]["current_attempt_id"]
            self.assertNotEqual(current, attempt_id)
            self.assertEqual((root / "pc" / "runs" / attempt_id).read_text(encoding="utf-8"), "start\n")
            names = {path.name for path in (root / "pc" / "runs").iterdir()}
            self.assertIn(current, names)
            self.assertGreaterEqual(len(names), 2)
        finally:
            pc.request_stop()
            await asyncio.gather(task, return_exceptions=True)

    async def _disconnect(self, root: Path, port: int) -> None:
        pc = Bridge(load_config(root / "pc.json"), (root / "pc.token").read_text(encoding="utf-8").strip())
        vm = Bridge(load_config(root / "vm.json"), (root / "vm.token").read_text(encoding="utf-8").strip())
        tasks = [asyncio.create_task(pc.run()), asyncio.create_task(vm.run())]
        try:
            await self._wait_socket(root / "pc.sock")
            created = await self._human(
                root,
                {
                    "command": "investigate",
                    "objective": "delegate a stalled peer observation",
                    "context": {"stall_child": True},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            self.assertTrue(created.get("ok"), created)
            await self._wait_run(root / "vm")
            child = await self._assigned(root, "hermes-vm")
            attempt_id = child["current_attempt_id"]
            self.assertEqual(child["state"], "running")
            vm.session_paused = True
            ws = vm.ws
            if ws is not None:
                await ws.close()
            await self._assert_running(root, child["task_id"], attempt_id)
            advanced = await asyncio.to_thread(_advance, port, "human-secret", 31000)
            self.assertTrue(advanced.get("ok"), advanced)
            expired = await self._human(root, {"command": "get", "task_id": child["task_id"]})
            task = expired["hub"]["payload"]["task"]
            attempts = expired["hub"]["payload"]["attempts"]
            self.assertEqual(task["state"], "queued", task)
            matched = [row for row in attempts if row["attempt_id"] == attempt_id]
            self.assertEqual(len(matched), 1, attempts)
            self.assertEqual(matched[0]["state"], "lost", matched)
            self.assertNotEqual(task["current_attempt_id"], attempt_id)
        finally:
            pc.request_stop()
            vm.request_stop()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _cancel_wins(self, root: Path) -> None:
        pc = Bridge(load_config(root / "pc.json"), (root / "pc.token").read_text(encoding="utf-8").strip())
        vm = Bridge(load_config(root / "vm.json"), (root / "vm.token").read_text(encoding="utf-8").strip())
        held = asyncio.Event()
        release = asyncio.Event()

        async def hold_complete(bridge: Bridge) -> None:
            pending = [item for item in bridge.journal.pending_outbox() if item.command_type == "task.complete"]
            if not pending:
                return
            held.set()
            await release.wait()

        pc.before_flush = hold_complete
        tasks = [asyncio.create_task(pc.run()), asyncio.create_task(vm.run())]
        try:
            await self._wait_socket(root / "pc.sock")
            created = await self._human(
                root,
                {
                    "command": "investigate",
                    "objective": "finish while a cancel is waiting",
                    "context": {},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            self.assertTrue(created.get("ok"), created)
            task_id = created["hub"]["payload"]["task_id"]
            await asyncio.wait_for(held.wait(), timeout=40)
            cancelled = await self._human(root, {"command": "cancel", "task_id": task_id, "reason": "stop"})
            self.assertTrue(cancelled.get("ok"), cancelled)
            release.set()
            view = await self._wait_state(root, task_id, "cancelled")
            self.assertIsNone(view["task"].get("result"))
            types = await self._event_types(root, task_id)
            self.assertIn("task.cancelled", types)
            self.assertIn("attempt.stale_result", types)
            self.assertNotIn("task.completed", types)
        finally:
            release.set()
            pc.request_stop()
            vm.request_stop()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _complete_wins(self, root: Path) -> None:
        pc = Bridge(load_config(root / "pc.json"), (root / "pc.token").read_text(encoding="utf-8").strip())
        vm = Bridge(load_config(root / "vm.json"), (root / "vm.token").read_text(encoding="utf-8").strip())
        tasks = [asyncio.create_task(pc.run()), asyncio.create_task(vm.run())]
        try:
            await self._wait_socket(root / "pc.sock")
            created = await self._human(
                root,
                {
                    "command": "investigate",
                    "objective": "finish before cancel",
                    "context": {},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            self.assertTrue(created.get("ok"), created)
            task_id = created["hub"]["payload"]["task_id"]
            view = await self._wait_state(root, task_id, "completed", timeout=40)
            summary = view["task"]["result"]["summary"]
            rejected = await self._human(root, {"command": "cancel", "task_id": task_id, "reason": "stop"})
            self.assertFalse(rejected.get("ok"), rejected)
            self.assertEqual(rejected["hub"]["payload"]["code"], "conflict")
            again = await self._human(root, {"command": "get", "task_id": task_id})
            task_view = again["hub"]["payload"]["task"]
            self.assertEqual(task_view["state"], "completed")
            self.assertEqual(task_view["result"]["summary"], summary)
            types = await self._event_types(root, task_id)
            self.assertIn("task.completed", types)
            self.assertIn("task.cancel_rejected", types)
            self.assertNotIn("task.cancelled", types)
        finally:
            pc.request_stop()
            vm.request_stop()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _human(self, root: Path, message: dict) -> dict:
        body = {"token": "socket-secret", **message}
        return await asyncio.to_thread(_unix_request, str(root / "pc.sock"), body)

    async def _wait_socket(self, path: Path) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if await asyncio.to_thread(path.exists):
                return
            await asyncio.sleep(0.05)
        self.fail("human socket was not created")

    async def _wait_run(self, state: Path) -> str:
        root = state / "runs"
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            files = list(root.glob("*")) if root.is_dir() else []
            if files:
                return files[0].name
            await asyncio.sleep(0.05)
        self.fail(f"worker did not start under {state}")
        return ""

    def _kill_worker(self, state: Path, attempt_id: str) -> None:
        payload = json.loads((state / "pids" / f"{attempt_id}.pid").read_text(encoding="utf-8"))
        os.killpg(int(payload["pid"]), signal.SIGKILL)

    async def _wait_resubmit(self, root: Path, captured: list[str]) -> None:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if captured and _count_requests(root / "hub.db", captured[0]) == 1:
                return
            await asyncio.sleep(0.1)
        self.fail(f"fail request was not resubmitted: {captured}")

    async def _wait_new_attempt(self, root: Path, task_id: str, first: str) -> dict:
        deadline = time.monotonic() + 20
        last: dict = {}
        while time.monotonic() < deadline:
            last = await self._human(root, {"command": "get", "task_id": task_id})
            task = last["hub"]["payload"]["task"]
            if task.get("current_attempt_id") and task["current_attempt_id"] != first:
                names = {path.name for path in (root / "pc" / "runs").glob("*")}
                if task["current_attempt_id"] in names:
                    return last["hub"]["payload"]
            await asyncio.sleep(0.1)
        self.fail(f"no new attempt after crash: {last}")
        return {}

    async def _assigned(self, root: Path, agent_id: str) -> dict:
        deadline = time.monotonic() + 20
        last: dict = {}
        while time.monotonic() < deadline:
            last = await self._human(root, {"command": "list", "assigned_to": agent_id})
            tasks = (last.get("hub") or {}).get("payload", {}).get("tasks") or []
            running = [task for task in tasks if task.get("state") == "running" and task.get("current_attempt_id")]
            if running:
                return running[0]
            await asyncio.sleep(0.1)
        self.fail(f"{agent_id} did not claim: {last}")
        return {}

    async def _assert_running(self, root: Path, task_id: str, attempt_id: str) -> None:
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            view = await self._human(root, {"command": "get", "task_id": task_id})
            task = view["hub"]["payload"]["task"]
            self.assertEqual(task["state"], "running", task)
            self.assertEqual(task["current_attempt_id"], attempt_id)
            await asyncio.sleep(0.1)

    async def _wait_state(self, root: Path, task_id: str, state: str, timeout: float = 20) -> dict:
        deadline = time.monotonic() + timeout
        last: dict = {}
        while time.monotonic() < deadline:
            last = await self._human(root, {"command": "get", "task_id": task_id})
            payload = last["hub"]["payload"]
            if payload["task"]["state"] == state:
                return payload
            await asyncio.sleep(0.1)
        self.fail(f"state did not become {state}: {last}")
        return {}

    async def _event_types(self, root: Path, task_id: str) -> set[str]:
        deadline = time.monotonic() + 10
        types: set[str] = set()
        while time.monotonic() < deadline:
            hist = await self._human(root, {"command": "history", "task_id": task_id})
            events = hist["hub"]["payload"]["events"]
            types = {str(event.get("type") or "") for event in events if event.get("task_id") == task_id}
            if "attempt.stale_result" in types or "task.cancel_rejected" in types or "task.completed" in types:
                return types
            await asyncio.sleep(0.1)
        return types


if __name__ == "__main__":
    unittest.main()
