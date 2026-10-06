"""Fake-agent flow against the hub binary.

The workers do not import Hermes. Diagnostic calls are real. This is not a
second machine and not a hosted model. If the hub binary cannot be built the
test skips; ``go test`` in collab/hub is owned by another change and may be red
while ``go build`` still produces a server.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from collab.bridge import Bridge
from collab.config import load_config
from collab.human import _unix_request


def _sha(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hub_binary(cache: Path) -> Path | None:
    binary = cache / "collab-hub"
    build = subprocess.run(
        ["go", "build", "-o", str(binary), "./hub/"],
        cwd="/workspace/collab",
        capture_output=True,
        text=True,
        timeout=120,
    )
    if build.returncode != 0:
        return None
    return binary


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class FakeAgentHubTests(unittest.TestCase):
    def test_two_bridges_complete_a_root_investigation(self) -> None:
        binary = _hub_binary(Path("/tmp"))
        if binary is None:
            self.skipTest("hub binary did not build; bridge unit coverage is in test_journal.py")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            port = _free_port()
            http_port = _free_port()
            tokens = {"human": "human-secret", "pc": "pc-secret", "vm": "vm-secret"}
            self._write_configs(root, port, http_port, tokens)
            hub = self._start_hub(binary, root, port)
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", http_port))
            listener.listen(8)
            try:
                asyncio.run(self._run(root, listener, http_port))
            finally:
                listener.close()
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

    def _write_configs(self, root: Path, port: int, http_port: int, tokens: dict[str, str]) -> None:
        for name, secret in tokens.items():
            path = root / f"{name}.token"
            path.write_text(secret + "\n", encoding="utf-8")
            path.chmod(0o600)
        (root / "socket.token").write_text("socket-secret\n", encoding="utf-8")
        hub = {
            "listen": f"127.0.0.1:{port}",
            "room_id": "lab",
            "db_path": str(root / "hub.db"),
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
            "model_base_url": "http://127.0.0.1:9/v1",
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
            "allowlist": [{"host": "127.0.0.2", "port": http_port}],
            "human_socket": str(root / "pc.sock"),
            "human_socket_token_file": str(root / "socket.token"),
            "human_hub_token_file": str(root / "human.token"),
        }
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
        (root / "pc.json").write_text(json.dumps(pc), encoding="utf-8")
        (root / "vm.json").write_text(json.dumps(vm), encoding="utf-8")

    def _start_hub(self, binary: Path, root: Path, port: int) -> subprocess.Popen:
        log = open(root / "hub.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [str(binary), "-config", str(root / "hub.json")],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        proc._hub_log = log  # closed by the caller after wait
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError((root / "hub.log").read_text(encoding="utf-8"))
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    return proc
            except OSError:
                time.sleep(0.05)
        raise AssertionError((root / "hub.log").read_text(encoding="utf-8"))

    async def _run(self, root: Path, listener: socket.socket, http_port: int) -> None:
        pc = Bridge(load_config(root / "pc.json"), (root / "pc.token").read_text(encoding="utf-8").strip())
        vm = Bridge(load_config(root / "vm.json"), (root / "vm.token").read_text(encoding="utf-8").strip())
        tasks = [asyncio.create_task(pc.run()), asyncio.create_task(vm.run())]
        try:
            await self._wait_socket(root / "pc.sock")
            mode = (root / "pc.sock").stat().st_mode & 0o777
            self.assertEqual(mode, 0o600)
            created = await asyncio.to_thread(
                _unix_request,
                str(root / "pc.sock"),
                {
                    "token": "socket-secret",
                    "command": "investigate",
                    "objective": "why is the vm service unreachable",
                    "context": {"host": "127.0.0.2", "port": http_port},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            self.assertTrue(created.get("ok"), created)
            task_id = created["hub"]["payload"]["task_id"]
            view = await self._wait_done(root, task_id)
            task = view["hub"]["payload"]["task"]
            self.assertEqual(task["state"], "completed", task)
            result = task["result"]
            summary = result["summary"] if isinstance(result, dict) else str(result)
            self.assertIn("127.0.0.2", summary)
            self.assertIn(str(http_port), summary)
            self.assertIn("fake-agent", summary)
            blob = (root / "pc.db").read_bytes()
            self.assertNotIn(b"pc-secret", blob)
            self.assertNotIn(b"human-secret", blob)
            self.assertNotIn(b"socket-secret", blob)
        finally:
            pc.request_stop()
            vm.request_stop()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _wait_socket(self, path: Path) -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if await asyncio.to_thread(path.exists):
                return
            await asyncio.sleep(0.05)
        self.fail("human socket was not created")

    async def _wait_done(self, root: Path, task_id: str) -> dict:
        deadline = time.monotonic() + 40
        last: dict = {}
        while time.monotonic() < deadline:
            last = await asyncio.to_thread(
                _unix_request,
                str(root / "pc.sock"),
                {"token": "socket-secret", "command": "get", "task_id": task_id},
            )
            task = ((last.get("hub") or {}).get("payload") or {}).get("task") or {}
            if task.get("state") in {"completed", "failed", "cancelled", "timed_out"}:
                return last
            await asyncio.sleep(0.2)
        self.fail(f"task did not finish: {last}")
        return last


if __name__ == "__main__":
    unittest.main()
