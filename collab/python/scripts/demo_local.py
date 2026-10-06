#!/usr/bin/env python3
"""Single-host stub-model demo.

LABEL: local stub model, single host. This is not a second machine and not a
hosted model. The PC probe target is 127.0.0.2:18080. The HTTP server listens
on 127.0.0.1:18080. Both bridges dial the hub on this host.
"""

from __future__ import annotations

import contextvars
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path[:0] = ["/workspace/collab/python", "/workspace"]

from collab.human import _unix_request  # noqa: E402
from tests.fakes.fake_llm_provider import MODEL_ID, FakeLLMServer, Text, ToolCall  # noqa: E402

LABEL = "LABEL: local stub model, single host. This is not a second machine and not a hosted model."
API_KEY = "sk-collab-local-stub"


def _tool_names(body: dict) -> set[str]:
    names = set()
    for tool in body.get("tools") or []:
        fn = tool.get("function") or {}
        if fn.get("name"):
            names.add(fn["name"])
    return names


def respond(record: dict) -> Text | ToolCall:
    body = record["body"]
    messages = body.get("messages") or []
    tool_msgs = [msg for msg in messages if msg.get("role") == "tool"]
    if "delegate_investigation" in _tool_names(body):
        if len(tool_msgs) == 0:
            return ToolCall("tcp_probe", {"host": "127.0.0.2", "port": 18080})
        if len(tool_msgs) == 1:
            return ToolCall(
                "delegate_investigation",
                {
                    "objective": "Which sockets are listening on this machine, especially port 18080?",
                    "context": {"port": 18080},
                    "profile": "vm-local",
                    "timeout_sec": 120,
                },
            )
        combined = "\n".join(str(msg.get("content") or "") for msg in tool_msgs)
        return Text("STUB SYNTHESIS\n" + combined)
    if len(tool_msgs) == 0:
        return ToolCall("listening_sockets", {"port": 18080})
    return Text(str(tool_msgs[-1].get("content") or ""))


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        raw = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: object) -> None:
        del fmt, args


def main() -> int:
    print(LABEL, flush=True)
    root = Path(tempfile.mkdtemp(prefix="collab-demo-"))
    print(f"demo state: {root}", flush=True)
    procs: list[subprocess.Popen] = []
    httpd: ThreadingHTTPServer | None = None
    try:
        _require_port_free(18080)
        hub_port = 8765 if _port_free(8765) else _free_port()
        binary = _build_hub(root)
        _write_tokens(root)
        httpd = ThreadingHTTPServer(("127.0.0.1", 18080), _Handler)
        threading.Thread(
            target=contextvars.copy_context().run,
            args=(httpd.serve_forever,),
            name="collab-demo-http",
            daemon=True,
        ).start()
        with FakeLLMServer(respond, api_key=API_KEY) as server:
            _write_configs(root, hub_port, server.base_url)
            procs.append(_popen([str(binary), "-config", str(root / "hub.json")], root / "hub.log"))
            _wait_tcp(hub_port, procs[0], root / "hub.log")
            procs.append(_popen([sys.executable, "-m", "collab", "bridge", "--config", str(root / "vm.json")], root / "vm-bridge.log"))
            procs.append(_popen([sys.executable, "-m", "collab", "bridge", "--config", str(root / "pc.json")], root / "pc-bridge.log"))
            _wait_file(root / "pc.sock", procs, root)
            created = _unix_request(
                str(root / "pc.sock"),
                {
                    "token": "demo-socket",
                    "command": "investigate",
                    "objective": "why is the vm service at 127.0.0.2:18080 unreachable from the pc",
                    "context": {"host": "127.0.0.2", "port": 18080},
                    "profile": "pc-net",
                    "timeout_sec": 180,
                },
            )
            if not created.get("ok"):
                _dump_logs(root)
                print(json.dumps(created, indent=2), flush=True)
                return 1
            task_id = created["hub"]["payload"]["task_id"]
            view = _poll(root, task_id, timeout=150)
            task = ((view.get("hub") or {}).get("payload") or {}).get("task") or {}
            if task.get("state") != "completed":
                _dump_logs(root)
                print(json.dumps(view, indent=2)[:4000], flush=True)
                return 1
            result = task.get("result") or {}
            summary = result.get("summary") if isinstance(result, dict) else str(result)
            print(LABEL, flush=True)
            print(summary, flush=True)
            return 0
    finally:
        if httpd is not None:
            httpd.shutdown()
        for proc in procs:
            _stop(proc)


def _sha(token: str) -> str:
    import hashlib

    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _write_tokens(root: Path) -> None:
    secrets = {"human": "demo-human", "pc": "demo-pc", "vm": "demo-vm", "socket": "demo-socket"}
    for name, secret in secrets.items():
        path = root / f"{name}.token"
        path.write_text(secret + "\n", encoding="utf-8")
        os.chmod(path, 0o600)
    (root / "model.key").write_text(API_KEY + "\n", encoding="utf-8")
    os.chmod(root / "model.key", 0o600)


def _write_configs(root: Path, hub_port: int, base_url: str) -> None:
    principals = {
        "human-operator": {"kind": "human", "token_sha256": _sha("demo-human")},
        "hermes-pc": {"kind": "agent", "machine_id": "pc", "capabilities": ["pc-network"], "token_sha256": _sha("demo-pc")},
        "hermes-vm": {"kind": "agent", "machine_id": "vm", "capabilities": ["vm-local"], "token_sha256": _sha("demo-vm")},
    }
    hub = {"listen": f"127.0.0.1:{hub_port}", "room_id": "lab", "db_path": str(root / "hub.db"), "principals": principals}
    (root / "hub.json").write_text(json.dumps(hub), encoding="utf-8")
    common = {
        "room_id": "lab",
        "worker_mode": "hermes",
        "pythonpath": "/workspace/collab/python:/workspace",
        "model": MODEL_ID,
        "model_base_url": base_url,
        "api_key_file": str(root / "model.key"),
        "hub_ws": f"ws://127.0.0.1:{hub_port}/ws",
    }
    pc = {
        **common,
        "agent_id": "hermes-pc",
        "machine_id": "pc",
        "token_file": str(root / "pc.token"),
        "journal_path": str(root / "pc.db"),
        "state_dir": str(root / "pc-state"),
        "peer_agent_id": "hermes-vm",
        "profile": "pc-net",
        "peer_profile": "vm-local",
        "hermes_home": str(root / "pc-hermes"),
        "allowlist": [{"host": "127.0.0.2", "port": 18080}],
        "human_socket": str(root / "pc.sock"),
        "human_socket_token_file": str(root / "socket.token"),
        "human_hub_token_file": str(root / "human.token"),
    }
    vm = {
        **common,
        "agent_id": "hermes-vm",
        "machine_id": "vm",
        "token_file": str(root / "vm.token"),
        "journal_path": str(root / "vm.db"),
        "state_dir": str(root / "vm-state"),
        "peer_agent_id": "hermes-pc",
        "profile": "vm-local",
        "peer_profile": "pc-net",
        "hermes_home": str(root / "vm-hermes"),
        "allowlist": [],
    }
    (root / "pc.json").write_text(json.dumps(pc), encoding="utf-8")
    (root / "vm.json").write_text(json.dumps(vm), encoding="utf-8")


def _build_hub(root: Path) -> Path:
    binary = root / "collab-hub"
    build = subprocess.run(
        ["go", "build", "-o", str(binary), "./hub/"],
        cwd="/workspace/collab",
        capture_output=True,
        text=True,
        timeout=120,
    )
    if build.returncode != 0:
        raise SystemExit(build.stderr or build.stdout)
    return binary


def _popen(argv: list[str], log_path: Path) -> subprocess.Popen:
    log = open(log_path, "w", encoding="utf-8")
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "PYTHONPATH": "/workspace/collab/python:/workspace",
    }
    return subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env)


def _wait_tcp(port: int, proc: subprocess.Popen, log_path: Path) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise SystemExit(log_path.read_text(encoding="utf-8"))
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise SystemExit(log_path.read_text(encoding="utf-8"))


def _wait_file(path: Path, procs: list[subprocess.Popen], root: Path) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if any(proc.poll() is not None for proc in procs):
            _dump_logs(root)
            raise SystemExit("a demo process exited")
        if path.exists():
            return
        time.sleep(0.05)
    _dump_logs(root)
    raise SystemExit("PC human socket did not appear")


def _poll(root: Path, task_id: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        last = _unix_request(str(root / "pc.sock"), {"token": "demo-socket", "command": "get", "task_id": task_id})
        task = ((last.get("hub") or {}).get("payload") or {}).get("task") or {}
        if task.get("state") in {"completed", "failed", "cancelled", "timed_out"}:
            return last
        time.sleep(0.5)
    return last


def _dump_logs(root: Path) -> None:
    for path in sorted(root.glob("*.log")):
        print(f"--- {path.name} ---", flush=True)
        print(path.read_text(encoding="utf-8")[-4000:], flush=True)


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)


def _port_free(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return False
    except OSError:
        return True


def _require_port_free(port: int) -> None:
    if not _port_free(port):
        raise SystemExit(f"127.0.0.1:{port} is already in use")


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


if __name__ == "__main__":
    raise SystemExit(main())
