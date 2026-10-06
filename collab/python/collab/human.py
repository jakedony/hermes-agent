"""PC-only local control socket.

The socket is mode 0600 and requires its own token. Hub commands issued for
the human use the human bearer token, never the agent token.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any, Awaitable, Callable

from collab.config import BridgeConfig, ConfigError, read_secret_file
from collab.http_hub import HubHTTPError, post_command
from collab.limits import MAX_FRAME_BYTES
from collab.protocol import command_frame, new_request_id

CommandFn = Callable[["HumanAPI", dict[str, Any]], Awaitable[dict[str, Any]]]


class HumanAPI:
    def __init__(self, cfg: BridgeConfig, human_token: str) -> None:
        self.cfg = cfg
        self.human_token = human_token

    async def investigate(self, msg: dict[str, Any]) -> dict[str, Any]:
        objective = str(msg.get("objective") or "").strip()
        if not objective:
            return {"ok": False, "error": "objective is required"}
        context = msg.get("context") if isinstance(msg.get("context"), dict) else {}
        payload = {
            "parent_task_id": "",
            "assigned_to": self.cfg.agent_id,
            "objective": objective,
            "context": context,
            "profile": str(msg.get("profile") or self.cfg.profile),
            "timeout_sec": _timeout(msg.get("timeout_sec"), self.cfg.root_deadline_sec),
        }
        return await self._hub("task.create", "", payload)

    async def list_tasks(self, msg: dict[str, Any]) -> dict[str, Any]:
        return await self._hub(
            "task.list",
            "",
            {"assigned_to": str(msg.get("assigned_to") or ""), "state": str(msg.get("state") or "")},
        )

    async def get_task(self, msg: dict[str, Any]) -> dict[str, Any]:
        return await self._hub("task.get", _task_id(msg), {})

    async def cancel(self, msg: dict[str, Any]) -> dict[str, Any]:
        return await self._hub("task.cancel", _task_id(msg), {"reason": str(msg.get("reason") or "")[:500]})

    async def history(self, msg: dict[str, Any]) -> dict[str, Any]:
        return await self._hub("task.history", _task_id(msg), {})

    async def _hub(self, command_type: str, task_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        env = command_frame(command_type, new_request_id("hum"), self.cfg.room_id, payload, task_id)
        try:
            body = await asyncio.to_thread(post_command, self.cfg.http_base, self.human_token, env, 15.0)
        except HubHTTPError as exc:
            return {"ok": False, "error": str(exc), "status": exc.status}
        return {"ok": body.get("type") == "ack", "hub": body}


COMMANDS: dict[str, CommandFn] = {
    "investigate": lambda api, msg: api.investigate(msg),
    "list": lambda api, msg: api.list_tasks(msg),
    "get": lambda api, msg: api.get_task(msg),
    "cancel": lambda api, msg: api.cancel(msg),
    "history": lambda api, msg: api.history(msg),
}


def load_or_create_socket_token(path: str) -> str:
    file = Path(path)
    if file.exists():
        token = file.read_text(encoding="utf-8").strip()
        if not token:
            raise ConfigError(f"socket token file {path} is empty")
        return token
    file.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    file.write_text(token + "\n", encoding="utf-8")
    os.chmod(file, 0o600)
    return token


def tokens_match(got: object, expected: str) -> bool:
    if not isinstance(got, str) or not expected:
        return False
    return hmac.compare_digest(got.encode("utf-8"), expected.encode("utf-8"))


def _prepare_socket_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_socket():
        path.unlink()


def _lock_socket_mode(path: Path) -> int:
    os.chmod(path, 0o600)
    return stat.S_IMODE(path.stat().st_mode)


async def serve(cfg: BridgeConfig, socket_token: str, human_token: str) -> asyncio.AbstractServer:
    path = Path(cfg.human_socket)
    await asyncio.to_thread(_prepare_socket_path, path)
    api = HumanAPI(cfg, human_token)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await reader.readuntil(b"\n")
        except (asyncio.LimitOverrunError, asyncio.IncompleteReadError):
            writer.close()
            return
        if len(raw) > MAX_FRAME_BYTES:
            writer.close()
            return
        reply = await _dispatch(api, socket_token, raw)
        writer.write(json.dumps(reply).encode("utf-8") + b"\n")
        await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(handle, path=str(path), limit=MAX_FRAME_BYTES)
    mode = await asyncio.to_thread(_lock_socket_mode, path)
    if mode != 0o600:
        raise ConfigError(f"human socket mode is {oct(mode)}, expected 0o600")
    return server


async def _dispatch(api: HumanAPI, socket_token: str, raw: bytes) -> dict[str, Any]:
    try:
        msg = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"ok": False, "error": "malformed command"}
    if not isinstance(msg, dict):
        return {"ok": False, "error": "command must be an object"}
    if not tokens_match(msg.get("token"), socket_token):
        return {"ok": False, "error": "unauthorized"}
    command = msg.get("command")
    fn = COMMANDS.get(command) if isinstance(command, str) else None
    if fn is None:
        return {"ok": False, "error": "unknown command"}
    return await fn(api, msg)


def client_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m collab human")
    parser.add_argument("--config", required=True)
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("--objective", default="")
    parser.add_argument("--context", default="{}")
    parser.add_argument("--task-id", default="")
    parser.add_argument("--reason", default="")
    parser.add_argument("--profile", default="")
    parser.add_argument("--timeout-sec", type=int, default=0)
    args = parser.parse_args(argv)
    from collab.config import load_config

    cfg = load_config(args.config)
    if not cfg.human_socket or not cfg.human_socket_token_file:
        raise SystemExit("this bridge config has no human socket")
    token = read_secret_file(cfg.human_socket_token_file)
    try:
        context = json.loads(args.context)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"context is not JSON: {exc}") from exc
    if not isinstance(context, dict):
        raise SystemExit("context must be a JSON object")
    message = {
        "token": token,
        "command": args.command,
        "objective": args.objective,
        "context": context,
        "task_id": args.task_id,
        "reason": args.reason,
        "profile": args.profile,
        "timeout_sec": args.timeout_sec,
    }
    reply = _unix_request(cfg.human_socket, message)
    print(json.dumps(reply, indent=2, sort_keys=True))
    return 0 if reply.get("ok") else 1


def _unix_request(path: str, message: dict[str, Any]) -> dict[str, Any]:
    import socket

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(20)
        sock.connect(path)
        sock.sendall(json.dumps(message).encode("utf-8") + b"\n")
        data = b""
        while b"\n" not in data:
            block = sock.recv(65536)
            if not block:
                break
            data += block
    finally:
        sock.close()
    return json.loads(data.decode("utf-8"))


def _timeout(value: Any, default: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return default
    return value


def _task_id(msg: dict[str, Any]) -> str:
    task_id = msg.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        return ""
    return task_id


def fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
