"""Worker process groups and pid files.

``start_new_session`` makes the worker a group leader so stop is ``killpg``,
which also reaches diagnostic children. Pid files record the kernel start
token so a recycled pid is not signalled.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from collab.frames import encode_frame


@dataclass
class WorkerHandle:
    proc: subprocess.Popen
    sock: object
    log_files: list[IO[bytes]]
    attempt_id: str
    pid_path: Path


def child_env(hermes_home: str, pythonpath: str, protocol_fd: int) -> dict[str, str]:
    """Explicit child environment. The parent environment is not copied."""
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "HERMES_HOME": hermes_home,
        "PYTHONPATH": pythonpath,
        "COLLAB_PROTOCOL_FD": str(protocol_fd),
        "PYTHONUNBUFFERED": "1",
    }


def proc_start_token(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as handle:
            raw = handle.read()
    except OSError:
        return ""
    end = raw.rfind(")")
    if end < 0:
        return ""
    fields = raw[end + 2 :].split()
    if len(fields) < 20:
        return ""
    return fields[19]


def write_pidfile(directory: Path, attempt_id: str, pid: int, start_token: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{attempt_id}.pid"
    path.write_text(json.dumps({"pid": pid, "start_token": start_token}), encoding="utf-8")
    os.chmod(path, 0o600)
    return path


def read_pidfile(path: Path) -> tuple[int, str] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    pid = data.get("pid")
    token = data.get("start_token")
    if isinstance(pid, bool) or not isinstance(pid, int) or not isinstance(token, str):
        return None
    return pid, token


def kill_recorded_pid(pid: int, start_token: str, timeout: float = 2.0) -> bool:
    """Kill the process group only when the start token still matches."""
    if proc_start_token(pid) != start_token or not start_token:
        return False
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc_start_token(pid) != start_token:
            return True
        time.sleep(0.05)
    if proc_start_token(pid) != start_token:
        return True
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    return True


def kill_pid_dir(directory: Path) -> list[int]:
    killed: list[int] = []
    if not directory.is_dir():
        return killed
    for path in sorted(directory.glob("*.pid")):
        parsed = read_pidfile(path)
        if parsed is not None and kill_recorded_pid(parsed[0], parsed[1]):
            killed.append(parsed[0])
        path.unlink(missing_ok=True)
    return killed


def spawn_worker(state_dir: Path, attempt_id: str, hermes_home: str, pythonpath: str, start: dict) -> WorkerHandle:
    import socket
    import sys

    parent, child = socket.socketpair()
    env = child_env(hermes_home, pythonpath, child.fileno())
    log_dir = state_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{attempt_id}.log"
    log_fp = open(stdout_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [sys.executable, "-m", "collab.worker"],
        stdin=subprocess.DEVNULL,
        stdout=log_fp,
        stderr=subprocess.STDOUT,
        pass_fds=(child.fileno(),),
        start_new_session=True,
        env=env,
        cwd=str(state_dir),
    )
    child.close()
    try:
        parent.sendall(encode_frame(start))
    except Exception:
        stop_process_group(proc.pid)
        log_fp.close()
        parent.close()
        raise
    parent.setblocking(False)
    token = proc_start_token(proc.pid)
    pid_path = write_pidfile(state_dir / "pids", attempt_id, proc.pid, token)
    return WorkerHandle(proc=proc, sock=parent, log_files=[log_fp], attempt_id=attempt_id, pid_path=pid_path)


def stop_process_group(pid: int, timeout: float = 2.0) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return


def wait_pid(pid: int, timeout: float) -> int | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            found, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return 0
        if found == pid:
            if os.WIFEXITED(status):
                return os.WEXITSTATUS(status)
            return -os.WTERMSIG(status) if os.WIFSIGNALED(status) else 1
        time.sleep(0.05)
    return None
