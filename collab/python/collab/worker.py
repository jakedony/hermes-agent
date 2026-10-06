"""Worker subprocess. Protocol frames use ``COLLAB_PROTOCOL_FD``; stdout does not."""

from __future__ import annotations

import logging
import os
import queue
from typing import Any, BinaryIO

from collab.frames import FrameError
from collab.protocol_client import ProtocolClient

_MODES = {}
log = logging.getLogger("collab.worker")


def main() -> int:
    raw = os.environ.get("COLLAB_PROTOCOL_FD", "")
    if not raw.isdigit():
        print("collab worker: COLLAB_PROTOCOL_FD is not set", flush=True)
        return 2
    fd = int(raw)
    os.set_inheritable(fd, False)
    fp: BinaryIO = os.fdopen(fd, "r+b", buffering=0)
    client = ProtocolClient(fp)
    try:
        start = client.wait_start(timeout=30)
    except (queue.Empty, FrameError, OSError) as exc:
        print(f"collab worker: no start frame ({exc.__class__.__name__})", flush=True)
        return 2
    mode = str(start.get("mode") or "")
    runner = _MODES.get(mode)
    if runner is None:
        client.send({"type": "fail", "class": "invalid_input", "message": "unknown worker mode"})
        return 2
    try:
        outcome = runner(start, client)
    except (FrameError, TimeoutError, OSError) as exc:
        outcome = {"type": "fail", "class": "crash", "message": exc.__class__.__name__}
    except (RuntimeError, ValueError, ImportError) as exc:
        log.warning("worker failed", exc_info=True)
        outcome = {"type": "fail", "class": "crash", "message": f"{exc.__class__.__name__}: {exc}"[:2000]}
    try:
        client.send(outcome)
    except OSError:
        return 1
    return 0 if outcome.get("type") == "result" else 1


def _run_fake(start: dict[str, Any], client: ProtocolClient) -> dict[str, Any]:
    from collab.fake_agent import run_fake

    return run_fake(start, client)


def _run_hermes(start: dict[str, Any], client: ProtocolClient) -> dict[str, Any]:
    from collab.hermes_run import run_hermes

    return run_hermes(start, client)


_MODES["fake"] = _run_fake
_MODES["hermes"] = _run_hermes


if __name__ == "__main__":
    raise SystemExit(main())
