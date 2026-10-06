"""Blocking protocol peer used inside a worker.

The reader runs on a thread started with ``contextvars.copy_context().run`` so
a Hermes tool thread can wait for ``delegate_investigation`` without reading
the fd itself. Diagnostics never touch this fd.
"""

from __future__ import annotations

import contextvars
import queue
import threading
from typing import Any, BinaryIO

from collab.frames import FrameError, read_frame, write_frame


class ProtocolClient:
    def __init__(self, fp: BinaryIO) -> None:
        self._fp = fp
        self._write_lock = threading.Lock()
        self._waiters: dict[str, queue.Queue] = {}
        self._start: queue.Queue = queue.Queue()
        self.cancelled = threading.Event()
        ctx = contextvars.copy_context()
        self._thread = threading.Thread(
            target=ctx.run,
            args=(self._read_loop,),
            name="collab-protocol",
            daemon=True,
        )
        self._thread.start()

    def _read_loop(self) -> None:
        while not self.cancelled.is_set():
            try:
                frame = read_frame(self._fp)
            except (FrameError, OSError):
                self.cancelled.set()
                self._wake_all({"type": "cancel", "ok": False, "body": {"error": "protocol closed"}})
                return
            kind = frame.get("type")
            if kind == "start":
                self._start.put(frame)
                continue
            if kind == "delegate_result":
                box = self._waiters.get(str(frame.get("request_id") or ""))
                if box is not None:
                    box.put(frame)
                continue
            if kind == "cancel":
                self.cancelled.set()
                self._wake_all(frame)
                return

    def _wake_all(self, frame: dict[str, Any]) -> None:
        for box in self._waiters.values():
            box.put(frame)

    def wait_start(self, timeout: float) -> dict[str, Any]:
        return self._start.get(timeout=timeout)

    def send(self, payload: dict[str, Any]) -> None:
        with self._write_lock:
            write_frame(self._fp, payload)

    def delegate(self, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
        request_id = str(payload["request_id"])
        box: queue.Queue = queue.Queue()
        self._waiters[request_id] = box
        self.send({"type": "delegate", **payload})
        try:
            frame = box.get(timeout=timeout)
        except queue.Empty as exc:
            raise TimeoutError("delegate_investigation timed out") from exc
        finally:
            self._waiters.pop(request_id, None)
        return frame
