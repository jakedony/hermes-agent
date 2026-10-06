"""Length-prefixed JSON frames for the worker socketpair.

stdout is never the protocol. One private fd carries these frames so logs
cannot be mistaken for a grant or a result.
"""

from __future__ import annotations

import json
import struct
from typing import Any, BinaryIO

from collab.limits import MAX_FRAME_BYTES

_HEADER = struct.Struct(">I")


class FrameError(Exception):
    pass


def encode_frame(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(body) > MAX_FRAME_BYTES:
        raise FrameError(f"frame is {len(body)} bytes; limit is {MAX_FRAME_BYTES}")
    return _HEADER.pack(len(body)) + body


def write_frame(fp: BinaryIO, payload: dict[str, Any]) -> None:
    fp.write(encode_frame(payload))
    flush = getattr(fp, "flush", None)
    if flush is not None:
        flush()


def _read_exact(fp: BinaryIO, n: int) -> bytes:
    chunks = []
    remaining = n
    while remaining:
        block = fp.read(remaining)
        if not block:
            raise FrameError("protocol fd closed")
        chunks.append(block)
        remaining -= len(block)
    return b"".join(chunks)


def read_frame(fp: BinaryIO) -> dict[str, Any]:
    raw_len = _read_exact(fp, _HEADER.size)
    (size,) = _HEADER.unpack(raw_len)
    if size > MAX_FRAME_BYTES:
        raise FrameError(f"peer frame is {size} bytes; limit is {MAX_FRAME_BYTES}")
    body = _read_exact(fp, size)
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameError("peer frame is not JSON") from exc
    if not isinstance(payload, dict):
        raise FrameError("peer frame is not an object")
    return payload
