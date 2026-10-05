#!/usr/bin/env python3
"""Deterministic OpenAI-compatible chat-completions stub for bridge tests.

This is NOT a model. It lets the real Hermes runtime (AIAgent, its turn loop,
history handling and tool policy) run end to end without provider credentials,
so the bridge can be exercised against actual Hermes code. Answers produced
here must never be reported as live Hermes integration.

Behaviour, keyed on the latest user message:
  * "What test word did I give you?" -> scans earlier user turns for
    "test word: <word>" and answers with it (proves history reaches the model).
  * "STUB_SLEEP <seconds>"           -> sleeps before answering (timeout tests).
  * anything else                    -> a short canned answer.

Every request is appended to --record as one JSON line (message roles, tool
names) so tests can assert what Hermes actually sent.
"""

import argparse
import json
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODEL_ID = "bridge-stub-model"
_WORD = re.compile(r"test word:\s*([A-Za-z]+)", re.IGNORECASE)
_SLEEP = re.compile(r"STUB_SLEEP\s+(\d+(?:\.\d+)?)")
_record_lock = threading.Lock()


def _text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(p.get("text", "") for p in content if isinstance(p, dict))
    return ""


def compose_answer(messages):
    users = [_text(m.get("content")) for m in messages if m.get("role") == "user"]
    last = users[-1] if users else ""
    sleep = _SLEEP.search(last)
    if sleep:
        time.sleep(float(sleep.group(1)))
        return "Finished sleeping."
    if "what test word" in last.lower():
        for earlier in reversed(users[:-1]):
            found = _WORD.search(earlier)
            if found:
                return f"The test word you gave me was {found.group(1)}."
        return "You have not given me a test word in this conversation."
    if _WORD.search(last):
        return "Noted. I will remember that test word."
    return f"Stub answer to a {len(last)}-character question ({len(users)} user turns so far)."


class Handler(BaseHTTPRequestHandler):
    server_version = "BridgeStub/1"

    def log_message(self, *_args):
        pass

    def _json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/").endswith("/models"):
            self._json(200, {"object": "list", "data": [{"id": MODEL_ID, "object": "model", "owned_by": "stub"}]})
        else:
            self._json(404, {"error": {"message": "not found"}})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            req = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._json(400, {"error": {"message": "bad json"}})
            return
        if not self.path.rstrip("/").endswith("/chat/completions"):
            self._json(404, {"error": {"message": "not found"}})
            return
        messages = req.get("messages") or []
        self.server.record(
            {
                "roles": [m.get("role") for m in messages],
                "tools": sorted(t.get("function", {}).get("name", "") for t in (req.get("tools") or [])),
                "stream": bool(req.get("stream")),
            }
        )
        answer = compose_answer(messages)
        cid = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        usage = {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}
        if req.get("stream"):
            self._stream(cid, answer, usage)
            return
        self._json(
            200,
            {
                "id": cid,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": MODEL_ID,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": answer}, "finish_reason": "stop"}],
                "usage": usage,
            },
        )

    def _stream(self, cid, answer, usage):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        base = {"id": cid, "object": "chat.completion.chunk", "created": int(time.time()), "model": MODEL_ID}
        chunks = [
            {"choices": [{"index": 0, "delta": {"role": "assistant", "content": answer}, "finish_reason": None}]},
            {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
            {"choices": [], "usage": usage},
        ]
        for chunk in chunks:
            self.wfile.write(f"data: {json.dumps({**base, **chunk})}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


class StubServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, addr, record_path):
        super().__init__(addr, Handler)
        self.record_path = record_path

    def record(self, entry):
        if not self.record_path:
            return
        with _record_lock, open(self.record_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--record", default="")
    args = parser.parse_args()
    server = StubServer(("127.0.0.1", args.port), args.record)
    print(f"stub model server on http://127.0.0.1:{args.port}/v1", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
