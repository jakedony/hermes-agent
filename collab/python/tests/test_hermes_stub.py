"""Local stub model: restricted tools and a delegate result inside one run_conversation.

This is not a hosted-model collaboration. FakeLLMServer scripts the provider.
"""

from __future__ import annotations

import os
import socket
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, "/workspace")

from collab.frames import read_frame, write_frame
from collab.hermes_run import run_hermes
from collab.protocol_client import ProtocolClient
from tests.fakes.fake_llm_provider import MODEL_ID, FakeLLMServer, Text, ToolCall


def _respond(record: dict) -> Text | ToolCall:
    messages = record["body"].get("messages") or []
    tool_msgs = [msg for msg in messages if msg.get("role") == "tool"]
    if len(tool_msgs) == 0:
        return ToolCall("listening_sockets", {})
    if len(tool_msgs) == 1:
        return ToolCall(
            "delegate_investigation",
            {"objective": "inspect listeners on the peer", "context": {"port": 9}, "timeout_sec": 30},
        )
    combined = "\n".join(str(msg.get("content") or "") for msg in tool_msgs)
    return Text("STUB SYNTHESIS\n" + combined)


def _peer(sock: socket.socket, captured: list) -> None:
    fp = sock.makefile("rwb", buffering=0)
    write_frame(fp, {"type": "start"})
    while True:
        frame = read_frame(fp)
        if frame.get("type") == "delegate":
            write_frame(
                fp,
                {
                    "type": "delegate_result",
                    "request_id": frame["request_id"],
                    "ok": True,
                    "body": {"summary": "VM_DELEGATED_MARKER", "state": "completed", "ok": True},
                },
            )
            continue
        if frame.get("type") in {"result", "fail"}:
            captured.append(frame)
            return


class HermesStubTests(unittest.TestCase):
    def test_delegate_result_continues_the_same_conversation(self) -> None:
        previous = os.environ.get("HERMES_HOME")
        parent, child = socket.socketpair()
        captured: list = []
        peer = threading.Thread(target=_peer, args=(parent, captured), daemon=True)
        peer.start()
        client = ProtocolClient(child.makefile("rwb", buffering=0))
        try:
            with tempfile.TemporaryDirectory() as tmp, FakeLLMServer(_respond, api_key="sk-collab-stub") as server:
                home = Path(tmp) / "hermes"
                os.environ["HERMES_HOME"] = str(home)
                start = {
                    "mode": "hermes",
                    "machine_id": "pc",
                    "tool_timeout_sec": 10,
                    "max_iterations": 12,
                    "allowlist": [{"host": "127.0.0.1", "port": 9}],
                    "hermes_home": str(home),
                    "model": {"id": MODEL_ID, "base_url": server.base_url, "api_key": "sk-collab-stub"},
                    "grant": {
                        "role": "coordinator",
                        "objective": "find why the peer service is unreachable",
                        "context": {},
                    },
                }
                outcome = run_hermes(start, client)
        finally:
            if previous is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous
            child.close()
            parent.close()
        self.assertEqual(outcome.get("type"), "result", outcome)
        self.assertEqual(outcome.get("api_calls"), 3)
        summary = str(outcome.get("summary") or "")
        self.assertIn("listening_sockets", summary)
        self.assertIn("VM_DELEGATED_MARKER", summary)
        self.assertEqual(len(server.main_requests()), 3)
        peer.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
