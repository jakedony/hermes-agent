"""Behaviour contracts for apps/hermes-bridge/worker/hermes_worker.py.

The stdout-guard and not-configured tests run the real worker (and real Hermes imports) in a
subprocess against a temp HERMES_HOME; the Conversation tests drive a scripted stand-in agent.
"""

import importlib.util
import json
import os
import subprocess
import sys
import textwrap
import threading
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKER = REPO / "apps" / "hermes-bridge" / "worker" / "hermes_worker.py"


def _load_worker():
    spec = importlib.util.spec_from_file_location("hermes_worker", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _env(tmp_path):
    env = dict(os.environ, PYTHONPATH=str(REPO), HERMES_HOME=str(tmp_path / "home"))
    (tmp_path / "home").mkdir(exist_ok=True)
    return env


def test_stray_output_cannot_reach_the_protocol_stream(tmp_path):
    script = textwrap.dedent(
        f"""
        import importlib.util, os, subprocess, sys
        spec = importlib.util.spec_from_file_location("w", {str(WORKER)!r})
        w = importlib.util.module_from_spec(spec); spec.loader.exec_module(w)
        channel = w.Channel(w._claim_stdout(), 1 << 20)
        print("python print noise")
        os.write(1, b"raw fd1 noise\\n")
        subprocess.run(["sh", "-c", "echo child noise"], check=True)
        channel.send({{"type": "ready"}})
        """
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=_env(tmp_path), timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.splitlines() == ['{"type":"ready"}']
    for noise in ("python print noise", "raw fd1 noise", "child noise"):
        assert noise in proc.stderr


def test_unconfigured_hermes_yields_one_fatal_record(tmp_path):
    proc = subprocess.run(
        [sys.executable, str(WORKER), "--hermes-home", str(tmp_path / "home"), "--workdir", str(tmp_path / "wd")],
        input="", capture_output=True, text=True, env=_env(tmp_path), timeout=120,
    )
    records = [json.loads(line) for line in proc.stdout.splitlines()]
    assert proc.returncode == 3
    assert [r["type"] for r in records] == ["fatal"]
    assert records[0]["code"] == "HERMES_NOT_CONFIGURED"


class _Sink:
    def __init__(self):
        self.records = []

    def write(self, data):
        self.records.extend(json.loads(line) for line in data.decode().splitlines())


class _ScriptedAgent:
    """Stand-in for AIAgent.run_conversation returning queued result dicts."""

    def __init__(self, results):
        self.results = list(results)
        self.seen_history = []
        self.interrupted = threading.Event()

    def run_conversation(self, message, conversation_history=None, task_id=None):
        self.seen_history.append(list(conversation_history or []))
        result = self.results.pop(0)
        if result == "wait-for-interrupt":
            self.interrupted.wait(10)
            return {"interrupted": True, "final_response": "Operation interrupted", "messages": []}
        return result

    def interrupt(self, *args, **kwargs):
        self.interrupted.set()


def _turn(user, answer):
    return [{"role": "user", "content": user}, {"role": "assistant", "content": answer}]


def _run(worker, conv, request_id, message):
    conv.start(request_id, message)
    conv.wait(10)
    return conv.channel._stream.records[-1]


def test_history_only_advances_on_successful_turns(tmp_path):
    worker = _load_worker()
    first = _turn("remember mango", "ok")
    agent = _ScriptedAgent([
        {"final_response": "ok", "messages": first},
        {"failed": True, "final_response": "provider error", "messages": first + [{"role": "user", "content": "x"}]},
        "wait-for-interrupt",
        {"final_response": "mango", "messages": first + _turn("recall", "mango")},
    ])
    conv = worker.Conversation(agent, worker.Channel(_Sink(), 1 << 20))

    assert _run(worker, conv, "r1", "remember mango")["ok"] is True
    failed = _run(worker, conv, "r2", "x")
    assert (failed["ok"], failed["code"]) == (False, "HERMES_ERROR")

    conv.start("r3", "slow")
    conv.cancel("r3")
    conv.wait(10)
    cancelled = conv.channel._stream.records[-1]
    assert (cancelled["ok"], cancelled["code"]) == (False, "CANCELLED")

    done = _run(worker, conv, "r4", "recall")
    assert done["ok"] is True and done["messageCount"] == 4
    # Every turn after the first saw exactly the committed first turn: neither the failed nor the
    # cancelled turn leaked into the history that Hermes receives.
    assert agent.seen_history == [[], first, first, first]


def test_oversized_answer_becomes_structured_error():
    worker = _load_worker()
    sink = _Sink()
    worker.Channel(sink, 256).send({"type": "result", "requestId": "r1", "ok": True, "text": "x" * 1000})
    assert sink.records == [{"type": "result", "requestId": "r1", "ok": False, "code": "RESPONSE_TOO_LARGE",
                             "message": "The answer exceeded the 256-byte record limit."}]
