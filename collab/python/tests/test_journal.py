"""Journal contracts: grant fence, outbox resubmit, no token column, pid-file kill."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from collab.journal import Journal
from collab.procman import kill_pid_dir, proc_start_token, write_pidfile
from collab.protocol import command_frame


def _grant(attempt_id: str = "att_abc") -> dict:
    return {
        "attempt_id": attempt_id,
        "task_id": "tsk_1",
        "attempt_n": 1,
        "role": "worker",
        "objective": "look",
        "context_json": "{}",
        "profile": "vm-local",
        "parent_task_id": "tsk_root",
        "root_id": "tsk_root",
        "deadline_at_ms": 1,
        "lease_sec": 30,
    }


class JournalTests(unittest.TestCase):
    def test_duplicate_grant_does_not_start_twice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Journal(Path(tmp) / "j.db")
            self.assertTrue(journal.insert_grant_if_absent(_grant()))
            self.assertFalse(journal.insert_grant_if_absent(_grant()))
            journal.close()

    def test_unsent_result_resubmits_original_request_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "j.db"
            journal = Journal(path)
            journal.insert_grant_if_absent(_grant())
            payload = {"attempt_id": "att_abc", "summary": "done", "machine_id": "vm", "limitations": [], "evidence": []}
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            journal.enqueue_result("done_att_abc", "tsk_1", "att_abc", "task.complete", encoded, "lab")
            journal.close()
            again = Journal(path)
            pending = again.pending_outbox()
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].request_id, "done_att_abc")
            self.assertEqual(pending[0].payload_json, encoded)
            again.mark_sent("done_att_abc")
            self.assertEqual(again.pending_outbox(), [])
            again.close()

    def test_restart_does_not_rerun_a_granted_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Journal(Path(tmp) / "j.db")
            journal.insert_grant_if_absent(_grant())
            abandoned = journal.recover_unfinished(lambda attempt_id: f"fail_{attempt_id}", "lab")
            self.assertEqual(abandoned, ["att_abc"])
            self.assertFalse(journal.insert_grant_if_absent(_grant()))
            pending = journal.pending_outbox()
            self.assertEqual(pending[0].command_type, "task.fail")
            self.assertEqual(pending[0].request_id, "fail_att_abc")
            body = json.loads(pending[0].payload_json)
            self.assertEqual(body["class"], "crash")
            self.assertIn("not rerun", body["message"])
            grant = journal.get_grant("att_abc")
            self.assertIsNotNone(grant)
            assert grant is not None
            self.assertEqual(grant.status, "abandoned")
            journal.close()

    def test_create_retry_keeps_the_original_payload_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Journal(Path(tmp) / "j.db")
            first_id, first_payload = journal.create_request_id("same", '{"z":1,"a":2}', lambda: "crt_1")
            second_id, second_payload = journal.create_request_id("same", '{"a":2,"z":1}', lambda: "crt_2")
            self.assertEqual(first_id, second_id)
            self.assertEqual(first_payload, '{"z":1,"a":2}')
            self.assertEqual(second_payload, first_payload)
            frame = command_frame("task.create", second_id, "lab", second_payload)
            self.assertIn('"payload":{"z":1,"a":2}', frame)
            reordered = command_frame("task.create", "crt_9", "lab", {"b": 1, "a": {"d": 1, "c": 2}})
            again = command_frame("task.create", "crt_9", "lab", {"a": {"c": 2, "d": 1}, "b": 1})
            self.assertEqual(reordered, again)
            claim = command_frame("task.claim", "clm_0123456789abcdef", "lab", {}, "tsk_1")
            self.assertIn('"payload":{}', claim)
            self.assertNotIn('"ok"', claim)
            self.assertNotIn('"replay"', claim)
            journal.close()

    def test_claim_request_id_is_stable_until_requeue(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            journal = Journal(Path(tmp) / "j.db")
            minted: list[str] = []

            def mint() -> str:
                minted.append(f"clm_{len(minted)}")
                return minted[-1]

            first = journal.claim_request_id("tsk_1", 0, mint)
            second = journal.claim_request_id("tsk_1", 0, mint)
            self.assertEqual(first, second)
            self.assertEqual(minted, ["clm_0"])
            journal.mark_claim_granted("tsk_1", 0, "att_abc")
            third = journal.claim_request_id("tsk_1", 1, mint)
            self.assertEqual(third, "clm_1")
            self.assertNotEqual(third, first)
            journal.close()

    def test_tokens_are_not_stored(self) -> None:
        secret = "collab-test-token-should-not-appear"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "j.db"
            journal = Journal(path)
            journal.insert_grant_if_absent(_grant())
            journal.enqueue_result("done_att_abc", "tsk_1", "att_abc", "task.complete", "{}", "lab")
            journal.backup(Path(tmp) / "backup.db")
            journal.close()
            blob = path.read_bytes()
            for extra in (Path(str(path) + "-wal"), Path(str(path) + "-shm"), Path(tmp) / "backup.db"):
                if extra.exists():
                    blob += extra.read_bytes()
            self.assertNotIn(secret.encode("utf-8"), blob)
            self.assertNotIn(b"token", path.read_bytes().split(b"CREATE TABLE")[0])

    def test_pidfile_kill_matches_start_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
            try:
                token = proc_start_token(proc.pid)
                self.assertTrue(token)
                write_pidfile(directory, "att_live", proc.pid, "not-the-start-token")
                self.assertEqual(kill_pid_dir(directory), [])
                self.assertIsNone(proc.poll())
                write_pidfile(directory, "att_live", proc.pid, token)
                self.assertEqual(kill_pid_dir(directory), [proc.pid])
                self.assertEqual(proc.wait(timeout=3), -15)
            finally:
                if proc.poll() is None:
                    os.killpg(proc.pid, 9)
                    proc.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
