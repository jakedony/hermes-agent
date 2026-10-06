"""Local SQLite journal.

A grant is inserted before a worker is spawned. A result is inserted before it
is sent. Restart resubmits an unsent result with the original request id and
does not execute an attempt that was already granted. Tokens are never written
here; callers must not put them in objectives either, but the schema has no
token column on purpose.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from collab.limits import IDEMPOTENCY_RETENTION_SEC

_SCHEMA = """
CREATE TABLE IF NOT EXISTS grants (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt_n INTEGER NOT NULL,
    role TEXT NOT NULL,
    objective TEXT NOT NULL,
    context_json TEXT NOT NULL,
    profile TEXT NOT NULL,
    parent_task_id TEXT NOT NULL,
    root_id TEXT NOT NULL,
    deadline_at_ms INTEGER NOT NULL,
    lease_sec INTEGER NOT NULL,
    status TEXT NOT NULL,
    pid INTEGER,
    start_token TEXT NOT NULL DEFAULT '',
    created_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
    task_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (task_id, generation)
);
CREATE TABLE IF NOT EXISTS outbox (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    command_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    room_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    terminal_code TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS creates (
    payload_key TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    task_id TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
"""


@dataclass(frozen=True)
class Grant:
    attempt_id: str
    task_id: str
    attempt_n: int
    role: str
    objective: str
    context_json: str
    profile: str
    parent_task_id: str
    root_id: str
    deadline_at_ms: int
    lease_sec: int
    status: str
    pid: int | None
    start_token: str
    created_at_ms: int


@dataclass(frozen=True)
class OutboxItem:
    request_id: str
    task_id: str
    attempt_id: str
    command_type: str
    payload_json: str
    room_id: str
    created_at_ms: int
    sent: int
    terminal_code: str


class Journal:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, isolation_level=None, timeout=5, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def backup(self, dest: str | Path) -> None:
        """Copy via SQLite's backup API. Do not copy a live WAL file by hand."""
        target = Path(dest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError(str(target))
        with self._lock:
            dest_conn = sqlite3.connect(target)
            try:
                self._conn.backup(dest_conn)
            finally:
                dest_conn.close()

    def insert_grant_if_absent(self, fields: dict[str, Any]) -> bool:
        """Persist the grant. False means this attempt was already accepted."""
        now = int(time.time() * 1000)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                found = self._conn.execute(
                    "SELECT attempt_id FROM grants WHERE attempt_id=?",
                    (fields["attempt_id"],),
                ).fetchone()
                if found is not None:
                    self._conn.execute("COMMIT")
                    return False
                self._conn.execute(
                    """INSERT INTO grants(attempt_id, task_id, attempt_n, role, objective, context_json,
                       profile, parent_task_id, root_id, deadline_at_ms, lease_sec, status, pid, start_token, created_at_ms)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        fields["attempt_id"],
                        fields["task_id"],
                        int(fields["attempt_n"]),
                        fields["role"],
                        fields["objective"],
                        fields["context_json"],
                        fields.get("profile") or "default",
                        fields.get("parent_task_id") or "",
                        fields.get("root_id") or "",
                        int(fields["deadline_at_ms"]),
                        int(fields["lease_sec"]),
                        "granted",
                        None,
                        "",
                        now,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return True

    def get_grant(self, attempt_id: str) -> Grant | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM grants WHERE attempt_id=?", (attempt_id,)).fetchone()
        return _grant(row) if row is not None else None

    def set_running(self, attempt_id: str, pid: int, start_token: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE grants SET status='running', pid=?, start_token=? WHERE attempt_id=?",
                (pid, start_token, attempt_id),
            )

    def set_status(self, attempt_id: str, status: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE grants SET status=? WHERE attempt_id=?", (status, attempt_id))

    def unfinished(self) -> list[Grant]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM grants WHERE status IN ('granted', 'running') ORDER BY created_at_ms"
            ).fetchall()
        return [_grant(row) for row in rows]

    def claim_request_id(self, task_id: str, generation: int, mint) -> str:
        """Return the stored claim id, creating it before any network send.

        ``mint`` is called only when this generation has no row yet. A lost ack
        retries the same id. A later generation (after requeue) is a new row.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT request_id, state FROM claims WHERE task_id=? AND generation=?",
                    (task_id, generation),
                ).fetchone()
                if row is not None and row["state"] in {"inflight", "granted"}:
                    request_id = str(row["request_id"])
                    self._conn.execute("COMMIT")
                    return request_id
                request_id = mint()
                self._conn.execute(
                    """INSERT INTO claims(task_id, generation, request_id, state, attempt_id)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(task_id, generation) DO UPDATE SET
                         request_id=excluded.request_id, state='inflight', attempt_id=''""",
                    (task_id, generation, request_id, "inflight", ""),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return request_id

    def mark_claim_granted(self, task_id: str, generation: int, attempt_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE claims SET state='granted', attempt_id=? WHERE task_id=? AND generation=?",
                (attempt_id, task_id, generation),
            )

    def create_request_id(self, payload_key: str, payload_json: str, mint) -> tuple[str, str]:
        """Return the request id and the payload bytes that were stored first.

        A later call with the same key resends the original payload text. The
        hub treats any other bytes for that request id as ``conflict``.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT request_id, payload_json FROM creates WHERE payload_key=?",
                    (payload_key,),
                ).fetchone()
                if row is not None:
                    found = (str(row["request_id"]), str(row["payload_json"]))
                    self._conn.execute("COMMIT")
                    return found
                request_id = mint()
                now = int(time.time() * 1000)
                self._conn.execute(
                    "INSERT INTO creates(payload_key, request_id, payload_json, created_at_ms) VALUES (?,?,?,?)",
                    (payload_key, request_id, payload_json, now),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return request_id, payload_json

    def remember_created_task(self, payload_key: str, task_id: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE creates SET task_id=? WHERE payload_key=?", (task_id, payload_key))

    def enqueue_result(
        self,
        request_id: str,
        task_id: str,
        attempt_id: str,
        command_type: str,
        payload_json: str,
        room_id: str,
    ) -> None:
        now = int(time.time() * 1000)
        with self._lock:
            self._conn.execute(
                """INSERT INTO outbox(request_id, task_id, attempt_id, command_type, payload_json, room_id, created_at_ms, sent)
                   VALUES (?,?,?,?,?,?,?,0)
                   ON CONFLICT(request_id) DO NOTHING""",
                (request_id, task_id, attempt_id, command_type, payload_json, room_id, now),
            )
            self._conn.execute(
                "UPDATE grants SET status='result_ready' WHERE attempt_id=? AND status IN ('granted', 'running')",
                (attempt_id,),
            )

    def pending_outbox(self) -> list[OutboxItem]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM outbox WHERE sent=0 AND terminal_code='' ORDER BY created_at_ms"
            ).fetchall()
        return [_outbox(row) for row in rows]

    def pending_for_attempt(self, attempt_id: str) -> OutboxItem | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM outbox WHERE attempt_id=? AND sent=0 AND terminal_code='' ORDER BY created_at_ms LIMIT 1",
                (attempt_id,),
            ).fetchone()
        return _outbox(row) if row is not None else None

    def mark_sent(self, request_id: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE outbox SET sent=1 WHERE request_id=?", (request_id,))
            row = self._conn.execute("SELECT attempt_id FROM outbox WHERE request_id=?", (request_id,)).fetchone()
            if row is not None:
                self._conn.execute(
                    "UPDATE grants SET status='submitted' WHERE attempt_id=? AND status='result_ready'",
                    (row["attempt_id"],),
                )

    def mark_terminal(self, request_id: str, code: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE outbox SET terminal_code=?, sent=1 WHERE request_id=?",
                (code, request_id),
            )

    def recover_unfinished(self, fail_request_id, room_id: str) -> list[str]:
        """Queue a crash fail for every granted attempt that has no durable result.

        Returns attempt ids that must not be executed again. ``fail_request_id``
        is called once per new fail so the id is stable for that payload.
        """
        abandoned: list[str] = []
        for grant in self.unfinished():
            if self.pending_for_attempt(grant.attempt_id) is not None:
                continue
            payload = {
                "attempt_id": grant.attempt_id,
                "class": "crash",
                "message": "bridge restarted before a result was durable; the attempt was not rerun",
            }
            import json

            encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            request_id = fail_request_id(grant.attempt_id)
            self.enqueue_result(request_id, grant.task_id, grant.attempt_id, "task.fail", encoded, room_id)
            self.set_status(grant.attempt_id, "abandoned")
            abandoned.append(grant.attempt_id)
        return abandoned

    def prune(self, retention_sec: int = IDEMPOTENCY_RETENTION_SEC, now_ms: int | None = None) -> None:
        cutoff = (now_ms if now_ms is not None else int(time.time() * 1000)) - retention_sec * 1000
        with self._lock:
            self._conn.execute("DELETE FROM outbox WHERE sent=1 AND created_at_ms < ?", (cutoff,))
            self._conn.execute(
                "DELETE FROM claims WHERE state='granted' AND task_id IN (SELECT task_id FROM grants WHERE created_at_ms < ? AND status IN ('submitted', 'abandoned'))",
                (cutoff,),
            )


def _grant(row: sqlite3.Row) -> Grant:
    return Grant(
        attempt_id=row["attempt_id"],
        task_id=row["task_id"],
        attempt_n=row["attempt_n"],
        role=row["role"],
        objective=row["objective"],
        context_json=row["context_json"],
        profile=row["profile"],
        parent_task_id=row["parent_task_id"],
        root_id=row["root_id"],
        deadline_at_ms=row["deadline_at_ms"],
        lease_sec=row["lease_sec"],
        status=row["status"],
        pid=row["pid"],
        start_token=row["start_token"],
        created_at_ms=row["created_at_ms"],
    )


def _outbox(row: sqlite3.Row) -> OutboxItem:
    return OutboxItem(
        request_id=row["request_id"],
        task_id=row["task_id"],
        attempt_id=row["attempt_id"],
        command_type=row["command_type"],
        payload_json=row["payload_json"],
        room_id=row["room_id"],
        created_at_ms=row["created_at_ms"],
        sent=row["sent"],
        terminal_code=row["terminal_code"],
    )
