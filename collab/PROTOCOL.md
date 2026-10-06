# Protocol

Version `v` is `1`. The same JSON envelope is used on WebSocket text frames and on `POST /v1/command`.

```json
{"v":1,"type":"task.claim","request_id":"clm_0123456789abcdef","room_id":"lab","task_id":"tsk_...","payload":{}}
```

`request_id` matches `^[A-Za-z0-9_.:-]{1,80}$`. Unknown envelope fields are rejected. An empty payload is `{}`.

## Transport

- WebSocket path is `GET /ws`. Commands are `POST /v1/command`. `Authorization: Bearer <token>`.
- HTTP 400: malformed frame, including when the bearer token is missing, or any query string containing `token`.
- HTTP 401: unauthorized.
- HTTP 200: the command was decided. Success is envelope type `ack`. A business failure is envelope type `error`. The error payload is `{"code","message","detail"}`. `detail` is `null` or an object. `not_ready` uses `{"retry_after_ms": N}`.
- HTTP 500: internal failure, including a database error. That command is not stored. A retry with the same bytes can still be accepted.
- `GET /v1/tasks`, `GET /v1/tasks/{id}`, and `GET /v1/tasks/{id}/history` exist. The bridge's human commands use `POST /v1/command` with the human token instead of the agent socket.

## Acknowledgements

There is no `ok` field and no `replay` field on the wire. A retry of a stored command is byte-for-byte the original `ack` or `error`. Correlate an acknowledgement by `type` (`ack` or `error`) plus `request_id`.

Events and acknowledgements share one socket. Either may arrive first. An event is not an acknowledgement.

## Events

A pushed event uses the event name as `type` and carries `event_id`, `seq`, `room_id`, `task_id`, and `payload`. It does not carry `actor` or `created_at_ms`. Those two fields exist only on `task.history`.

Names: `task.created`, `task.started`, `attempt.renewed`, `task.completed`, `task.requeued`, `task.failed`, `task.timed_out`, `task.cancelled`, `task.cancel_rejected`, `attempt.abandoned`, `attempt.stale_result`.

Events are hints. A claim, get, or complete acknowledgement is the authority.

## Session

The first WebSocket frame is `agent.hello`. The bridge sends `{"takeover":true}`. A second socket that does not send `{"takeover":true}` is rejected with `session_conflict`. Takeover replaces the live socket. It does not create an attempt.

## Commands

`task.create` payload:

| Field | Meaning |
| --- | --- |
| `parent_task_id` | Empty for a human root. Set for a coordinator child. |
| `assigned_to` | Agent principal id. |
| `objective` | Non-empty text. |
| `context` | JSON object. |
| `profile` | `[a-z0-9-]{1,32}`. |
| `timeout_sec` | Capped by the hub (root 180s, child 120s). |
| `requested_by`, `actor` | Accepted and ignored. The bearer token is the actor. |

Any other payload field is `bad_request`. The hub sets `requested_by` from the bearer token. A human creates only a root. Only the root coordinator creates a direct child, and not assigned to itself. Child budget default is 3.

`task.claim` returns `attempt_id`, `attempt_n`, `lease_sec`, `deadline_at_ms`, `objective`, `context`, `profile`, `parent_task_id`, `root_id`, and `role`. Role is `coordinator` when there is no parent, otherwise `worker`.

`attempt.renew` payload is `{"attempt_id":"..."}`. A connected socket does not renew by itself. A live lease does not keep a task running past its deadline: renew and complete fail once `deadline_at_ms` has passed, and the sweeper times the task out even if the lease is still in the future.

`task.complete` payload: `attempt_id`, `summary`, `machine_id`, `limitations` (array of strings), `evidence` (array of `{operation, source, observed_at, excerpt}`). `machine_id` must match the authenticated agent.

`task.fail` classes:

| Class | Retryable |
| --- | --- |
| `transient`, `crash`, `lease_lost` | yes, while deadline, attempt budget (default 2), and execution budget remain |
| `invalid_input`, `unauthorized`, `cancelled`, `deadline` | no |

`not_ready` is not stored. Retry the same claim request id after `detail.retry_after_ms`. `lease_expired` on renew is also not stored. `internal` is not stored.

## Idempotency

The key is `(token principal, request_id)`. The hash is SHA-256 of `type`, `room_id`, `task_id`, and the raw payload bytes, separated by NUL:

```text
type || 0x00 || room_id || 0x00 || task_id || 0x00 || payload_bytes
```

The hub does not canonicalize JSON. Different bytes for the same request id are `conflict`. A stored success or business failure is replayed as the original body.

The bridge encodes a new dict once (compact JSON, sorted keys) and stores that text before it needs to survive a lost acknowledgement. A resend splices those stored bytes into the frame. It does not re-encode them.

A claim request id is unique per logical claim. Store it before the send. A lost ack retries that same id. After the hub requeues the task, the next claim uses a new id. `not_ready` is not sticky, so the same claim id is safe to send again once `retry_after_ms` has elapsed.

Default idempotency retention is 7 days (`idempotency_retention_sec` 604800).

## Client rules that are not exactly-once

The bridge persists the grant locally before it starts a worker. A duplicate claim acknowledgement for the same `attempt_id` does not start a second worker. It persists the result before it sends complete or fail, and resubmits that row with the original request id and the original payload bytes. After a restart it does not execute an attempt that was already granted; it fails that attempt as `crash` (or learns from the hub that the attempt is already `stale`) and only a later queued attempt can run.
