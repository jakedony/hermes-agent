# Hermes bridge protocol, version 1

Two protocols are defined here:

1. **Client protocol**: JSON over WebSocket at `ws://127.0.0.1:8765/ws`, spoken by the Windows
   application, `bridge-client` and anything else.
2. **Worker protocol**: newline-delimited JSON over a subprocess's stdin and stdout, spoken between
   the Go server and `worker/hermes_worker.py`. It is internal and not a public API.

## 1. Client protocol

### 1.1 Transport and handshake

- Endpoint: `GET /ws` (WebSocket, RFC 6455). Every message is one UTF-8 **text** frame holding one
  JSON object. Binary frames are rejected with `MALFORMED_MESSAGE`.
- **Authentication** (required) uses a shared bearer token, which the server creates in `state/token`
  (mode 0600) on first start. A client sends it in exactly one of two ways:
  - Native clients (the Windows application, `bridge-client`) send the header
    `Authorization: Bearer <token>`.
  - Browser clients cannot set headers, so they offer two subprotocols:
    `new WebSocket(url, ["hermes-bridge.v1", "hermes-bridge.token.<token>"])`. The server selects
    only `hermes-bridge.v1` and never echoes the token. A client that offers any subprotocol must
    include `hermes-bridge.v1`, or the server answers HTTP 400.
- **Origin policy:** a request without an `Origin` header (a native client) is allowed. A request
  with an `Origin` must match an entry in `allowedOrigins` **exactly** (scheme, host and port). The
  default list is empty, so every browser origin is rejected. Wildcards, patterns and `null` are
  refused at config load. There is deliberately no "same origin as Host" shortcut: this server
  serves no pages, so an Origin that equals the Host only indicates DNS rebinding.
- **Host check:** the `Host` header must be a loopback name or address (`127.0.0.1`, `[::1]`,
  `localhost`). An SSH tunnel always presents one of these.
- **Handshake failures** happen before the upgrade, so they are plain HTTP responses and no protocol
  event is sent: `401` for a missing or wrong token, `403` for the origin or host, `400` for a
  missing subprotocol name, and `503` when shutting down or at `maxConnections`.

### 1.2 Client messages

Every client message carries `protocolVersion: 1` and a `type`. Unknown fields are rejected
(`INVALID_FIELD`), because v1 is strict: a field the server does not understand could change the
answer, so it is never silently ignored.

`requestId` and `sessionId` are 1–128 characters from `A-Z a-z 0-9 . _ : -`.

#### `ask`

```json
{
  "protocolVersion": 1,
  "type": "ask",
  "requestId": "req-001",
  "sessionId": "session-001",
  "message": "Explain the difference between authentication and authorisation.",
  "context": null
}
```

- `message`: required. It must contain non-whitespace text and be at most `maxQuestionBytes`
  (32 KiB) of UTF-8.
- `context`: optional, and reserved for the PDF reader:

  ```json
  { "documentId": "document-001", "page": 4, "selectedText": "The passage being discussed" }
  ```

  The fields are validated: `documentId` must be 1–128 bytes, `page` an integer of at least 1, and
  `selectedText` at most 16 KiB; any other key is `INVALID_FIELD`. A missing, `null` or `{}`
  context means "no context". **Any present field is rejected with `UNSUPPORTED_CONTEXT`** in this
  version, because answering without the context would answer a different question.

#### `end_session`

```json
{ "protocolVersion": 1, "type": "end_session", "requestId": "req-009", "sessionId": "session-001" }
```

This stops the session's worker and discards its history. The reply is `session_ended`, with
`existed: false` if there was no such session on this connection. If the session is busy, the
message is rejected with `SESSION_BUSY`.

### 1.3 Server events

Every event carries `protocolVersion: 1` and a `type`. Request-related events always carry
`requestId` and `sessionId`; the value is `null` only when the identifier could not be parsed
safely.

| type | when | extra fields |
|---|---|---|
| `hello` | immediately after connect | `connectionId`, `limits` |
| `accepted` | the request was admitted | `newSession` (true when this request started a fresh conversation) |
| `answer` | Hermes produced the complete final response | `text` |
| `error` | a structured failure | `code`, `message`, `retryable`, `rejected`, `sessionReset?`, `field?` |
| `done` | the request finished | `status` (`"ok"` or `"error"`), `durationMs` |
| `session_ended` | a session was closed | `reason` (`client_request`, `idle_timeout`, `worker_exited`), `existed`, `requestId` (null unless `client_request`) |

Example `answer` and `error` events:

```json
{"protocolVersion":1,"type":"answer","requestId":"req-001","sessionId":"session-001","text":"..."}
{"protocolVersion":1,"type":"error","requestId":"req-001","sessionId":"session-001","code":"HERMES_TIMEOUT",
 "message":"The agent did not finish within the configured timeout. ...","retryable":true,"rejected":false}
```

### 1.4 Event sequences

- **Accepted request:** `accepted`, then exactly one `answer` **or** `error` (with
  `rejected: false`), then exactly one `done`, as long as the connection stays open.
- **Rejected request:** exactly one `error` with `rejected: true`. No `accepted` precedes it and no
  `done` follows it. A rejected request did not run and does not consume its `requestId`, so a
  `retryable` rejection may be resent unchanged.
- A client therefore treats a request as finished on `done`, or on an `error` with
  `rejected: true`.
- Events for different sessions may interleave on one connection; correlate them by `requestId`.

### 1.5 Error codes

| code | rejected | retryable | meaning |
|---|---|---|---|
| `MALFORMED_MESSAGE` | yes | no | not a JSON object, a binary frame, or undecodable; identifiers are `null` |
| `UNSUPPORTED_PROTOCOL_VERSION` | yes | no | `protocolVersion` is not `1` (the message names the supported versions) |
| `UNSUPPORTED_MESSAGE_TYPE` | yes | no | `type` is not `ask` or `end_session` |
| `MISSING_FIELD` / `INVALID_FIELD` | yes | no | `field` names the offending field |
| `EMPTY_MESSAGE` | yes | no | the question is empty or whitespace |
| `MESSAGE_TOO_LARGE` | yes | no | question > `maxQuestionBytes` (ids kept), or frame > `maxMessageBytes` (ids `null`, frame never parsed) |
| `UNSUPPORTED_CONTEXT` | yes | no | `context` carries fields this version cannot use |
| `DUPLICATE_REQUEST_ID` | yes | no | `requestId` already admitted on this connection (the last 1024 are remembered) |
| `SESSION_BUSY` | yes | yes | the session is still running a request; there is no queueing |
| `SESSION_LIMIT` | yes | yes | `maxSessions` reached server-wide |
| `SERVER_BUSY` | yes | yes | `maxConcurrentRequests` reached |
| `HISTORY_LIMIT` | yes | no | the session reached `maxTurnsPerSession` or `maxHistoryBytes`; use a new `sessionId` |
| `SHUTTING_DOWN` | either | yes | the server is stopping; in-flight requests are cancelled with this code |
| `HERMES_TIMEOUT` | no | yes | `requestTimeout` elapsed; see `sessionReset` |
| `HERMES_ERROR` | no | yes | Hermes ran but did not produce an answer (provider error, empty answer); history is unchanged |
| `HERMES_NOT_CONFIGURED` | no | no | Hermes on the server has no provider; run `hermes model` there |
| `WORKER_START_FAILED` | no | yes | the worker could not start or become ready |
| `WORKER_CRASHED` | no | yes | the worker died mid-request (`sessionReset: true`) |
| `WORKER_PROTOCOL_ERROR` | no | yes | the worker wrote invalid output and was killed (`sessionReset: true`) |
| `RESPONSE_TOO_LARGE` | no | no | the answer exceeds `maxWorkerRecordBytes` |
| `INTERNAL_ERROR` | no | no | a bug |

`sessionReset: true` means the session's conversation history is gone. The next `ask` with that
`sessionId` starts a new conversation, and its `accepted.newSession` is `true`.

### 1.6 Sessions, ownership and reconnects

- A session is **owned by the connection that created it**. The `sessionId` is only a label
  within that connection: another connection that sends the same label gets a new, empty session.
  Guessing an ID therefore never reveals a conversation.
- Requests within a session are serialized; a second request while one is running gets
  `SESSION_BUSY`. Different sessions on one connection run concurrently, up to
  `maxConcurrentRequests`.
- **Disconnects destroy every session of the connection** and stop its workers. In-flight
  requests are abandoned and their events are not delivered. Conversations do **not** survive a
  reconnect or a server restart. There is no replay and no exactly-once guarantee: if the
  connection drops after `accepted`, the client cannot know whether Hermes finished, and
  resending runs the question again in a new conversation.
- An idle session (no request for `idleSessionTimeout`) is closed with
  `session_ended{reason:"idle_timeout"}`. A worker that dies while idle produces
  `session_ended{reason:"worker_exited"}`.

### 1.7 Timeouts

When `requestTimeout` elapses, the server asks the worker to cancel, and the worker calls Hermes's
`AIAgent.interrupt()`:

- If Hermes stops within `cancelGrace`, the client gets `HERMES_TIMEOUT` with `sessionReset`
  absent. The cancelled turn is dropped and earlier history is kept.
- Otherwise the worker's whole process group is killed, and the client gets `HERMES_TIMEOUT` with
  `sessionReset: true`.

Either way, the timed-out work does not keep running.

### 1.8 Extension points (not implemented)

- **Streaming:** a future `protocolVersion: 2`, or an opt-in `ask.stream: true` (rejected today as
  an unknown field), would add `answer_delta` events between `accepted` and `answer`. `answer`
  stays the complete final text, so v1 clients keep working. The worker already runs the agent in
  a thread, so Hermes's `stream_callback` can emit records without changing the lifecycle.
- **Document context:** `context` is already parsed and validated. Supporting it means delivering
  it to Hermes as part of the user turn (never by editing the system prompt mid-conversation, which
  would break Hermes's prompt cache) and then accepting it instead of returning
  `UNSUPPORTED_CONTEXT`.

## 2. Worker protocol (internal)

The server launches the worker with an explicit argument vector (no shell), for example
`run-in-hermes-env python3 hermes_worker.py --max-iterations 4 --run-budget-seconds 170
--max-record-bytes 4194304 --workdir ... [--hermes-home ...] [--memory]`. User text only ever
travels inside JSON on stdin.

- One JSON object per line, UTF-8, at most `maxWorkerRecordBytes` per line in each direction.
- The worker's **stdout carries protocol records only.** Before importing Hermes, the worker
  duplicates fd 1 for its private protocol stream and points fd 1 at stderr. That way a Python
  `print`, a C-level write or a child process cannot corrupt the stream. This is necessary: Hermes
  prints some interrupt notices to stdout even with `quiet_mode=True`. Diagnostics go to stderr.

| direction | record |
|---|---|
| worker → server | `{"type":"ready","workerProtocolVersion":1,"pid":N,"hermesVersion":"...","tools":[],"memory":false}` |
| worker → server | `{"type":"fatal","code":"HERMES_NOT_CONFIGURED"\|"HERMES_INIT_FAILED","message":"..."}`, after which the worker exits |
| server → worker | `{"type":"ask","requestId":"...","message":"..."}` |
| server → worker | `{"type":"cancel","requestId":"..."}` |
| server → worker | `{"type":"shutdown"}`, or EOF on stdin (the worker interrupts any turn, calls `AIAgent.close()` and exits) |
| worker → server | `{"type":"result","requestId":"...","ok":true,"text":"...","messageCount":N,"historyBytes":N,"apiCalls":N,"elapsedMs":N}` |
| worker → server | `{"type":"result","requestId":"...","ok":false,"code":"HERMES_ERROR"\|"CANCELLED"\|"RESPONSE_TOO_LARGE"\|...,"message":"..."}` |

The server treats any of the following as `WORKER_PROTOCOL_ERROR` and kills the worker: invalid
JSON, an unexpected record type, a result for the wrong request, an oversized line, or a partial
line at EOF. A process exit while a request is outstanding is `WORKER_CRASHED`.
