# Hermes WebSocket bridge (VM side)

A small bridge that lets a future Windows application, a PDF reader with a conversation pane, talk
to Hermes Agent running on a cloud Debian VM:

```
Windows                         │ SSH tunnel │  Debian VM (everything binds to 127.0.0.1)
bridge-client.exe / app ──ws──► localhost:8765 ═══► 127.0.0.1:8765  hermes-bridge (Go)
                                                         │ stdin/stdout NDJSON, one worker per session
                                                         ▼
                                     scripts/run-in-hermes-env python3 worker/hermes_worker.py
                                                         │ in-process
                                                         ▼
                                     run_agent.AIAgent  →  your configured provider (Hermes resolves auth)
```

- `server/`: the Go module. `cmd/hermes-bridge` is the server, `cmd/bridge-client` is the CLI test
  client (it also builds as a Windows `.exe`), and `internal/` holds the protocol, config, worker
  and bridge code.
- `worker/hermes_worker.py`: the Python worker (stdlib only, plus the Hermes checkout).
- `PROTOCOL.md`: the versioned client protocol and the internal worker protocol.
- `bridge.example.json`: an example configuration with every default and no secrets.
- `testing/`: a stub OpenAI-compatible model and a full-stack script (`run_stub_e2e.sh`).
- Python worker tests live at `tests/apps/hermes_bridge/` (repository convention).

## Status

| check | result |
|---|---|
| Go unit and transport tests (mocked worker), run with `-race` | pass |
| Python worker tests (stdout guard, not-configured and model routing use real Hermes imports) | pass |
| Full stack against the **real Hermes runtime** with a **stub model** in a throwaway `HERMES_HOME` | pass (19/19 selftest checks, plus timeout, disconnect, origin, auth, log-leak and shutdown checks) |
| Full stack against the **real `~/.hermes` configuration** with a **live model** (custom provider: Mistral, `mistral-medium-3-5`) | **pass**: 19/19 selftest checks with real answers, including the in-session follow-up recall, a streamed follow-up and cross-session isolation; a separate two-turn run over subprotocol auth recalled the earlier turn; a 935-character answer arrived as 22 `answer_delta` events over about 1.3 s before the final `answer` |

The stub-model run proves the bridge, the worker and Hermes's own turn loop and history handling;
the live run proves the same path against a real provider.

## Environment found on the VM (inspected, not modified)

| item | value |
|---|---|
| OS | Ubuntu 24.04.4 LTS (Debian trixie/sid base). The commands are the same on Debian 12/13. |
| Go | 1.22.2. The module needs Go ≥ 1.22; Debian 12's `golang` package (1.19) is too old, so use bookworm-backports or the go.dev tarball. |
| Python | 3.14.7, the PM-managed interpreter `~/.hermes/tools/python-3.14.7+…/bin/python3` |
| Hermes install | git checkout at `/workspace`, version `git.7157422 (2026.9.24)`, commit `7157422022ff`, install method `git` |
| Hermes Python environment | PM-managed (`pm/`, `uv.lock`). `hermes` runs `scripts/run-in-hermes-env python3 /workspace/hermes`, which sets `PYTHONPATH=/workspace:<PM venv site-packages>`. Do not `pip install` into it. |
| Programmatic interface | `run_agent.AIAgent(...)`, then `.run_conversation(user_message, conversation_history=…, task_id=…) -> dict` (`final_response`, `messages`, `interrupted`, `failed`, …), plus `.interrupt()` and `.close()`. Verified against the installed code, not only the docs. |
| Provider, model and auth loading | The worker resolves the route the way the gateway and `hermes -z` do: `model.default` from `config.yaml`, and `hermes_cli.runtime_provider.resolve_runtime_with_fallback` for provider, endpoint and credentials (API keys in `.env`, subscription/OAuth credentials and pools in `auth.json`, the fallback chain), all from `HERMES_HOME` (default `~/.hermes`). Hermes reads the credentials; the bridge never logs or forwards them. A bare `AIAgent()` is not enough: it sends no model id, which real endpoints reject (`HTTP 400: Missing model parameter`). |
| API key or OAuth on this VM | Initially **neither**; a custom Mistral provider (key in `.env`) was later configured with `hermes model`. Without one, the worker reports `HERMES_NOT_CONFIGURED`. |

So programmatic invocation reuses whatever authentication the CLI uses, with no incompatibility.
If nothing is configured yet, the smallest supported fix is to configure Hermes itself on the VM,
in an SSH session as the user who will run the bridge:

```bash
hermes model          # pick a provider: OAuth login (e.g. Nous Portal) or paste an API key
hermes status         # should now show Model / Provider
```

No direct model API call bypasses Hermes anywhere in this project.

## Hermes behaviour inside the worker

| behaviour | setting | why |
|---|---|---|
| Provider, model, auth, fallback chain, credential pools | **retained** (resolved by Hermes) | the requirement: reuse the existing configuration |
| `SOUL.md` persona | **retained** (`load_soul_identity=True`) | the user's chosen identity applies to this app too |
| Tools | **none** (`enabled_toolsets=[]`; verified to send zero tool schemas) | Q&A milestone: no terminal, filesystem, browser, web or delegation by default |
| Skills | **inactive** (follows from no tools) | skills load through tools; the system prompt advertises none without them |
| Memory (`MEMORY.md`/`USER.md` and memory-provider plugins) | **off by default** (`skip_memory=True`); `worker.memory: true` enables it | providers do background writes and LLM calls outside the request bounds, and app conversations stay separate from the CLI's memory unless you opt in |
| Background review (post-turn memory/skill curation fork) | **off** (`skip_background_review=True`) | it is an extra LLM call that outlives the request |
| Project context files (`AGENTS.md` and similar from the cwd) | **off** (`skip_context_files=True`); the cwd is an empty `state/workdir` | the Hermes checkout's developer docs must not leak into answers |
| Platform hint | `platform="api_server"` | Hermes's existing hint for programmatic callers (plain text, rendering unknown) |
| Iterations | `maxIterations` (default 4) and `run_budget_seconds` = timeout − grace | bounded; Q&A needs one model call |
| CLI output | `quiet_mode=True`, plus an fd-level stdout guard | Hermes still prints interrupt notices to stdout; the guard reroutes them to stderr |
| Session persistence | not written to Hermes's `state.db` (no `session_db`) | durable storage is deferred; history lives only in the worker's memory |
| Hermes's own logs | unchanged (`$HERMES_HOME/logs/agent.log`, `errors.log`) | existing Hermes behaviour; they may contain content, as for any Hermes use |

## Build and run on the VM

```bash
cd /workspace/apps/hermes-bridge
(cd server && go build -o bin/ ./cmd/hermes-bridge ./cmd/bridge-client)
cp bridge.example.json bridge.json        # adjust /workspace if Hermes lives elsewhere
mkdir -p state
server/bin/hermes-bridge -config bridge.json 2>>state/server.log &   # or run it in tmux/systemd
curl -s http://127.0.0.1:8765/healthz      # "hermes":{"status":"ok"} once a provider is configured
```

On first start the server writes a random token to `state/token` (mode 0600). It refuses a token
file that group or others can read. Relative paths in the config resolve against the config
file's directory.

`/healthz` is unauthenticated and reveals no secrets. It reports liveness (`200`, or `503` while
shutting down), counts, limits, and a **startup preflight** of Hermes: one worker is started and
stopped without a model call, and the result is reported as `ok`, `HERMES_NOT_CONFIGURED` or
`WORKER_START_FAILED`, with the message and the Hermes version.

### Run as a systemd user service (optional)

```ini
# ~/.config/systemd/user/hermes-bridge.service
[Service]
WorkingDirectory=/workspace/apps/hermes-bridge
ExecStart=/workspace/apps/hermes-bridge/server/bin/hermes-bridge -config bridge.json
KillSignal=SIGTERM
TimeoutStopSec=30
[Install]
WantedBy=default.target
```

Then run `systemctl --user daemon-reload && systemctl --user enable --now hermes-bridge`, and read
the logs with `journalctl --user -u hermes-bridge`.

## Verify

```bash
cd /workspace/apps/hermes-bridge

# 1. Go tests: transport, limits and failure paths against a MOCK worker (no Hermes, no model)
(cd server && go test -race ./...)

# 2. Python worker tests (repository runner; builds a disposable test venv once)
cd /workspace && ~/.hermes/tools/python-*/bin/python3 -m pm.build_env --source . --out .venv --group dev --group test
HERMES_PYTHON="$PWD/.venv/bin/python" scripts/run_tests.sh tests/apps/hermes_bridge/
cd apps/hermes-bridge

# 3. Full stack with the real worker and real Hermes, using a STUB model in a temp HERMES_HOME
testing/run_stub_e2e.sh

# 4. LIVE round trip against your real Hermes config (needs `hermes model` done first)
server/bin/bridge-client -token-file state/token -selftest
server/bin/bridge-client -token-file state/token -ask "Remember this test word: mango." -ask "What test word did I give you?"
server/bin/bridge-client -token-file state/token -interactive
```

`-selftest` checks health, a real question, the mango follow-up, a streamed follow-up
(`stream: true`, at least one `answer_delta` before the answer), that a plain ask gets no deltas,
isolation across sessions and across connections, rejection of empty, malformed, unsupported,
oversized and context-bearing messages, the busy policy, duplicate IDs and `end_session`. It
exits non-zero on any failure.

`-ask` and `-interactive` stream by default: the answer is printed as its `answer_delta` events
arrive, and reprinted in full only if the authoritative `answer` differs from the streamed
draft. `-stream=false` waits for the complete answer instead. `-raw` shows every event.
`-raw` prints every event; `-browser-auth` sends the token the way a browser must; `-origin` sets
an Origin header.

## Access from Windows (PowerShell)

Nothing listens publicly. Forward a local port over SSH. Mosh is irrelevant here: it only carries
interactive terminals and cannot forward ports.

```powershell
# One-time: fetch the client and the token (the token is a credential; keep it private)
scp user@vm.example.com:/workspace/apps/hermes-bridge/state/token "$env:USERPROFILE\.hermes-bridge-token"
icacls "$env:USERPROFILE\.hermes-bridge-token" /inheritance:r /grant:r "$($env:USERNAME):(R)"
# Build bridge-client.exe on the VM:  cd server && GOOS=windows GOARCH=amd64 go build -o bin/bridge-client.exe ./cmd/bridge-client
scp user@vm.example.com:/workspace/apps/hermes-bridge/server/bin/bridge-client.exe .

# Tunnel (keep this window open; Ctrl-C closes it)
ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -L 8765:127.0.0.1:8765 user@vm.example.com

# In a second window
.\bridge-client.exe -token-file "$env:USERPROFILE\.hermes-bridge-token" -selftest
.\bridge-client.exe -token-file "$env:USERPROFILE\.hermes-bridge-token" -interactive
```

To run the tunnel in the background instead:
`Start-Process ssh -ArgumentList '-N','-o','ExitOnForwardFailure=yes','-L','8765:127.0.0.1:8765','user@vm.example.com' -WindowStyle Hidden`.

The Windows client holds only the bridge token, never provider credentials. Those stay in
`~/.hermes` on the VM.

## Stop and clean up

- **Stop:** send `SIGTERM` or `SIGINT` (Ctrl-C, `kill <pid>`, `systemctl --user stop`). The server
  stops accepting connections and requests. In-flight requests are cancelled through
  `AIAgent.interrupt()` and their clients receive `SHUTTING_DOWN` followed by `done`. Connections
  close with code 1001, and every worker gets `shutdown`, then `SIGTERM`, then `SIGKILL` to its
  process group. All of this is bounded by `shutdownTimeout`. Workers run in their own process
  group with `PDEATHSIG=SIGKILL`, so even a `kill -9` of the server takes them down.
- **Check:** `ps -eo pid,args | grep '[h]ermes_worker.py'` prints nothing.
- **Clean up:** `rm -rf state/` removes the token and the empty workdir; a new token is created on
  the next start. Hermes's own state in `~/.hermes` is untouched.

## Do conversations survive?

No. History lives only in the session's worker process:

- **Reconnect:** the old connection's sessions are destroyed when it drops, and the same
  `sessionId` on a new connection starts fresh (`accepted.newSession: true`).
- **Server restart:** all history is lost.
- **Timeout:** the history is kept if Hermes honoured the interrupt; if the worker had to be
  killed, the history is lost (`sessionReset: true`).
- **Worker crash or protocol error:** the history is lost (`sessionReset: true`).

There is no automatic replay and no exactly-once execution.

## Configuration (defaults)

| key | default | bound |
|---|---|---|
| `listenAddr` | `127.0.0.1:8765` | must be loopback (config load fails otherwise) |
| `allowedOrigins` | `[]` | exact origins only, for a future browser/webview client |
| `maxMessageBytes` | 64 KiB | WebSocket frame. Up to 4× (min 1 MiB) gets a structured error; beyond that the connection closes with 1009 |
| `maxQuestionBytes` | 32 KiB | question text |
| `maxConnections` | 8 | concurrent WebSocket connections |
| `maxSessions` | 4 | live sessions, which equals worker processes (~160 MB RSS each, measured) |
| `maxConcurrentRequests` | 2 | requests running at once, server-wide |
| `maxTurnsPerSession` / `maxHistoryBytes` | 50 / 512 KiB | per-session history |
| `maxWorkerRecordBytes` | 4 MiB | one worker record, which bounds an answer |
| `requestTimeout` / `cancelGrace` | 180 s / 10 s | per request, then the kill deadline |
| `idleSessionTimeout` | 15 min | idle sessions are closed |
| `workerStartTimeout` / `workerStopGrace` / `shutdownTimeout` | 90 s / 5 s / 20 s | worker lifecycle |
| `forwardWorkerStderr` | `false` | when true, worker stderr is logged (it may quote provider errors or content) |
| `worker.memory` | `false` | Hermes memory and memory providers |

## Logs

Structured JSON on stderr. The logs record connection and request lifecycle, request and session
IDs, timings, byte counts, error codes, worker PIDs and handshake rejections with a reason. They
never contain question text, answers, document content, tokens or environment variables. Worker
stderr is drained and counted but not logged unless `forwardWorkerStderr` is set. The stub
end-to-end run greps the server log for the questions, answers and token as a regression check.

## Design decisions

- **WebSocket library:** [`github.com/coder/websocket`](https://github.com/coder/websocket)
  v1.8.13 (formerly nhooyr.io/websocket, maintained by Coder). It is context-aware, has read
  limits and close codes built in, and has no transitive dependencies; v1.8.13 is the newest
  release that builds with the VM's Go 1.22. Its built-in origin check allows `Origin == Host`,
  which DNS rebinding can satisfy, so the server enforces its own exact allowlist and a loopback
  `Host` check first.
- **One writer goroutine per connection** carries every outbound event; request goroutines only
  enqueue. The read loop only parses and admits, so Hermes work never runs on it.
- **One persistent worker per session.** The trade-off: about 2–3 s of startup on a session's
  first question and about 160 MB per live session, hence the strict `maxSessions`. In exchange
  there is no shared mutable agent state, Hermes keeps a byte-stable system prompt per
  conversation (its prompt cache stays warm), a timeout or crash affects only one session, and
  history stays in Hermes's own message format, including any tool messages.
- **Busy policy:** reject with `SESSION_BUSY` / `SERVER_BUSY` rather than queueing, so clients
  see explicit, retryable backpressure.
- **Auth:** a static shared token (header, or subprotocol for browsers) plus the origin allowlist
  and loopback binding. It is adequate for one user behind an SSH tunnel; it is not multi-user
  auth.

## Known limitations and next steps

- The live run used one provider (Mistral through Hermes's custom-provider path). Other providers
  go through the same Hermes resolver but were not exercised here.
- The worker resolves its model once at start, so a `hermes model` change applies to sessions
  started afterwards; running sessions keep their model (by design, for prompt caching).
- Conversations are connection-scoped and in-memory only. Next steps: resumable session tokens
  issued by the server, and persistence through Hermes's `SessionDB`.
- Streamed deltas are a draft, not a guarantee: after a provider retry inside Hermes they can
  differ from the final `answer`, which clients must treat as authoritative (`PROTOCOL.md` §1.4).
  Reasoning ("thinking") text is not streamed.
- No document context (`UNSUPPORTED_CONTEXT`), and no tools.
- One static token, no rotation endpoint (to rotate: delete `state/token` and restart), and no
  TLS (the SSH tunnel provides transport security).
- The Go server is Linux-first (process groups, `PDEATHSIG`); the client is cross-platform.
- `run-in-hermes-env` re-syncs the Hermes environment when its inputs change, as `hermes` itself
  does. A worker start right after `hermes update` can therefore be slow; `workerStartTimeout`
  covers it.
