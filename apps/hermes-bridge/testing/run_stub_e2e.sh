#!/usr/bin/env bash
# Full-stack check with the REAL Go server, REAL Python worker and REAL Hermes AIAgent, but a
# deterministic stub model endpoint in a throwaway HERMES_HOME. It proves the plumbing (history,
# isolation, errors, timeouts, shutdown) without provider credentials. It does NOT prove a live
# model answer: use `bridge-client -selftest` against a server using your real Hermes config.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
app="$(dirname "$here")"
repo="$(cd "$app/../.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/hermes-bridge-e2e.XXXXXX")"
stub_port="${STUB_PORT:-18181}"
bridge_port="${BRIDGE_PORT:-18765}"
pids=()
cleanup() {
    for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT
fail() { echo "E2E FAIL: $*" >&2; exit 1; }

echo "work dir: $work"
mkdir -p "$work/hermes-home"
cat > "$work/hermes-home/config.yaml" <<EOF
model:
  default: bridge-stub-model
  provider: custom
  base_url: http://127.0.0.1:${stub_port}/v1
  api_key: stub-not-a-secret
EOF

python3 "$here/stub_model_server.py" --port "$stub_port" --record "$work/model-requests.jsonl" >"$work/stub.log" 2>&1 &
pids+=($!)

(cd "$app/server" && GOTOOLCHAIN=local go build -o "$work/bin/" ./cmd/hermes-bridge ./cmd/bridge-client)

cat > "$work/bridge.json" <<EOF
{
  "listenAddr": "127.0.0.1:${bridge_port}",
  "tokenFile": "token",
  "allowedOrigins": ["http://localhost:5173"],
  "requestTimeout": "8s",
  "cancelGrace": "3s",
  "worker": {
    "command": ["${repo}/scripts/run-in-hermes-env", "python3", "${app}/worker/hermes_worker.py"],
    "workdir": "workdir",
    "hermesHome": "hermes-home",
    "maxIterations": 4
  }
}
EOF

"$work/bin/hermes-bridge" -config "$work/bridge.json" 2>"$work/server.log" &
server_pid=$!
pids+=($server_pid)

for _ in $(seq 1 120); do
    status="$(curl -fsS "http://127.0.0.1:${bridge_port}/healthz" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["hermes"]["status"])' 2>/dev/null || true)"
    [ "$status" = ok ] && break
    case "$status" in HERMES_*|WORKER_*) fail "preflight reported $status";; esac
    sleep 1
done
[ "$status" = ok ] || fail "server never became healthy"
client=("$work/bin/bridge-client" -url "ws://127.0.0.1:${bridge_port}/ws" -token-file "$work/token")

echo "== selftest (header auth)"
"${client[@]}" -selftest

echo "== browser-style auth (subprotocol token) with an allowlisted Origin"
"${client[@]}" -browser-auth -origin http://localhost:5173 -ask "Say hello." | tee "$work/browser.out"

echo "== origin and auth rejections"
"${client[@]}" -origin http://evil.example -ask "hi" 2>&1 | tee "$work/origin.out" || true
grep -q "HTTP 403" "$work/origin.out" || fail "foreign origin not rejected"
echo "0000000000000000000000000000000000000000" > "$work/wrong-token"; chmod 600 "$work/wrong-token"
"$work/bin/bridge-client" -url "ws://127.0.0.1:${bridge_port}/ws" -token-file "$work/wrong-token" -ask hi 2>&1 | tee "$work/auth.out" || true
grep -q "HTTP 401" "$work/auth.out" || fail "wrong token not rejected"

echo "== timeout: Hermes is interrupted, the session survives"
"${client[@]}" -session timeout-1 -keep-going -ask "Remember this test word: kiwi." -ask "STUB_SLEEP 30" \
    -ask "What test word did I give you?" 2>&1 | tee "$work/timeout.out" || true
grep -q "error HERMES_TIMEOUT retryable=true sessionReset=false" "$work/timeout.out" || fail "no recoverable HERMES_TIMEOUT"
grep -q "The test word you gave me was kiwi." "$work/timeout.out" || fail "history lost after a cancelled turn"

echo "== abrupt client disconnect mid-request stops that session's worker"
ready_before="$(grep -c '"msg":"worker_ready"' "$work/server.log")"
"${client[@]}" -session disconnect-1 -ask "STUB_SLEEP 30" >/dev/null 2>&1 &
client_pid=$!
for _ in $(seq 1 60); do
    [ "$(grep -c '"msg":"worker_ready"' "$work/server.log")" -gt "$ready_before" ] && break
    sleep 0.5
done
disc_worker="$(grep '"msg":"worker_ready"' "$work/server.log" | tail -1 | grep -o '"workerPid":[0-9]*' | cut -d: -f2)"
kill -0 "$disc_worker" || fail "disconnect worker never started"
kill -KILL "$client_pid"; wait "$client_pid" 2>/dev/null || true
for _ in $(seq 1 40); do kill -0 "$disc_worker" 2>/dev/null || break; sleep 0.25; done
kill -0 "$disc_worker" 2>/dev/null && fail "worker $disc_worker survived its client's disconnect"
grep -q '"msg":"connection_closed".*"sessionsClosed":1' "$work/server.log" || fail "no connection_closed for the disconnect"
echo "worker $disc_worker stopped after its client vanished"

echo "== leak scan of normal server logs"
for needle in mango kiwi "authentication and authorisation" "Remember this" "$(cat "$work/token")" stub-not-a-secret; do
    if grep -qiF -- "$needle" "$work/server.log"; then fail "server log contains '$needle'"; fi
done
echo "no question text, answers or token in server.log ($(wc -l < "$work/server.log") lines)"

echo "== tool policy as seen by the model endpoint"
python3 - "$work/model-requests.jsonl" <<'EOF'
import json, sys
rows = [json.loads(l) for l in open(sys.argv[1])]
tools = {t for r in rows for t in r["tools"]}
print(f"{len(rows)} model requests; tool schemas offered: {sorted(tools) or 'none'}")
sys.exit(1 if tools else 0)
EOF

echo "== shutdown"
worker_pids="$(grep -o '"workerPid":[0-9]*' "$work/server.log" | cut -d: -f2 | sort -u | tr '\n' ' ')"
kill -TERM "$server_pid"
wait "$server_pid" || fail "server exited non-zero"
grep -q '"msg":"shutdown_complete"' "$work/server.log" || fail "no shutdown_complete log"
for p in $worker_pids; do
    if kill -0 "$p" 2>/dev/null; then fail "worker $p still running after shutdown"; fi
done
echo "server stopped; all workers gone: $worker_pids"
echo "E2E PASS (stub model; not a live-model result). Logs: $work"
