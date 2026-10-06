# Two-machine Hermes collaboration (prototype)

A VM runs the hub. A PC and the VM each run a bridge. The PC is the coordinator:
it can probe an allowlisted address and can delegate one child task to the VM.
The VM worker only has local diagnostics. Hermes, when used, runs inside a
worker subprocess with a fixed tool surface. The hub is the authority for
tasks, leases, and idempotency. The human principal in the sample hub config
is `human-jacob`. The agent principals are `hermes-pc` and `hermes-vm`.

This tree is a prototype. It does not modify Hermes core.

## Layout

| Path | Role |
| --- | --- |
| `hub/` | Go hub. Do not treat client retries as a second execution; the hub dedups request ids. |
| `python/collab/` | Bridge, journal, diagnostics, worker. |
| `python/scripts/mint_token.py` | Print a bearer token and its SHA-256. |
| `python/scripts/demo_local.py` | Single-host stub-model demo. Not a second machine. |
| `configs/` | Sample JSON. Token hashes and file paths are placeholders. |
| `systemd/` | User units for the hub, the bridge, and an SSH local forward. |
| `PROTOCOL.md` | Wire contract the Python client implements. |
| `DEPLOYMENT.md` | What to run on the PC and on the VM. |
| `DEMO.md` | How to read the local demo. |
| `hub/ui/index.html` | Loopback lab desk: roster, hub thread, and a machine workspace. |
| `INTEGRATION.md` | How the worker calls Hermes, and what the prototype does not promise. |

Run Python as:

```bash
PYTHONPATH=/workspace/collab/python:/workspace python3 -m collab bridge --config configs/bridge-vm.json
```

Adjust `PYTHONPATH` so it contains this checkout's `collab/python` directory and the Hermes checkout. Config files are JSON. PyYAML is not required.

## Commands a person runs

Mint tokens (once per principal). The first line is the secret; the second is what `hub.json` stores:

```bash
PYTHONPATH=/workspace/collab/python python3 collab/python/scripts/mint_token.py
```

Put each secret in a mode `0600` file. Put each digest in `configs/hub.json`. Do not put secrets in the journal, the unit files, or git.

Human control (PC bridge only) uses the socket token, and the bridge then calls the hub with the human token:

```bash
PYTHONPATH=/workspace/collab/python python3 -m collab human --config ~/.config/collab/bridge.json \
  investigate --objective "why is the vm service unreachable" \
  --context '{"host":"203.0.113.10","port":18080}'
PYTHONPATH=/workspace/collab/python python3 -m collab human --config ~/.config/collab/bridge.json list
PYTHONPATH=/workspace/collab/python python3 -m collab human --config ~/.config/collab/bridge.json get --task-id tsk_...
PYTHONPATH=/workspace/collab/python python3 -m collab human --config ~/.config/collab/bridge.json history --task-id tsk_...
PYTHONPATH=/workspace/collab/python python3 -m collab human --config ~/.config/collab/bridge.json cancel --task-id tsk_... --reason stop
```

`203.0.113.10` stands for the VM's real address. It is not the SSH forward (`127.0.0.1:18765`), which only carries hub traffic.

## Desk

With the hub listening on `127.0.0.1:8765`, open `http://127.0.0.1:8765/`. `GET /` is the unauthenticated page. Data calls send `Authorization: Bearer`. Paste the `human-jacob` token into Settings. The desk stores it in `sessionStorage` for this tab only. It is not written into the URL, the HTML, or a hub log line.

The desk is three panes on a dark shell:

- **Left rail.** Room name Lab. Jacob, Hermes-PC, and Hermes-VM are people: a mark, a short role, and a live status from hub tasks (running, queued, reported, or idle). Selecting one opens that agent's conversation. Unselected agents stay in the rail with their status. Selecting Hermes-PC or Hermes-VM also switches the workspace machine. Settings at the bottom hold the token.
- **Center thread.** Each agent has its own transcript from `task.list`, `task.get`, and `task.history`. Hermes-PC shows the human ask, the delegation, and the synthesis. Hermes-VM shows the delegated objective and the VM findings. Jacob shows what he sent and the final answer addressed to him. System lines record started, requeued, abandoned, cancelled, stale, and timed out. The composer creates a root task assigned to `hermes-pc` with profile `pc-net` and timeout 180. On Hermes-VM the composer says the worker cannot open a root and the coordinator has to delegate; Send still creates that human root for Hermes-PC. Cancel applies to the selected root while it is queued or running.
- **Right workspace.** Titled `WORKSPACE ON THE PC` or `WORKSPACE ON THE VM`. Browser chrome has a tab per evidence operation (listening sockets, tcp probe, route, service, logs, dns) and another tab when a probed or listening address can be named. The page is that operation's attributed excerpt, with source, time, and machine. The address bar is a desk URL such as `desk://vm/listening_sockets`. Give to agent copies the open tab into the next `task.create` context and arms directing. A picture-in-picture of the other machine shows its latest one-line finding and Take control, which switches both the thread and the workspace. The footer says "You are watching", or "You are directing" when a tab has been given to the agent or a message is drafted, and "Diagnostic workspace, not a remote desktop."

The desk does not run a model, does not start a bridge, and does not open a remote desktop. A queued task stays queued until a bridge claims it. There is still no second machine, no SSH between two hosts, no hosted model, and no live desktop of a Mac or a VPS.

## Diagnostics

Both roles can call `listening_sockets`, `tcp_probe`, `service_status`, `service_logs`, `route_show`, and `dns_lookup`. The coordinator also has `delegate_investigation`. The worker does not. Each operation uses a fixed executable and a fixed argument vector, an allowlist, a 10s timeout, an output cap, and resource limits. A model string never becomes a shell word. `tcp_probe` refuses the hub websocket address even if someone puts that forward on the allowlist. Service and DNS names must match a token regex and the machine allowlist. `service_logs` records a permission denial as an observation and does not use sudo.

`tools.tool_search.enabled` is `off` in the worker home. After `AIAgent` init, `valid_tool_names` must equal that role's approved set or the worker refuses the attempt.

## Tests

`go test ./hub/ -count=1` covers the hub, including the test-only clock. `collab/python/tests/` covers diagnostics, the Hermes stub model, the fake-agent hub flow, and bridge faults (worker crash, VM disconnect until lease expiry, cancel racing complete). Fake-agent runs and the stub model are not a second machine and not a hosted model.

## What this does not promise

There is no exactly-once shell. Cancellation does not roll back a probe that already ran. A crashed coordinator does not resume in-flight reasoning. Fencing does not stop a repeated local observation during a partition. `max_iterations` is not a token cap.
