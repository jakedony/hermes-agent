# Two-machine Hermes collaboration (prototype)

A VM runs the hub. A PC and the VM each run a bridge. The PC is the coordinator:
it can probe an allowlisted address and can delegate one child task to the VM.
The VM worker only has local diagnostics. Hermes, when used, runs inside a
worker subprocess with a fixed tool surface. The hub is the authority for
tasks, leases, and idempotency.

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
