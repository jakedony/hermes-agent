# Deployment

Two machines. The hub listens on the VM at `127.0.0.1:8765`. The VM bridge dials `ws://127.0.0.1:8765/ws`. The PC bridge dials `ws://127.0.0.1:18765/ws`. Port 18765 is an SSH local forward of the hub and nothing else. PC probes use the VM's real address (the sample uses documentation address `203.0.113.10` and port `18080`). They do not use `127.0.0.1:18765` or `127.0.0.1:8765`.

Sample files:

- `configs/hub.json` on the VM
- `configs/bridge-vm.json` on the VM
- `configs/bridge-pc.json` on the PC
- `systemd/collab-hub.service` on the VM
- `systemd/collab-bridge.service` on both
- `systemd/collab-tunnel.service` on the PC

Token hashes in the samples are placeholders (`1111…`, `2222…`, `3333…`). Replace them with the second line of `collab/python/scripts/mint_token.py`. Replace token file paths with mode `0600` files that contain the first line. Do not commit the secrets.

`model` and `model_base_url` in the bridge samples are placeholders. Point them at the endpoint that worker should call. `model.context_length` in the generated worker `HERMES_HOME` is 128000 and must match that model and base URL. See `INTEGRATION.md`.

## VM

```bash
install -d -m 0700 "$HOME/.config/collab" /var/lib/collab/tokens
# edit hub.json and bridge-vm.json into ~/.config/collab/
install -m 0755 collab-hub /usr/local/bin/collab-hub
mkdir -p "$HOME/.config/systemd/user"
cp collab/systemd/collab-hub.service collab/systemd/collab-bridge.service "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user enable --now collab-hub.service collab-bridge.service
sudo loginctl enable-linger "$USER"
```

Linger lets the user units keep running without an interactive login. The hub and the bridge are unprivileged (`NoNewPrivileges=yes`). The sample human principal is `human-jacob`.

Diagnostics do not escalate. Each one is a fixed executable and a fixed argv: `ss` for listeners, `/usr/bin/systemctl show` for an allowlisted unit, `/usr/bin/journalctl` for a bounded excerpt of that unit, `ip route show` for routes, and `/usr/bin/getent ahosts` for one allowlisted DNS name. `tcp_probe` dials only an allowlisted IP and port and refuses the hub forward (`127.0.0.1:18765` on the PC, `127.0.0.1:8765` on the VM). A journal the unprivileged user cannot read is an observation, not a sudo. Missing binaries are recorded the same way. Timeout is 10s. Output is capped. The child is resource-limited. `services` and `dns_names` in the bridge JSON are the allowlists; they are not secrets.

Build the hub from this checkout (CGO sqlite):

```bash
cd collab && go build -o collab-hub ./hub/
```

## PC

The tunnel unit is exactly a local forward:

```text
ssh -N -T -L 127.0.0.1:18765:127.0.0.1:8765 \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
  -o BatchMode=yes \
  VM_SSH_ALIAS
```

`VM_SSH_ALIAS` is a placeholder. Define it in the SSH config you already manage. This prototype does not edit SSH config, does not set `StrictHostKeyChecking=no`, and does not enable agent forwarding (`-A` is absent). `BatchMode=yes` means an unattended key must already authenticate that alias (an `IdentityFile` on the alias, not a prompt and not `ssh-agent` forwarding). Host key checking stays at SSH's default, so the VM host key has to be in `known_hosts` before the unit starts.

The forward is only hub traffic: TCP to the VM's loopback port 8765. It is not a shell, not a VPN, and not the path PC probes use to reach a service on the VM.

```bash
mkdir -p "$HOME/.config/systemd/user" "$HOME/.config/collab"
cp collab/systemd/collab-tunnel.service collab/systemd/collab-bridge.service "$HOME/.config/systemd/user/"
# edit bridge-pc.json: real VM address in allowlist, real token files, real model URL
systemctl --user daemon-reload
systemctl --user enable --now collab-tunnel.service collab-bridge.service
sudo loginctl enable-linger "$USER"
```

The PC bridge also listens on a Unix socket (`human_socket`) created mode `0600`. Clients present `human_socket_token_file`. Hub calls from that socket use `human_hub_token_file`, not the agent token.

## Worker process

Each attempt is a new process group (`start_new_session`). Stop is `killpg`. The protocol is a socketpair on `COLLAB_PROTOCOL_FD`. stdout is a log file, not the protocol. The child environment is only `PATH`, `HOME`, `LANG`, `HERMES_HOME`, `PYTHONPATH`, `COLLAB_PROTOCOL_FD`, and `PYTHONUNBUFFERED`. It is not a copy of the bridge environment. `HERMES_HOME` is the worker home from the bridge config, not a hardcoded `~/.hermes`.

The bridge renews the 30s lease every 10s. It stops the worker on a monotonic clock `lease_margin_sec` (default 5) before lease expiry. A live websocket does not extend the lease.

## SQLite

Hub and bridge journals use WAL. Do not copy a live database file plus its `-wal` as a backup. Use SQLite's backup API (`Connection.backup` in the bridge journal) or `VACUUM INTO 'fresh.db'`, or the `.backup` command. Idempotency rows default to 7 days.

## Limits

Root deadline 180s, child 120s, diagnostic tool 10s, Hermes `max_iterations` 12 (not a token cap), child budget 3, max attempts 2, lease 30s.
