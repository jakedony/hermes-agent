# Local demo

`collab/python/scripts/demo_local.py` starts a hub, two bridges, a loopback HTTP server on `127.0.0.1:18080`, and `tests.fakes.fake_llm_provider.FakeLLMServer`. The PC allowlist is `127.0.0.2:18080`. The VM worker calls `listening_sockets`. The coordinator's scripted stub calls `tcp_probe` and then `delegate_investigation`, and the final text includes both tool results.

```bash
PYTHONPATH=/workspace/collab/python:/workspace python3 collab/python/scripts/demo_local.py
```

The script prints this label and the coordinator synthesis:

```text
LABEL: local stub model, single host. This is not a second machine and not a hosted model.
```

Both bridges dial the hub on one host. There is no SSH tunnel and no second kernel. `127.0.0.2` is only a different loopback address so the PC probe is not the VM's `127.0.0.1:18080` listener. A hosted model is not configured and is not contacted.

Fake-agent tests (`worker_mode: fake`) are a separate path. They do not import Hermes. They still call the real diagnostic functions. Do not describe those runs as model collaboration.
