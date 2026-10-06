# Local demo

`collab/python/scripts/demo_local.py` starts a hub, two bridges, a loopback HTTP server on `127.0.0.1:18080`, and `tests.fakes.fake_llm_provider.FakeLLMServer`. The human principal is `human-jacob`. The PC allowlist is `127.0.0.2:18080`. The coordinator's scripted stub calls `tcp_probe` and then `delegate_investigation`. The VM worker calls `listening_sockets`, `route_show`, `service_status`, `service_logs`, and `dns_lookup` against the sample allowlists (`collab-http.service`, `vm.example.test`). The final text includes the tool results. Those extra tools are real local observations on this host. The model is still the stub.

```bash
PYTHONPATH=/workspace/collab/python:/workspace python3 collab/python/scripts/demo_local.py
```

The script prints this label and the coordinator synthesis:

```text
LABEL: local stub model, single host. This is not a second machine and not a hosted model.
```

Both bridges dial the hub on one host. There is no SSH tunnel and no second kernel. `127.0.0.2` is only a different loopback address so the PC probe is not the VM's `127.0.0.1:18080` listener. A hosted model is not configured and is not contacted.

Fake-agent tests (`worker_mode: fake`) are a separate path. They do not import Hermes. They still call the real diagnostic functions. `test_bridge_faults.py` drives the hub binary and both bridges: a killed worker is not rerun (a retry is a new attempt id, and an unsent fail resubmits the original request id), a VM socket drop does not requeue until the test clock passes the 30s lease, and cancel versus complete yields one outcome. Do not describe those runs, or this demo, as hosted-model collaboration or as a second machine.
