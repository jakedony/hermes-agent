# Hermes integration

Recorded against Hermes checkout `33c9b1d7796b34d76e6b71fc4251a7197f7ffd80` (Hermes Agent `vgit.33c9b1d`), Python 3.14.7, OpenAI SDK 2.24.0. No Hermes core file is modified. No new `HERMES_*` variable is introduced. `HERMES_HOME` is the existing home variable and is set explicitly in the worker environment.

## Call

The worker registers tools, then constructs one `AIAgent` per attempt:

```python
AIAgent(
    model=model,
    provider="custom",
    base_url=base_url,
    api_key=api_key,
    api_mode="chat_completions",
    max_iterations=12,
    enabled_toolsets=["collab_diag"],
    quiet_mode=True,
    skip_context_files=True,
    skip_memory=True,
    skip_background_review=True,
    platform="collab",
)
result = agent.run_conversation(objective)
```

`run_conversation` is the turn entry. The worker reads `final_response`, `messages`, `api_calls`, and `completed`. `tool_complete_callback(tool_call_id, name, args, result)` records evidence. `agent.interrupt(hard_cancel=True)` exists; the bridge's hard stop is `killpg` on the worker process group, which ends the process. `agent.close()` runs before the worker exits on the Hermes path.

`max_iterations` is 12. That bounds tool-calling rounds. It is not a token cap. A round can still be large, and the hub does not meter tokens.

Chat completions are streamed SSE. The local proof uses `tests.fakes.fake_llm_provider.FakeLLMServer`, which speaks SSE. A scripted continuation is `listening_sockets`, then `delegate_investigation`, then a final text that contains both tool results, in the same `run_conversation`, with `api_calls == 3` and `completed` true. That test is labeled as a local stub model.

## Tool surface

Tools are registered on `tools.registry.registry` with toolset `collab_diag` before `AIAgent` is constructed. Handlers return a JSON string.

| Role | Tools |
| --- | --- |
| coordinator (no parent) | `listening_sockets`, `tcp_probe`, `delegate_investigation` |
| worker | `listening_sockets`, `tcp_probe` |

The worker home's `config.yaml` sets `tools.tool_search.enabled` to `"off"` and `model.context_length` to `128000` (>= `MINIMUM_CONTEXT_LENGTH` 64000). The pin is kept only when `model.default` and `model.base_url` match the `model` and `base_url` passed to `AIAgent`.

`collab_diag` is not a core toolset. With tool search left on, Hermes treats it as deferrable and can replace it with `tool_search`, `tool_describe`, and `tool_call`. That would be an escape hatch out of the restricted surface. After init the worker compares `agent.valid_tool_names` to the approved set and refuses the attempt (`invalid_input`) on any difference.

`delegate_investigation` blocks inside the worker tool handler and asks the bridge on the protocol socketpair. The bridge network loop stays on its own tasks: it creates the child as `hermes-pc` (the coordinator principal), waits until `task.get` shows a terminal state, and writes the child result back into that same tool handler so `run_conversation` continues.

## What this does not promise

Fencing stops a stale attempt from completing on the hub. It does not stop a local observation that already happened. A retried attempt can run `ss` or a TCP connect again. There is no exactly-once shell.

Cancellation does not undo a completed action. A probe that already connected, or a child task that already finished, stays finished. The hub may still record `cancelled` if cancel wins the race before complete.

A crashed coordinator does not restore in-flight reasoning. The bridge will not rerun that attempt id. A later attempt, if the hub requeues, starts a new Hermes process with a new conversation. The previous model's scratch state is gone.

VM retries do not recover the PC conversation. The PC's `run_conversation` only sees what the delegate tool returned into that process. If the PC worker is gone, a VM retry updates the hub and does not resume the PC model.

Completed remote results stay on the hub. The PC journal is an outbox, not a second authority. `task.get` on the hub is the record after the acknowledgement.

Back up SQLite with `VACUUM INTO` or the `.backup` / `Connection.backup` API. Copying a live WAL file is not a backup. Idempotency retention defaults to 7 days.
