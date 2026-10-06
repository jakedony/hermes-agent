"""Capacity numbers shared with the hub defaults.

``MAX_ITERATIONS`` bounds Hermes tool-calling rounds inside one attempt. It is
not a token cap: a round can still be large, and the hub does not meter tokens.
"""

ROOT_DEADLINE_SEC = 180
CHILD_DEADLINE_SEC = 120
TOOL_TIMEOUT_SEC = 10
MAX_ITERATIONS = 12
CHILD_BUDGET = 3
MAX_ATTEMPTS = 2
LEASE_SEC = 30
RENEW_EVERY_SEC = 10
LEASE_MARGIN_SEC = 5
IDEMPOTENCY_RETENTION_SEC = 7 * 24 * 3600
MAX_FRAME_BYTES = 262144
MAX_SUMMARY_BYTES = 8000
MAX_EVIDENCE_ITEMS = 8
MAX_EXCERPT_BYTES = 2000
MAX_LIMITATION_ITEMS = 8
MAX_LIMITATION_BYTES = 500
MAX_OBJECTIVE_BYTES = 2000
MAX_CONTEXT_BYTES = 8000
SS_OUTPUT_CAP = 32_000
DIAG_OUTPUT_CAP = 16_000

RETRYABLE_FAIL_CLASSES = frozenset({"transient", "crash", "lease_lost"})
FAIL_CLASSES = RETRYABLE_FAIL_CLASSES | frozenset(
    {"invalid_input", "unauthorized", "cancelled", "deadline"}
)
TERMINAL_TASK_STATES = frozenset({"completed", "failed", "cancelled", "timed_out"})

# Coordinator surface includes delegation. A worker never sees that tool.
COORDINATOR_TOOLS = frozenset({"listening_sockets", "tcp_probe", "delegate_investigation"})
WORKER_TOOLS = frozenset({"listening_sockets", "tcp_probe"})
TOOLSET_NAME = "collab_diag"
