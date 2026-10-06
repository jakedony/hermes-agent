"""Two-machine Hermes collaboration prototype (Python side).

The hub in ``collab/hub`` is the authority for tasks, leases, and idempotency.
This package is the agent bridge, the restricted diagnostics, and the worker
that runs one Hermes conversation per granted attempt.
"""

__version__ = "0.1.0"
