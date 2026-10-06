"""Set resource limits, then exec a fixed diagnostic vector.

The parent is threaded (the worker protocol reader), so ``preexec_fn`` is not
used there. This module is a fresh interpreter: it applies limits and replaces
itself. It refuses every argument list that is not one of the fixed diagnostics.
"""

from __future__ import annotations

import os
import resource
import sys


def _limits() -> None:
    # Address space is capped above a fresh interpreter mapping so exec still works,
    # and still far below an unbounded helper. CPU, file size, and fds are hard caps.
    cpu = 3
    address = 1024 * 1024 * 1024
    files = 64
    fsize = 1_000_000
    resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
    resource.setrlimit(resource.RLIMIT_NOFILE, (files, files))
    resource.setrlimit(resource.RLIMIT_FSIZE, (fsize, fsize))
    _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard == resource.RLIM_INFINITY or hard >= address:
        resource.setrlimit(resource.RLIMIT_AS, (address, address if hard == resource.RLIM_INFINITY else hard))


def main(argv: list[str]) -> None:
    from collab.diagnostics import argv_is_fixed

    if not argv_is_fixed(argv):
        raise SystemExit(2)
    _limits()
    os.execv(argv[0], argv)


if __name__ == "__main__":
    main(sys.argv[1:])
