#!/usr/bin/env python3
"""Print a new bearer token and its SHA-256 hex digest.

The digest is what hub JSON stores. The secret goes in a mode-0600 file and
is never written to the bridge journal.
"""

from __future__ import annotations

import hashlib
import secrets


def main() -> None:
    secret = secrets.token_urlsafe(32)
    digest = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    print(secret)
    print(digest)


if __name__ == "__main__":
    main()
