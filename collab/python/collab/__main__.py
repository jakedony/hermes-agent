"""Entrypoints: ``python -m collab bridge|worker|human``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    command = args[0] if args else ""
    handlers = {
        "bridge": _bridge_main,
        "worker": _worker_main,
        "human": _human_main,
    }
    handler = handlers.get(command)
    if handler is None:
        print("usage: python -m collab bridge|worker|human ...", file=sys.stderr)
        return 2
    return handler(args[1:])


def _bridge_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="python -m collab bridge")
    parser.add_argument("--config", required=True)
    parsed = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    from collab.bridge import Bridge
    from collab.config import load_config, read_secret_file

    cfg = load_config(parsed.config)
    token = read_secret_file(cfg.token_file)
    bridge = Bridge(cfg, token)

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, bridge.request_stop)
        await bridge.run()

    asyncio.run(_run())
    return 0


def _worker_main(argv: list[str]) -> int:
    del argv
    from collab.worker import main as worker_main

    return worker_main()


def _human_main(argv: list[str]) -> int:
    from collab.human import client_main

    return client_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
