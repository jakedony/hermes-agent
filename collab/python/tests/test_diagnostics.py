"""Diagnostic rejection: bad hosts, ports, and option-shaped input never become a command."""

from __future__ import annotations

import socket
import unittest

from collab.diagnostics import (
    SS_ARGS,
    classify_probe,
    filter_listeners,
    line_has_port,
    listening_sockets,
    ss_argv,
    ss_path,
    tcp_probe,
)


class DiagnosticTests(unittest.TestCase):
    def test_ss_argv_is_fixed_and_ignores_the_port(self) -> None:
        path = ss_path() or "/usr/bin/ss"
        argv = ss_argv(path)
        self.assertEqual(argv[-len(SS_ARGS) :], list(SS_ARGS))
        self.assertNotIn("18080", argv)
        self.assertNotIn("-18080", argv)
        self.assertTrue(all(not part.startswith("-p") for part in argv))

    def test_port_filter_does_not_match_a_prefix(self) -> None:
        sample = "LISTEN 0 5 127.0.0.1:18080 0.0.0.0:*\nLISTEN 0 5 127.0.0.1:1808 0.0.0.0:*\n"
        self.assertTrue(line_has_port(sample.splitlines()[0], 18080))
        self.assertFalse(line_has_port(sample.splitlines()[0], 1808))
        self.assertEqual(len(filter_listeners(sample, 18080)), 1)

    def test_probe_rejects_bad_targets_without_connecting(self) -> None:
        allow = frozenset({("127.0.0.1", 9)})
        rejected = [
            ("-n", 9, "option"),
            ("127.0.0.1 -flag", 9, "whitespace"),
            ("example.com", 9, "IP"),
            ("127.0.0.1", 0, "1..65535"),
            ("127.0.0.1", True, "integer"),
            ("127.0.0.1", 10, "allowlist"),
            ("10.1.2.3", 9, "allowlist"),
        ]
        for host, port, needle in rejected:
            reason = classify_probe(host, port, allow)
            self.assertIsNotNone(reason, msg=repr((host, port)))
            assert reason is not None
            self.assertIn(needle, reason)
            body = tcp_probe(host, port, allow, timeout_sec=0.2)
            self.assertFalse(body["ok"])
            self.assertEqual(body["error"], reason)

    def test_probe_allowlist_hit_and_listening_sockets_run(self) -> None:
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        try:
            body = tcp_probe("127.0.0.1", port, frozenset({("127.0.0.1", port)}), timeout_sec=2)
            self.assertTrue(body["ok"], body)
            self.assertIsNone(body["limitation"])
            listed = listening_sockets(port, timeout_sec=5)
            if listed["ok"]:
                self.assertTrue(any(f":{port}" in line for line in listed["listeners"]), listed)
            else:
                self.assertIn("ss", str(listed["limitation"]))
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
