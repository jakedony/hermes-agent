"""Diagnostic rejection: bad hosts, ports, and option-shaped input never become a command."""

from __future__ import annotations

import socket
import unittest

import json
import tempfile
from pathlib import Path

from collab.config import ConfigError, load_config
from collab.diagnostics import (
    JOURNAL_PREFIX,
    JOURNALCTL_PATH,
    SS_ARGS,
    STATUS_PREFIX,
    SYSTEMCTL_PATH,
    argv_is_fixed,
    classify_dns,
    classify_probe,
    classify_service,
    dns_lookup,
    filter_listeners,
    line_has_port,
    listening_sockets,
    route_show,
    service_logs,
    service_status,
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


    def test_probe_rejects_the_hub_forward_even_when_allowlisted(self) -> None:
        allow = frozenset({("127.0.0.1", 18765)})
        forbidden = frozenset({("127.0.0.1", 18765)})
        reason = classify_probe("127.0.0.1", 18765, allow, forbidden)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("hub", reason)
        body = tcp_probe("127.0.0.1", 18765, allow, timeout_sec=0.2, forbidden=forbidden)
        self.assertFalse(body["ok"])
        self.assertEqual(body["error"], reason)

    def test_service_and_dns_reject_options_and_unknown_names(self) -> None:
        services = frozenset({"collab-http.service"})
        names = frozenset({"vm.example.test"})
        for unit, needle in (("-n", "option"), ("nginx.service", "allowlist"), ("bad name", "option"), ("a;rm", "token")):
            reason = classify_service(unit, services)
            self.assertIsNotNone(reason)
            assert reason is not None
            self.assertIn(needle, reason)
            self.assertFalse(service_status(unit, services, timeout_sec=0.2)["ok"])
            self.assertFalse(service_logs(unit, services, timeout_sec=0.2)["ok"])
        canonical, reason = classify_dns("VM.Example.Test", names)
        self.assertEqual(canonical, "vm.example.test")
        self.assertIsNone(reason)
        rejected, why = classify_dns("-f", names)
        self.assertIsNone(rejected)
        self.assertIsNotNone(why)
        assert why is not None
        self.assertIn("option", why)
        missing, why = classify_dns("other.example", names)
        self.assertIsNone(missing)
        self.assertIsNotNone(why)
        assert why is not None
        self.assertIn("allowlist", why)
        self.assertFalse(dns_lookup("other.example", names, timeout_sec=0.2)["ok"])

    def test_fixed_argv_rejects_extra_options(self) -> None:
        status = [SYSTEMCTL_PATH, *STATUS_PREFIX, "collab-http.service"]
        logs = [JOURNALCTL_PATH, *JOURNAL_PREFIX, "collab-http.service"]
        self.assertTrue(argv_is_fixed(status))
        self.assertTrue(argv_is_fixed(logs))
        self.assertTrue(argv_is_fixed(["/usr/sbin/ip", "route", "show"]))
        self.assertTrue(argv_is_fixed(["/usr/bin/getent", "ahosts", "vm.example.test"]))
        self.assertFalse(argv_is_fixed(status + ["--user"]))
        self.assertFalse(argv_is_fixed(["/tmp/systemctl", *STATUS_PREFIX, "collab-http.service"]))
        self.assertFalse(argv_is_fixed(["/usr/bin/systemctl", "status", "collab-http.service"]))
        self.assertFalse(argv_is_fixed([JOURNALCTL_PATH, *JOURNAL_PREFIX, "-rf"]))
        self.assertFalse(argv_is_fixed(["/usr/sbin/ip", "route", "show", "dev"]))
        self.assertFalse(argv_is_fixed(["/usr/bin/getent", "ahosts", "-f"]))
        self.assertNotIn("sudo", status)
        self.assertNotIn("sudo", logs)

    def test_real_diagnostics_stay_unprivileged(self) -> None:
        services = frozenset({"collab-http.service"})
        status = service_status("collab-http.service", services, timeout_sec=5)
        logs = service_logs("collab-http.service", services, timeout_sec=5)
        routes = route_show(timeout_sec=5)
        looked = dns_lookup("localhost", frozenset({"localhost"}), timeout_sec=5)
        blob = json.dumps({"status": status, "logs": logs, "routes": routes, "dns": looked}).lower()
        self.assertNotIn("sudo", blob)
        self.assertEqual(status["executable"], SYSTEMCTL_PATH)
        self.assertEqual(logs["executable"], JOURNALCTL_PATH)
        self.assertIn(logs["ok"], (True, False))
        if logs["ok"] and logs.get("limitation"):
            self.assertIn("unprivileged", logs["limitation"])
        if routes["ok"]:
            self.assertTrue(routes["lines"])
        if looked["ok"]:
            self.assertTrue(any("127.0.0.1" in line or "::1" in line for line in looked["answers"]))
            self.assertLessEqual(len(looked["answers"]), 8)

    def test_config_rejects_hub_forward_and_unsafe_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bad_hub = _bridge_json(root, allowlist=[{"host": "127.0.0.1", "port": 18765}])
            with self.assertRaises(ConfigError) as raised:
                load_config(bad_hub)
            self.assertIn("hub", str(raised.exception))
            bad_unit = _bridge_json(root, services=["-rf"])
            with self.assertRaises(ConfigError):
                load_config(bad_unit)
            good = _bridge_json(root, services=["collab-http.service"], dns_names=["vm.example.test"])
            cfg = load_config(good)
            self.assertEqual(cfg.services, ("collab-http.service",))
            self.assertEqual(cfg.dns_names, ("vm.example.test",))


def _bridge_json(root: Path, **overrides: object) -> Path:
    data: dict = {
        "agent_id": "hermes-pc",
        "machine_id": "pc",
        "hub_ws": "ws://127.0.0.1:18765/ws",
        "token_file": str(root / "token"),
        "journal_path": str(root / "journal.db"),
        "state_dir": str(root / "state"),
        "worker_mode": "fake",
        "peer_agent_id": "hermes-vm",
        "allowlist": [{"host": "203.0.113.10", "port": 18080}],
    }
    data.update(overrides)
    path = root / "bridge.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


if __name__ == "__main__":
    unittest.main()
