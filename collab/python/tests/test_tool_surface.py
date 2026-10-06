"""Fail closed unless Hermes exposes exactly the approved collab tools.

tool_search is off in the worker home. Plugin-like toolsets are deferrable, so
leaving tool search on would replace this surface with the search bridge.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, "/workspace")

from collab.hermes_run import prepare_hermes_home
from collab.limits import COORDINATOR_TOOLS, WORKER_TOOLS
from collab.surface import SurfaceError, assert_surface, install_tools, remove_tools
from tests.fakes.fake_llm_provider import MODEL_ID, FakeLLMServer


class ToolSurfaceTests(unittest.TestCase):
    def test_agent_surface_matches_the_approved_set(self) -> None:
        from collab.protocol_client import ProtocolClient
        import socket

        previous = os.environ.get("HERMES_HOME")
        parent, child = socket.socketpair()
        client = ProtocolClient(child.makefile("rwb", buffering=0))
        try:
            with tempfile.TemporaryDirectory() as tmp, FakeLLMServer(api_key="sk-collab-surface") as server:
                home = Path(tmp) / "hermes"
                os.environ["HERMES_HOME"] = str(home)
                prepare_hermes_home(home, MODEL_ID, server.base_url, "sk-collab-surface")
                self._assert_role("coordinator", COORDINATOR_TOOLS, client, server)
                remove_tools()
                self._assert_role("worker", WORKER_TOOLS, client, server)
        finally:
            remove_tools()
            parent.close()
            if previous is None:
                os.environ.pop("HERMES_HOME", None)
            else:
                os.environ["HERMES_HOME"] = previous

    def _assert_role(self, role: str, approved: frozenset[str], client, server: FakeLLMServer) -> None:
        from run_agent import AIAgent

        install_tools(role, frozenset({("127.0.0.1", 9)}), client, 10, 30)
        agent = AIAgent(
            model=MODEL_ID,
            provider="custom",
            base_url=server.base_url,
            api_key="sk-collab-surface",
            api_mode="chat_completions",
            max_iterations=12,
            enabled_toolsets=["collab_diag"],
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
            skip_background_review=True,
            platform="collab",
        )
        try:
            assert_surface(agent.valid_tool_names, role)
            self.assertEqual(set(agent.valid_tool_names), approved)
        finally:
            agent.close()
        with self.assertRaises(SurfaceError):
            assert_surface(set(approved) | {"tool_search"}, role)


if __name__ == "__main__":
    unittest.main()
