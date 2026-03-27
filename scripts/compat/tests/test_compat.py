from __future__ import annotations

import unittest
from pathlib import Path
from unittest import mock

from mockbucket_compat import compat


class CompatTest(unittest.TestCase):
    def test_server_command_prefers_built_binary(self) -> None:
        with mock.patch.object(compat, "BINARY_PATH", Path("/tmp/mockbucketd")):
            with mock.patch.object(Path, "exists", return_value=True):
                self.assertEqual(compat._server_command(), ["/tmp/mockbucketd"])

    def test_server_command_falls_back_to_go_run(self) -> None:
        with mock.patch.object(compat, "BINARY_PATH", Path("/tmp/missing-mockbucketd")):
            with mock.patch.object(Path, "exists", return_value=False):
                self.assertEqual(compat._server_command(), ["go", "run", "./cmd/mockbucketd"])
