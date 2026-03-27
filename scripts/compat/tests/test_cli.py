from __future__ import annotations

import unittest
from unittest import mock

from mockbucket_compat import cli
from mockbucket_compat.suite import CompatSuite


class StubSuite(CompatSuite):
    def __init__(self, name: str) -> None:
        self.name = name
        self.frontend = "s3"
        self.run_calls = 0

    def run(self) -> int:
        self.run_calls += 1
        return 0


class CLITest(unittest.TestCase):
    def test_build_parser_parses_test_clouds(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["test", "aws"])

        self.assertEqual(args.command, "test")
        self.assertEqual(args.clouds, ["aws"])

    def test_main_defaults_to_test_when_no_subcommand_is_provided(self) -> None:
        with mock.patch.object(cli, "cmd_test") as cmd_test:
            exit_code = cli.main([])

        self.assertEqual(exit_code, 0)
        cmd_test.assert_called_once()
        args = cmd_test.call_args.args[0]
        self.assertEqual(args.command, "test")
        self.assertEqual(args.clouds, [])

    def test_main_returns_one_on_compat_error(self) -> None:
        with mock.patch.object(cli, "cmd_test", side_effect=cli.CompatError("boom")):
            exit_code = cli.main(["test"])

        self.assertEqual(exit_code, 1)

    def test_cmd_test_runs_selected_suite_instances(self) -> None:
        suite = StubSuite("aws")

        with mock.patch.dict(cli.COMPAT_SUITES, {"aws": suite}, clear=True):
            with mock.patch.object(cli, "start_server") as start_server:
                cli.cmd_test(cli.build_parser().parse_args(["test", "aws"]))

        start_server.assert_called_once_with("s3", {}, seed=None)
        self.assertEqual(suite.run_calls, 1)
