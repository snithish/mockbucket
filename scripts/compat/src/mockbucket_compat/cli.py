"""Compatibility tests for MockBucket.

Usage:
    uv run --project scripts/compat mockbucket-compat serve s3
    uv run --project scripts/compat mockbucket-compat test
    uv run --project scripts/compat mockbucket-compat test aws
    uv run --project scripts/compat mockbucket-compat test gcs
    uv run --project scripts/compat mockbucket-compat --debug test
"""

from __future__ import annotations

import argparse
import http.client
import logging
import sys
import time
from typing import Sequence

from .aws import AWSCompatSuite
from .compat import ENDPOINT, CompatError, _BOLD, _CYAN, _DIM, _c, fail, heading, ok, start_server
from .gcs import GCSCompatSuite
from .suite import CompatSuite

COMPAT_SUITES: dict[str, CompatSuite] = {
    suite.name: suite
    for suite in (
        AWSCompatSuite(),
        GCSCompatSuite(),
    )
}


def cmd_serve(args: argparse.Namespace) -> None:
    """Start the server with the specified frontend and block."""
    frontend = args.frontend if args.frontend else "s3"
    start_server(frontend)
    print()
    print(f"  {_c(_BOLD, 'mockbucketd')} running")
    print()
    print(f"    frontend  {_c(_CYAN, frontend)}")
    print(f"    endpoint  {_c(_CYAN, ENDPOINT)}")
    print(f"    readyz    {_c(_CYAN, f'{ENDPOINT}/readyz')}")
    print()
    print(f"  {_c(_DIM, 'S3: admin / admin-secret')}")
    print()
    print(f"  {_c(_DIM, 'Ctrl-C to stop')}")
    print()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print()


def cmd_test(args: argparse.Namespace) -> None:
    """Start servers and run compat tests for the selected cloud(s)."""
    selected = args.clouds if args.clouds else list(COMPAT_SUITES.keys())
    heading(f"compat tests ({', '.join(selected)})")

    errors = 0
    for name in selected:
        suite = COMPAT_SUITES[name]
        heading(suite.name)
        start_server(suite.frontend, suite.export_env(), seed=suite.seed())
        errors += suite.run()

    print()
    if errors:
        raise CompatError(f"{errors} test(s) failed")
    ok("all passed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="mockbucket compat test runner")
    parser.add_argument("--debug", action="store_true", help="enable verbose HTTP logging")
    subparsers = parser.add_subparsers(dest="command")

    serve_parser = subparsers.add_parser("serve", help="start the server only")
    serve_parser.add_argument(
        "frontend",
        nargs="?",
        default="s3",
        choices=["s3", "gcs"],
        help="frontend to start (default: s3)",
    )

    test_parser = subparsers.add_parser("test", help="run compat tests")
    test_parser.add_argument(
        "clouds",
        nargs="*",
        choices=list(COMPAT_SUITES.keys()),
        help="cloud(s) to test (default: all)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "test"
        args.clouds = []

    if args.debug:
        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig(level=logging.DEBUG)
        for name in ("botocore", "urllib3"):
            logging.getLogger(name).setLevel(logging.DEBUG)
            logging.getLogger(name).propagate = True

    try:
        {"serve": cmd_serve, "test": cmd_test}[args.command](args)
    except CompatError as err:
        fail(str(err))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
