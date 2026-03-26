#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "awscli",
#     "boto3",
#     "duckdb",
#     "google-cloud-storage",
#     "azure-storage-blob",
#     "azure-storage-file-datalake",
# ]
# ///
"""Compatibility tests for mockbucket.

Usage:
    uv run python scripts/compat/run_all.py serve s3       # start S3 server
    uv run python scripts/compat/run_all.py serve azure_blob  # start Azure Blob server
    uv run python scripts/compat/run_all.py serve azure_datalake  # start Azure DataLake server
    uv run python scripts/compat/run_all.py test           # run all cloud tests (default)
    uv run python scripts/compat/run_all.py test aws       # run AWS S3/STS tests only
    uv run python scripts/compat/run_all.py test gcs       # run GCS tests only
    uv run python scripts/compat/run_all.py test azure_blob   # run Azure Blob tests only
    uv run python scripts/compat/run_all.py test azure_datalake  # run Azure DataLake tests only
    uv run python scripts/compat/run_all.py --debug test   # verbose HTTP logging
"""
from __future__ import annotations

import argparse
import http.client
import logging
import sys
import time

from compat import ENDPOINT, _c, _BOLD, _CYAN, _DIM, heading, ok, fail, start_server

import aws
import azure_blob
import azure_datalake
import gcs

# Map cloud names to modules and their frontend types
CLOUDS = {
    "aws": {"module": aws, "frontend": "s3"},
    "gcs": {"module": gcs, "frontend": "gcs"},
    "azure_blob": {"module": azure_blob, "frontend": "azure_blob"},
    "azure_datalake": {"module": azure_datalake, "frontend": "azure_datalake"},
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
    print(f"  {_c(_DIM, 'Azure: mockstorage / base64(mockstorage-key-32bytes!!)')}")
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
    selected = args.clouds if args.clouds else list(CLOUDS.keys())
    heading(f"compat tests ({', '.join(selected)})")

    errors = 0
    for name in selected:
        cloud_info = CLOUDS[name]
        mod = cloud_info["module"]
        frontend = cloud_info["frontend"]
        heading(f"{name}")

        extra_env = mod.export_env() if hasattr(mod, "export_env") else {}
        seed = mod.seed() if hasattr(mod, "seed") else None
        start_server(frontend, extra_env, seed=seed)

        errors += mod.run()

    print()
    if errors:
        fail(f"{errors} test(s) failed")
        sys.exit(1)
    ok("all passed")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="mockbucket compat test runner",
        usage="%(prog)s [serve | test] [--debug] [aws | gcs | azure_blob | azure_datalake]",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="test",
        choices=["serve", "test"],
        help="serve: start server only; test: run compat tests (default: test)",
    )
    parser.add_argument(
        "clouds",
        nargs="*",
        choices=list(CLOUDS.keys()),
        help="cloud(s) to test (default: all)",
    )
    parser.add_argument("--debug", action="store_true", help="enable verbose HTTP logging")
    args = parser.parse_args()

    if args.debug:
        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig(level=logging.DEBUG)
        for name in ("botocore", "urllib3"):
            logging.getLogger(name).setLevel(logging.DEBUG)
            logging.getLogger(name).propagate = True

    {"serve": cmd_serve, "test": cmd_test}[args.command](args)


if __name__ == "__main__":
    main()
