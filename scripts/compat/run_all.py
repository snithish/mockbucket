#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "awscli",
#     "boto3",
# ]
# ///
"""Compatibility tests for mockbucket.

Subcommands:
    uv run python scripts/compat/run_all.py serve     # start server, print connection info, block
    uv run python scripts/compat/run_all.py test      # start server, run all compat tests
    uv run python scripts/compat/run_all.py           # same as "test"

Flags:
    --debug   enable verbose HTTP logging
"""
from __future__ import annotations

import argparse
import atexit
import http.client
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

# ── Pretty output ────────────────────────────────────────────────────────
_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_DIM = "\033[2m"
_RESET = "\033[0m"
_BOLD = "\033[1m"


def _supports_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


_COLOR = _supports_color()


def _c(code: str, text: str) -> str:
    return f"{code}{text}{_RESET}" if _COLOR else text


def ok(text: str) -> None:
    print(f"  {_c(_GREEN, '✓')} {text}")


def skip(text: str) -> None:
    print(f"  {_c(_YELLOW, '⊘')} {text}")


def fail(text: str) -> None:
    print(f"  {_c(_RED, '✗')} {text}", file=sys.stderr)


def heading(text: str) -> None:
    print(f"\n{_c(_BOLD, text)}")


# ── Config ───────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent
PORT = int(os.environ.get("MOCKBUCKET_PORT", "19000"))
ENDPOINT = f"http://127.0.0.1:{PORT}"


def write_config(tmp_dir: Path) -> Path:
    cfg = tmp_dir / "mockbucket.yaml"
    cfg.write_text(f"""\
server:
  address: 127.0.0.1:{PORT}
  request_log: false
  shutdown_timeout: 5s
storage:
  root_dir: {tmp_dir}/objects
  sqlite_path: {tmp_dir}/mockbucket.db
seed:
  path: {ROOT}/seed.example.yaml
frontends:
  s3: true
  sts: true
  gcs: false
  azure: false
auth:
  session_duration: 1h
""")
    return cfg


def export_env() -> None:
    os.environ["MOCKBUCKET_ENDPOINT"] = ENDPOINT
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    os.environ["AWS_ACCESS_KEY_ID"] = "admin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "admin-secret"
    os.environ["AWS_REGION"] = "us-east-1"


# ── Server ───────────────────────────────────────────────────────────────
_server_proc: subprocess.Popen | None = None
_tmp_dir: Path | None = None


def _cleanup() -> None:
    global _server_proc, _tmp_dir
    if _server_proc and _server_proc.poll() is None:
        _server_proc.terminate()
        try:
            _server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_proc.kill()
        _server_proc = None
    if _tmp_dir:
        shutil.rmtree(_tmp_dir, ignore_errors=True)
        _tmp_dir = None


atexit.register(_cleanup)


def start_server() -> Path:
    global _server_proc, _tmp_dir
    _tmp_dir = Path(tempfile.mkdtemp(prefix="mockbucket-compat."))
    cfg = write_config(_tmp_dir)
    export_env()

    _server_proc = subprocess.Popen(
        ["go", "run", "./cmd/mockbucketd", "--config", str(cfg)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{ENDPOINT}/readyz")
            return _tmp_dir
        except Exception:
            time.sleep(0.2)
    fail("mockbucketd did not become ready")
    sys.exit(1)


# ── Tests ────────────────────────────────────────────────────────────────
def test_awscli() -> int:
    aws = shutil.which("aws")
    if not aws:
        skip("aws cli — not found")
        return 0

    def _aws(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [aws, "--endpoint-url", ENDPOINT, *args],
            capture_output=True,
            text=True,
        )

    result = _aws("s3api", "list-buckets", "--query", "Buckets[].Name", "--output", "text")
    if "demo" not in result.stdout:
        fail("awscli list-buckets — expected demo bucket")
        return 1

    tmpfile = Path(tempfile.mktemp())
    try:
        tmpfile.write_text("cli-compat")
        _aws("s3api", "put-object", "--bucket", "demo", "--key", "compat/awscli.txt", "--body", str(tmpfile))
        _aws("s3api", "head-object", "--bucket", "demo", "--key", "compat/awscli.txt")
        _aws("s3api", "get-object", "--bucket", "demo", "--key", "compat/awscli.txt", str(tmpfile) + ".out")
        out = (Path(str(tmpfile) + ".out")).read_text()
        if out != "cli-compat":
            fail("awscli get-object — content mismatch")
            return 1
    finally:
        tmpfile.unlink(missing_ok=True)
        Path(str(tmpfile) + ".out").unlink(missing_ok=True)

    ok("awscli")
    return 0


def test_boto3() -> int:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name=os.environ["AWS_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if "demo" not in buckets:
        fail("boto3 list_buckets — expected demo bucket")
        return 1

    s3.put_object(Bucket="demo", Key="compat/boto3.txt", Body=b"boto3-compat")

    head = s3.head_object(Bucket="demo", Key="compat/boto3.txt")
    if head.get("ContentLength") != 12:
        fail(f"boto3 head_object — content_length={head.get('ContentLength')}, want 12")
        return 1

    obj = s3.get_object(Bucket="demo", Key="compat/boto3.txt")
    body = obj["Body"].read().decode("utf-8")
    if body != "boto3-compat":
        fail("boto3 get_object — content mismatch")
        return 1

    resp = s3.list_objects_v2(Bucket="demo", Prefix="compat/")
    keys = [item["Key"] for item in resp.get("Contents", [])]
    if "compat/boto3.txt" not in keys:
        fail("boto3 list_objects_v2 — missing key")
        return 1

    ok("boto3")
    return 0


def test_spark() -> int:
    if os.environ.get("MOCKBUCKET_ENABLE_SPARK") != "1":
        skip("spark s3a — disabled (MOCKBUCKET_ENABLE_SPARK=1)")
        return 0
    if not shutil.which("spark-submit"):
        fail("spark-submit not found")
        return 1
    fail("spark s3a — not yet implemented")
    return 1


def test_duckdb() -> int:
    if os.environ.get("MOCKBUCKET_ENABLE_DUCKDB") != "1":
        skip("duckdb — disabled (MOCKBUCKET_ENABLE_DUCKDB=1)")
        return 0
    if not shutil.which("duckdb"):
        fail("duckdb cli not found")
        return 1
    fail("duckdb — not yet implemented")
    return 1


# ── Subcommands ──────────────────────────────────────────────────────────
def cmd_serve(_args: argparse.Namespace) -> None:
    """Start the server and block until interrupted."""
    start_server()
    print()
    print(f"  {_c(_BOLD, 'mockbucketd')} running")
    print()
    print(f"    endpoint  {_c(_CYAN, ENDPOINT)}")
    print(f"    readyz    {_c(_CYAN, f'{ENDPOINT}/readyz')}")
    print(f"    access    {_c(_DIM, 'admin / admin-secret')}")
    print()
    print(f"  {_c(_DIM, 'Ctrl-C to stop')}")
    print()
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print()


def cmd_test(args: argparse.Namespace) -> None:
    """Start the server and run all compat tests."""
    heading("compat tests")
    start_server()
    errors = 0
    errors += test_awscli()
    errors += test_boto3()
    errors += test_spark()
    errors += test_duckdb()
    print()
    if errors:
        fail(f"{errors} test(s) failed")
        sys.exit(1)
    ok("all passed")


# ── CLI ──────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="mockbucket compat test runner",
        usage="%(prog)s [serve | test] [--debug]",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="test",
        choices=["serve", "test"],
        help="serve: start server only; test: run compat tests (default: test)",
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
