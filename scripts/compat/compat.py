"""Shared utilities for mockbucket compatibility tests."""
from __future__ import annotations

import atexit
import os
import shutil
import signal
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


def _write_config(tmp_dir: Path, frontends: dict[str, bool], seed: str | None = None) -> Path:
    cfg = tmp_dir / "mockbucket.yaml"
    if seed is not None:
        seed_file = tmp_dir / "seed.yaml"
        seed_file.write_text(seed)
        seed_path = str(seed_file)
    else:
        seed_path = f"{ROOT}/seed.example.yaml"
    cfg.write_text(f"""\
server:
  address: 127.0.0.1:{PORT}
  request_log: false
  shutdown_timeout: 5s
storage:
  root_dir: {tmp_dir}/objects
  sqlite_path: {tmp_dir}/mockbucket.db
seed:
  path: {seed_path}
frontends:
  s3: {"true" if frontends.get("s3") else "false"}
  sts: {"true" if frontends.get("sts") else "false"}
  gcs: {"true" if frontends.get("gcs") else "false"}
  azure: false
auth:
  session_duration: 1h
""")
    return cfg


# ── Server lifecycle ─────────────────────────────────────────────────────
_server_proc: subprocess.Popen | None = None
_tmp_dir: Path | None = None


def _cleanup() -> None:
    global _server_proc, _tmp_dir
    if _server_proc and _server_proc.poll() is None:
        # Kill the entire process group to catch go-run child processes.
        try:
            os.killpg(os.getpgid(_server_proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            _server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(_server_proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        _server_proc = None
    if _tmp_dir:
        shutil.rmtree(_tmp_dir, ignore_errors=True)
        _tmp_dir = None


atexit.register(_cleanup)


def start_server(frontends: dict[str, bool], extra_env: dict[str, str] | None = None, seed: str | None = None) -> Path:
    """Start mockbucketd with the given frontend config. Returns temp dir."""
    global _server_proc, _tmp_dir

    # Stop any previously running server.
    _cleanup()

    _tmp_dir = Path(tempfile.mkdtemp(prefix="mockbucket-compat."))
    cfg = _write_config(_tmp_dir, frontends, seed=seed)

    # Apply extra env to current process so child tools (awscli, boto3) see them.
    env = {**os.environ}
    env["MOCKBUCKET_ENDPOINT"] = ENDPOINT
    if extra_env:
        env.update(extra_env)
        os.environ.update(extra_env)

    _server_proc = subprocess.Popen(
        ["go", "run", "./cmd/mockbucketd", "--config", str(cfg)],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    for _ in range(50):
        try:
            urllib.request.urlopen(f"{ENDPOINT}/readyz")
            return _tmp_dir
        except Exception:
            time.sleep(0.2)
    fail("mockbucketd did not become ready")
    sys.exit(1)
