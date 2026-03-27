"""Shared utilities for MockBucket compatibility tests."""

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
from typing import Final

_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_DIM = "\033[2m"
_RESET = "\033[0m"
_BOLD = "\033[1m"
READY_TIMEOUT_SECONDS: Final[float] = 10.0
READY_POLL_INTERVAL_SECONDS: Final[float] = 0.2


def _supports_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


_COLOR = _supports_color()


def _c(code: str, text: str) -> str:
    return f"{code}{text}{_RESET}" if _COLOR else text


def ok(text: str) -> None:
    print(f"  {_c(_GREEN, 'OK')} {text}")


def skip(text: str) -> None:
    print(f"  {_c(_YELLOW, 'SKIP')} {text}")


def fail(text: str) -> None:
    print(f"  {_c(_RED, 'FAIL')} {text}", file=sys.stderr)


def heading(text: str) -> None:
    print(f"\n{_c(_BOLD, text)}")


def _find_repo_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "go.mod").exists() and (parent / "cmd" / "mockbucketd").exists():
            return parent
    raise RuntimeError(f"could not locate repository root from {current}")


ROOT = _find_repo_root()
BINARY_PATH = ROOT / "bin" / "mockbucketd"
PORT = int(os.environ.get("MOCKBUCKET_PORT", "19000"))
ENDPOINT = f"http://127.0.0.1:{PORT}"

_DEFAULT_SEED = """\
buckets:
  - demo
roles:
  - name: data-reader
s3:
  access_keys:
    - id: admin
      secret: admin-secret
    - id: restricted
      secret: restricted-secret
      allowed_roles:
        - data-reader
objects:
  - bucket: demo
    key: bootstrap/hello.txt
    content: hello from mockbucket
"""


def _write_config(tmp_dir: Path, frontend_type: str, seed: str | None = None) -> Path:
    cfg = tmp_dir / "mockbucket.yaml"
    if seed is None:
        seed = _DEFAULT_SEED
    indented = "\n".join("  " + line if line.strip() else "" for line in seed.splitlines())
    cfg.write_text(
        f"""\
server:
  address: 127.0.0.1:{PORT}
  request_log: false
  shutdown_timeout: 5s
storage:
  root_dir: {tmp_dir}/objects
  sqlite_path: {tmp_dir}/mockbucket.db
frontends:
  type: {frontend_type}
seed:
{indented}
"""
    )
    return cfg


_server_proc: subprocess.Popen | None = None
_tmp_dir: Path | None = None
_server_log_path: Path | None = None


class CompatError(RuntimeError):
    """Raised when the compatibility runner cannot continue."""


def _server_command() -> list[str]:
    if BINARY_PATH.exists():
        return [str(BINARY_PATH)]
    return ["go", "run", "./cmd/mockbucketd"]


def _ready_url() -> str:
    return f"{ENDPOINT}/readyz"


def _cleanup() -> None:
    global _server_proc, _tmp_dir, _server_log_path
    if _server_proc and _server_proc.poll() is None:
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
    _server_log_path = None


atexit.register(_cleanup)


def start_server(frontend_type: str, extra_env: dict[str, str] | None = None, seed: str | None = None) -> Path:
    """Start mockbucketd with the given frontend type. Returns temp dir."""
    global _server_proc, _tmp_dir, _server_log_path

    _cleanup()

    _tmp_dir = Path(tempfile.mkdtemp(prefix="mockbucket-compat."))
    cfg = _write_config(_tmp_dir, frontend_type, seed=seed)
    _server_log_path = _tmp_dir / "mockbucketd.log"

    env = {**os.environ}
    env["MOCKBUCKET_ENDPOINT"] = ENDPOINT
    if extra_env:
        env.update(extra_env)
        os.environ.update(extra_env)

    command = [*_server_command(), "--config", str(cfg)]
    with _server_log_path.open("w", encoding="utf-8") as log_file:
        _server_proc = subprocess.Popen(
            command,
            cwd=ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(_ready_url(), timeout=1.0)
            return _tmp_dir
        except Exception:
            if _server_proc.poll() is not None:
                break
            time.sleep(READY_POLL_INTERVAL_SECONDS)

    details = ""
    if _server_log_path and _server_log_path.exists():
        log_tail = _server_log_path.read_text(encoding="utf-8").strip()
        if log_tail:
            details = f"\nserver log:\n{log_tail}"
        else:
            details = f"\nserver log: {_server_log_path}"
    _cleanup()
    raise CompatError(f"mockbucketd did not become ready for frontend {frontend_type}{details}")
