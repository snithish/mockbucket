#!/usr/bin/env bash
set -euo pipefail

if ! command -v duckdb >/dev/null 2>&1; then
  echo "duckdb cli not found" >&2
  exit 1
fi

echo "duckdb compat not yet implemented" >&2
exit 1
