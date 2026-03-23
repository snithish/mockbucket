#!/usr/bin/env bash
set -euo pipefail

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "spark-submit not found" >&2
  exit 1
fi

echo "spark s3a compat not yet implemented" >&2
exit 1
