#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

start_server

"${SCRIPT_DIR}/awscli.sh"
python3 "${SCRIPT_DIR}/boto3.py"

if [[ "${MOCKBUCKET_ENABLE_SPARK:-}" == "1" ]]; then
  "${SCRIPT_DIR}/spark_s3a.sh"
else
  echo "spark s3a compat disabled (set MOCKBUCKET_ENABLE_SPARK=1 to run)"
fi

if [[ "${MOCKBUCKET_ENABLE_DUCKDB:-}" == "1" ]]; then
  "${SCRIPT_DIR}/duckdb.sh"
else
  echo "duckdb compat disabled (set MOCKBUCKET_ENABLE_DUCKDB=1 to run)"
fi
