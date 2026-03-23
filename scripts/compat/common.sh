#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PORT="${MOCKBUCKET_PORT:-19000}"
ENDPOINT="http://127.0.0.1:${PORT}"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mockbucket-compat.XXXXXX")"
CONFIG_PATH="${TMP_DIR}/mockbucket.yaml"
PID_FILE="${TMP_DIR}/mockbucket.pid"

cleanup() {
  if [[ -f "${PID_FILE}" ]]; then
    kill "$(cat "${PID_FILE}")" >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

cat <<CFG > "${CONFIG_PATH}"
server:
  address: 127.0.0.1:${PORT}
  request_log: false
  shutdown_timeout: 5s
storage:
  root_dir: ${TMP_DIR}/objects
  sqlite_path: ${TMP_DIR}/mockbucket.db
seed:
  path: ${ROOT}/seed.example.yaml
frontends:
  s3: true
  sts: true
  gcs: false
  azure: false
auth:
  session_duration: 1h
CFG

start_server() {
  (cd "${ROOT}" && go run ./cmd/mockbucketd --config "${CONFIG_PATH}") >/dev/null 2>&1 &
  echo $! > "${PID_FILE}"

  for _ in {1..50}; do
    if curl -fs "${ENDPOINT}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "mockbucketd did not become ready" >&2
  return 1
}

export MOCKBUCKET_ENDPOINT="${ENDPOINT}"
export AWS_EC2_METADATA_DISABLED=true
export AWS_ACCESS_KEY_ID="admin"
export AWS_SECRET_ACCESS_KEY="admin-secret"
export AWS_REGION="us-east-1"
