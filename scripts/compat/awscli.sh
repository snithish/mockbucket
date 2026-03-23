#!/usr/bin/env bash
set -euo pipefail

if ! command -v aws >/dev/null 2>&1; then
  echo "aws cli not found" >&2
  exit 1
fi

ENDPOINT="${MOCKBUCKET_ENDPOINT:?missing endpoint}"

buckets=$(aws --endpoint-url "${ENDPOINT}" s3api list-buckets --query 'Buckets[].Name' --output text)
if [[ "${buckets}" != *"demo"* ]]; then
  echo "expected demo bucket in awscli list-buckets" >&2
  exit 1
fi

tmpfile=$(mktemp)
trap 'rm -f "${tmpfile}"' EXIT
printf "cli-compat" > "${tmpfile}"

aws --endpoint-url "${ENDPOINT}" s3api put-object --bucket demo --key compat/awscli.txt --body "${tmpfile}" >/dev/null
aws --endpoint-url "${ENDPOINT}" s3api head-object --bucket demo --key compat/awscli.txt >/dev/null

out=$(aws --endpoint-url "${ENDPOINT}" s3api get-object --bucket demo --key compat/awscli.txt "${tmpfile}.out" >/dev/null && cat "${tmpfile}.out")
if [[ "${out}" != "cli-compat" ]]; then
  echo "awscli get-object content mismatch" >&2
  exit 1
fi
