"""AWS S3/STS compatibility tests using awscli and boto3."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from compat import ENDPOINT, fail, ok, skip
from parquet import read_count, s3_con, write_parquet_s3


def configure() -> dict:
    """Return config overrides for the AWS (S3+STS) frontend."""
    return {"s3": True, "sts": True, "gcs": False}


def export_env() -> dict[str, str]:
    """Return extra env vars needed by AWS tools."""
    return {
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_ACCESS_KEY_ID": "admin",
        "AWS_SECRET_ACCESS_KEY": "admin-secret",
        "AWS_REGION": "us-east-1",
    }


def run() -> int:
    """Run all AWS compat tests. Returns error count."""
    errors = 0
    errors += _test_awscli()
    errors += _test_boto3()
    errors += _test_multipart()
    errors += _test_duckdb()
    return errors


def _test_awscli() -> int:
    aws = shutil.which("aws")
    if not aws:
        skip("awscli — not found")
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


def _test_boto3() -> int:
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


def _test_multipart() -> int:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name=os.environ["AWS_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Create a multipart upload.
    create = s3.create_multipart_upload(Bucket="demo", Key="compat/multipart.txt")
    upload_id = create["UploadId"]

    parts = []
    bodies = [b"part-one-", b"part-two-"]
    try:
        for i, body in enumerate(bodies):
            part = s3.upload_part(
                Bucket="demo",
                Key="compat/multipart.txt",
                UploadId=upload_id,
                PartNumber=i + 1,
                Body=body,
            )
            parts.append({"ETag": part["ETag"], "PartNumber": i + 1})

        s3.complete_multipart_upload(
            Bucket="demo",
            Key="compat/multipart.txt",
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception:
        s3.abort_multipart_upload(Bucket="demo", Key="compat/multipart.txt", UploadId=upload_id)
        fail("boto3 multipart — upload failed")
        return 1

    # Verify the assembled object.
    obj = s3.get_object(Bucket="demo", Key="compat/multipart.txt")
    content = obj["Body"].read()
    if content != b"part-one-part-two-":
        fail(f"boto3 multipart — content={content!r}, want b'part-one-part-two-'")
        return 1

    ok("boto3 multipart")
    return 0


def _test_duckdb() -> int:
    ROWS = 15_000_000
    con = s3_con(
        endpoint="127.0.0.1:19000",
        key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ["AWS_REGION"],
    )
    try:
        write_parquet_s3(con, "s3://demo/duckdb", rows_per_file=ROWS, num_files=2)
    except Exception as e:
        fail(f"duckdb parquet — write failed: {e}")
        return 1
    count = read_count(con, "s3://demo/duckdb/*.parquet")
    if count != 2 * ROWS:
        fail(f"duckdb parquet — count={count}, want {2 * ROWS}")
        return 1
    ok("duckdb parquet")
    return 0
