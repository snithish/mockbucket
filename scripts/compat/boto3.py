import os
import sys

import boto3

endpoint = os.environ.get("MOCKBUCKET_ENDPOINT")
if not endpoint:
    print("missing MOCKBUCKET_ENDPOINT", file=sys.stderr)
    sys.exit(1)

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
)

buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if "demo" not in buckets:
    print("expected demo bucket from list_buckets", file=sys.stderr)
    sys.exit(1)

s3.put_object(Bucket="demo", Key="compat/boto3.txt", Body=b"boto3-compat")
head = s3.head_object(Bucket="demo", Key="compat/boto3.txt")
if head.get("ContentLength") != 12:
    print("unexpected content length", file=sys.stderr)
    sys.exit(1)

obj = s3.get_object(Bucket="demo", Key="compat/boto3.txt")
body = obj["Body"].read().decode("utf-8")
if body != "boto3-compat":
    print("boto3 get_object mismatch", file=sys.stderr)
    sys.exit(1)

resp = s3.list_objects_v2(Bucket="demo", Prefix="compat/")
keys = [item["Key"] for item in resp.get("Contents", [])]
if "compat/boto3.txt" not in keys:
    print("list_objects_v2 missing boto3 key", file=sys.stderr)
    sys.exit(1)
