"""AWS S3/STS compatibility tests using awscli and boto3."""

from __future__ import annotations

import os
import shutil
import subprocess
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory

from .compat import ENDPOINT, CompatError, fail, ok, skip
from .parquet import read_count, s3_con, write_parquet_s3
from .pyspark import s3a_roundtrip
from .suite import CompatSuite


class AWSCompatSuite(CompatSuite):
    name = "aws"
    frontend = "s3"

    def export_env(self) -> dict[str, str]:
        return {
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "admin-secret",
            "AWS_REGION": "us-east-1",
        }

    def run(self, with_pyspark: bool = False) -> int:
        errors = 0
        errors += self._test_awscli()
        errors += self._test_boto3()
        errors += self._test_presigned_urls()
        errors += self._test_multipart()
        errors += self._test_sts_assume_role()
        errors += self._test_sts_get_caller_identity()
        errors += self._test_sts_get_session_token()
        errors += self._test_sts_allowed_roles()
        errors += self._test_duckdb()
        if with_pyspark:
            errors += self._test_pyspark()
        return errors

    def _make_s3_client(self, *, path_style: bool = True):
        import boto3
        from botocore.config import Config

        endpoint = ENDPOINT if path_style else ENDPOINT.replace("127.0.0.1", "localhost")
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            config=Config(s3={"addressing_style": "path" if path_style else "virtual"}),
        )

    def _make_sts_client(self, access_key: str | None = None, secret_key: str | None = None):
        import boto3

        return boto3.client(
            "sts",
            endpoint_url=ENDPOINT,
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=access_key or os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=secret_key or os.environ["AWS_SECRET_ACCESS_KEY"],
        )

    def _test_awscli(self) -> int:
        aws = shutil.which("aws")
        if not aws:
            skip("awscli - not found")
            return 0

        def _aws(*args: str) -> subprocess.CompletedProcess[str]:
            result = subprocess.run(
                [aws, "--endpoint-url", ENDPOINT, *args],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                raise CompatError(
                    f"awscli command failed: {' '.join(args)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
                )
            return result

        result = _aws("s3api", "list-buckets", "--query", "Buckets[].Name", "--output", "text")
        if "demo" not in result.stdout:
            fail("awscli list-buckets - expected demo bucket")
            return 1

        with TemporaryDirectory(prefix="mockbucket-compat-awscli.") as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            tmpfile = tmp_dir / "awscli.txt"
            outfile = tmp_dir / "awscli.out"
            tmpfile.write_text("cli-compat")
            _aws("s3api", "put-object", "--bucket", "demo", "--key", "compat/awscli.txt", "--body", str(tmpfile))
            _aws("s3api", "head-object", "--bucket", "demo", "--key", "compat/awscli.txt")
            _aws("s3api", "get-object", "--bucket", "demo", "--key", "compat/awscli.txt", str(outfile))
            out = outfile.read_text()
            if out != "cli-compat":
                fail("awscli get-object - content mismatch")
                return 1

        ok("awscli")
        return 0

    def _test_boto3(self) -> int:
        s3 = self._make_s3_client()

        buckets = [bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])]
        if "demo" not in buckets:
            fail("boto3 list_buckets - expected demo bucket")
            return 1

        s3.put_object(Bucket="demo", Key="compat/boto3.txt", Body=b"boto3-compat")

        head = s3.head_object(Bucket="demo", Key="compat/boto3.txt")
        if head.get("ContentLength") != 12:
            fail(f"boto3 head_object - content_length={head.get('ContentLength')}, want 12")
            return 1

        obj = s3.get_object(Bucket="demo", Key="compat/boto3.txt")
        body = obj["Body"].read().decode("utf-8")
        if body != "boto3-compat":
            fail("boto3 get_object - content mismatch")
            return 1

        resp = s3.list_objects_v2(Bucket="demo", Prefix="compat/")
        keys = [item["Key"] for item in resp.get("Contents", [])]
        if "compat/boto3.txt" not in keys:
            fail("boto3 list_objects_v2 - missing key")
            return 1

        ok("boto3")
        return 0

    def _test_boto3_virtual_hosted(self) -> int:
        skip("boto3 virtual-hosted - covered by server tests; custom endpoint SDK behavior is environment-sensitive")
        return 0

    def _test_presigned_urls(self) -> int:
        s3 = self._make_s3_client()

        put_url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": "demo", "Key": "compat/presigned.txt"},
            ExpiresIn=300,
            HttpMethod="PUT",
        )
        put_req = urllib.request.Request(put_url, data=b"presigned-compat", method="PUT")
        with urllib.request.urlopen(put_req) as resp:
            if resp.status != 200:
                fail(f"presigned PUT - status={resp.status}, want 200")
                return 1

        head_url = s3.generate_presigned_url(
            "head_object",
            Params={"Bucket": "demo", "Key": "compat/presigned.txt"},
            ExpiresIn=300,
            HttpMethod="HEAD",
        )
        head_req = urllib.request.Request(head_url, method="HEAD")
        with urllib.request.urlopen(head_req) as resp:
            if resp.status != 200:
                fail(f"presigned HEAD - status={resp.status}, want 200")
                return 1

        get_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "demo", "Key": "compat/presigned.txt"},
            ExpiresIn=300,
            HttpMethod="GET",
        )
        with urllib.request.urlopen(get_url) as resp:
            body = resp.read()
        if body != b"presigned-compat":
            fail(f"presigned GET - content={body!r}, want b'presigned-compat'")
            return 1

        ok("presigned urls")
        return 0

    def _test_multipart(self) -> int:
        s3 = self._make_s3_client()

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
        except Exception as err:
            s3.abort_multipart_upload(Bucket="demo", Key="compat/multipart.txt", UploadId=upload_id)
            fail(f"boto3 multipart - upload failed: {err}")
            return 1

        obj = s3.get_object(Bucket="demo", Key="compat/multipart.txt")
        content = obj["Body"].read()
        if content != b"part-one-part-two-":
            fail(f"boto3 multipart - content={content!r}, want b'part-one-part-two-'")
            return 1

        ok("boto3 multipart")
        return 0

    def _test_sts_assume_role(self) -> int:
        sts = self._make_sts_client()

        resp = sts.assume_role(
            RoleArn="arn:mockbucket:iam::role/data-reader",
            RoleSessionName="compat-test",
        )

        creds = resp.get("Credentials")
        if not creds:
            fail("sts assume_role - missing Credentials in response")
            return 1

        access_key = creds.get("AccessKeyId")
        secret_key = creds.get("SecretAccessKey")
        session_token = creds.get("SessionToken")

        if not access_key or not secret_key or not session_token:
            fail(f"sts assume_role - incomplete credentials: {creds}")
            return 1

        assumed_user = resp.get("AssumedRoleUser", {})
        if not assumed_user.get("Arn"):
            fail("sts assume_role - missing AssumedRoleUser.Arn")
            return 1

        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
        )

        buckets = [bucket["Name"] for bucket in s3.list_buckets().get("Buckets", [])]
        if "demo" not in buckets:
            fail("sts assume_role - S3 list_buckets with session creds failed")
            return 1

        obj = s3.get_object(Bucket="demo", Key="bootstrap/hello.txt")
        body = obj["Body"].read().decode("utf-8")
        if body != "hello from mockbucket":
            fail(f"sts assume_role - S3 get_object body={body!r}, want 'hello from mockbucket'")
            return 1

        ok("sts assume_role")
        return 0

    def _test_sts_allowed_roles(self) -> int:
        sts = self._make_sts_client(access_key="restricted", secret_key="restricted-secret")

        resp = sts.assume_role(
            RoleArn="arn:mockbucket:iam::role/data-reader",
            RoleSessionName="restricted-test",
        )
        if not resp.get("Credentials"):
            fail("sts allowed_roles - assume data-reader with restricted key failed")
            return 1

        ok("sts allowed_roles")
        return 0

    def _test_sts_get_caller_identity(self) -> int:
        sts = self._make_sts_client()

        resp = sts.get_caller_identity()
        if resp.get("Arn") != "arn:mockbucket:iam:::user/admin":
            fail(f"sts get_caller_identity - arn={resp.get('Arn')!r}, want 'arn:mockbucket:iam:::user/admin'")
            return 1
        if resp.get("UserId") != "admin":
            fail(f"sts get_caller_identity - user_id={resp.get('UserId')!r}, want 'admin'")
            return 1

        ok("sts get_caller_identity")
        return 0

    def _test_sts_get_session_token(self) -> int:
        import boto3

        sts = self._make_sts_client()
        resp = sts.get_session_token(DurationSeconds=1800)
        creds = resp.get("Credentials")
        if not creds:
            fail("sts get_session_token - missing Credentials in response")
            return 1

        session_sts = boto3.client(
            "sts",
            endpoint_url=ENDPOINT,
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
        identity = session_sts.get_caller_identity()
        if identity.get("Arn") != "arn:mockbucket:iam:::user/admin":
            fail(f"sts get_session_token - caller identity arn={identity.get('Arn')!r}, want 'arn:mockbucket:iam:::user/admin'")
            return 1

        ok("sts get_session_token")
        return 0

    def _test_duckdb(self) -> int:
        rows = 15_000_000
        con = s3_con(
            endpoint=ENDPOINT.removeprefix("http://"),
            key_id=os.environ["AWS_ACCESS_KEY_ID"],
            secret=os.environ["AWS_SECRET_ACCESS_KEY"],
            region=os.environ["AWS_REGION"],
        )
        try:
            write_parquet_s3(con, "s3://demo/duckdb", rows_per_file=rows, num_files=2)
        except Exception as err:
            fail(f"duckdb parquet - write failed: {err}")
            return 1
        count = read_count(con, "s3://demo/duckdb/*.parquet")
        if count != 2 * rows:
            fail(f"duckdb parquet - count={count}, want {2 * rows}")
            return 1
        ok("duckdb parquet")
        return 0

    def _test_pyspark(self) -> int:
        try:
            scenarios = s3a_roundtrip(
                endpoint=ENDPOINT.removeprefix("http://"),
                access_key=os.environ["AWS_ACCESS_KEY_ID"],
                secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                region=os.environ["AWS_REGION"],
                bucket="demo",
                key_prefix="compat/pyspark",
            )
        except Exception as err:
            fail(f"pyspark s3a compatibility - failed: {err}")
            return 1
        if scenarios <= 0:
            fail(f"pyspark s3a compatibility - scenarios={scenarios}, want > 0")
            return 1
        ok("pyspark s3a compatibility")
        return 0
