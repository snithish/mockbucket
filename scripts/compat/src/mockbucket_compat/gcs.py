"""GCS compatibility tests using the google-cloud-storage Python SDK."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from .compat import ENDPOINT, fail, ok, skip
from .pyspark import gcs_roundtrip
from .suite import CompatSuite

SEED = """\
buckets:
  - compat-demo
gcs:
  service_credentials:
    - client_email: gcs-admin@mockbucket.iam.gserviceaccount.com
      principal: gcs-admin
"""


class GCSCompatSuite(CompatSuite):
    name = "gcs"
    frontend = "gcs"

    def seed(self) -> str:
        return SEED

    def _fetch_service_account_info(self) -> dict[str, Any]:
        url = f"{ENDPOINT}/api/v1/gcs/service-account"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read())
            accounts = data.get("service_accounts", [])
            if not accounts:
                raise RuntimeError("No service accounts returned from /api/v1/gcs/service-account")
            for account in accounts:
                if account.get("client_email", "").startswith("gcs-admin@"):
                    return account["secret_json"]
            return accounts[0]["secret_json"]
        except Exception as err:
            raise RuntimeError(f"Failed to fetch service account JSON: {err}") from err

    def _make_client(self) -> "storage.Client":
        from google.cloud import storage
        from google.oauth2 import service_account

        service_account_info = self._fetch_service_account_info()
        creds = service_account.Credentials.from_service_account_info(service_account_info)
        return storage.Client(
            credentials=creds,
            project="mockbucket",
            client_options={"api_endpoint": ENDPOINT},
        )

    def _make_blob(self, bucket: str, key: str):
        client = self._make_client()
        return client, client.bucket(bucket).blob(key)

    def run(self, with_pyspark: bool = False) -> int:
        errors = 0
        errors += self._test_buckets()
        errors += self._test_objects()
        errors += self._test_multipart()
        self._test_duckdb()
        if with_pyspark:
            errors += self._test_pyspark()
        return errors

    def _test_buckets(self) -> int:
        client = self._make_client()

        buckets = [bucket.name for bucket in client.list_buckets()]
        if "compat-demo" not in buckets:
            fail(f"gcs list_buckets - expected compat-demo, got {buckets}")
            return 1

        bucket = client.get_bucket("compat-demo")
        if bucket.name != "compat-demo":
            fail(f"gcs get_bucket - name={bucket.name}, want compat-demo")
            return 1

        ok("gcs buckets")
        return 0

    def _test_objects(self) -> int:
        client, blob = self._make_blob("compat-demo", "compat/gcs-sdk-test.txt")
        bucket = client.bucket("compat-demo")
        blob.upload_from_string(b"gcs-sdk-compat-content")

        blob = bucket.get_blob("compat/gcs-sdk-test.txt")
        if blob is None:
            fail("gcs upload - blob not found after upload")
            return 1

        content = blob.download_as_bytes()
        if content != b"gcs-sdk-compat-content":
            fail(f"gcs get_object - content={content!r}")
            return 1

        blobs = list(client.list_blobs("compat-demo", prefix="compat/"))
        keys = [item.name for item in blobs]
        if "compat/gcs-sdk-test.txt" not in keys:
            fail(f"gcs list_objects - compat/gcs-sdk-test.txt not in {keys}")
            return 1

        blob.delete()

        deleted = bucket.get_blob("compat/gcs-sdk-test.txt")
        if deleted is not None:
            fail("gcs delete_object - still exists after delete")
            return 1

        ok("gcs objects")
        return 0

    def _test_multipart(self) -> int:
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2 import service_account
        from google.resumable_media.requests import MultipartUpload

        service_account_info = self._fetch_service_account_info()
        creds = service_account.Credentials.from_service_account_info(service_account_info)
        transport = AuthorizedSession(creds)

        key = "compat/multipart-test.txt"
        upload_url = f"{ENDPOINT}/upload/storage/v1/b/compat-demo/o?uploadType=multipart"

        upload = MultipartUpload(upload_url)
        data = b"part-one-part-two-"
        metadata = {"name": key}
        resp = upload.transmit(transport, data, metadata, "application/octet-stream")

        if resp.status_code >= 400:
            fail(f"gcs multipart - upload failed: status={resp.status_code} body={resp.text}")
            return 1

        client = self._make_client()
        blob = client.bucket("compat-demo").blob(key)
        content = blob.download_as_bytes()
        if content != data:
            fail(f"gcs multipart - content={content!r}, want {data!r}")
            return 1

        blob.delete()

        ok("gcs multipart")
        return 0

    def _test_duckdb(self) -> None:
        skip("duckdb - GCS requires native extension (github.com/northpolesec/duckdb-gcs) which does not support custom endpoints")

    def _test_pyspark(self) -> int:
        try:
            scenarios = gcs_roundtrip(
                endpoint=ENDPOINT,
                service_account_info=self._fetch_service_account_info(),
                bucket="compat-demo",
                key_prefix="pyspark-gcs",
            )
        except Exception as err:
            fail(f"pyspark gcs compatibility - failed: {err}")
            return 1
        if scenarios <= 0:
            fail(f"pyspark gcs compatibility - scenarios={scenarios}, want > 0")
            return 1
        ok("pyspark gcs compatibility")
        return 0
