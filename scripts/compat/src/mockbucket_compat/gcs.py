"""GCS compatibility tests using the google-cloud-storage Python SDK."""

from __future__ import annotations

import json
import urllib.request
import uuid
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
        errors += self._test_listing_edges()
        errors += self._test_multipart()
        errors += self._test_compose()
        errors += self._test_signed_urls()
        errors += self._test_bucket_deletion()
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
        from google.api_core.exceptions import PreconditionFailed

        client, blob = self._make_blob("compat-demo", "compat/gcs-sdk-test.txt")
        bucket = client.bucket("compat-demo")
        blob.upload_from_string(b"gcs-sdk-compat-content", if_generation_match=0)

        blob = bucket.get_blob("compat/gcs-sdk-test.txt")
        if blob is None:
            fail("gcs upload - blob not found after upload")
            return 1
        if blob.generation is None or blob.metageneration is None:
            fail(f"gcs upload - generation={blob.generation} metageneration={blob.metageneration}")
            return 1

        first_generation = blob.generation
        first_metageneration = blob.metageneration

        matched = bucket.get_blob(
            "compat/gcs-sdk-test.txt",
            if_generation_match=first_generation,
            if_metageneration_match=first_metageneration,
        )
        if matched is None:
            fail("gcs get_object - matching generation preconditions returned no blob")
            return 1

        content = blob.download_as_bytes()
        if content != b"gcs-sdk-compat-content":
            fail(f"gcs get_object - content={content!r}")
            return 1

        blob.upload_from_string(b"gcs-sdk-compat-content-v2", if_generation_match=first_generation)
        blob.reload()
        if blob.generation == first_generation:
            fail(f"gcs overwrite - generation={blob.generation}, want increment from {first_generation}")
            return 1
        if blob.metageneration != 1:
            fail(f"gcs overwrite - metageneration={blob.metageneration}, want 1")
            return 1

        try:
            bucket.blob("compat/gcs-sdk-test.txt").upload_from_string(
                b"stale-write",
                if_generation_match=first_generation,
            )
        except PreconditionFailed:
            pass
        else:
            fail("gcs overwrite - stale if_generation_match unexpectedly succeeded")
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

    def _test_listing_edges(self) -> int:
        client = self._make_client()
        bucket = client.bucket("compat-demo")
        keys = [
            "compat/listing/a.txt",
            "compat/listing/sub/b.txt",
            "compat/listing/sub/c.txt",
        ]
        for key in keys:
            bucket.blob(key).upload_from_string(key.encode("utf-8"))

        iterator = client.list_blobs("compat-demo", prefix="compat/listing/", delimiter="/", max_results=1)
        first_page = next(iterator.pages)
        first_items = [blob.name for blob in first_page]
        if first_items != ["compat/listing/a.txt"]:
            fail(f"gcs list_blobs delimiter page 1 - items={first_items!r}, want ['compat/listing/a.txt']")
            return 1
        if "compat/listing/sub/" not in iterator.prefixes:
            fail(f"gcs list_blobs delimiter - prefixes={iterator.prefixes!r}, want compat/listing/sub/")
            return 1

        for key in keys:
            bucket.blob(key).delete()

        ok("gcs listing edges")
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

    def _test_compose(self) -> int:
        client = self._make_client()
        bucket = client.bucket("compat-demo")

        src1 = bucket.blob("compat/compose-src-1.txt")
        src2 = bucket.blob("compat/compose-src-2.txt")
        dst = bucket.blob("compat/compose-dst.txt")
        src1.upload_from_string(b"hello ")
        src2.upload_from_string(b"world")

        dst.compose([src1, src2])
        content = dst.download_as_bytes()
        if content != b"hello world":
            fail(f"gcs compose - content={content!r}, want b'hello world'")
            return 1

        src1.delete()
        src2.delete()
        dst.delete()

        ok("gcs compose")
        return 0

    def _test_signed_urls(self) -> int:
        service_account_info = self._fetch_service_account_info()
        client = self._make_client()
        bucket = client.bucket("compat-demo")
        blob = bucket.blob("compat/signed-url.txt")

        signed_put = blob.generate_signed_url(
            expiration=300,
            method="PUT",
            content_type="text/plain",
            api_access_endpoint=ENDPOINT,
            version="v4",
            service_account_email=service_account_info["client_email"],
            credentials=client._credentials,
        )
        put_req = urllib.request.Request(
            signed_put,
            data=b"signed-url-body",
            method="PUT",
            headers={"Content-Type": "text/plain"},
        )
        try:
            with urllib.request.urlopen(put_req, timeout=1.0) as resp:
                put_status = resp.status
        except Exception as err:
            fail(f"gcs signed urls - PUT failed: {err}")
            return 1
        if put_status >= 400:
            fail(f"gcs signed urls - PUT status={put_status}")
            return 1

        signed_get = blob.generate_signed_url(
            expiration=300,
            method="GET",
            api_access_endpoint=ENDPOINT,
            version="v4",
            service_account_email=service_account_info["client_email"],
            credentials=client._credentials,
        )
        try:
            with urllib.request.urlopen(signed_get, timeout=1.0) as resp:
                content = resp.read()
        except Exception as err:
            fail(f"gcs signed urls - GET failed: {err}")
            return 1
        if content != b"signed-url-body":
            fail(f"gcs signed urls - content={content!r}, want b'signed-url-body'")
            return 1

        blob.delete()

        ok("gcs signed urls")
        return 0

    def _test_bucket_deletion(self) -> int:
        from google.api_core.exceptions import Conflict

        client = self._make_client()
        suffix = uuid.uuid4().hex[:12]
        empty_bucket_name = f"compat-empty-bucket-{suffix}"
        nonempty_bucket_name = f"compat-nonempty-bucket-{suffix}"

        empty_bucket = client.bucket(empty_bucket_name)
        empty_bucket.create()
        empty_bucket.delete()

        nonempty_bucket = client.bucket(nonempty_bucket_name)
        nonempty_bucket.create()
        nonempty_bucket.blob("file.txt").upload_from_string(b"x")
        try:
            nonempty_bucket.delete()
        except Conflict:
            pass
        else:
            fail("gcs bucket deletion - non-empty bucket delete unexpectedly succeeded")
            return 1
        finally:
            nonempty_bucket.blob("file.txt").delete()
            nonempty_bucket.delete()

        ok("gcs bucket deletion")
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
            if _is_gcs_connector_bootstrap_error(err):
                skip(f"pyspark gcs compatibility - connector unavailable in this environment ({err})")
                return 0
            fail(f"pyspark gcs compatibility - failed: {err}")
            return 1
        if scenarios <= 0:
            fail(f"pyspark gcs compatibility - scenarios={scenarios}, want > 0")
            return 1
        ok("pyspark gcs compatibility")
        return 0


def _is_gcs_connector_bootstrap_error(err: Exception) -> bool:
    text = str(err)
    markers = (
        "GoogleHadoopFileSystem not found",
        "NoSuchMethodError",
        "unknown resolver",
        "gcs-connector",
    )
    return any(marker in text for marker in markers)
