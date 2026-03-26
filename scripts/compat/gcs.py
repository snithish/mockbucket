"""GCS compatibility tests using the google-cloud-storage Python SDK."""
from __future__ import annotations

import json
import os
import sys
import urllib.request

from compat import ENDPOINT, fail, ok, skip


SEED = """\
buckets:
  - compat-demo
gcs:
  service_credentials:
    - client_email: gcs-admin@mockbucket.iam.gserviceaccount.com
      principal: gcs-admin
"""


def seed() -> str:
    """Return the GCS-specific seed YAML."""
    return SEED


def export_env() -> dict[str, str]:
    """Return env vars for GCS tests."""
    return {}


def _fetch_service_account_info() -> dict:
    """Fetch service account JSON from the mock server."""
    url = f"{ENDPOINT}/api/v1/gcs/service-account"
    try:
        resp = urllib.request.urlopen(url)
        data = json.loads(resp.read())
        accounts = data.get("service_accounts", [])
        if not accounts:
            raise RuntimeError("No service accounts returned from /api/v1/gcs/service-account")
        # Use the first service account (gcs-admin)
        for account in accounts:
            if account.get("client_email", "").startswith("gcs-admin@"):
                return account["secret_json"]
        # Fall back to first account if no match
        return accounts[0]["secret_json"]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch service account JSON: {e}") from e


def _make_client() -> "storage.Client":
    """Create a google-cloud-storage client pointed at our mock server."""
    from google.cloud import storage
    from google.oauth2 import service_account

    service_account_info = _fetch_service_account_info()
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    return storage.Client(
        credentials=creds,
        project="mockbucket",
        client_options={"api_endpoint": ENDPOINT},
    )


def run() -> int:
    """Run all GCS compat tests. Returns error count."""
    errors = 0
    errors += _test_buckets()
    errors += _test_objects()
    errors += _test_multipart()
    _test_duckdb()
    return errors


def _test_buckets() -> int:
    """Test bucket list and get via the GCS SDK."""
    client = _make_client()

    # List buckets — compat-demo should exist from seed.
    buckets = [b.name for b in client.list_buckets()]
    if "compat-demo" not in buckets:
        fail(f"gcs list_buckets — expected compat-demo, got {buckets}")
        return 1

    # Get bucket metadata.
    bucket = client.get_bucket("compat-demo")
    if bucket.name != "compat-demo":
        fail(f"gcs get_bucket — name={bucket.name}, want compat-demo")
        return 1

    ok("gcs buckets")
    return 0


def _test_objects() -> int:
    """Test object CRUD via the GCS SDK."""
    client = _make_client()
    bucket = client.bucket("compat-demo")

    # Upload an object.
    blob = bucket.blob("compat/gcs-sdk-test.txt")
    blob.upload_from_string(b"gcs-sdk-compat-content")

    # Verify metadata.
    blob = bucket.get_blob("compat/gcs-sdk-test.txt")
    if blob is None:
        fail("gcs upload — blob not found after upload")
        return 1

    # Download and verify content.
    content = blob.download_as_bytes()
    if content != b"gcs-sdk-compat-content":
        fail(f"gcs get_object — content={content!r}")
        return 1

    # List objects with prefix.
    blobs = list(client.list_blobs("compat-demo", prefix="compat/"))
    keys = [b.name for b in blobs]
    if "compat/gcs-sdk-test.txt" not in keys:
        fail(f"gcs list_objects — compat/gcs-sdk-test.txt not in {keys}")
        return 1

    # Delete object.
    blob.delete()

    # Verify deletion.
    deleted = bucket.get_blob("compat/gcs-sdk-test.txt")
    if deleted is not None:
        fail("gcs delete_object — still exists after delete")
        return 1

    ok("gcs objects")
    return 0


def _test_multipart() -> int:
    """Test multipart upload via google-resumable-media."""
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account
    from google.resumable_media.requests import MultipartUpload

    service_account_info = _fetch_service_account_info()
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    transport = AuthorizedSession(creds)

    key = "compat/multipart-test.txt"
    upload_url = f"{ENDPOINT}/upload/storage/v1/b/compat-demo/o?uploadType=multipart"

    upload = MultipartUpload(upload_url)
    data = b"part-one-part-two-"
    metadata = {"name": key}
    resp = upload.transmit(transport, data, metadata, "application/octet-stream")

    if resp.status_code >= 400:
        fail(f"gcs multipart — upload failed: status={resp.status_code} body={resp.text}")
        return 1

    # Verify the assembled object.
    client = _make_client()
    blob = client.bucket("compat-demo").blob(key)
    content = blob.download_as_bytes()
    if content != data:
        fail(f"gcs multipart — content={content!r}, want {data!r}")
        return 1

    ok("gcs multipart")
    return 0


def _test_duckdb() -> None:
    skip("duckdb — GCS requires native extension (github.com/northpolesec/duckdb-gcs) which does not support custom endpoints")
