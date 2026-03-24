"""GCS compatibility tests using the google-cloud-storage Python SDK."""
from __future__ import annotations

import os
import sys

from compat import ENDPOINT, fail, ok

from google.auth import credentials


SEED = """\
buckets:
  - compat-demo
principals:
  - name: gcs-admin
    policies:
      - statements:
          - effect: Allow
            actions: ["*"]
            resources: ["*"]
gcs:
  accounts:
    - client_email: gcs-admin@mock.iam.gserviceaccount.com
      token: gcs-static-test-token
      principal: gcs-admin
"""

STATIC_TOKEN = "gcs-static-test-token"


class SeedCredentials(credentials.Credentials):
    """google-auth credentials that reads a static bearer token from env."""

    def __init__(self) -> None:
        super().__init__()
        self.token = os.environ.get("MOCKBUCKET_GCS_TOKEN", STATIC_TOKEN)

    def refresh(self, _request) -> None:
        pass

    def before_request(self, request, method, url, headers):
        headers["authorization"] = f"Bearer {self.token}"


def configure() -> dict:
    """Return config overrides for the GCS frontend."""
    return {"s3": False, "sts": False, "gcs": True}


def seed() -> str:
    """Return the GCS-specific seed YAML."""
    return SEED


def export_env() -> dict[str, str]:
    """Return env vars for GCS tests."""
    return {"MOCKBUCKET_GCS_TOKEN": STATIC_TOKEN}


def _make_client() -> "storage.Client":
    """Create a google-cloud-storage client pointed at our mock server."""
    from google.cloud import storage

    creds = SeedCredentials()
    return storage.Client(
        credentials=creds,
        project="mock-project",
        client_options={"api_endpoint": ENDPOINT},
    )


def run() -> int:
    """Run all GCS compat tests. Returns error count."""
    errors = 0
    errors += _test_buckets()
    errors += _test_objects()
    errors += _test_multipart()
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
    from google.resumable_media.requests import MultipartUpload

    creds = SeedCredentials()
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
