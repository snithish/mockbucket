"""Azure Blob Storage compatibility tests using azure-storage-blob SDK."""
from __future__ import annotations

import base64
import os
import urllib.request

from compat import ENDPOINT, fail, ok, skip

ACCOUNT_NAME = "mockstorage"
ACCOUNT_KEY = base64.b64encode(b"mockstorage-key-32bytes!!").decode()


def seed() -> str:
    """Return seed YAML with Azure accounts."""
    return f"""\
buckets:
  - demo
azure:
  accounts:
    - name: {ACCOUNT_NAME}
      key: {ACCOUNT_KEY}
objects:
  - bucket: demo
    key: bootstrap/hello.txt
    content: hello from mockbucket
"""


def export_env() -> dict[str, str]:
    """Return extra env vars needed by Azure tools."""
    return {
        "AZURE_STORAGE_ACCOUNT": ACCOUNT_NAME,
        "AZURE_STORAGE_KEY": ACCOUNT_KEY,
        "AZURE_STORAGE_CONNECTION_STRING": f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};BlobEndpoint={ENDPOINT};FileEndpoint={ENDPOINT};EndpointSuffix=core.windows.net",
    }


def run() -> int:
    """Run Azure Blob compat tests. Returns error count."""
    return _test_blob_sdk() + _test_blob_sdk_containers()


def _test_blob_sdk() -> int:
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        skip("azure-storage-blob — not installed")
        return 0

    conn_str = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};BlobEndpoint={ENDPOINT};EndpointSuffix=core.windows.net"

    try:
        client = BlobServiceClient.from_connection_string(conn_str)
    except Exception as e:
        fail(f"azure-storage-blob — failed to create client: {e}")
        return 1

    try:
        containers = list(client.list_containers())
        container_names = [c.name for c in containers]
        if "demo" not in container_names:
            fail(f"azure-storage-blob list_containers — missing demo, got: {container_names}")
            return 1
    except Exception as e:
        fail(f"azure-storage-blob list_containers — {e}")
        return 1

    try:
        container_client = client.get_container_client("demo")
        blob_client = container_client.get_blob_client("compat/sdk.txt")
        blob_client.upload_blob(b"sdk-compat", overwrite=True)

        props = blob_client.get_blob_properties()
        if props.size != 10:
            fail(f"azure-storage-blob get_blob_properties — size={props.size}, want 10")
            return 1

        content = blob_client.download_blob().readall()
        if content != b"sdk-compat":
            fail(f"azure-storage-blob download_blob — content={content!r}, want b'sdk-compat'")
            return 1
    except Exception as e:
        fail(f"azure-storage-blob blob operations — {e}")
        return 1

    ok("azure-storage-blob")
    return 0


def _test_blob_sdk_containers() -> int:
    try:
        from azure.storage.blob import BlobServiceClient
    except ImportError:
        skip("azure-storage-blob containers — not installed")
        return 0

    conn_str = f"DefaultEndpointsProtocol=http;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};BlobEndpoint={ENDPOINT};EndpointSuffix=core.windows.net"

    client = BlobServiceClient.from_connection_string(conn_str)

    test_container = "sdk-test-container"

    try:
        container_client = client.create_container(test_container)
        props = container_client.get_container_properties()
        if props is None:
            fail("azure-storage-blob create_container — props is None")
            return 1
    except Exception as e:
        fail(f"azure-storage-blob create_container — {e}")
        return 1

    try:
        client.delete_container(test_container)
    except Exception as e:
        fail(f"azure-storage-blob delete_container — {e}")
        return 1

    ok("azure-storage-blob containers")
    return 0
