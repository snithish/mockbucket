"""Azure Data Lake Gen2 compatibility tests using azure-storage-file-datalake SDK."""
from __future__ import annotations

import base64

from compat import ENDPOINT, fail, ok, skip

ACCOUNT_NAME = "mockstorage"
ACCOUNT_KEY = base64.b64encode(b"mockstorage-key-32bytes!!").decode()


def configure() -> dict:
    """Return config overrides for the Azure Data Lake frontend."""
    return {"s3": False, "gcs": False, "azure": True}


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
    """Run Azure Data Lake compat tests. Returns error count."""
    return _test_datalake_sdk()


def _test_datalake_sdk() -> int:
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        skip("azure-storage-file-datalake — not installed")
        return 0

    account_url = f"{ENDPOINT}/{ACCOUNT_NAME}"

    try:
        service_client = DataLakeServiceClient(account_url=account_url, credential=ACCOUNT_KEY)
    except Exception as e:
        fail(f"azure-storage-file-datalake — failed to create client: {e}")
        return 1

    try:
        filesystems = list(service_client.list_file_systems())
        fs_names = [fs.name for fs in filesystems]
        if "demo" not in fs_names:
            fail(f"azure-storage-file-datalake list_file_systems — missing demo, got: {fs_names}")
            return 1
    except Exception as e:
        fail(f"azure-storage-file-datalake list_file_systems — {e}")
        return 1

    try:
        fs_client = service_client.get_file_system_client("demo")
        file_client = fs_client.get_file_client("compat/datalake.txt")
        file_client.create_file()
        file_client.append_data(b"datalake-compat", offset=0)
        file_client.flush_data(offset=9)

        props = file_client.get_file_properties()
        if props.size != 15:
            fail(f"azure-storage-file-datalake get_file_properties — size={props.size}, want 15")
            return 1

        download = file_client.download_file()
        content = download.readall()
        if content != b"datalake-compat":
            fail(f"azure-storage-file-datalake download_file — content={content!r}")
            return 1
    except Exception as e:
        fail(f"azure-storage-file-datalake file operations — {e}")
        return 1

    ok("azure-storage-file-datalake")
    return 0
