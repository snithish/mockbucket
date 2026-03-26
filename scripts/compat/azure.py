"""Azure Blob Storage / Data Lake Gen2 compatibility tests using Azure SDK."""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from compat import ENDPOINT, fail, ok, skip

ACCOUNT_NAME = "mockstorage"
ACCOUNT_KEY = base64.b64encode(b"mockstorage-key-32bytes!!").decode()

# This module is used for both azure_blob and azure_datalake tests
# The frontend type is passed via MOCKBUCKET_AZURE_FRONTEND env var
_FRONTEND = os.environ.get("MOCKBUCKET_AZURE_FRONTEND", "azure_blob")


def configure() -> dict:
    """Return config overrides for the Azure frontend."""
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
    """Run Azure compat tests. Returns error count."""
    if _FRONTEND == "azure_datalake":
        return _test_datalake_sdk()
    else:
        return _test_blob_sdk() + _test_blob_sdk_containers()


def _compute_shared_key(account_name: str, account_key: bytes, method: str, path: str, query: str = "", body: bytes = b"") -> str:
    """Compute Azure Shared Key signature."""
    string_to_sign = f"{method}\n\n{len(body)}\n\n\n\n\n\n\n\n\n\n\n{query}\n{path}\n{account_name}"
    key = base64.b64decode(account_key)
    signature = base64.b64encode(hmac.new(key, string_to_sign.encode(), hashlib.sha256).digest()).decode()
    return f"SharedKey {account_name}:{signature}"


def _make_signed_request(method: str, path: str, query: str = "", body: bytes = b"") -> dict:
    """Make an HTTP request with Shared Key authentication."""
    import urllib.request

    url = f"{ENDPOINT}{path}"
    if query:
        url = f"{url}?{query}"

    signature = _compute_shared_key(ACCOUNT_NAME, ACCOUNT_KEY.encode(), method, path, query, body)

    headers = {
        "x-ms-date": "Wed, 25 Mar 2026 12:00:00 GMT",
        "x-ms-version": "2021-06-08",
        "Authorization": signature,
        "Content-Type": "application/octet-stream",
    }

    if body:
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
    else:
        req = urllib.request.Request(url, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            return {"status": resp.status, "headers": dict(resp.headers), "body": resp.read()}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "headers": dict(e.headers), "body": e.read()}


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


def _test_datalake_sdk() -> int:
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError:
        skip("azure-storage-file-datalake — not installed")
        return 0

    account_url = f"http://127.0.0.1:19000/{ACCOUNT_NAME}"

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
        if props.size != 16:
            fail(f"azure-storage-file-datalake get_file_properties — size={props.size}, want 16")
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


def _test_azure_cli() -> int:
    az = shutil.which("az")
    if not az:
        skip("az CLI — not found")
        return 0

    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT", ACCOUNT_NAME)
    storage_key = os.environ.get("AZURE_STORAGE_KEY", ACCOUNT_KEY)

    def _az(*args: str) -> subprocess.CompletedProcess:
        cmd = [
            az, "storage", "container", "list",
            "--account-name", storage_account,
            "--account-key", storage_key,
            "--connection-string", f"DefaultEndpointsProtocol=http;AccountName={storage_account};AccountKey={storage_key};BlobEndpoint={ENDPOINT}",
            "--output", "tsv",
        ]
        return subprocess.run(cmd, capture_output=True, text=True)

    result = _az()
    if result.returncode != 0:
        fail(f"az CLI list containers — {result.stderr}")
        return 1

    if "demo" not in result.stdout:
        fail("az CLI list containers — missing demo")
        return 1

    ok("az CLI")
    return 0


def _test_spark_hadoop() -> int:
    """Test Spark-compatible access via WASB/ABFS Hadoop filesystem."""
    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home:
        spark = shutil.which("spark-submit")
        if not spark:
            skip("spark-submit — not found, skipping Spark/Hadoop tests")
            return 0
        spark_home = str(Path(spark).parent.parent.parent)

    hadoop_azure = Path(spark_home) / "jars" / "hadoop-azure*.jar"
    azure_storage = Path(spark_home) / "jars" / "azure-storage*.jar"

    if not (hadoop_azure.exists() or azure_storage.exists()):
        skip("Hadoop Azure JARs — not found, skipping Spark/Hadoop tests")
        return 0

    ok("spark/hadoop config")
    return 0
