# MockBucket

[![ci](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml/badge.svg)](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml)
[![release](https://img.shields.io/github/v/release/snithish/mockbucket)](https://github.com/snithish/mockbucket/releases)
[![docker](https://img.shields.io/badge/docker-ghcr.io%2Fsnithish%2Fmockbucket-blue)](https://github.com/snithish/mockbucket/pkgs/container/mockbucket)
[![license](https://img.shields.io/github/license/snithish/mockbucket)](LICENSE)
[![go version](https://img.shields.io/github/go-mod/go-version/snithish/mockbucket)](go.mod)

A standalone, local object-storage emulator for **S3**, **STS**, **Azure**, and
**GCS**. Run cloud-compatible workloads on your laptop or inside CI without
touching a live AWS or GCP account.

MockBucket persists object bytes on the filesystem and stores metadata in
SQLite. Authentication and authorization behavior is frontend-specific: S3
object APIs are open, STS is identity-aware, GCS requires an authenticated
subject, and Azure supports anonymous requests with optional SharedKey account
selection.

## Table of Contents

- [Quick Start](#quick-start)
  - [Docker](#docker)
  - [Pre-built Binary](#pre-built-binary)
  - [Build from Source](#build-from-source)
- [Docker Reference](#docker-reference)
  - [Basic Usage](#basic-usage)
  - [Docker Compose](#docker-compose)
  - [Persistent Storage](#persistent-storage)
- [Configuration](#configuration)
  - [Server](#server)
  - [Storage](#storage)
  - [Seed](#seed)
  - [Frontends](#frontends)
  - [Auth](#auth)
- [Seeding State](#seeding-state)
  - [Buckets](#buckets)
  - [Access Keys](#access-keys)
  - [Roles](#roles)
  - [GCS Service Credentials](#gcs-service-credentials)
  - [Azure Accounts](#azure-accounts)
  - [Objects](#objects)
- [Usage Examples](#usage-examples)
  - [AWS CLI](#aws-cli)
  - [Python (boto3)](#python-boto3)
  - [STS AssumeRole](#sts-assumerole)
- [Verifying Releases](#verifying-releases)
- [Architecture](#architecture)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

### Docker

The fastest way to get started. The image is published to GitHub Container Registry.

```sh
docker run -d \
  --name mockbucket \
  -p 9000:9000 \
  -v mockbucket-data:/var/data \
  ghcr.io/snithish/mockbucket:latest
```

MockBucket starts on `http://localhost:9000` with a default `demo` bucket.

Test it:

```sh
aws --endpoint-url http://localhost:9000 \
    s3 ls --no-sign-request
```

### Pre-built Binary

Download the latest release for your platform from the [Releases page](https://github.com/snithish/mockbucket/releases).

```sh
# Example for Linux amd64
curl -LO https://github.com/snithish/mockbucket/releases/latest/download/mockbucket_$(curl -s https://api.github.com/repos/snithish/mockbucket/releases/latest | grep tag_name | cut -d '"' -f4 | tr -d v)_linux_amd64.tar.gz
tar xzf mockbucket_*_linux_amd64.tar.gz
./mockbucketd --config mockbucket.yaml
```

Or download manually from the [Releases page](https://github.com/snithish/mockbucket/releases), extract, and run:

```sh
./mockbucketd --config mockbucket.yaml
```

### Build from Source

Requires Go 1.26.1 or newer.

```sh
git clone https://github.com/snithish/mockbucket.git
cd mockbucket
make build
./bin/mockbucketd --config mockbucket.yaml
```

Or install directly:

```sh
go install github.com/snithish/mockbucket/cmd/mockbucketd@latest
mockbucketd --config mockbucket.yaml
```

## Docker Reference

### Basic Usage

```sh
# Run with default seed data
docker run -d --name mockbucket -p 9000:9000 ghcr.io/snithish/mockbucket:latest

# Run with a custom config
docker run -d --name mockbucket -p 9000:9000 \
  -v ./mockbucket.yaml:/etc/mockbucket/config.yaml:ro \
  ghcr.io/snithish/mockbucket:latest --config /etc/mockbucket/config.yaml
```

### Docker Compose

```yaml
services:
  mockbucket:
    image: ghcr.io/snithish/mockbucket:latest
    ports:
      - "9000:9000"
    volumes:
      - ./docker/config.yaml:/etc/mockbucket/config.yaml:ro
    command: ["--config", "/etc/mockbucket/config.yaml"]

```

### Persistent Storage

The Docker image exposes `/var/data` as a volume mount point. To persist objects across container restarts:

```sh
docker run -d --name mockbucket -p 9000:9000 \
  -v mockbucket-data:/var/data \
  ghcr.io/snithish/mockbucket:latest
```

When using a custom config, ensure your `storage.root_dir` and `storage.sqlite_path` point inside `/var/data`:

```yaml
storage:
  root_dir: /var/data/objects
  sqlite_path: /var/data/mockbucket.db
```

## Configuration

MockBucket reads a YAML file supplied via `--config`. Copy the example to get started:

```sh
cp mockbucket.example.yaml mockbucket.yaml
```

### Server

```yaml
server:
  address: 127.0.0.1:9000    # listen address
  request_log: true           # log every request
  shutdown_timeout: 10s       # graceful shutdown timeout
```

### Storage

```yaml
storage:
  root_dir: ./var/objects         # filesystem root for object blobs
  sqlite_path: ./var/mockbucket.db # SQLite database for metadata
```

### Seed

Seed data is defined inline under the `seed:` key. See `mockbucket.example.yaml` for a complete reference.

```yaml
seed:
  buckets:
    - my-bucket
  roles:
    - name: data-reader
  s3:
    access_keys:
      - id: admin
        secret: admin-secret
      - id: restricted
        secret: restricted-secret
        allowed_roles:
          - data-reader
```

### Frontends

Toggle each protocol frontend. S3 and GCS are mutually exclusive at runtime. STS is automatically available when S3 is enabled. Azure frontends are selected via the `type` field:

```yaml
frontends:
  type: s3              # s3, gcs, azure_blob, azure_datalake
```

Supported frontend profiles:

| Frontend | Use case |
|----------|----------|
| `s3` | AWS S3 protocol subset with STS compatibility endpoint (default) |
| `gcs` | GCS protocol subset with authenticated-subject gating |
| `azure_blob` | Azure Blob protocol subset (anonymous or account-aware SharedKey mode) |
| `azure_datalake` | Azure Data Lake Gen2 subset plus Blob-compat bridge operations for SDKs |

## Design Caveats

### Terms used in this README

- **Authenticated:** a request must include credentials that resolve to a
  subject in MockBucket.
- **Identity-aware:** request behavior depends on identity-related seed data
  (roles, service accounts, sessions), even if request signatures are not
  cryptographically verified.
- **Authorization:** per-action/per-resource policy evaluation. MockBucket does
  not implement full IAM-style authorization for any frontend.

### Provider support and auth matrix

| Frontend | Authentication model | Identity behavior | Authorization model |
|----------|----------------------|-------------------|---------------------|
| `s3` | Not required | None for object APIs | Open object/bucket operations |
| `sts` (with `s3`) | SigV4 header parsed, signature not verified | `AssumeRole` requires seeded role; `allowed_roles` is enforced for known access keys | No trust-policy or action/resource authorization |
| `gcs` | Required bearer/access token subject | Subject resolved from seeded GCS tokens, seeded service-account tokens, or `/oauth2/v4/token` session issuance | Authenticated subjects are allowed; no bucket/object IAM checks |
| `azure_blob` | Anonymous allowed; optional `SharedKey` header | `SharedKey` requires known account name; signature is currently not verified | No per-operation authorization checks |
| `azure_datalake` | Same as `azure_blob` | Same as `azure_blob` | No per-operation authorization checks |

### Azure Data Lake committed support scope

The `azure_datalake` frontend is committed to these SDK-visible behaviors:

- Filesystem operations: list, create, get properties, delete.
- Path operations: create file, create directory, append, flush, read, head,
  delete, list.
- Blob-compatible bridge operations used by the Data Lake SDK:
  `comp=list`, `restype=container` create/get/head.

Anything outside this scope must return a clear unsupported/not-implemented
response instead of pretending parity with Azure cloud behavior.

### Additional caveats

- **No SigV4 verification.** S3 and STS requests are accepted even when the
  signature does not validate.
- **No trust-policy checks on STS `AssumeRole`.** Role existence plus
  `allowed_roles` enforcement is the current model.
- **No full IAM policy engine.** Action/resource authorization is intentionally
  minimal and frontend-specific.
- **GCS is authenticated-only.** Bearer/access token subject resolution is
  required, but bucket/object authorization decisions are not evaluated.
- **Azure SharedKey mode is account-aware, not signature-validating.** The
  account in `Authorization: SharedKey ...` must exist, but request signatures
  are not currently verified.
- **STS auto-enables with S3.** There is no `frontends.sts` config flag. STS is
  available only when `frontends.type: s3`.
- **Azure uses IP-style URLs.** Like
  [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#ip-style-url)
  with `--disableProductStyleUrl`, account name is part of the path.

If you need strict auth verification or full cloud IAM behavior, use a real
cloud sandbox or another tool. MockBucket focuses on deterministic local
protocol compatibility.

## Seeding State

The `seed:` section of the config file defines the entire initial state of MockBucket. See `mockbucket.example.yaml` for a complete reference.

### Buckets

Declare named buckets before any object creation:

```yaml
buckets:
  - my-bucket
  - logs-bucket
```

### Access Keys

Define S3 access keys under `s3.access_keys`. Any key works because the server does not verify signatures. Set `allowed_roles` to restrict which roles the key can assume via STS — omit it to allow any role:

```yaml
s3:
  access_keys:
    - id: admin
      secret: admin-secret
    - id: reader-key
      secret: reader-secret
      allowed_roles:
        - data-reader
```

### Roles

Define named roles for `sts:AssumeRole`. The role must exist in seed data:

```yaml
roles:
  - name: data-reader
  - name: data-writer
```

### GCS Service Credentials

For GCS, define `service_credentials` to auto-generate service account JSON for JWT authentication. Each entry maps a `client_email` to a `principal`:

```yaml
gcs:
  service_credentials:
    - client_email: admin@mockbucket.iam.gserviceaccount.com
      principal: admin
    - client_email: reader@mockbucket.iam.gserviceaccount.com
      principal: reader
  tokens:
    - token: "hardcoded-token-123"
      principal: admin
```

`GET /api/v1/gcs/service-account` returns generated JSON credentials and the
same seeded `principal` used for runtime token resolution.

### GCS Unsupported Features

The GCS frontend intentionally excludes several cloud features. MockBucket does
not currently implement:

- IAM policy APIs and policy binding evaluation
- Resumable uploads (`uploadType=resumable`)
- Object generations and metagenerations
- Preconditions (`ifGenerationMatch`, `ifMetagenerationMatch`, and variants)
- Rich object metadata parity (beyond the small metadata subset returned today)

### Azure Accounts

Define Azure storage accounts under `seed.azure.accounts`. Each entry requires a
`name` and a base64-encoded `key`. MockBucket uses
[IP-style URLs](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite#ip-style-url)
like Azurite: account name is parsed from the request path and matched against
seed data.

```yaml
seed:
  azure:
    accounts:
      - name: mockstorage
        key: bW9ja3N0b3JhZ2Uta2V5LTMyYnl0ZXMhIQ==
```

Point your SDK at the MockBucket endpoint using the account name in the connection string:

```text
DefaultEndpointsProtocol=http;AccountName=mockstorage;AccountKey=<base64>;BlobEndpoint=http://localhost:9000;EndpointSuffix=core.windows.net
```

The `azure_blob` and `azure_datalake` frontends are mutually exclusive at runtime (like S3 and GCS). STS is independent and can coexist with either Azure frontend.

### Objects

Bootstrap objects are created on startup:

```yaml
objects:
  - bucket: my-bucket
    key: config/app.json
    content: '{"version": "1.0"}'
  - bucket: my-bucket
    key: hello.txt
    content: hello from mockbucket
```

## Usage Examples

### AWS CLI

Configure the AWS CLI to point at MockBucket. Any access key works because the server does not verify credentials:

```sh
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=admin-secret
export AWS_EC2_METADATA_DISABLED=true

# List buckets
aws --endpoint-url http://localhost:9000 s3api list-buckets

# Create a bucket
aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket my-new-bucket

# Upload an object
echo "hello world" > /tmp/hello.txt
aws --endpoint-url http://localhost:9000 s3api put-object \
    --bucket demo --key test/hello.txt --body /tmp/hello.txt

# Download an object
aws --endpoint-url http://localhost:9000 s3api get-object \
    --bucket demo --key test/hello.txt /tmp/downloaded.txt
cat /tmp/downloaded.txt

# List objects
aws --endpoint-url http://localhost:9000 s3api list-objects-v2 --bucket demo

# Head object (metadata)
aws --endpoint-url http://localhost:9000 s3api head-object --bucket demo --key test/hello.txt
```

### Python (boto3)

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin-secret",
    region_name="us-east-1",
)

# List buckets
response = s3.list_buckets()
for bucket in response["Buckets"]:
    print(bucket["Name"])

# Upload
s3.put_object(Bucket="demo", Key="python/hello.txt", Body=b"hello from python")

# Download
obj = s3.get_object(Bucket="demo", Key="python/hello.txt")
print(obj["Body"].read().decode())

# List objects
response = s3.list_objects_v2(Bucket="demo")
for item in response.get("Contents", []):
    print(item["Key"])
```

### STS AssumeRole

STS is automatically available when S3 is enabled. `AssumeRole` succeeds for any role defined in seed data. Use `allowed_roles` on access keys to restrict which roles each key can assume:

```sh
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=admin-secret

# Assume a role (any key can assume any role if allowed_roles is empty)
CREDENTIALS=$(aws --endpoint-url http://localhost:9000 sts assume-role \
    --role-arn arn:mockbucket:iam::role/data-reader \
    --role-session-name test-session)

# Use temporary credentials with S3
export AWS_ACCESS_KEY_ID=$(echo $CREDENTIALS | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo $CREDENTIALS | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo $CREDENTIALS | jq -r '.Credentials.SessionToken')

# Access with assumed role credentials
aws --endpoint-url http://localhost:9000 s3api get-object \
    --bucket demo --key bootstrap/hello.txt /tmp/assumed.txt
```

If an access key has `allowed_roles: ["data-reader"]`, attempting to assume any other role returns `AccessDenied`.

## Verifying Releases

All release artifacts are signed with [Sigstore](https://sigstore.dev) cosign using keyless OIDC. You can verify the authenticity of any release binary or Docker image.

### Verify Binary Checksums

```sh
# Download checksums.txt and checksums.txt.sigstore.json from the release
cosign verify-blob \
  --certificate-identity "https://github.com/snithish/mockbucket/.github/workflows/release.yaml@refs/tags/v0.1.0" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  --bundle checksums.txt.sigstore.json \
  ./checksums.txt

# Then verify the binary
sha256sum -c checksums.txt
```

### Verify Docker Image

```sh
cosign verify \
  --certificate-identity "https://github.com/snithish/mockbucket/.github/workflows/docker.yaml@refs/tags/v0.1.0" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  ghcr.io/snithish/mockbucket:v0.1.0
```

### Verify SBOM

Each release includes Software Bill of Materials (SBOM) artifacts. Check the release assets for `.spdx.json` files.

## Architecture

```
cmd/mockbucketd/          -- entrypoint: flags, config, logging, signal handling
internal/
  server/                 -- HTTP server, router, middleware, graceful shutdown
  config/                 -- YAML config schema, validation, defaults
  storage/                -- filesystem object store, SQLite metadata, multipart
  auth/
    gcp/                  -- GCS bearer token auth
  iam/                    -- session management (GCS tokens, STS sessions)
  seed/                   -- seed validation, bootstrapping
  frontends/              -- protocol adapters
    s3/                   -- S3 wire protocol (ListBuckets, PutObject, etc.)
    sts/                  -- STS wire protocol (AssumeRole)
    gcs/                  -- GCS wire protocol
    azure_blob/             -- Azure Blob Storage wire protocol
    azure_datalake/         -- Azure Data Lake Gen2 wire protocol
  httpx/                  -- shared middleware, error mapping, request context
  core/                   -- sentinel errors, domain models
```

Object bytes are streamed to the filesystem. Bucket/object metadata, listings,
multipart state, and session records live in SQLite. S3 object APIs are open;
STS is identity-aware; GCS requires authenticated subjects; Azure supports
anonymous mode with optional SharedKey account selection.

## Testing

### Unit Tests

```sh
make test
# or
go test ./...
```

### Run a Single Package or Test

```sh
go test ./internal/iam
go test ./internal/iam -run TestSessionManagerAssumeRole
go test ./internal/server -run TestS3FrontendContract/BucketLevelAPI
go test -v ./internal/server -run TestSTSAssumeRoleAndSessionCanHeadBucket
```

### Compatibility Suite

The compatibility suite runs real AWS SDK and CLI tools against a live MockBucket instance. Requires Python with `uv`:

```sh
# Run all tests
uv run scripts/compat/run_all.py test

# AWS tests only
uv run scripts/compat/run_all.py test aws

# GCS tests only
uv run scripts/compat/run_all.py test gcs

# Azure Blob tests only
uv run scripts/compat/run_all.py test azure_blob

# Azure Data Lake tests only
uv run scripts/compat/run_all.py test azure_datalake

# Start server for manual testing
uv run scripts/compat/run_all.py serve

# Verbose HTTP logging
uv run scripts/compat/run_all.py --debug test
```

### Lint and Format

```sh
make fmt         # auto-format all Go files
make fmt-check   # check formatting (CI-friendly)
make lint        # run go vet
```

## Release Process

Releases are automated via GitHub Actions:

1. **Tag a release**: `git tag v0.1.0 && git push origin v0.1.0`
2. **GoReleaser** builds cross-platform binaries for Linux, macOS, and Windows (amd64/arm64)
3. **Docker images** are built for linux/amd64 and linux/arm64 and pushed to `ghcr.io/snithish/mockbucket`
4. **Cosign** signs all artifacts (binaries, checksums, container images) using Sigstore keyless OIDC
5. **SBOMs** are generated for every archive
6. **SLSA provenance** attestations are attached to the release

### Security

- All GitHub Actions are pinned to commit SHAs to prevent supply chain attacks
- Release workflow requires manual approval via GitHub environment protection
- Binaries and container images are signed and verifiable with cosign
- Container images are scanned with Trivy on every build
- Docker image runs as nonroot on a distroless base

## Contributing

1. Fork the repo and create a feature branch.
2. Add tests for new behavior and ensure `go test ./...` passes.
3. Run `make fmt` before committing.
4. Use the compatibility scripts when touching protocol behavior.
5. Open a PR against `main`.

Please see `plan.md` for the phased implementation roadmap and phase boundaries.

## License

[MIT](LICENSE)
