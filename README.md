# MockBucket

[![ci](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml/badge.svg)](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml)
[![release](https://img.shields.io/github/v/release/snithish/mockbucket)](https://github.com/snithish/mockbucket/releases)
[![docker](https://img.shields.io/badge/docker-ghcr.io%2Fsnithish%2Fmockbucket-blue)](https://github.com/snithish/mockbucket/pkgs/container/mockbucket)
[![license](https://img.shields.io/github/license/snithish/mockbucket)](LICENSE)
[![go version](https://img.shields.io/github/go-mod/go-version/snithish/mockbucket)](go.mod)

> Fast, deterministic object-storage emulator for S3, STS, and GCS, built for testing, not production.

MockBucket is a standalone local emulator for cloud object-storage workflows.
Run integration tests and CI jobs against S3-, STS-, and GCS-compatible
endpoints without touching live AWS or GCP accounts.

Object bytes are stored on the filesystem and metadata lives in SQLite.
Authentication and authorization behavior is intentionally frontend-specific:
S3 object APIs are open, STS is identity-aware, and GCS requires an
authenticated subject.

## Why MockBucket?

Testing cloud storage locally is still painful:

- full cloud environments are slow, flaky, and expensive
- some local emulators are heavy or non-deterministic in CI
- auth and identity flows are often missing or hard to control

MockBucket focuses on the local testing path:

- deterministic behavior
- no cloud dependencies
- seeded state for buckets, objects, access keys, roles, and tokens
- real protocol coverage for S3, STS, and GCS

## What Is This?

MockBucket is designed for:

- integration tests
- CI pipelines
- local development
- compatibility testing against real SDKs and CLIs

It currently simulates:

- S3 object-storage workflows, including copy, multipart upload, virtual-hosted-style addressing, and common presigned URL access
- STS `AssumeRole` flows alongside S3
- GCS object-storage workflows, including media, multipart, resumable upload, and rewrite

Azure is not implemented in this repository yet. If you need Azure support,
that is future work rather than a current feature.

## Table of Contents

- [Quickstart](#quickstart)
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
  - [Objects](#objects)
- [Usage Examples](#usage-examples)
  - [AWS CLI](#aws-cli)
  - [Python (boto3)](#python-boto3)
  - [Python (google-cloud-storage)](#python-google-cloud-storage)
  - [STS AssumeRole](#sts-assumerole)
- [Verifying Releases](#verifying-releases)
- [Architecture](#architecture)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

## Quickstart

Get a local S3-compatible endpoint running in about 30 seconds:

```sh
docker run -d \
  --name mockbucket \
  -p 9000:9000 \
  -v mockbucket-data:/var/data \
  ghcr.io/snithish/mockbucket:latest
```

That starts MockBucket on `http://localhost:9000` with the default seeded
`demo` bucket.

Smoke-test it with the AWS CLI:

```sh
aws --endpoint-url http://localhost:9000 \
    s3 ls --no-sign-request
```

If you are working from a source checkout, the shortest local path is:

```sh
cp mockbucket.example.yaml mockbucket.yaml
make run
```

### Docker

The image is published to GitHub Container Registry.

```sh
docker run -d \
  --name mockbucket \
  -p 9000:9000 \
  -v mockbucket-data:/var/data \
  ghcr.io/snithish/mockbucket:latest
```

MockBucket starts on `http://localhost:9000` with a default `demo` bucket.

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
  request_capture:
    enabled: false            # write requests to .http files when enabled
    path: ./var/requests      # destination directory for captured requests
  shutdown_timeout: 10s       # graceful shutdown timeout
```

Captured requests are written as one `.http` file per request for manual
inspection. This feature is enabled only through the YAML config.

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

Toggle each protocol frontend. S3 and GCS are mutually exclusive at runtime.
STS is automatically available when S3 is enabled:

```yaml
frontends:
  type: s3              # s3, gcs
```

Supported frontend profiles:

| Frontend | Use case |
|----------|----------|
| `s3` | AWS S3 protocol subset with STS compatibility endpoint (default) |
| `gcs` | GCS protocol subset with authenticated-subject gating |

Azure is not currently implemented in this repo. If you need Azure-adjacent
testing today, use the `gcs` or `s3` frontends depending on the client behavior
you need, and contributions to add Azure support are welcome.

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

### Additional caveats

- **No SigV4 verification.** S3 and STS requests are accepted even when header-
  or query-based signatures do not validate.
- **No trust-policy checks on STS `AssumeRole`.** Role existence plus
  `allowed_roles` enforcement is the current model.
- **No full IAM policy engine.** Action/resource authorization is intentionally
  minimal and frontend-specific.
- **GCS is authenticated-only.** Bearer/access token subject resolution is
  required, but bucket/object authorization decisions are not evaluated.
- **STS auto-enables with S3.** There is no `frontends.sts` config flag. STS is
  available only when `frontends.type: s3`.

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
- Durable generation and metageneration counters
- Preconditions (`ifGenerationMatch`, `ifMetagenerationMatch`, and variants)
- Rich object metadata parity (beyond the small metadata subset returned today)

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

### Python (google-cloud-storage)

Switch the frontend to `gcs` in your config, then use the generated service
account JSON exposed by MockBucket:

```python
import json
import urllib.request

from google.cloud import storage
from google.oauth2 import service_account

ENDPOINT = "http://localhost:9000"

with urllib.request.urlopen(f"{ENDPOINT}/api/v1/gcs/service-account") as resp:
    payload = json.load(resp)

service_account_info = payload["service_accounts"][0]["secret_json"]
creds = service_account.Credentials.from_service_account_info(service_account_info)

client = storage.Client(
    credentials=creds,
    project="mockbucket",
    client_options={"api_endpoint": ENDPOINT},
)

for bucket in client.list_buckets():
    print(bucket.name)

blob = client.bucket("demo").blob("python/gcs-hello.txt")
blob.upload_from_string(b"hello from gcs")
print(blob.download_as_bytes().decode())
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
  --certificate-identity "https://github.com/snithish/mockbucket/.github/workflows/release.yaml@refs/tags/v0.1.0" \
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
  httpx/                  -- shared middleware, error mapping, request context
  core/                   -- sentinel errors, domain models
```

Object bytes are streamed to the filesystem. Bucket/object metadata, listings,
multipart state, and session records live in SQLite. S3 object APIs are open;
STS is identity-aware; GCS requires authenticated subjects.

## Testing

### Unit Tests

```sh
make test
```

### Run a Single Package or Test

```sh
make test TEST_ARGS=./internal/iam
make test TEST_ARGS='./internal/iam -run TestSessionManagerAssumeRole'
make test TEST_ARGS='./internal/server -run TestS3FrontendContract/BucketLevelAPI'
make test TEST_ARGS='-v ./internal/server -run TestSTSAssumeRoleAndSessionCanHeadBucket'
```

### Compatibility Suite

The compatibility suite runs real AWS SDK and CLI tools against a live MockBucket instance. Requires Python with `uv`:

```sh
# Run all tests
uv run --project scripts/compat mockbucket-compat test

# AWS tests only
uv run --project scripts/compat mockbucket-compat test aws

# GCS tests only
uv run --project scripts/compat mockbucket-compat test gcs

# Start server for manual testing
uv run --project scripts/compat mockbucket-compat serve

# Verbose HTTP logging
uv run --project scripts/compat mockbucket-compat --debug test
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

Azure support is planned for a future release. If you want to work on it,
please open a proposal or PR. Until then, prefer the existing `s3` and `gcs`
frontends.

## License

[MIT](LICENSE)
