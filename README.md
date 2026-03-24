# MockBucket

[![ci](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml/badge.svg)](https://github.com/snithish/mockbucket/actions/workflows/ci.yaml)
[![release](https://img.shields.io/github/v/release/snithish/mockbucket)](https://github.com/snithish/mockbucket/releases)
[![docker](https://img.shields.io/badge/docker-ghcr.io%2Fsnithish%2Fmockbucket-blue)](https://github.com/snithish/mockbucket/pkgs/container/mockbucket)
[![license](https://img.shields.io/github/license/snithish/mockbucket)](LICENSE)
[![go version](https://img.shields.io/github/go-mod/go-version/snithish/mockbucket)](go.mod)

A standalone, local object-storage emulator for **S3**, **STS**, and **GCS** (planned). Run cloud-compatible workloads on your laptop or inside CI without touching a live AWS or GCP account.

MockBucket persists object bytes on the filesystem, stores metadata in SQLite, and evaluates IAM policies in-process so your tests are fast, deterministic, and free.

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
  - [Principals](#principals)
  - [Access Keys](#access-keys)
  - [Roles](#roles)
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

MockBucket starts on `http://localhost:9000` with a default `demo` bucket and `admin` / `admin-secret` credentials.

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
      - mockbucket-data:/var/data
      - ./mockbucket.yaml:/etc/mockbucket/config.yaml:ro
      - ./seed.yaml:/etc/mockbucket/seed.yaml:ro
    command: ["--config", "/etc/mockbucket/config.yaml"]
    healthcheck:
      test: ["CMD", "/mockbucketd", "--healthz"]
      interval: 10s
      timeout: 3s
      retries: 3

volumes:
  mockbucket-data:
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

```yaml
seed:
  path: ./seed.example.yaml   # path to seed data (buckets, principals, roles, objects)
```

### Frontends

Toggle each protocol frontend. S3 and GCS are mutually exclusive at runtime; STS requires S3.

```yaml
frontends:
  s3: true       # enable S3-compatible API
  sts: true      # enable STS (requires s3: true)
  gcs: false     # GCS frontend (future)
  azure: false   # Azure frontend (scaffold, disabled)
```

Supported profiles:

| S3 | STS | GCS | Use case |
|----|-----|-----|----------|
| true | true | false | AWS-compatible testing (default) |
| true | false | false | S3-only testing |
| false | false | true | GCS-only testing |

### Auth

```yaml
auth:
  session_duration: 1h   # STS session token lifetime
```

## Seeding State

The seed file defines the entire initial state of MockBucket. See `seed.example.yaml` for a complete reference.

### Buckets

Declare named buckets before any object creation:

```yaml
buckets:
  - my-bucket
  - logs-bucket
```

### Principals

Assign policies to IAM users:

```yaml
principals:
  - name: admin
    policies:
      - statements:
          - effect: Allow
            actions: ["*"]
            resources: ["*"]
  - name: reader
    policies:
      - statements:
          - effect: Allow
            actions: ["s3:GetObject", "s3:ListBucket"]
            resources: ["arn:mockbucket:s3:::my-bucket", "arn:mockbucket:s3:::my-bucket/*"]
```

### Access Keys

Define S3 access keys under `s3.access_keys`, referencing a principal:

```yaml
s3:
  access_keys:
    - id: admin
      secret: admin-secret
      principal: admin
    - id: reader-key
      secret: reader-secret
      principal: reader
```

### Roles

Define IAM roles with trust policies for `sts:AssumeRole`:

```yaml
roles:
  - name: data-reader
    trust:
      statements:
        - effect: Allow
          principals: ["admin"]
          actions: ["sts:AssumeRole"]
    policies:
      - statements:
          - effect: Allow
            actions: ["s3:GetObject", "s3:ListBucket"]
            resources: ["arn:mockbucket:s3:::my-bucket", "arn:mockbucket:s3:::my-bucket/*"]
```

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

Configure the AWS CLI to point at MockBucket:

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

```sh
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=admin-secret

# Assume a role
CREDENTIALS=$(aws --endpoint-url http://localhost:9000 sts assume-role \
    --role-arn arn:mockbucket:iam::role/data-reader \
    --role-session-name test-session)

# Use temporary credentials
export AWS_ACCESS_KEY_ID=$(echo $CREDENTIALS | jq -r '.Credentials.AccessKeyId')
export AWS_SECRET_ACCESS_KEY=$(echo $CREDENTIALS | jq -r '.Credentials.SecretAccessKey')
export AWS_SESSION_TOKEN=$(echo $CREDENTIALS | jq -r '.Credentials.SessionToken')

# Access with assumed role permissions
aws --endpoint-url http://localhost:9000 s3api get-object \
    --bucket demo --key bootstrap/hello.txt /tmp/assumed.txt
```

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
  auth/                   -- request authentication
    aws/                  -- SigV4 verification, bearer tokens
  iam/                    -- policy evaluation, trust model, sessions
  seed/                   -- seed parser, validator, bootstrapper
  frontends/              -- protocol adapters
    s3/                   -- S3 wire protocol (ListBuckets, PutObject, etc.)
    sts/                  -- STS wire protocol (AssumeRole)
    gcs/                  -- GCS scaffold (disabled)
    azure/                -- Azure scaffold (disabled)
  httpx/                  -- shared middleware, error mapping, request context
  core/                   -- sentinel errors, domain models
```

Object bytes are streamed to the filesystem. Bucket/object metadata, listings, multipart state, and session records live in SQLite. IAM policies are evaluated in-process with explicit-deny semantics.

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
go test ./internal/iam -run TestEvaluatorHonorsExplicitDeny
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
