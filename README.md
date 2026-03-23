# MockBucket

A Go-based local emulator for cloud object storage and IAM that lets you run S3/STS-compatible workloads on your laptop or inside CI without talking to AWS. MockBucket wires a lightweight daemon, durable filesystem+SQLite persistence, policy-driven identity, and future-ready adapter scaffolds so you can build and test compatibility before hitting a live cloud account.

## Table of Contents
- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Seeding State](#seeding-state)
- [Architecture & Packages](#architecture--packages)
- [Testing & Compatibility](#testing--compatibility)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Features
- **AWS S3 & STS surface** – exposes `ListBuckets`, `CreateBucket`, bucket metadata, object CRUD, and `AssumeRole` with SigV4 verification and temporary credentials.
- **Durable storage stack** – object data is streamed into a filesystem root while SQLite stores bucket/object metadata, listings, multipart uploads, and session records.
- **Seed-driven identity** – define buckets, principals, roles, trust/policy statements, access keys, and bootstrap objects through YAML so every run is deterministic.
- **Sensible observability** – health/readiness probes, structured logging, tracing hooks, request IDs, and policy-aware middleware sit in front of every frontend.
- **Provider-ready scaffolds** – pluggable frontends for GCS and Azure remain disabled until their enablement phases, making it easy to add new protocols without reshaping the core.

## Quick Start
1. **Install Go 1.26.1 or newer.**
2. **Build the daemon**
   ```sh
   go build -o mockbucketd ./cmd/mockbucketd
   ```
3. **Run with the default config**
   ```sh
   ./mockbucketd --config mockbucket.yaml
   ```
   This spins up `mockbucketd` on `127.0.0.1:9000`, persists objects under `./var/objects`, and loads identities from `seed.example.yaml`.
4. **Talk to it**
   Use any AWS SDK, the AWS CLI, or compatible tooling against `http://127.0.0.1:9000` with the `admin/admin-secret` keys from `seed.example.yaml`.

## Configuration
MockBucket reads the YAML file supplied via `--config`. The default `mockbucket.yaml` contains these sections:

- `server` – controls the listen address, readiness/health endpoints, request logs, and shutdown timeout.
- `storage` – points at the filesystem root for object blobs (`root_dir`) and the SQLite database path used for metadata.
- `seed` – a single `path` pointing to a YAML description of buckets, principals, roles, policies, and initial objects. See `seed.example.yaml` for the schema.
- `frontends` – toggle each protocol (`s3`, `sts`, `gcs`, `azure`). Only S3/STS are enabled today.
- `auth` – session duration, token lifetimes, and any other future auth knobs.

Copy `mockbucket.example.yaml` to `mockbucket.yaml` to customize ports, storage locations, frontends, or logging without editing your main config in place.

## Seeding State
The seed file defines everything you need to start with:

```yaml
buckets:
  - demo
principals:
  - name: admin
    policies: ...
roles:
  - name: data-reader
    trust: ...
objects:
  - bucket: demo
    key: bootstrap/hello.txt
    content: hello from mockbucket
```

- **Buckets** – declare named buckets before any object creation.
- **Principals** – assign allow/deny policies and access key/secret pairs.
- **Roles** – trust statements (e.g., `sts:AssumeRole`) plus role-specific policies.
- **Objects** – optional bootstrapping content stored inside a bucket.

Reloading the daemon picks up new seeds; invalid references fail fast so you can iterate safely.

## Architecture & Packages
- `cmd/mockbucketd` – entrypoint that parses flags, loads config, wires logging, and starts the runtime.
- `internal/server` – builds the HTTP server, middleware, router registration, and graceful shutdown.
- `internal/config` – schema validation and defaults for the config file.
- `internal/storage` – filesystem-backed object store and SQLite metadata plus multipart helpers.
- `internal/auth` & `internal/iam` – policy evaluator, trust/role model, session store, and SigV4 verifier.
- `internal/seed` – seed parser/validator and bootstrapper that wires identities to storage and auth.
- `internal/frontends` – adapters for S3, STS, GCS, and Azure; GCS/Azure are wired but disabled until their enablement phase.
- `internal/httpx` – shared middleware for request IDs, tracing, and error mapping.

## Testing & Compatibility
- Run Go unit tests: `go test ./...`
- Compatibility suites are under `scripts/compat`:
  - `awscli.sh`, `boto3.py`, `spark_s3a.sh`, `duckdb.sh`, and others exercise the S3 surface with real tooling.
  - `scripts/compat/run_all.sh` chains them together for a full compatibility gate;
  ensure you have AWS CLI, Python deps, DuckDB, and Spark installed if you run the suite locally.

## Roadmap
See `plan.md` for the full atomic phased implementation plan. Highlights:
1. Durable storage + authorization core (done).
2. STS + S3 compatibility gate (current focus).
3. GCS and Azure frontends (future phases).

## Contributing
1. Fork the repo and work on a phase branch prefixed with `codex/`.
2. Add tests for new behavior and confirm `go test ./...` passes.
3. Use the compatibility scripts when touching protocol behavior.
4. Open a PR against `main` once your phase meets the committed boundary in `plan.md`.

## License
TBD – add a `LICENSE` file when you decide on a formal open-source license.
