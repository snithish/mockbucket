# Atomic Phased Implementation

## Summary
- Build the standalone emulator as a new Go repo with one daemon, `mockbucketd`.
- Keep each phase fully committable, logically isolated, and safe to land independently, even when it introduces breaking internal interfaces.
- Sequence work so every later phase builds on a runnable baseline, with correctness and compatibility proved before widening surface area.
- GCS is supported as a live frontend; Azure remains a disabled scaffold.
- S3 (+STS) and GCS are mutually exclusive in a single runtime configuration.

## Phase Plan

1. **Phase 1: Bootable Repo Foundation**
- Goal: create a runnable daemon with config loading, seed loading, health/readiness endpoints, logging/tracing hooks, and startup wiring.
- Changes: initialize Go module, add `cmd/mockbucketd`, define server config structs, add `serve --config mockbucket.yaml`, add health endpoints, add startup lifecycle and graceful shutdown.
- Commit boundary: repo builds, daemon starts, reads config/seed paths, exposes health/readiness, but does not yet serve cloud APIs.
- Validation: compile, startup/shutdown tests, config parse tests, health endpoint tests.

2. **Phase 2: Durable Storage Core**
- Goal: land the real persistence model before any protocol behavior.
- Changes: define `ObjectStore` and `MetadataStore`; implement filesystem-backed object storage and SQLite-backed metadata schema; add bucket/object metadata tables and indexed listing model.
- Commit boundary: internal APIs support streaming object write/read/delete and metadata queries, but no auth or HTTP mapping yet.
- Validation: unit tests for streaming I/O, metadata CRUD, prefix listing queries, schema init/migration, restart persistence.

3. **Phase 3: Seed Model and Identity State**
- Goal: make startup state deterministic and file-driven.
- Changes: define `seed.yaml` schema for buckets, principals, access keys, roles, identity policies, trust policies, and optional initial objects; implement validation and startup seeding.
- Commit boundary: daemon can boot into a fully defined local state from YAML; invalid seed references fail fast at startup.
- Validation: schema validation tests, bad-reference rejection, idempotent startup seeding, initial object load tests.

4. **Phase 4: IAM, Trust, and Session Core**
- Goal: complete the shared authorization engine before exposing frontends.
- Changes: implement `PolicyEvaluator`, `TrustEvaluator`, and `SessionStore`; support `Allow`/`Deny`, `Action`, `Resource`, wildcard matching, explicit deny precedence, and expiring session records.
- Commit boundary: core auth decisions are executable in-process, but not yet exposed through STS or S3.
- Validation: policy matrix tests, wildcard tests, explicit deny precedence tests, trust-policy tests, session expiry tests.

5. **Phase 5: Shared HTTP Middleware and Error Model**
- Goal: lock the request pipeline once, then reuse it across every frontend.
- Changes: add request ID/tracing middleware, auth-context injection, policy-check middleware, and provider-neutral internal error taxonomy with adapter mapping hooks.
- Commit boundary: daemon has a stable HTTP execution model and disabled router registration points for S3, GCS, Azure, and STS.
- Validation: middleware tests for malformed auth, unauthenticated access, denied requests, trace/request ID propagation, error mapping tests.

6. **Phase 6: Disabled Provider Scaffolds**
- Goal: prove the repo boundaries for future providers without exposing fake APIs.
- Changes: add `frontend/gcs`, `frontend/azure`, `auth/gcp`, and `auth/azure` package scaffolds, config sections, and router registration hooks; keep them disabled by default and not bound to listeners.
- Commit boundary: extension points are real and compile, but only S3/STS are intended to go live later.
- Validation: build tests, disabled-config tests, config validation for enabled/disabled frontend selection.

7. **Phase 7: STS `AssumeRole`**
- Goal: expose the first live auth API on top of the core trust/session engine.
- Changes: implement AWS-style `AssumeRole`, credential issuance, expiration handling, and wire response rendering; connect issued credentials back into auth middleware.
- Commit boundary: clients can obtain temporary credentials, and those credentials authorize later requests through the shared auth path.
- Validation: integration tests for allowed assume-role, denied principal, expired session, malformed request, restart persistence of issued sessions.

8. **Phase 8: S3 Bucket-Level API**
- Goal: expose the smallest S3 discovery and bucket-management surface needed by real clients.
- Changes: implement `ListBuckets`, `CreateBucket`, `HeadBucket`, and `GetBucketLocation`; wire SigV4 request verification for these routes.
- Commit boundary: real AWS-compatible clients can discover and create buckets against the emulator.
- Validation: route tests, SigV4 verification tests, authz tests, `awscli` and `boto3` bucket-operation compatibility tests.

9. **Phase 9: S3 Object CRUD**
- Goal: make single-object reads and writes fully usable before adding list and multipart complexity.
- Changes: implement `PutObject`, `GetObject`, `HeadObject`, and `DeleteObject`; enforce streaming uploads/downloads with bounded memory and metadata updates.
- Commit boundary: basic object lifecycle works end to end with real clients and persisted state.
- Validation: large-object streaming tests, overwrite/delete tests, restart persistence tests, `awscli`/`boto3` CRUD compatibility tests, Spark S3A and DuckDB single-object read tests.

10. **Phase 10: S3 Listing and Query Semantics**
- Goal: complete the common table-engine and SDK read path.
- Changes: implement `ListObjectsV2` using indexed SQLite metadata, pagination markers/tokens, and prefix filtering.
- Commit boundary: engines and SDKs can enumerate realistic buckets without filesystem scans.
- Validation: prefix/pagination tests, large-bucket list tests, authz tests on listing, Spark S3A and DuckDB listing/read compatibility tests.

11. **Phase 11: Multipart Upload**
- Goal: add the last required S3 compatibility block for larger and SDK-managed uploads.
- Changes: implement `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`; persist multipart state in SQLite and part data on disk.
- Commit boundary: multipart uploads survive restarts and complete into final objects through the same metadata model.
- Validation: multipart success/abort tests, concurrent part upload tests, restart-during-upload recovery tests, `awscli`/`boto3` multipart compatibility tests.

12. **Phase 12: Compatibility Gate and CI Hardening**
- Goal: turn the implemented S3/STS surface into a reliable merge gate.
- Changes: add required unit/integration suites, compatibility runners for `awscli`, `boto3`, Spark S3A, and DuckDB S3 reads, and CI workflows; make GCS/Azure suites placeholders only.
- Commit boundary: merges are blocked on the real S3/STS contract, not just unit coverage.
- Validation: CI passes on the full required S3 gate; failure output is actionable and deterministic.

13. **Phase 13: GCS Enablement**
- Goal: turn the existing disabled GCS scaffold into a real frontend/auth implementation without reshaping the core.
- Changes: implement GCS protocol subset, auth mapping into the shared policy/session model, and compatibility coverage.
- Commit boundary: GCS becomes a supported live frontend; no core redesign should be required.
- Validation: GCS unit/integration tests and `gsutil`/`gcloud` compatibility suites.

14. **Phase 14: Azure Blob Enablement**
- Goal: turn the existing disabled Azure scaffold into a real frontend/auth implementation.
- Changes: implement Azure Blob subset, shared key/SAS/AAD-like token mapping into the core permissions model, and compatibility coverage.
- Commit boundary: Azure becomes a supported live frontend; core contracts remain stable.
- Validation: Azure unit/integration tests and `azcopy`/Azure SDK compatibility suites.

## Delivery Rules
- Each phase lands as one mergeable commit or a small linear commit set that is squashed into one logical change.
- No phase should start before the previous phase is runnable or testable at its own boundary.
- Breaking internal refactors are allowed only at phase boundaries, never mixed into unrelated behavior changes.
- GCS and Azure stay disabled until their enablement phases; no `501` live endpoints before then.
- S3/STS compatibility is the first required external contract and must be complete before widening provider scope.

## Test Strategy
- Every phase adds the tests for the behavior it introduces; no deferred test-debt phases.
- Restart/recovery coverage is mandatory beginning with durable state, STS sessions, and multipart flows.
- Performance-sensitive paths must be tested where they are introduced:
  - object I/O must remain streaming
  - listings must use indexed metadata queries
  - multipart state must remain durable and restart-safe

## Assumptions
- Phase 1 ends with a bootable daemon, not scaffolding only.
- Runtime is Go, topology is one daemon, persistence is filesystem plus SQLite.
- Seed state is YAML loaded at startup; there is no runtime bootstrap/admin API in v1.
- IAM v1 is identity policies plus trust policies for `AssumeRole`; bucket policies remain out of scope.
- Breaking changes are acceptable between phases while the repo is still converging on its stable v1 contracts.
- The intended threat model is local dev and ephemeral CI.
- The consistency contract is read-after-write.
- Backward-compatibility requirements begin at v1.0.
