# Agent Constraints And Best Practices

## Purpose
- This repo builds a standalone object-storage emulator in phased, commit-safe increments.
- Agents must preserve the phased architecture in `plan.md` and avoid skipping ahead in ways that collapse boundaries between core, auth, and provider frontends.

## Delivery Rules
- Keep work atomic and logically committable.
- Only mix changes that belong to the same phase boundary.
- Breaking internal refactors are acceptable only when they serve the current phase directly.
- Do not expose fake live endpoints for GCS or Azure before their enablement phases.
- Prefer minimal diffs that preserve the intended layering.

## Architecture Constraints
- One daemon: `mockbucketd` is the only runtime process.
- Shared logic belongs in core packages, not provider frontends.
- Object bytes live on the filesystem.
- Metadata, principals, roles, sessions, and indexed listings live in SQLite.
- Seed data is YAML loaded at startup. No runtime admin/bootstrap API in v1.
- S3 and STS are the first live protocols. GCS and Azure remain disabled scaffolds until later phases.

## Package Boundaries
- `internal/storage`: persistence only.
- `internal/iam`: policy, trust, and session logic only.
- `internal/auth/<provider>`: request authentication and signature/token validation only.
- `internal/frontends/<provider>`: wire protocol translation only.
- `internal/httpx`: shared HTTP middleware, request context, and error mapping.
- Do not move provider-specific wire logic into `storage` or `iam`.

## Protocol Rules
- S3 and STS behavior should be AWS-compatible where implemented.
- Prefer one shared AWS root surface over incompatible parallel routes when STS and S3 need to coexist.
- SigV4 verification must use canonical request reconstruction, not ad hoc header checks.
- Temporary credentials must require the matching session token.
- Explicit deny always wins in policy evaluation.

## Performance And Correctness
- Keep object I/O streaming; do not buffer large payloads unnecessarily.
- Bucket/object listing must use indexed metadata queries, not filesystem walks.
- Persist multipart and session state durably before adding client-visible flows that depend on them.
- Add restart/recovery tests when introducing durable flows.

## Testing Rules
- Every behavior change must land with focused tests.
- Prefer black-box HTTP tests for protocol handlers and direct unit tests for storage/IAM/auth internals.
- Server-level protocol tests should use the AWS SDK (no custom signing helpers) to
  validate compatibility, even if they run slower.
- Run `gofmt -w $(rg --files -g '*.go')` and `go test ./...` before finishing.
- If dependency resolution is required, use `go mod tidy` and keep `go.mod`/`go.sum` tidy.

## Editing Practices
- Use ASCII unless the file already requires otherwise.
- Keep comments sparse and only where the code would otherwise be non-obvious.
- Do not add speculative abstractions for phases that are not being implemented.
- Do not revert user changes you did not make.

## Near-Term Implementation Order
- Finish STS `AssumeRole` on the existing IAM/session core.
- Finish S3 bucket-level API with SigV4 verification.
- Only after that move to object CRUD, listing, and multipart.
