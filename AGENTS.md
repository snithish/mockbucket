# AGENTS.md - Guide for AI Coding Agents

MockBucket is a standalone object-storage emulator (S3, STS, GCS) built in Go 1.26.1+.

## Build / Lint / Test Commands

```sh
make build          # build ./bin/mockbucketd with version metadata
make run            # build first, then run ./bin/mockbucketd --config mockbucket.yaml
make test           # run go test ./...
make fmt            # run gofmt over tracked Go files
make fmt-check      # list unformatted Go files and fail if any exist
make lint           # run go vet ./...
make tidy           # run go mod tidy
make compat         # build first, then run the compatibility suite
make docker         # build the Docker image
make clean          # remove ./bin
```

Prefer `make` targets over raw tool invocations when a target exists.

Run a single package: `make test TEST_ARGS=./internal/iam`
Run a single test: `make test TEST_ARGS='./internal/iam -run TestEvaluatorHonorsExplicitDeny'`
Run a subtest: `make test TEST_ARGS='./internal/server -run TestS3FrontendContract/BucketLevelAPI'`
Verbose: `make test TEST_ARGS='-v ./internal/server -run TestSTSAssumeRoleAndSessionCanHeadBucket'`
Compatibility tests with extra args: `make compat COMPAT_ARGS='--with-pyspark aws gcs'`

**Always run before finishing:** `make fmt && make test`

## Python

All Python work in this project uses `uv`. Never install packages globally or with `pip`/`pip3` directly.
Use `uv run` to execute scripts and `uv pip` to manage dependencies within the project's virtual environment.

```sh
uv run --project scripts/compat mockbucket-compat serve          # start server for manual testing
uv run --project scripts/compat mockbucket-compat test           # run all cloud tests (default)
uv run --project scripts/compat mockbucket-compat test aws       # run AWS S3/STS tests only
uv run --project scripts/compat mockbucket-compat test gcs       # run GCS tests only
uv run --project scripts/compat mockbucket-compat --debug test   # verbose HTTP logging
```

## Architecture

- Single daemon `mockbucketd`. Object bytes on filesystem, metadata in SQLite.
- `internal/storage` - persistence (SQLiteStore, FilesystemObjectStore).
- `internal/iam` - policy evaluation, trust, sessions.
- `internal/auth/<provider>` - request authentication, SigV4, bearer tokens.
- `internal/frontends/<provider>` - wire protocol translation (S3, STS, GCS).
- `internal/httpx` - shared middleware, error mapping, request context.
- `internal/core` - sentinel errors, domain models.
- `internal/config` - YAML config loading and validation.
- `internal/seed` - YAML seed data parsing and application.
- `internal/server` - HTTP server bootstrap, health endpoints.
- Seed YAML drives startup state. No runtime admin API in v1.
- S3 and GCS are mutually exclusive at runtime. STS requires S3.

## Code Style

### Imports
- Alias cross-provider imports: `authaws ".../auth/aws"`, `mbconfig ".../config"`.
- Group stdlib, external, then internal imports separated by blank lines.

### Naming
- Exported: PascalCase (`SQLiteStore`, `PolicyDocument`).
- Unexported: camelCase (`withTx`, `initSchema`, `escapeLike`).
- Acronyms stay uppercase: `SQL`, `XML`, `ETag`, `ARN`, `CRC32C`, `S3`, `GCS`, `STS`.

### Error Handling
- Sentinel errors in `internal/core/errors.go` using `errors.New()`.
- Wrap with `fmt.Errorf("context: %w", err)`.
- Map errors to HTTP via `httpx.StatusCode()`.
- Discard explicitly: `_ = runtime.Close()`, `_, _ = w.Write(...)`.

### HTTP Handlers
- Signature: `func handleXxx(w http.ResponseWriter, r *http.Request, deps common.Dependencies, ...)`.
- Early return on error: `writeError(w, err); return`.
- XML responses: `writeXML(w, status, payload)`. JSON: `writeJSON(w, status, payload)`.
- Auth check: `allow(r, deps, action, resource)`.

### Struct Tags
- Config: `yaml:"field_name"` (snake_case).
- Domain: `json:"field" yaml:"field"`.
- XML responses: `xml:"ElementName"`.
- Permissions: `0o755`, `0o644` (octal with `0o` prefix).

### Testing
- Stdlib `testing` only. No testify or gomega.
- Fatal format: `t.Fatalf("FunctionName() error = %v", err)` or `t.Fatalf("field = %v, want %v", got, want)`.
- Helpers use `t.Helper()`. Temp dirs via `t.TempDir()`. Cleanup via `t.Cleanup()`.
- Table-driven tests with anonymous struct slices.
- Subtests: `t.Run("name", func(t *testing.T) { ... })`.
- HTTP unit tests: `httptest.NewRequest` + `httptest.NewRecorder`.
- Integration tests: `httptest.NewServer` + real AWS/GCS SDK.
- Test seed data as `const` strings in test files.
- Protocol tests MUST use real AWS SDK, not custom signing helpers.

### Logging
- `log/slog` (stdlib structured logging): `slog.String("key", v)`, `slog.Any("error", err)`.

### General
- ASCII only unless file requires otherwise.
- Sparse comments - only where code is non-obvious.
- Streaming object I/O - never buffer full objects in memory.
- Keep `go.mod`/`go.sum` tidy via `go mod tidy`.

## Constraints
- Keep diffs minimal and atomic per phase.
- Every behavior change lands with focused tests.
- Do not expose new protocol endpoints before their enablement phases.
- Do not move provider wire logic into `storage` or `iam`.
- Explicit deny always wins in policy evaluation.
- Do not revert changes you did not make.
